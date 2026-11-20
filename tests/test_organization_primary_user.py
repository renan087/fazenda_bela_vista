import json
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from jinja2 import ChoiceLoader, DictLoader

from app.db.base import Base
from app.models import AuditLog, Organization, User
from app.web import routes


def test_primary_user_assignment_audit_transfer_and_deletion(monkeypatch):
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    db = Session(engine)
    org_a = Organization(name="Fazenda Bela Vista", slug="bela-vista")
    org_b = Organization(name="Fazenda Bela Vista", slug="bela-vista-2")
    db.add_all([org_a, org_b])
    db.flush()
    admin = User(name="Admin", email="admin@example.com", hashed_password="x", organization_id=org_a.id, is_active=True)
    first = User(name="Ana", email="ana@example.com", hashed_password="x", organization_id=org_a.id, is_active=True)
    second = User(name="Beto", email="beto@example.com", hashed_password="x", organization_id=org_a.id, is_active=True)
    outsider = User(name="Clara", email="clara@example.com", hashed_password="x", organization_id=org_b.id, is_active=True)
    inactive = User(name="Dora", email="dora@example.com", hashed_password="x", organization_id=org_a.id, is_active=False)
    db.add_all([admin, first, second, outsider, inactive])
    db.commit()
    request = SimpleNamespace(session={})
    monkeypatch.setattr(routes, "validate_csrf", lambda *args: None)
    monkeypatch.setattr(routes, "_require_super_admin", lambda *args: None)
    monkeypatch.setattr(routes, "_require_admin", lambda *args: None)
    monkeypatch.setattr(routes, "quota_write_entitlements", lambda *args: object())
    monkeypatch.setattr(routes, "user_limit_error", lambda *args: None)
    monkeypatch.setattr(routes, "sync_roles_from_admin_flag", lambda *args: None)

    org_b.name = "Fazenda Bela Vista 2"
    db.commit()
    routes.rename_organization_action(org_b.id, request, "x", "Fazenda Bela Vista", db, admin)
    assert org_a.name == org_b.name and org_b.slug == "bela-vista-2"
    rename_event = db.query(AuditLog).filter_by(event_type="comercial.organizacao.renomear").one()
    assert json.loads(rename_event.metadata_json)["nome_anterior"] == "Fazenda Bela Vista 2"
    routes.update_organization_primary_user_action(org_a.id, request, "x", str(first.id), db, admin)
    assert org_a.primary_user_id == first.id
    routes.update_organization_primary_user_action(org_a.id, request, "x", str(outsider.id), db, admin)
    assert org_a.primary_user_id == first.id and request.session["flash"]["kind"] == "error"
    routes.update_organization_primary_user_action(org_a.id, request, "x", str(inactive.id), db, admin)
    assert org_a.primary_user_id == first.id and request.session["flash"]["kind"] == "error"
    routes.update_organization_primary_user_action(org_a.id, request, "x", str(second.id), db, admin)
    events = db.query(AuditLog).filter_by(event_type="comercial.organizacao.definir_responsavel").order_by(AuditLog.id).all()
    assert len(events) == 2
    details = json.loads(events[-1].metadata_json)
    assert details["anterior"]["email"] == first.email and details["novo"]["email"] == second.email
    assert events[-1].actor_email == admin.email
    env = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({"base.html": "{% block content %}{% endblock %}"}), routes.templates.env.loader,
    ]))
    page = env.get_template("organizations.html").render(
        organizations=[org_a, org_b], users=[admin, first, second, outsider, inactive], farms=[],
        organization_stats={}, primary_users={org_a.id: second}, responsible_history={
            org_a.id: [{"date": events[-1].created_at, "actor": admin.email,
                        "old": details["anterior"], "new": details["novo"]}],
        }, billing_by_organization={}, signup_suggestions={}, csrf_token="x",
    )
    assert f"#{org_a.id}" in page and f"#{org_b.id}" in page
    assert "beto@example.com" in page and "ana@example.com" in page
    assert "Responsável principal não definido" in page and "Últimas alterações" in page

    routes.link_user_organization_action(request, "x", second.id, org_b.id, db, admin)
    assert second.organization_id == org_b.id and org_a.primary_user_id is None
    transfer = json.loads(db.query(AuditLog).order_by(AuditLog.id.desc()).first().metadata_json)
    assert transfer["motivo"] == "usuario_transferido" and transfer["novo"] is None

    routes.update_organization_primary_user_action(org_a.id, request, "x", str(first.id), db, admin)
    routes.delete_user_action(first.id, request, "x", db, admin)
    assert org_a.primary_user_id is None and db.get(User, first.id) is None
    deleted = json.loads(db.query(AuditLog).order_by(AuditLog.id.desc()).first().metadata_json)
    assert deleted["motivo"] == "usuario_excluido" and deleted["anterior"]["email"] == "ana@example.com"
    db.close()
