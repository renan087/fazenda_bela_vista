from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import Farm, Organization, Plot, User
from app.services.organizations_overview import build_organizations_overview
from app.web import routes


def test_build_organizations_overview_detects_duplicates_and_hides_secrets():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        org_a = Organization(name="Grupo Serra Negra", slug="serra-negra", is_active=True)
        org_b = Organization(name="Café Alto da Mata", slug="alto-da-mata", is_active=True)
        db.add_all([org_a, org_b])
        db.flush()

        user_a = User(name="Carlos", email="carlos@serra.test", hashed_password="secret-hash", organization_id=org_a.id, is_active=True)
        user_orphan = User(name="Lucas", email="lucas@agro.test", hashed_password="secret-hash", organization_id=None, is_active=True)
        db.add_all([user_a, user_orphan])
        db.flush()
        org_a.primary_user_id = user_a.id

        farm_a = Farm(name="Fazenda Bela Vista", location="Patrocínio / MG", total_area=250, organization_id=org_a.id)
        farm_b = Farm(name="Fazenda Bela Vista", location="Carmo / MG", total_area=110, organization_id=org_b.id)
        db.add_all([farm_a, farm_b])
        db.flush()
        db.add(Plot(farm_id=farm_a.id, name="Talhão 1", area_hectares=50, plant_count=10000, spacing_row_meters=3.5, spacing_plant_meters=0.7))
        db.commit()

        overview = build_organizations_overview(
            db,
            organizations=[org_a, org_b],
            users=[user_a, user_orphan],
            farms=[farm_a, farm_b],
            primary_users={org_a.id: user_a, org_b.id: None},
            billing_by_organization={},
            signup_suggestions={},
            responsible_history={},
            blocked_user_ids={user_orphan.id},
            current_user_id=user_a.id,
        )

        assert overview["duplicate_farm_names"]["fazenda bela vista"] == 2
        assert overview["plot_counts_by_farm"][farm_a.id] == 1
        farms_payload = {item["id"]: item for item in overview["payload"]["farms"]}
        assert farms_payload[farm_a.id]["duplicateCount"] == 2
        assert farms_payload[farm_a.id]["plots"] == 1
        users_payload = {item["id"]: item for item in overview["payload"]["users"]}
        assert users_payload[user_orphan.id]["isBlocked"] is True
        assert "hashed_password" not in users_payload[user_a.id]


def test_link_farm_blocks_duplicate_name_in_target_organization(monkeypatch):
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        org_a = Organization(name="Org A", slug="org-a", is_active=True)
        org_b = Organization(name="Org B", slug="org-b", is_active=True)
        db.add_all([org_a, org_b])
        db.flush()

        admin = User(name="Admin", email="admin@test.com", hashed_password="x", organization_id=org_a.id, is_active=True)
        farm_a = Farm(name="Fazenda Bela Vista", location="Patrocínio / MG", total_area=200, organization_id=org_a.id)
        farm_b = Farm(name="Fazenda Bela Vista", location="Carmo / MG", total_area=100, organization_id=org_b.id)
        farm_c = Farm(name="Fazenda Única", location="Guaxupé / MG", total_area=80, organization_id=org_a.id)
        db.add_all([admin, farm_a, farm_b, farm_c])
        db.commit()

        monkeypatch.setattr(routes, "validate_csrf", lambda *args: None)
        monkeypatch.setattr(routes, "_require_super_admin", lambda *args: None)
        monkeypatch.setattr(routes, "quota_write_entitlements", lambda *args: object())
        monkeypatch.setattr(routes, "farm_limit_error", lambda *args, **kwargs: None)

        request = SimpleNamespace(session={})
        resp = routes.link_farm_organization_action(
            request=request, csrf_token="x", farm_id=farm_a.id, organization_id=org_b.id,
            return_tab="farms", return_org=None, db=db, user=admin,
        )
        db.refresh(farm_a)
        assert farm_a.organization_id == org_a.id
        assert request.session["flash"]["kind"] == "error"
        assert "já possui uma fazenda" in request.session["flash"]["message"]
        assert resp.headers["location"] == "/organizacoes?aba=farms"

        # Fazenda com nome único transfere normalmente e pausa/retoma via superadmin
        resp_ok = routes.link_farm_organization_action(
            request=request, csrf_token="x", farm_id=farm_c.id, organization_id=org_b.id,
            return_tab="farms", return_org=str(org_b.id), db=db, user=admin,
        )
        db.refresh(farm_c)
        assert farm_c.organization_id == org_b.id
        assert resp_ok.headers["location"] == f"/organizacoes?aba=farms&organizacao={org_b.id}"

        routes.toggle_organization_farm_operations_action(
            farm_id=farm_c.id, request=request, csrf_token="x", action="pause",
            return_tab="farms", return_org=None, db=db, user=admin,
        )
        db.refresh(farm_c)
        assert farm_c.operations_paused is True

        routes.toggle_organization_farm_operations_action(
            farm_id=farm_c.id, request=request, csrf_token="x", action="resume",
            return_tab="farms", return_org=None, db=db, user=admin,
        )
        db.refresh(farm_c)
        assert farm_c.operations_paused is False
