import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import Organization, OrganizationBilling


def test_each_organization_has_independent_billing_contact():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        first = Organization(name="Fazenda A", slug="fazenda-a", is_active=True)
        second = Organization(name="Fazenda B", slug="fazenda-b", is_active=True)
        db.add_all([first, second])
        db.flush()
        db.add_all([
            OrganizationBilling(organization_id=first.id, contact_name="Ana", contact_email="ana@example.com"),
            OrganizationBilling(organization_id=second.id, contact_name="Bruno", contact_email="bruno@example.com"),
        ])
        db.commit()
        assert db.query(OrganizationBilling).filter_by(organization_id=first.id).one().contact_name == "Ana"
        assert db.query(OrganizationBilling).filter_by(organization_id=second.id).one().contact_name == "Bruno"
        db.add(OrganizationBilling(organization_id=first.id, contact_name="Outra", contact_email="outra@example.com"))
        with pytest.raises(IntegrityError):
            db.commit()


def test_save_and_consult_uses_newly_entered_address(monkeypatch):
    from types import SimpleNamespace
    from app.web import routes

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    monkeypatch.setattr(routes, "_require_super_admin", lambda *args: None)
    monkeypatch.setattr(routes, "validate_csrf", lambda *args: None)
    observed = {}

    def consult(**kwargs):
        row = kwargs["db"].query(OrganizationBilling).filter_by(organization_id=kwargs["organization_id"]).one()
        observed.update(cpf=row.cpf_cnpj, cep=row.postal_code, number=row.address_number, city=row.city, state=row.state)
        return "consulted"

    monkeypatch.setattr(routes, "organization_asaas_sandbox_action", consult)
    with Session(engine) as db:
        org = Organization(name="Fazenda C", slug="fazenda-c", is_active=True)
        db.add(org)
        db.commit()
        result = routes.save_organization_billing_action(
            organization_id=org.id, request=SimpleNamespace(), csrf_token="token",
            contact_name="Pessoa Teste", contact_email="teste@example.com", contact_phone="11999999999",
            cpf_cnpj="123.456.789-01", postal_code="46.430-000", address_number="50", address="Rua A",
            address_complement=None, province="Centro", city="Guanambi", state="ba", intent="consultar",
            db=db, user=SimpleNamespace(),
        )
        assert result == "consulted"
        assert observed == {"cpf": "12345678901", "cep": "46430000", "number": "50", "city": "Guanambi", "state": "BA"}
