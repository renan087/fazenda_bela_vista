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
