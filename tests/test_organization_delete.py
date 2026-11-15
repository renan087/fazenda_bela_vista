from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import AuditLog, CoffeeVariety, Organization, OrganizationBilling
from app.models.rbac import Role
from app.repositories.farm import FarmRepository as Repository


def _db():
    engine = create_engine('sqlite:///:memory:')
    event.listen(engine, 'connect', lambda conn, _: conn.execute('PRAGMA foreign_keys=ON'))
    Base.metadata.create_all(engine)
    return Session(engine)


def test_delete_organization_removes_dependents():
    db = _db()
    org = Organization(name='Teste', slug='teste', is_active=True)
    db.add(org)
    db.flush()
    db.add_all([
        Role(organization_id=org.id, name='Administrador', slug='admin'),
        OrganizationBilling(organization_id=org.id, contact_name='P', contact_email='p@example.com'),
        AuditLog(organization_id=org.id, event_type='x', outcome='success'),
    ])
    db.commit()
    Repository(db).delete(org)
    assert db.query(Organization).count() == 0
    assert db.query(Role).count() == 0 and db.query(OrganizationBilling).count() == 0
