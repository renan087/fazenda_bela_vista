from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import Organization, OrganizationBilling, OrganizationSubscription
from app.web import routes


def test_sandbox_creation_records_attempt_and_prevents_duplicate(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_flash', lambda *args: None)
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(asaas_api_base_url='https://api-sandbox.asaas.com', asaas_api_key='sandbox-key'))
    calls = []
    def create(**kwargs):
        calls.append(kwargs['payload'])
        return {'id': 'sub_test', 'status': 'ACTIVE'}, None
    monkeypatch.setattr(routes, 'create_sandbox_subscription', create)
    with Session(engine) as db:
        org = Organization(name='Cliente Teste', slug='cliente-teste', is_active=True)
        db.add(org)
        db.flush()
        db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_test', asaas_environment='sandbox'))
        db.commit()
        args = dict(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_name='Teste', amount='99,90', cycle='MONTHLY', billing_type='PIX', first_due_date='2099-01-01', db=db, user=SimpleNamespace())
        routes.create_organization_sandbox_subscription_action(**args)
        routes.create_organization_sandbox_subscription_action(**args)
        row = db.query(OrganizationSubscription).one()
        assert row.status == 'criada' and row.asaas_subscription_id == 'sub_test'
        assert len(calls) == 1 and calls[0]['customer'] == 'cus_test'


def test_uncertain_creation_is_not_retried(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_flash', lambda *args: None)
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(asaas_api_base_url='https://api-sandbox.asaas.com', asaas_api_key='key'))
    calls = []
    monkeypatch.setattr(routes, 'create_sandbox_subscription', lambda **kwargs: (calls.append(1) or None, 'timeout'))
    with Session(engine) as db:
        org = Organization(name='Outro', slug='outro', is_active=True)
        db.add(org); db.flush()
        db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_other', asaas_environment='sandbox'))
        db.commit()
        args = dict(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_name='Teste', amount='99,90', cycle='MONTHLY', billing_type='PIX', first_due_date='2099-01-01', db=db, user=SimpleNamespace())
        routes.create_organization_sandbox_subscription_action(**args)
        routes.create_organization_sandbox_subscription_action(**args)
        assert db.query(OrganizationSubscription).one().status == 'revisao'
        assert len(calls) == 1
