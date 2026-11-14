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


def test_edit_subscription_updates_only_after_confirmation(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_flash', lambda *args: None)
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(asaas_api_base_url='https://api-sandbox.asaas.com', asaas_api_key='key'))
    calls = []
    def update(**kwargs):
        calls.append(kwargs)
        return {'id': 'sub_test', 'customer': 'cus_test', 'status': 'ACTIVE', **kwargs['payload']}, None
    monkeypatch.setattr(routes, 'update_sandbox_subscription', update)
    with Session(engine) as db:
        from datetime import date
        from decimal import Decimal
        org = Organization(name='Teste', slug='edicao', is_active=True)
        db.add(org); db.flush()
        db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_test', asaas_environment='sandbox'))
        db.add(OrganizationSubscription(organization_id=org.id, plan_name='Antigo', amount=Decimal('99.90'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), status='criada', environment='sandbox', asaas_subscription_id='sub_test', asaas_status='ACTIVE'))
        db.commit()
        args = dict(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_name='Novo', amount='120,50', cycle='YEARLY', billing_type='BOLETO', next_due_date='2099-01-01', db=db, user=SimpleNamespace())
        routes.edit_organization_sandbox_subscription_action(**args)
        row = db.query(OrganizationSubscription).one()
        assert row.plan_name == 'Novo' and row.amount == Decimal('120.50')
        assert row.next_due_date == date(2099, 1, 1) and row.status == 'criada'
        assert calls[0]['payload']['billingType'] == 'BOLETO'
        monkeypatch.setattr(routes, 'update_sandbox_subscription', lambda **kwargs: (None, 'timeout'))
        routes.edit_organization_sandbox_subscription_action(**args)
        assert row.status == 'revisao' and row.plan_name == 'Novo'


def test_edit_sends_no_pending_payment_update(monkeypatch):
    from app.services import asaas_subscriptions as service
    monkeypatch.setattr(service, 'get_sandbox_subscription', lambda **kwargs: ({'id': 'sub_test', 'customer': 'cus_test', 'status': 'ACTIVE', 'billingType': 'PIX'}, None))
    calls = []
    class Response:
        status_code = 200
        def json(self): return {'id': 'sub_test', 'customer': 'cus_test'}
    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def put(self, url, **kwargs):
            calls.append(kwargs['json'])
            return Response()
    monkeypatch.setattr(service.httpx, 'Client', Client)
    data, error = service.update_sandbox_subscription(api_key='key', subscription_id='sub_test', customer_id='cus_test', payload={'description': 'Plano', 'updatePendingPayments': True})
    assert data and not error
    assert calls[0]['updatePendingPayments'] is False


def test_card_subscription_edit_preserves_payment_method(monkeypatch):
    from datetime import date
    from decimal import Decimal
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_flash', lambda *args: None)
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(asaas_api_base_url='https://api-sandbox.asaas.com', asaas_api_key='key'))
    calls = []
    def update(**kwargs):
        calls.append(kwargs['payload'])
        return {'id': 'sub_card', 'customer': 'cus_test', 'status': 'ACTIVE', 'billingType': 'CREDIT_CARD', **kwargs['payload']}, None
    monkeypatch.setattr(routes, 'update_sandbox_subscription', update)
    with Session(engine) as db:
        org = Organization(name='Teste', slug='card-edit', is_active=True)
        db.add(org); db.flush()
        db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_test', asaas_environment='sandbox'))
        db.add(OrganizationSubscription(organization_id=org.id, plan_name='Antigo', amount=Decimal('99.90'), cycle='MONTHLY', billing_type='CREDIT_CARD', first_due_date=date(2026, 1, 1), status='criada', environment='sandbox', asaas_subscription_id='sub_card', asaas_status='ACTIVE'))
        db.commit()
        args = dict(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_name='Novo', amount='120,50', cycle='MONTHLY', billing_type='CREDIT_CARD', next_due_date='', db=db, user=SimpleNamespace())
        routes.edit_organization_sandbox_subscription_action(**args)
        row = db.query(OrganizationSubscription).one()
        assert row.plan_name == 'Novo' and row.billing_type == 'CREDIT_CARD'
        assert 'billingType' not in calls[0] and 'creditCard' not in calls[0]
        args['billing_type'] = 'PIX'
        routes.edit_organization_sandbox_subscription_action(**args)
        assert len(calls) == 2 and row.billing_type == 'PIX'
        assert calls[1]['billingType'] == 'PIX'
        args['billing_type'] = 'CREDIT_CARD'
        routes.edit_organization_sandbox_subscription_action(**args)
        assert len(calls) == 2 and row.billing_type == 'PIX'


def test_subscription_service_refuses_implicit_card_conversion(monkeypatch):
    from app.services import asaas_subscriptions as service
    monkeypatch.setattr(service, 'get_sandbox_subscription', lambda **kwargs: ({'id': 'sub_test', 'customer': 'cus_test', 'status': 'ACTIVE', 'billingType': 'PIX'}, None))
    result, error = service.update_sandbox_subscription(api_key='key', subscription_id='sub_test', customer_id='cus_test', payload={'billingType': 'CREDIT_CARD'})
    assert result is None and error


def test_card_to_boleto_does_not_update_pending_payments(monkeypatch):
    from app.services import asaas_subscriptions as service
    monkeypatch.setattr(service, 'get_sandbox_subscription', lambda **kwargs: ({'id': 'sub_test', 'customer': 'cus_test', 'status': 'ACTIVE', 'billingType': 'CREDIT_CARD'}, None))
    calls = []
    class Response:
        status_code = 200
        def json(self): return {'id': 'sub_test', 'customer': 'cus_test', 'billingType': 'BOLETO'}
    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def put(self, url, **kwargs):
            calls.append(kwargs['json'])
            return Response()
    monkeypatch.setattr(service.httpx, 'Client', Client)
    result, error = service.update_sandbox_subscription(api_key='key', subscription_id='sub_test', customer_id='cus_test', payload={'billingType': 'BOLETO'})
    assert error is None and result['billingType'] == 'BOLETO'
    assert calls[0]['updatePendingPayments'] is False


def test_history_preserves_payments_and_allows_only_one_current_subscription():
    from datetime import date
    from decimal import Decimal
    from sqlalchemy.exc import IntegrityError
    from app.models import OrganizationSubscriptionPayment
    import pytest
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        org = Organization(name='Teste', slug='historico', is_active=True)
        db.add(org); db.flush()
        def subscription(remote_id, current):
            return OrganizationSubscription(organization_id=org.id, plan_name='Plano', amount=Decimal('99.90'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', asaas_subscription_id=remote_id, is_current=current)
        old = subscription('sub_old', True)
        db.add(old); db.flush()
        db.add(OrganizationSubscriptionPayment(subscription_id=old.id, asaas_payment_id='pay_old', status='RECEIVED'))
        old.is_current = False
        db.flush()
        current = subscription('sub_new', True)
        db.add(current); db.commit()
        assert db.query(OrganizationSubscriptionPayment).one().subscription_id == old.id
        db.add(subscription('sub_duplicate', True))
        with pytest.raises(IntegrityError):
            db.commit()
