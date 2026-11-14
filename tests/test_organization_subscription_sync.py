from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import AsaasWebhookEvent, Organization, OrganizationBilling, OrganizationSubscription, OrganizationSubscriptionPayment
from app.services.organization_subscription_sync import persist_organization_webhook
from app.web import routes


def test_payment_events_are_idempotent_and_cannot_overwrite_newer_status():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        org = Organization(name='Teste', slug='teste', is_active=True)
        db.add(org)
        db.flush()
        db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_test', asaas_environment='sandbox'))
        db.add(OrganizationSubscription(organization_id=org.id, plan_name='Teste', amount=Decimal('99.90'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', asaas_subscription_id='sub_test'))
        db.commit()
        def event(identifier, timestamp, status, customer='cus_test'):
            return {'id': identifier, 'event': 'PAYMENT_UPDATED', 'dateCreated': timestamp, 'payment': {'id': 'pay_test', 'subscription': 'sub_test', 'customer': customer, 'status': status, 'value': 99.90, 'dueDate': '2026-01-01', 'paymentDate': '2026-01-02'}}
        paid = event('evt_paid', '2026-01-02T10:00:00', 'RECEIVED')
        assert persist_organization_webhook(db, paid)
        db.commit()
        assert persist_organization_webhook(db, paid)
        assert persist_organization_webhook(db, event('evt_old', '2026-01-01T10:00:00', 'PENDING'))
        db.commit()
        payment = db.query(OrganizationSubscriptionPayment).one()
        assert payment.status == 'RECEIVED'
        assert payment.paid_date == date(2026, 1, 2)
        assert db.query(AsaasWebhookEvent).count() == 2
        with pytest.raises(ValueError):
            persist_organization_webhook(db, event('evt_wrong', '2026-01-03T10:00:00', 'PENDING', 'cus_wrong'))
        db.rollback()
        persist_organization_webhook(db, event('evt_same', '2026-01-02T10:00:00', 'REFUNDED'))
        db.commit()
        assert payment.status == 'VERIFICAR'


def test_manual_reconciliation_updates_subscription_and_charges_atomically(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    messages = []
    monkeypatch.setattr(routes, '_flash', lambda request, category, message: messages.append((category, message)))
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(asaas_api_base_url='https://api-sandbox.asaas.com', asaas_api_key='key'))
    remote = {'id': 'sub_test', 'customer': 'cus_test', 'status': 'INACTIVE'}
    monkeypatch.setattr(routes, 'get_sandbox_subscription', lambda **kwargs: (remote, None))
    monkeypatch.setattr(routes, 'list_sandbox_subscription_payments', lambda **kwargs: ([{'id': 'pay_test', 'subscription': 'sub_test', 'customer': 'cus_test', 'status': 'RECEIVED', 'value': 99.90}], None))
    with Session(engine) as db:
        org = Organization(name='Teste', slug='reconcile', is_active=True)
        db.add(org)
        db.flush()
        db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_test', asaas_environment='sandbox'))
        db.add(OrganizationSubscription(organization_id=org.id, plan_name='Teste', amount=Decimal('99.90'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', asaas_subscription_id='sub_test', asaas_status='ACTIVE'))
        db.commit()
        args = dict(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', db=db, user=SimpleNamespace())
        routes.sync_organization_subscription_payments_action(**args)
        assert db.query(OrganizationSubscription).one().asaas_status == 'INACTIVE'
        assert db.query(OrganizationSubscriptionPayment).one().status == 'RECEIVED'
        assert messages[-1][0] == 'success'
        remote['customer'] = 'cus_wrong'
        remote['status'] = 'ACTIVE'
        routes.sync_organization_subscription_payments_action(**args)
        assert db.query(OrganizationSubscription).one().asaas_status == 'INACTIVE'
        assert messages[-1][0] == 'error'


def test_deleted_payment_does_not_appear_as_paid():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        org = Organization(name='Teste', slug='deleted', is_active=True)
        db.add(org)
        db.flush()
        db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_test', asaas_environment='sandbox'))
        db.add(OrganizationSubscription(organization_id=org.id, plan_name='Teste', amount=Decimal('99.90'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', asaas_subscription_id='sub_test'))
        db.commit()
        event = {'id': 'evt_deleted', 'event': 'PAYMENT_DELETED', 'dateCreated': '2026-01-02T10:00:00', 'payment': {'id': 'pay_test', 'subscription': 'sub_test', 'customer': 'cus_test', 'status': 'RECEIVED'}}
        persist_organization_webhook(db, event)
        db.commit()
        assert db.query(OrganizationSubscriptionPayment).one().status == 'DELETED'


def test_subscription_actions_reserve_and_block_retry_after_uncertain_response(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_flash', lambda *args: None)
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(asaas_api_base_url='https://api-sandbox.asaas.com', asaas_api_key='key'))
    calls = []
    def action(**kwargs):
        calls.append(kwargs)
        return None, 'Resposta inconclusiva'
    monkeypatch.setattr(routes, 'manage_sandbox_subscription', action)
    with Session(engine) as db:
        org = Organization(name='Teste', slug='actions', is_active=True)
        db.add(org)
        db.flush()
        db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_test', asaas_environment='sandbox'))
        db.add(OrganizationSubscription(organization_id=org.id, plan_name='Teste', amount=Decimal('99.90'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', asaas_subscription_id='sub_test', asaas_status='ACTIVE', status='criada'))
        db.commit()
        args = dict(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', action='cancelar', next_due_date='', db=db, user=SimpleNamespace())
        routes.manage_organization_sandbox_subscription_action(**args)
        routes.manage_organization_sandbox_subscription_action(**args)
        assert len(calls) == 1
        assert db.query(OrganizationSubscription).one().status == 'revisao'
