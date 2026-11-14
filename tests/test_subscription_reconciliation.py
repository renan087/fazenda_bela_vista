from datetime import date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import (
    Organization,
    OrganizationBilling,
    OrganizationCardMigration,
    OrganizationSubscription,
    OrganizationSubscriptionPayment,
)
from app.services import subscription_reconciliation as rec
from app.services.subscription_portfolio import build_subscription_portfolio
from app.web import routes

CHECKOUT = 'c7b1c696-b27b-4d3d-80b9-d1c018e387f8'
NOW = datetime(2026, 11, 14, 12, 0, 0)


@pytest.fixture
def db(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setattr(rec, '_now', lambda: NOW)
    with Session(engine) as session:
        yield session


def _org(db, slug='org'):
    org = Organization(name=slug.title(), slug=slug, is_active=True)
    db.add(org)
    db.flush()
    db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email=f'{slug}@example.com', asaas_customer_id=f'cus_{slug}', asaas_environment='sandbox'))
    db.flush()
    return org


def _sub(db, org, **overrides):
    values = dict(organization_id=org.id, plan_name='Plano', amount=Decimal('100.00'), cycle='MONTHLY', billing_type='CREDIT_CARD', first_due_date=date(2026, 11, 1),
                  status='criada', asaas_status='ACTIVE', asaas_subscription_id=f'sub_{org.slug}', environment='sandbox', is_current=True)
    values.update(overrides)
    sub = OrganizationSubscription(**values)
    db.add(sub)
    db.flush()
    return sub


def _remote_sub(org, **overrides):
    data = {'id': f'sub_{org.slug}', 'customer': f'cus_{org.slug}', 'status': 'ACTIVE', 'value': 100, 'cycle': 'MONTHLY', 'billingType': 'CREDIT_CARD', 'nextDueDate': '2026-12-01', 'description': 'Plano', 'deleted': False}
    data.update(overrides)
    return data


def _remote_pay(org, pay_id, status, **overrides):
    data = {'id': pay_id, 'subscription': f'sub_{org.slug}', 'customer': f'cus_{org.slug}', 'status': status, 'value': 100, 'dueDate': '2026-11-01', 'billingType': 'CREDIT_CARD'}
    data.update(overrides)
    return data


def _mock_asaas(monkeypatch, *, subscription=None, payments=(), single=None, missing=None, customer_subs=None, checkout=None):
    monkeypatch.setattr(rec, 'get_sandbox_subscription', lambda **kw: (subscription, None) if subscription else (None, 'HTTP 404'))
    monkeypatch.setattr(rec, 'sandbox_subscription_is_missing', lambda **kw: (missing, None))
    monkeypatch.setattr(rec, 'list_sandbox_subscription_payments', lambda **kw: (list(payments), None))
    monkeypatch.setattr(rec, 'get_sandbox_payment', lambda **kw: ((single or {}).get(kw['payment_id']), None) if single and kw['payment_id'] in single else (None, 'HTTP 500'))
    monkeypatch.setattr(rec, 'list_customer_sandbox_subscriptions', lambda **kw: (customer_subs or [], None))
    monkeypatch.setattr(rec, 'cancel_sandbox_checkout', lambda **kw: checkout or (None, 'HTTP 500'))


def test_confirmed_card_charge_with_pending_operation_is_released_automatically(db, monkeypatch):
    org = _org(db)
    sub = _sub(db, org)
    db.add(OrganizationSubscriptionPayment(subscription_id=sub.id, asaas_payment_id='pay_1', status='PENDING', operation_state='verificar', last_event_at=NOW - timedelta(hours=1)))
    db.commit()
    assert build_subscription_portfolio(db, [org], NOW.date()).rows[0].situation == 'verificar'
    _mock_asaas(monkeypatch, subscription=_remote_sub(org), payments=[_remote_pay(org, 'pay_1', 'CONFIRMED', invoiceUrl='https://sandbox.asaas.com/i/1')])
    assert rec.reconcile_organization(db, org.id, 'key') == []
    db.commit()
    charge = db.query(OrganizationSubscriptionPayment).one()
    assert charge.status == 'CONFIRMED' and charge.operation_state is None and charge.invoice_url == 'https://sandbox.asaas.com/i/1'
    assert sub.last_synced_at == NOW and sub.sync_issue is None
    assert build_subscription_portfolio(db, [org], NOW.date()).rows[0].situation == 'ativa'


def test_subscription_in_review_is_resolved_from_asaas(db, monkeypatch):
    org = _org(db)
    sub = _sub(db, org, status='revisao', amount=Decimal('90.00'))
    db.commit()
    _mock_asaas(monkeypatch, subscription=_remote_sub(org, value=120, status='INACTIVE'))
    assert rec.reconcile_organization(db, org.id, 'key', full=False) == []
    assert (sub.status, sub.amount, sub.asaas_status) == ('criada', Decimal('120.00'), 'INACTIVE')


def test_inconclusive_creation_is_linked_by_external_reference(db, monkeypatch):
    org = _org(db)
    sub = _sub(db, org, status='revisao', asaas_subscription_id=None, asaas_status=None)
    db.commit()
    _mock_asaas(monkeypatch, subscription=_remote_sub(org), customer_subs=[{'id': f'sub_{org.slug}', 'externalReference': f'sisfarm-sub-{sub.id}', 'status': 'ACTIVE', 'deleted': False, 'customer': f'cus_{org.slug}'}])
    assert rec.reconcile_organization(db, org.id, 'key') == []
    assert sub.asaas_subscription_id == f'sub_{org.slug}' and sub.status == 'criada' and sub.is_current


def test_creation_that_never_reached_asaas_is_closed_after_grace(db, monkeypatch):
    org = _org(db)
    sub = _sub(db, org, status='revisao', asaas_subscription_id=None, asaas_status=None)
    db.commit()
    _mock_asaas(monkeypatch)
    sub.created_at = NOW - timedelta(minutes=2)
    rec.reconcile_organization(db, org.id, 'key')
    assert sub.is_current and sub.status == 'revisao'
    sub.created_at = NOW - timedelta(minutes=11)
    rec.reconcile_organization(db, org.id, 'key')
    assert not sub.is_current and sub.status == 'falhou'


def test_subscription_missing_in_asaas_is_closed_and_history_kept(db, monkeypatch):
    org = _org(db)
    sub = _sub(db, org)
    db.add(OrganizationSubscriptionPayment(subscription_id=sub.id, asaas_payment_id='pay_old', status='RECEIVED'))
    db.commit()
    _mock_asaas(monkeypatch, subscription=None, missing=True)
    assert rec.reconcile_organization(db, org.id, 'key') == []
    assert not sub.is_current and sub.status == 'encerrada' and sub.asaas_status == 'NOT_FOUND'
    assert db.query(OrganizationSubscriptionPayment).count() == 1


def test_deleted_subscription_is_released(db, monkeypatch):
    org = _org(db)
    sub = _sub(db, org, asaas_status='DELETED')
    db.commit()
    _mock_asaas(monkeypatch, subscription=_remote_sub(org, deleted=True))
    rec.reconcile_organization(db, org.id, 'key')
    assert not sub.is_current and sub.status == 'encerrada'


def test_charge_removed_from_asaas_is_marked_deleted(db, monkeypatch):
    org = _org(db)
    sub = _sub(db, org)
    db.add(OrganizationSubscriptionPayment(subscription_id=sub.id, asaas_payment_id='pay_gone', status='OVERDUE', due_date=date(2026, 10, 1)))
    db.commit()
    _mock_asaas(monkeypatch, subscription=_remote_sub(org), payments=[], single={'pay_gone': {'id': 'pay_gone', 'deleted': True}})
    rec.reconcile_organization(db, org.id, 'key')
    assert db.query(OrganizationSubscriptionPayment).one().status == 'DELETED'
    assert build_subscription_portfolio(db, [org], NOW.date()).rows[0].situation == 'ativa'


def test_failure_keeps_previous_data_and_records_reason(db, monkeypatch):
    org = _org(db)
    sub = _sub(db, org, status='revisao')
    db.add(OrganizationSubscriptionPayment(subscription_id=sub.id, asaas_payment_id='pay_1', status='PENDING', operation_state='verificar'))
    db.commit()
    _mock_asaas(monkeypatch, subscription=_remote_sub(org, customer='cus_outro'))
    issues = rec.reconcile_organization(db, org.id, 'key')
    db.commit()
    assert issues and 'diverge' in sub.sync_issue
    assert sub.status == 'revisao' and db.query(OrganizationSubscriptionPayment).one().operation_state == 'verificar'
    row = build_subscription_portfolio(db, [org], NOW.date()).rows[0]
    assert row.situation == 'verificar' and 'diverge' in row.attention_reason


@pytest.mark.parametrize('checkout, expected_open, expected_status', [
    (('CANCELED', None), False, 'encerrado'),
    (('EXPIRED', None), False, 'encerrado'),
    (('PAID', None), True, 'pago'),
    ((None, 'HTTP 500'), True, 'revisao'),
])
def test_card_checkout_in_review_is_resolved(db, monkeypatch, checkout, expected_open, expected_status):
    org = _org(db)
    old = _sub(db, org, billing_type='PIX', status='migrando')
    migration = OrganizationCardMigration(organization_id=org.id, old_subscription_id=old.id, checkout_id=CHECKOUT, status='revisao')
    db.add(migration)
    db.commit()
    _mock_asaas(monkeypatch, subscription=_remote_sub(org, billingType='PIX'), checkout=checkout)
    rec.reconcile_organization(db, org.id, 'key')
    assert migration.is_open is expected_open and migration.status == expected_status
    assert old.status == ('criada' if not expected_open else 'migrando')


def test_card_checkout_without_id_is_closed(db, monkeypatch):
    org = _org(db)
    migration = OrganizationCardMigration(organization_id=org.id, old_subscription_id=None, plan_name='P', amount=Decimal('10'), cycle='MONTHLY', status='revisao')
    db.add(migration)
    db.commit()
    _mock_asaas(monkeypatch)
    rec.reconcile_organization(db, org.id, 'key')
    assert not migration.is_open and migration.status == 'encerrado'


def test_organizations_needing_reconciliation(db):
    fresh, stale, pending_charge, review_checkout, idle = (_org(db, s) for s in ('fresh', 'stale', 'charge', 'checkout', 'idle'))
    _sub(db, fresh, last_synced_at=NOW - timedelta(minutes=5))
    _sub(db, stale, last_synced_at=NOW - timedelta(hours=2))
    charged = _sub(db, pending_charge, last_synced_at=NOW)
    db.add(OrganizationSubscriptionPayment(subscription_id=charged.id, asaas_payment_id='pay_x', status='PENDING', operation_state='verificar'))
    db.add(OrganizationCardMigration(organization_id=review_checkout.id, status='revisao'))
    db.commit()
    assert rec.organizations_needing_reconciliation(db, NOW) == sorted([stale.id, pending_charge.id, review_checkout.id])


def test_checkout_expired_webhook_closes_and_restores(db, monkeypatch):
    import asyncio

    from app.routers import asaas_webhook

    monkeypatch.setattr(asaas_webhook, 'get_settings', lambda: SimpleNamespace(asaas_webhook_token='secret'))
    org = _org(db)
    old = _sub(db, org, billing_type='PIX', status='migrando')
    migration = OrganizationCardMigration(organization_id=org.id, old_subscription_id=old.id, checkout_id=CHECKOUT, status='aguardando')
    db.add(migration)
    db.commit()

    class Request:
        headers = {'asaas-access-token': 'secret'}
        async def json(self):
            return {'event': 'CHECKOUT_EXPIRED', 'checkout': {'id': CHECKOUT, 'customer': f'cus_{org.slug}', 'status': 'EXPIRED'}}

    assert asyncio.run(asaas_webhook.asaas_webhook(Request(), db)) == {'received': True}
    assert not migration.is_open and migration.status == 'encerrado' and old.status == 'criada'


def test_payment_action_is_confirmed_automatically(db, monkeypatch):
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    flashes = []
    monkeypatch.setattr(routes, '_flash', lambda request, kind, message: flashes.append((kind, message)))
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(asaas_api_base_url='https://api-sandbox.asaas.com', asaas_api_key='key'))
    org = _org(db)
    sub = _sub(db, org, billing_type='PIX')
    db.add(OrganizationSubscriptionPayment(subscription_id=sub.id, asaas_payment_id='pay_1', status='PENDING', billing_type='PIX'))
    db.commit()
    charge_id = db.query(OrganizationSubscriptionPayment).one().id
    monkeypatch.setattr(routes, 'execute_payment_action', lambda **kw: ({'id': 'pay_1', 'deleted': True}, None, True))
    _mock_asaas(monkeypatch, subscription=_remote_sub(org, billingType='PIX'), payments=[], single={'pay_1': {'id': 'pay_1', 'deleted': True}})
    routes.manage_organization_payment_action(organization_id=org.id, payment_id=charge_id, request=SimpleNamespace(), csrf_token='x', action='cancelar', payment_date='', value='', db=db, user=SimpleNamespace())
    charge = db.query(OrganizationSubscriptionPayment).one()
    assert charge.operation_state is None and charge.status == 'DELETED'
    assert flashes[-1] == ('success', 'Cobrança cancelada. Situação confirmada com o Asaas.')
