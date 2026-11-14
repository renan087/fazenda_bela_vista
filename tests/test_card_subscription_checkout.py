from datetime import date
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
from app.services import asaas_card_checkout as checkout
from app.services import asaas_card_migration_worker as worker
from app.services import subscription_plans as plans
from app.services.organization_subscription_sync import persist_organization_webhook
from app.web import routes

CHECKOUT = 'c7b1c696-b27b-4d3d-80b9-d1c018e387f8'
LINK = f'https://sandbox.asaas.com/checkoutSession/show/{CHECKOUT}'


def _db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return Session(engine)


def _org(db):
    org = Organization(name='Cliente', slug='cliente', is_active=True)
    db.add(org)
    db.flush()
    db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_test', asaas_environment='sandbox'))
    db.commit()
    return org


def _plan(db, **overrides):
    fields = dict(name='Profissional', description=None, price_monthly='149,90', price_quarterly='', price_semiannual='799,00', price_yearly='',
                  max_farms='', max_users='', max_hectares='', modules=[], trial_days='', display_order='', is_featured=None)
    fields.update(overrides)
    plan = plans.create_plan(db, plans.parse_plan_form(**fields))
    plans.activate_plan(db, plan)
    db.commit()
    return plan


def _route_env(monkeypatch):
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    flashes = []
    monkeypatch.setattr(routes, '_flash', lambda request, kind, message: flashes.append((kind, message)))
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(asaas_api_base_url='https://api-sandbox.asaas.com', asaas_api_key='sandbox-key'))
    monkeypatch.setattr(routes, 'list_customer_sandbox_subscriptions', lambda **kwargs: ([], None))
    monkeypatch.setattr(routes, 'create_sandbox_subscription', lambda **kwargs: pytest.fail('cartão não deve criar assinatura direta'))
    return flashes


def test_safe_asaas_link_only_accepts_https_asaas_hosts():
    assert checkout.safe_asaas_link('https://www.asaas.com/i/123') == 'https://www.asaas.com/i/123'
    assert checkout.safe_asaas_link(LINK) == LINK
    for bad in ['http://www.asaas.com/i/1', 'https://asaas.com.evil.io/i/1', 'https://evilasaas.com/x', 'javascript:alert(1)', 'https://user@www.asaas.com/i', None, 'x' * 300]:
        assert checkout.safe_asaas_link(bad) is None


def test_checkout_session_returns_link_and_uses_24h_and_subscriptions_callback(monkeypatch):
    calls = []

    class Response:
        status_code = 200
        def json(self): return {'id': CHECKOUT, 'link': LINK}

    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def post(self, url, **kwargs):
            calls.append(kwargs['json'])
            return Response()

    monkeypatch.setattr(checkout.httpx, 'Client', Client)
    session, error = checkout.create_sandbox_card_checkout_session(api_key='k', customer_id='cus_test', reference='r', plan_name='P', amount=10, cycle='SEMIANNUALLY', next_due_date='2099-01-01', callback_base='https://app.sisfarm.com.br')
    assert error is None and session == {'id': CHECKOUT, 'link': LINK}
    assert calls[0]['minutesToExpire'] == 1440
    assert calls[0]['callback']['successUrl'] == 'https://app.sisfarm.com.br/minha-assinatura'
    assert calls[0]['subscription']['cycle'] == 'SEMIANNUALLY'


def test_card_creation_generates_checkout_without_creating_subscription(monkeypatch):
    flashes = _route_env(monkeypatch)
    captured = {}
    def fake_session(**kwargs):
        captured.update(kwargs)
        return {'id': CHECKOUT, 'link': LINK}, None
    monkeypatch.setattr(routes, 'create_sandbox_card_checkout_session', fake_session)
    with _db() as db:
        org = _org(db)
        plan = _plan(db)
        routes.create_organization_sandbox_subscription_action(
            organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_id=str(plan.id), cycle='SEMIANNUALLY',
            billing_type='CREDIT_CARD', first_due_date='2099-01-01', db=db, user=SimpleNamespace(),
        )
        assert db.query(OrganizationSubscription).count() == 0
        migration = db.query(OrganizationCardMigration).one()
        assert migration.old_subscription_id is None and migration.is_open
        assert (migration.status, migration.checkout_id, migration.checkout_link) == ('aguardando', CHECKOUT, LINK)
        assert (migration.plan_id, migration.plan_name, migration.amount, migration.cycle) == (plan.id, 'Profissional', Decimal('799.00'), 'SEMIANNUALLY')
        assert captured['amount'] == Decimal('799.00') and captured['reference'] == f'sisfarm-contratacao-{migration.id}'
        assert flashes[-1][0] == 'success'
        # Com checkout pendente, nova tentativa e bloqueada.
        routes.create_organization_sandbox_subscription_action(
            organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_id=str(plan.id), cycle='MONTHLY',
            billing_type='PIX', first_due_date='2099-01-01', db=db, user=SimpleNamespace(),
        )
        assert db.query(OrganizationCardMigration).count() == 1 and flashes[-1][0] == 'error'


def test_card_creation_failure_marks_review(monkeypatch):
    _route_env(monkeypatch)
    monkeypatch.setattr(routes, 'create_sandbox_card_checkout_session', lambda **kwargs: (None, 'HTTP 500'))
    with _db() as db:
        org = _org(db)
        routes.create_organization_sandbox_subscription_action(
            organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_id='', plan_name='Negociado', amount='50,00',
            cycle='MONTHLY', billing_type='CREDIT_CARD', first_due_date='2099-01-01', db=db, user=SimpleNamespace(),
        )
        migration = db.query(OrganizationCardMigration).one()
        assert migration.status == 'revisao' and migration.checkout_id is None and migration.is_open


def _new_contract(db, org, plan=None, **overrides):
    values = dict(organization_id=org.id, old_subscription_id=None, plan_id=plan.id if plan else None, plan_name='Profissional', amount=Decimal('799.00'),
                  cycle='SEMIANNUALLY', first_due_date=date(2099, 1, 1), checkout_id=CHECKOUT, checkout_link=LINK, status='pago')
    values.update(overrides)
    migration = OrganizationCardMigration(**values)
    db.add(migration)
    db.commit()
    return migration


def test_worker_creates_card_subscription_after_payment(monkeypatch):
    monkeypatch.setattr(worker, 'manage_sandbox_subscription', lambda **kwargs: pytest.fail('não há assinatura anterior para suspender'))
    monkeypatch.setattr(worker, 'list_sandbox_subscription_payments', lambda **kwargs: ([{'id': 'pay_1', 'subscription': 'sub_card', 'customer': 'cus_test', 'status': 'CONFIRMED', 'value': 799, 'dueDate': '2099-01-01', 'invoiceUrl': 'https://sandbox.asaas.com/i/pay_1'}], None))
    with _db() as db:
        org = _org(db)
        plan = _plan(db)
        migration = _new_contract(db, org, plan)
        monkeypatch.setattr(worker, 'find_paid_checkout_subscription', lambda **kwargs: ({'id': 'sub_card', 'value': 700, 'cycle': 'SEMIANNUALLY', 'nextDueDate': '2099-07-01'}, None))
        with pytest.raises(ValueError, match='não correspondem'):
            worker.process_paid_card_migration(db, migration, 'key')
        db.rollback()
        monkeypatch.setattr(worker, 'find_paid_checkout_subscription', lambda **kwargs: ({'id': 'sub_card', 'value': 799, 'cycle': 'SEMIANNUALLY', 'nextDueDate': '2099-07-01'}, None))
        worker.process_paid_card_migration(db, migration, 'key')
        db.commit()
        sub = db.query(OrganizationSubscription).one()
        assert (sub.plan_id, sub.billing_type, sub.asaas_subscription_id, sub.is_current, sub.amount) == (plan.id, 'CREDIT_CARD', 'sub_card', True, Decimal('799.00'))
        assert migration.status == 'concluido' and not migration.is_open and migration.new_subscription_id == sub.id
        assert db.query(OrganizationSubscriptionPayment).one().invoice_url == 'https://sandbox.asaas.com/i/pay_1'


def test_worker_refuses_new_contract_when_org_already_has_subscription(monkeypatch):
    monkeypatch.setattr(worker, 'find_paid_checkout_subscription', lambda **kwargs: pytest.fail('não deve consultar'))
    with _db() as db:
        org = _org(db)
        db.add(OrganizationSubscription(organization_id=org.id, plan_name='X', amount=Decimal('1'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', is_current=True))
        db.commit()
        migration = _new_contract(db, org)
        with pytest.raises(ValueError, match='já possui'):
            worker.process_paid_card_migration(db, migration, 'key')


def test_card_migration_keeps_catalog_plan(monkeypatch):
    monkeypatch.setattr(worker, 'find_paid_checkout_subscription', lambda **kwargs: ({'id': 'sub_new', 'value': 149.9, 'cycle': 'MONTHLY', 'nextDueDate': '2099-02-01'}, None))
    monkeypatch.setattr(worker, 'get_sandbox_subscription', lambda **kwargs: ({'customer': 'cus_test', 'status': 'ACTIVE'}, None))
    monkeypatch.setattr(worker, 'manage_sandbox_subscription', lambda **kwargs: ({'customer': 'cus_test', 'status': 'INACTIVE'}, None))
    monkeypatch.setattr(worker, 'list_sandbox_subscription_payments', lambda **kwargs: ([], None))
    with _db() as db:
        org = _org(db)
        plan = _plan(db)
        old = OrganizationSubscription(organization_id=org.id, plan_id=plan.id, plan_name=plan.name, amount=Decimal('149.90'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', status='migrando', asaas_subscription_id='sub_old', is_current=True)
        db.add(old)
        db.flush()
        migration = OrganizationCardMigration(organization_id=org.id, old_subscription_id=old.id, checkout_id=CHECKOUT, first_due_date=date(2099, 1, 1), status='pago')
        db.add(migration)
        db.commit()
        worker.process_paid_card_migration(db, migration, 'key')
        db.commit()
        new = db.query(OrganizationSubscription).filter_by(is_current=True).one()
        assert new.plan_id == plan.id and new.billing_type == 'CREDIT_CARD'


@pytest.mark.parametrize('remote, expected_open, expected_status', [
    (('CANCELED', None), False, 'encerrado'),
    (('EXPIRED', None), False, 'encerrado'),
    (('PAID', None), True, 'pago'),
    (('ACTIVE', None), True, 'aguardando'),
    ((None, 'HTTP 500'), True, 'aguardando'),
])
def test_close_checkout_only_after_asaas_confirms(monkeypatch, remote, expected_open, expected_status):
    _route_env(monkeypatch)
    monkeypatch.setattr(routes, 'cancel_sandbox_checkout', lambda **kwargs: remote)
    with _db() as db:
        org = _org(db)
        migration = _new_contract(db, org, status='aguardando')
        routes.close_card_checkout_action(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', db=db, user=SimpleNamespace())
        db.refresh(migration)
        assert migration.is_open is expected_open and migration.status == expected_status


def test_closing_migration_checkout_restores_previous_subscription(monkeypatch):
    _route_env(monkeypatch)
    monkeypatch.setattr(routes, 'cancel_sandbox_checkout', lambda **kwargs: ('CANCELED', None))
    with _db() as db:
        org = _org(db)
        old = OrganizationSubscription(organization_id=org.id, plan_name='Plano', amount=Decimal('20'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', status='migrando', asaas_subscription_id='sub_old', asaas_status='ACTIVE', is_current=True)
        db.add(old)
        db.flush()
        db.add(OrganizationCardMigration(organization_id=org.id, old_subscription_id=old.id, checkout_id=CHECKOUT, status='revisao'))
        db.commit()
        routes.close_card_checkout_action(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', db=db, user=SimpleNamespace())
        db.refresh(old)
        assert old.status == 'criada' and db.query(OrganizationCardMigration).one().is_open is False


def test_payment_links_are_stored_only_from_asaas():
    with _db() as db:
        org = _org(db)
        sub = OrganizationSubscription(organization_id=org.id, plan_name='P', amount=Decimal('10'), cycle='MONTHLY', billing_type='BOLETO', first_due_date=date(2026, 1, 1), environment='sandbox', asaas_subscription_id='sub_1', is_current=True)
        db.add(sub)
        db.commit()
        base = {'id': 'pay_1', 'subscription': 'sub_1', 'customer': 'cus_test', 'status': 'PENDING', 'value': 10, 'dueDate': '2099-01-01'}
        persist_organization_webhook(db, {'id': 'evt_1', 'event': 'PAYMENT_CREATED', 'dateCreated': '2026-11-14 10:00:00', 'payment': {**base, 'invoiceUrl': 'https://sandbox.asaas.com/i/1', 'bankSlipUrl': 'https://sandbox.asaas.com/b/pdf/1'}})
        persist_organization_webhook(db, {'id': 'evt_2', 'event': 'PAYMENT_UPDATED', 'dateCreated': '2026-11-14 11:00:00', 'payment': {**base, 'invoiceUrl': 'https://phishing.example/i/1'}})
        row = db.query(OrganizationSubscriptionPayment).one()
        assert row.invoice_url == 'https://sandbox.asaas.com/i/1' and row.bank_slip_url == 'https://sandbox.asaas.com/b/pdf/1'


def _edit_env(monkeypatch, db, plan, *, current_plan):
    _route_env(monkeypatch)
    sent = []
    def update(**kwargs):
        sent.append(kwargs['payload'])
        payload = kwargs['payload']
        return {'description': payload['description'], 'value': payload['value'], 'cycle': payload['cycle'], 'billingType': payload.get('billingType', 'PIX'), 'status': 'ACTIVE', 'customer': 'cus_test'}, None
    monkeypatch.setattr(routes, 'update_sandbox_subscription', update)
    org = _org(db)
    sub = OrganizationSubscription(organization_id=org.id, plan_id=current_plan.id if current_plan else None, plan_name='Antigo', amount=Decimal('99.00'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', status='criada', asaas_subscription_id='sub_1', asaas_status='ACTIVE', is_current=True)
    db.add(sub)
    db.commit()
    return org, sub, sent


def test_edit_switches_to_catalog_plan_with_catalog_price(monkeypatch):
    with _db() as db:
        plan = _plan(db)
        org, sub, sent = _edit_env(monkeypatch, db, plan, current_plan=None)
        routes.edit_organization_sandbox_subscription_action(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_id=str(plan.id), plan_name='Ignorado', amount='1,00', cycle='SEMIANNUALLY', billing_type='PIX', next_due_date='', db=db, user=SimpleNamespace())
        db.refresh(sub)
        assert sent[0]['value'] == 799.0 and sent[0]['description'] == 'Profissional'
        assert (sub.plan_id, sub.amount, sub.cycle) == (plan.id, Decimal('799.00'), 'SEMIANNUALLY')


def test_edit_keeps_archived_contracted_plan_but_refuses_other_archived(monkeypatch):
    with _db() as db:
        contracted = _plan(db)
        other = _plan(db, name='Antigo Essencial')
        plans.archive_plan(contracted)
        plans.archive_plan(other)
        db.commit()
        org, sub, sent = _edit_env(monkeypatch, db, contracted, current_plan=contracted)
        routes.edit_organization_sandbox_subscription_action(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_id=str(other.id), cycle='MONTHLY', billing_type='PIX', next_due_date='', db=db, user=SimpleNamespace())
        assert sent == []
        routes.edit_organization_sandbox_subscription_action(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_id=str(contracted.id), cycle='MONTHLY', billing_type='BOLETO', next_due_date='', db=db, user=SimpleNamespace())
        db.refresh(sub)
        assert sent[0]['value'] == 149.9 and sub.plan_id == contracted.id


def test_edit_to_custom_unlinks_plan(monkeypatch):
    with _db() as db:
        plan = _plan(db)
        org, sub, sent = _edit_env(monkeypatch, db, plan, current_plan=plan)
        routes.edit_organization_sandbox_subscription_action(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_id='', plan_name='Negociado', amount='120,00', cycle='MONTHLY', billing_type='PIX', next_due_date='', db=db, user=SimpleNamespace())
        db.refresh(sub)
        assert sub.plan_id is None and sub.amount == Decimal('120.00') and sub.plan_name == 'Negociado'
