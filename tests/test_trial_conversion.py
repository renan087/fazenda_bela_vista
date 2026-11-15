from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.core.permissions_catalog import PAGE_FINANCE, WRITE_FINANCE
from app.db.base import Base
from app.models import Organization, OrganizationAccessGrant, OrganizationBilling, OrganizationCardMigration, OrganizationSubscription, User
from app.services import billing_notices
from app.services import subscription_plans as plans
from app.services.entitlements import compute_entitlements, invalidate_entitlements
from app.web import routes

TODAY = date(2026, 11, 14)


def _db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return Session(engine)


def _trial_org(db, ends_on):
    plan = plans.create_plan(db, plans.parse_plan_form(name='Completo', description=None, price_monthly='199,00', price_quarterly='', price_semiannual='', price_yearly='1.990,00',
                                                       max_farms='', max_users='', max_hectares='', modules=[], trial_days='', display_order='', is_featured=None))
    plans.activate_plan(db, plan)
    org = Organization(name='Sítio Sol', slug='sitio-sol', is_active=True)
    db.add(org)
    db.flush()
    db.add(User(name='Ana Souza', email='ana@x.com', hashed_password='h', is_admin=True, is_active=True, organization_id=org.id, phone='(35) 99999-8888'))
    db.add(OrganizationAccessGrant(organization_id=org.id, plan_id=plan.id, reason='teste_gratis', justification='Teste grátis de 14 dias pelo autocadastro.',
                                   starts_on=ends_on - timedelta(days=14), ends_on=ends_on))
    db.commit()
    invalidate_entitlements(org.id)
    return org, plan


def test_trial_active_then_read_only_after_end():
    db = _db()
    org, plan = _trial_org(db, TODAY + timedelta(days=5))
    ent = compute_entitlements(db, org.id, TODAY)
    assert ent.billing_state == 'teste' and ent.trial_ends_on == TODAY + timedelta(days=5) and not ent.read_only and ent.plan_id == plan.id
    ended = compute_entitlements(db, org.id, TODAY + timedelta(days=6))
    assert ended.trial_ended and ended.read_only and ended.trial_ends_on == TODAY + timedelta(days=5)


def test_paid_subscription_after_trial_lifts_restriction():
    db = _db()
    org, plan = _trial_org(db, TODAY - timedelta(days=1))
    db.add(OrganizationSubscription(organization_id=org.id, plan_id=plan.id, plan_name=plan.name, amount=Decimal('199'), cycle='MONTHLY', billing_type='PIX',
                                    first_due_date=TODAY, status='criada', asaas_status='ACTIVE', asaas_subscription_id='sub_1', environment='sandbox', is_current=True))
    db.commit()
    ent = compute_entitlements(db, org.id, TODAY)
    assert ent.billing_state == 'ok' and not ent.trial_ended


def test_trial_notices_three_days_before_and_after_end_only_once():
    assert billing_notices.trial_notice_kind(TODAY + timedelta(days=3), TODAY, '') == 'teste_3_dias'
    assert billing_notices.trial_notice_kind(TODAY + timedelta(days=4), TODAY, '') is None
    assert billing_notices.trial_notice_kind(TODAY - timedelta(days=1), TODAY, 'teste_3_dias') == 'teste_fim'
    assert billing_notices.trial_notice_kind(TODAY - timedelta(days=1), TODAY, 'teste_3_dias,teste_fim') is None
    db = _db()
    org, _ = _trial_org(db, TODAY + timedelta(days=2))
    sent = []
    send = lambda to, subject, body, ctx: sent.append((to, subject, body))
    assert billing_notices.process_trial_notices(db, TODAY, send=send) == [(org.id, 'teste_3_dias')]
    assert billing_notices.process_trial_notices(db, TODAY, send=send) == []
    assert sent[0][0] == 'ana@x.com' and '/minha-assinatura/contratar' in sent[0][2]


def _env(monkeypatch, *, card=False):
    calls = {}
    monkeypatch.setattr(routes, '_base_context', lambda *args, **kwargs: kwargs)
    monkeypatch.setattr(routes, '_repository', lambda _db: None)
    monkeypatch.setattr(routes, 'permission_codes_for_user', lambda db, user: frozenset({PAGE_FINANCE, WRITE_FINANCE}))
    monkeypatch.setattr(routes, 'validate_csrf', lambda *a: None)
    monkeypatch.setattr(routes, 'today_in_app_timezone', lambda: TODAY)
    monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: SimpleNamespace(name=name, context=context, status_code=200))
    monkeypatch.setattr(routes, 'asaas_ready', lambda *a: True)
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(asaas_api_key='key', asaas_api_base_url='https://api-sandbox.asaas.com', support_email=None))
    monkeypatch.setattr(routes, 'find_asaas_customers_by_document', lambda **k: ([], None))
    monkeypatch.setattr(routes, 'create_asaas_customer', lambda **k: calls.setdefault('customer', k) and ({'id': 'cus_new'}, None))
    monkeypatch.setattr(routes, 'list_customer_sandbox_subscriptions', lambda **k: ([], None))
    monkeypatch.setattr(routes, 'create_sandbox_subscription', lambda **k: calls.setdefault('subscription', k) and ({'id': 'sub_new', 'status': 'ACTIVE'}, None))
    monkeypatch.setattr(routes, '_auto_reconcile', lambda *a, **k: [])
    monkeypatch.setattr(routes, 'create_sandbox_card_checkout_session', lambda **k: calls.setdefault('checkout', k) and (
        {'id': 'c7b1c696-b27b-4d3d-80b9-d1c018e387f8', 'link': 'https://sandbox.asaas.com/checkoutSession/show/c7b1c696-b27b-4d3d-80b9-d1c018e387f8'}, None))
    monkeypatch.setattr(routes, '_flash', lambda request, kind, message: calls.setdefault('flash', message))
    return calls


def _form(plan, **extra):
    data = dict(plan_id=str(plan.id), cycle='MONTHLY', billing_type='PIX', person_type='FISICA', name='Ana Souza', cpf_cnpj='529.982.247-25',
                email='ana@x.com', mobile_phone='(35) 99999-8888', postal_code='37000-000', address_number='10')
    data.update(extra)
    return data


def test_customer_contracts_pix_with_first_due_at_trial_end(monkeypatch):
    calls = _env(monkeypatch)
    db = _db()
    org, plan = _trial_org(db, TODAY + timedelta(days=5))
    user = db.query(User).one()
    response = routes.subscribe_action(request=SimpleNamespace(), csrf_token='t', db=db, user=user, **_form(plan))
    assert response.headers['location'] == '/minha-assinatura'
    sub = db.query(OrganizationSubscription).one()
    assert sub.asaas_subscription_id == 'sub_new' and sub.amount == Decimal('199.00') and calls['subscription']['payload']['nextDueDate'] == (TODAY + timedelta(days=5)).isoformat()
    billing = db.query(OrganizationBilling).one()
    assert billing.asaas_customer_id == 'cus_new' and billing.cpf_cnpj == '52998224725'
    again = routes.subscribe_action(request=SimpleNamespace(), csrf_token='t', db=db, user=user, **_form(plan))
    assert again.headers['location'] == '/minha-assinatura' and db.query(OrganizationSubscription).count() == 1


def test_customer_card_goes_to_asaas_checkout_and_expired_trial_charges_today(monkeypatch):
    calls = _env(monkeypatch)
    db = _db()
    org, plan = _trial_org(db, TODAY - timedelta(days=2))
    user = db.query(User).one()
    response = routes.subscribe_action(request=SimpleNamespace(), csrf_token='t', db=db, user=user, **_form(plan, billing_type='CREDIT_CARD', cycle='YEARLY'))
    assert response.status_code == 303 and response.headers['location'].startswith('https://sandbox.asaas.com/checkoutSession/')
    assert calls['checkout']['next_due_date'] == TODAY.isoformat() and calls['checkout']['callback_path'] == '/minha-assinatura'
    assert db.query(OrganizationCardMigration).one().amount == Decimal('1990.00')


def test_invalid_billing_data_and_unavailable_cycle_are_rejected(monkeypatch):
    _env(monkeypatch)
    db = _db()
    _, plan = _trial_org(db, TODAY + timedelta(days=5))
    user = db.query(User).one()
    bad_doc = routes.subscribe_action(request=SimpleNamespace(), csrf_token='t', db=db, user=user, **_form(plan, cpf_cnpj='123'))
    assert bad_doc.status_code == 400 and 'CPF' in bad_doc.context['error']
    no_cycle = routes.subscribe_action(request=SimpleNamespace(), csrf_token='t', db=db, user=user, **_form(plan, cycle='QUARTERLY'))
    assert no_cycle.status_code == 400 and 'periodicidade' in no_cycle.context['error']
    assert not db.query(OrganizationSubscription).count()
