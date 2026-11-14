from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.core.path_access import required_permissions_for_web_path
from app.core.permissions_catalog import PAGE_FINANCE, WRITE_FINANCE
from app.db.base import Base
from app.models import Organization, OrganizationBilling, OrganizationCardMigration, OrganizationSubscription, OrganizationSubscriptionPayment
from app.services import subscription_plans as plans
from app.web import routes

CHECKOUT = 'c7b1c696-b27b-4d3d-80b9-d1c018e387f8'
LINK = f'https://sandbox.asaas.com/checkoutSession/show/{CHECKOUT}'


def _db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return Session(engine)


def _org(db, slug, *, billing_type='PIX', overdue=False):
    org = Organization(name=slug.title(), slug=slug, is_active=True)
    db.add(org)
    db.flush()
    db.add(OrganizationBilling(organization_id=org.id, contact_name='P', contact_email=f'{slug}@example.com', asaas_customer_id=f'cus_{slug}', asaas_environment='sandbox'))
    sub = OrganizationSubscription(organization_id=org.id, plan_name=f'Plano {slug}', amount=Decimal('149.90'), cycle='MONTHLY', billing_type=billing_type, first_due_date=date(2026, 1, 5),
                                   next_due_date=date(2099, 2, 5), status='criada', asaas_status='ACTIVE', asaas_subscription_id=f'sub_{slug}', environment='sandbox', is_current=True)
    db.add(sub)
    db.flush()
    db.add(OrganizationSubscriptionPayment(subscription_id=sub.id, asaas_payment_id=f'pay_{slug}_paid', status='RECEIVED', billing_type=billing_type, amount=Decimal('149.90'), due_date=date(2026, 1, 5), paid_date=date(2026, 1, 4), invoice_url=f'https://sandbox.asaas.com/i/{slug}1'))
    if overdue:
        db.add(OrganizationSubscriptionPayment(subscription_id=sub.id, asaas_payment_id=f'pay_{slug}_late', status='OVERDUE', billing_type=billing_type, amount=Decimal('149.90'), due_date=date(2026, 2, 5), invoice_url=f'https://sandbox.asaas.com/i/{slug}2', bank_slip_url=f'https://sandbox.asaas.com/b/pdf/{slug}2'))
    db.commit()
    return org, sub


def _user(org, can_change=True):
    return SimpleNamespace(organization=org, organization_id=org.id, email='cliente@example.com', id=99, is_admin=False)


def _page_env(monkeypatch, *, codes=frozenset({PAGE_FINANCE, WRITE_FINANCE})):
    monkeypatch.setattr(routes, '_base_context', lambda *args, **kwargs: kwargs)
    monkeypatch.setattr(routes, '_repository', lambda _db: None)
    monkeypatch.setattr(routes, 'permission_codes_for_user', lambda db, user: codes)
    monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: (name, context))


def _render(context):
    from jinja2 import ChoiceLoader, DictLoader
    env = routes.templates.env.overlay(loader=ChoiceLoader([DictLoader({'base.html': '{% block content %}{% endblock %}'}), routes.templates.env.loader]))
    return env.get_template('my_subscription.html').render(**context, request=None, csrf_token='t')


def test_paths_require_finance_and_write_to_change():
    assert required_permissions_for_web_path('/minha-assinatura', 'GET') == (PAGE_FINANCE,)
    assert required_permissions_for_web_path('/minha-assinatura/cartao', 'POST') == (PAGE_FINANCE, WRITE_FINANCE)


def test_page_shows_only_the_users_organization(monkeypatch):
    _page_env(monkeypatch)
    with _db() as db:
        mine, _ = _org(db, 'minha', overdue=True)
        other, _ = _org(db, 'outra', overdue=True)
        name, context = routes.my_subscription_page(request=SimpleNamespace(), db=db, user=_user(mine), csrf_token='t')
        html = _render(context)
    assert name == 'my_subscription.html' and context['organization'].id == mine.id
    assert {charge.asaas_payment_id for charge in context['open_charges']} == {'pay_minha_late'}
    assert 'Plano minha' in html and 'Plano outra' not in html and '/i/outra' not in html and 'Outra' not in html
    assert 'Pagamento em atraso' in html and 'Pagar agora' in html and 'Boleto (PDF)' in html
    assert 'Cadastrar cartão de crédito' in html and 'Ver fatura' in html


def test_read_only_user_sees_no_payment_buttons(monkeypatch):
    _page_env(monkeypatch, codes=frozenset({PAGE_FINANCE}))
    with _db() as db:
        mine, _ = _org(db, 'minha')
        _, context = routes.my_subscription_page(request=SimpleNamespace(), db=db, user=_user(mine), csrf_token='t')
        html = _render(context)
    assert 'Cadastrar cartão de crédito' not in html and 'responsável financeiro' in html


def test_page_without_subscription_shows_empty_state(monkeypatch):
    _page_env(monkeypatch)
    with _db() as db:
        org = Organization(name='Nova', slug='nova', is_active=True)
        db.add(org)
        db.commit()
        _, context = routes.my_subscription_page(request=SimpleNamespace(), db=db, user=_user(org), csrf_token='t')
        html = _render(context)
    assert 'ainda não tem uma assinatura ativa' in html


def test_page_lists_plan_limits_and_modules(monkeypatch):
    _page_env(monkeypatch)
    with _db() as db:
        mine, sub = _org(db, 'minha')
        plan = plans.create_plan(db, plans.parse_plan_form(name='Profissional', description='Para médias fazendas', price_monthly='149,90', price_quarterly='', price_semiannual='', price_yearly='',
                                                           max_farms='3', max_users='', max_hectares='300', modules=['page.finance', 'page.map'], trial_days='', display_order='', is_featured=None))
        sub.plan_id = plan.id
        db.commit()
        _, context = routes.my_subscription_page(request=SimpleNamespace(), db=db, user=_user(mine), csrf_token='t')
        html = _render(context)
    assert 'Para médias fazendas' in html and '300,00 ha' in html and 'Ilimitados' in html
    assert 'Mapa da fazenda' in html and 'Gestão financeira' in html


def _action_env(monkeypatch):
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    flashes = []
    monkeypatch.setattr(routes, '_flash', lambda request, kind, message: flashes.append((kind, message)))
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(asaas_api_base_url='https://api-sandbox.asaas.com', asaas_api_key='k', app_public_url='https://app.sisfarm.com.br', support_email=None, asaas_webhook_token='t'))
    return flashes


def test_customer_card_checkout_keeps_period_and_redirects_to_asaas(monkeypatch):
    _action_env(monkeypatch)
    sent = {}
    def session(**kwargs):
        sent.update(kwargs)
        return {'id': CHECKOUT, 'link': LINK}, None
    monkeypatch.setattr(routes, 'create_sandbox_card_checkout_session', session)
    with _db() as db:
        mine, sub = _org(db, 'minha')
        response = routes.my_subscription_card_action(request=SimpleNamespace(), csrf_token='x', db=db, user=_user(mine))
        assert response.status_code == 303 and response.headers['location'] == LINK
        assert sent['next_due_date'] == '2099-02-05' and sent['amount'] == Decimal('149.90')
        migration = db.query(OrganizationCardMigration).one()
        assert migration.old_subscription_id == sub.id and migration.status == 'aguardando'
        # Segundo clique reaproveita o mesmo link, sem novo checkout.
        monkeypatch.setattr(routes, 'create_sandbox_card_checkout_session', lambda **kw: pytest.fail('não deve gerar outro checkout'))
        again = routes.my_subscription_card_action(request=SimpleNamespace(), csrf_token='x', db=db, user=_user(mine))
        assert again.headers['location'] == LINK


def test_customer_can_replace_card_on_card_subscription(monkeypatch):
    _action_env(monkeypatch)
    monkeypatch.setattr(routes, 'create_sandbox_card_checkout_session', lambda **kw: ({'id': CHECKOUT, 'link': LINK}, None))
    with _db() as db:
        mine, sub = _org(db, 'minha', billing_type='CREDIT_CARD')
        response = routes.my_subscription_card_action(request=SimpleNamespace(), csrf_token='x', db=db, user=_user(mine))
        assert response.headers['location'] == LINK


def test_customer_card_checkout_failure_stays_on_page(monkeypatch):
    flashes = _action_env(monkeypatch)
    monkeypatch.setattr(routes, 'create_sandbox_card_checkout_session', lambda **kw: (None, 'HTTP 500'))
    with _db() as db:
        mine, _ = _org(db, 'minha')
        response = routes.my_subscription_card_action(request=SimpleNamespace(), csrf_token='x', db=db, user=_user(mine))
    assert response.headers['location'] == '/minha-assinatura' and flashes[-1][0] == 'error'


def test_customer_saved_card_switch_is_audited_as_customer(monkeypatch):
    import json
    from app.models import AuditLog
    flashes = _action_env(monkeypatch)
    monkeypatch.setattr(routes, 'client_ip', lambda request: '10.0.0.2')
    monkeypatch.setattr(routes, 'switch_sandbox_subscription_to_saved_card', lambda **kw: ({'id': 'sub_minha', 'customer': 'cus_minha', 'status': 'ACTIVE', 'billingType': 'CREDIT_CARD', 'nextDueDate': '2099-02-05'}, None))
    with _db() as db:
        mine, sub = _org(db, 'minha')
        db.add(OrganizationSubscriptionPayment(subscription_id=sub.id, asaas_payment_id='pay_card', status='CONFIRMED', billing_type='CREDIT_CARD', due_date=date(2026, 1, 10), card_last4='4242', card_tokenized=True))
        db.commit()
        routes.my_subscription_saved_card_action(request=SimpleNamespace(), csrf_token='x', db=db, user=_user(mine))
        db.refresh(sub)
        assert sub.billing_type == 'CREDIT_CARD' and flashes[-1][0] == 'success'
        events = [json.loads(row.metadata_json) for row in db.query(AuditLog).all()]
        assert any(item.get('por_cliente') is True for item in events)


def test_legacy_asaas_test_page_was_removed():
    paths = {route.path for route in routes.router.routes}
    assert not any(path.startswith('/gestao-financeira/assinatura') for path in paths)
    assert 'gestao-financeira/assinatura/cliente-asaas' not in open('app/templates/base.html').read()
