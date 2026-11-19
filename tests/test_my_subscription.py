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


def test_page_is_compact_with_plan_and_payment_buttons(monkeypatch):
    _page_env(monkeypatch)
    with _db() as db:
        mine, _ = _org(db, 'minha')
        _, context = routes.my_subscription_page(request=SimpleNamespace(), db=db, user=_user(mine), csrf_token='t')
        html = _render(context)
    assert 'href="/planos-disponiveis"' in html and 'Alterar plano' in html
    assert 'data-dialog-open="payment-method-dialog"' in html and '<dialog id="payment-method-dialog"' in html
    assert 'O pagamento é feito na página segura' not in html and '<details' in html


def test_landing_page_for_visitors_and_redirect_for_logged_users(monkeypatch):
    from types import SimpleNamespace as NS
    monkeypatch.setattr(routes, 'get_settings', lambda: NS(support_email='contato@sisfarm.test'))
    logged = routes.home(request=NS(session={'user_email': 'a@b.com'}), db=None)
    assert logged.headers['location'] == '/dashboard'
    with _db() as db:
        plan = plans.create_plan(db, plans.parse_plan_form(name='Essencial', description='Para começar', price_monthly='79,90', price_quarterly='', price_semiannual='', price_yearly='',
                                                           max_farms='1', max_users='', max_hectares='100', modules=['page.map'], trial_days='', display_order='', is_featured='1'))
        plans.activate_plan(db, plan)
        db.commit()
        monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: (name, context))
        name, ctx = routes.home(request=NS(session={}), db=db)
        html = routes.templates.env.get_template(name).render(**ctx)
    assert name == 'landing.html'
    assert 'Sua fazenda de café' in html and 'R$ 79,90' in html and 'Mais escolhido' in html
    assert 'mailto:contato@sisfarm.test' in html and 'href="/login"' in html
    assert 'Teste grátis por 14 dias' in html and 'href="/cadastro"' in html and 'Depoimento' not in html
    assert 'href="/termos"' in html and 'O que cada plano inclui' in html
    assert 'Incluído no Essencial' in html and 'Não incluído no Essencial' in html and 'Contas a receber' in html
    assert '1 de 15 módulos' in html


def test_landing_page_shows_trial_from_catalog(monkeypatch):
    from types import SimpleNamespace as NS
    monkeypatch.setattr(routes, 'get_settings', lambda: NS(support_email=''))
    with _db() as db:
        plan = plans.create_plan(db, plans.parse_plan_form(name='Profissional', description='', price_monthly='149,90', price_quarterly='', price_semiannual='', price_yearly='799,00',
                                                           max_farms='3', max_users='', max_hectares='', modules=[code for code, _ in plans.PLAN_MODULES], trial_days='14', display_order='', is_featured='1'))
        plans.activate_plan(db, plan)
        db.commit()
        monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: (name, context))
        name, ctx = routes.home(request=NS(session={}), db=db)
        html = routes.templates.env.get_template(name).render(**ctx)
    assert 'Teste grátis por 14 dias' in html and 'O SiSFarm oferece teste gratuito?' in html
    assert 'Todos os 15 módulos' in html and 'data-cycle-btn="YEARLY"' in html and 'R$ 799,00' in html


def test_seo_robots_sitemap_and_noindex_outside_landing():
    import asyncio
    from types import SimpleNamespace
    from starlette.responses import Response as StarletteResponse
    from app.main import search_engine_indexing
    from app.web.routes import robots_txt, sitemap_xml

    assert "Sitemap: https://" in robots_txt().body.decode()
    assert "<loc>https://" in sitemap_xml().body.decode()

    async def call_next(_request):
        return StarletteResponse("ok")

    def headers_for(path):
        request = SimpleNamespace(url=SimpleNamespace(path=path))
        return asyncio.run(search_engine_indexing(request, call_next)).headers

    assert headers_for("/login").get("X-Robots-Tag") == "noindex, nofollow"
    assert "X-Robots-Tag" not in headers_for("/")
    assert "X-Robots-Tag" not in headers_for("/static/images/og-sisfarm.jpg")


def test_landing_structured_data_has_organization_and_offers():
    import json
    from types import SimpleNamespace
    from decimal import Decimal
    from app.web.routes import _landing_structured_data

    plans = [SimpleNamespace(price_monthly=Decimal("99.90")), SimpleNamespace(price_monthly=Decimal("299")), SimpleNamespace(price_monthly=None)]
    data = json.loads(_landing_structured_data("https://app.sisfarm.com.br", plans))
    types = {item["@type"]: item for item in data["@graph"]}
    assert types["Organization"]["logo"].startswith("https://")
    offers = types["SoftwareApplication"]["offers"]
    assert offers["lowPrice"] == "99.90" and offers["highPrice"] == "299.00" and offers["priceCurrency"] == "BRL"


def test_trial_checkout_shows_payment_action_only_before_payment(monkeypatch):
    from datetime import timedelta
    from app.models import OrganizationAccessGrant

    _page_env(monkeypatch)
    with _db() as db:
        org = Organization(name='Fazenda Teste', slug='teste-checkout', is_active=True)
        db.add(org)
        db.flush()
        today = routes.today_in_app_timezone()
        grant = OrganizationAccessGrant(
            organization_id=org.id, reason='teste_gratis', justification='Período de avaliação da organização.',
            starts_on=today - timedelta(days=1), ends_on=today + timedelta(days=10),
        )
        migration = OrganizationCardMigration(
            organization_id=org.id, status='aguardando', plan_name='SiSFarm Essencial', amount=Decimal('99'),
            cycle='MONTHLY', checkout_link='https://sandbox.asaas.com/checkoutSession/show/teste',
        )
        db.add_all([grant, migration])
        db.commit()
        user = _user(org)

        _, context = routes.my_subscription_page(SimpleNamespace(), db, user, 't')
        html = _render(context)
        assert 'Teste grátis ativo até' in html and 'Acesso temporário liberado' in html
        assert 'data-status="pending"' in html and 'SiSFarm Essencial' in html
        assert 'Concluir pagamento' in html and migration.checkout_link in html

        migration.status = 'pago'
        db.commit()
        _, context = routes.my_subscription_page(SimpleNamespace(), db, user, 't')
        html = _render(context)
        assert 'data-status="confirming"' in html and 'Aguardando confirmação' in html
        assert 'Concluir pagamento' not in html and migration.checkout_link not in html


def test_confirmed_current_payment_marks_plan_active_even_with_trial(monkeypatch):
    from datetime import timedelta
    from app.models import OrganizationAccessGrant

    _page_env(monkeypatch)
    with _db() as db:
        org, _ = _org(db, 'ativa')
        today = routes.today_in_app_timezone()
        db.add(OrganizationAccessGrant(
            organization_id=org.id, reason='teste_gratis', justification='Período de avaliação da organização.',
            starts_on=today - timedelta(days=1), ends_on=today + timedelta(days=10),
        ))
        db.commit()
        _, context = routes.my_subscription_page(SimpleNamespace(), db, _user(org), 't')
        html = _render(context)
        assert 'Teste grátis ativo até' in html and 'data-status="active"' in html
        assert '3. Assinatura ativa' in html and 'Concluir pagamento' not in html
