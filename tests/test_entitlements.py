import asyncio
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from jinja2 import ChoiceLoader, DictLoader
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.middleware import subscription_enforcement as mw
from app.models import Farm, Organization, OrganizationBilling, OrganizationSubscription, OrganizationSubscriptionPayment, SubscriptionNotice, User
from app.services import access_grants, billing_notices
from app.services import entitlements as ent
from app.services import subscription_plans as plans
from app.web import routes

TODAY = date(2026, 11, 14)


@pytest.fixture
def db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def _plan(db, **overrides):
    fields = dict(name='Essencial', description=None, price_monthly='79,90', price_quarterly='', price_semiannual='', price_yearly='', max_farms='1', max_users='2',
                  max_hectares='50', modules=['page.productive_unit', 'page.rainfall'], trial_days='', display_order='', is_featured=None)
    fields.update(overrides)
    plan = plans.create_plan(db, plans.parse_plan_form(**fields))
    plans.activate_plan(db, plan)
    db.flush()
    return plan


def _org(db, slug='fazenda', *, plan=None, overdue_days=None, with_subscription=True):
    org = Organization(name=slug.title(), slug=slug, is_active=True)
    db.add(org)
    db.flush()
    db.add(OrganizationBilling(organization_id=org.id, contact_name='P', contact_email=f'financeiro@{slug}.com', asaas_customer_id=f'cus_{slug}', asaas_environment='sandbox'))
    if with_subscription:
        sub = OrganizationSubscription(organization_id=org.id, plan_id=plan.id if plan else None, plan_name=plan.name if plan else 'Personalizado', amount=Decimal('79.90'), cycle='MONTHLY',
                                       billing_type='BOLETO', first_due_date=date(2026, 1, 1), status='criada', asaas_status='ACTIVE', asaas_subscription_id=f'sub_{slug}', environment='sandbox', is_current=True)
        db.add(sub)
        db.flush()
        if overdue_days is not None:
            db.add(OrganizationSubscriptionPayment(subscription_id=sub.id, asaas_payment_id=f'pay_{slug}', status='OVERDUE', amount=Decimal('79.90'),
                                                   due_date=TODAY - timedelta(days=overdue_days), invoice_url='https://sandbox.asaas.com/i/1'))
    db.commit()
    return org


@pytest.mark.parametrize('days, state', [(None, 'ok'), (1, 'aviso'), (10, 'aviso'), (11, 'restrito'), (40, 'restrito')])
def test_billing_state_follows_grace_period(db, days, state):
    org = _org(db, overdue_days=days)
    result = ent.compute_entitlements(db, org.id, TODAY)
    assert result.billing_state == state and result.read_only is (state == 'restrito')
    if days:
        assert result.days_overdue == days and result.regularize_until == TODAY - timedelta(days=days) + timedelta(days=10)


def test_grant_and_internal_org_are_never_restricted(db):
    org = _org(db, overdue_days=30)
    access_grants.create_grant(db, organization_id=org.id, reason='erro_interno', justification='Erro de cobrança da SiSFarm em novembro.', ends_on=TODAY + timedelta(days=5), today=TODAY, plan_id=None, user=None)
    db.commit()
    assert ent.compute_entitlements(db, org.id, TODAY).billing_state == 'liberado'
    sisfarm = _org(db, 'sisfarm', overdue_days=30)
    assert ent.compute_entitlements(db, sisfarm.id, TODAY).exempt


def test_org_without_subscription_or_custom_plan_has_no_limits(db):
    legacy = _org(db, 'legado', with_subscription=False)
    custom = _org(db, 'custom')
    for org in (legacy, custom):
        result = ent.compute_entitlements(db, org.id, TODAY)
        assert result.modules is None and result.max_farms is None and result.module_allowed('page.finance')


def test_plan_modules_lock_others(db):
    org = _org(db, plan=_plan(db))
    result = ent.compute_entitlements(db, org.id, TODAY)
    assert result.module_allowed('page.rainfall') and not result.module_allowed('page.finance')
    assert result.module_allowed('page.dashboard') and result.module_allowed(None)
    assert 'page.finance' in result.locked_modules


def test_farm_area_and_user_limits(db):
    org = _org(db, plan=_plan(db))
    result = ent.compute_entitlements(db, org.id, TODAY)
    assert ent.farm_limit_error(db, result, adding_farm=True, area_delta=Decimal('30')) is None
    assert 'ha' in ent.farm_limit_error(db, result, adding_farm=True, area_delta=Decimal('60'))
    db.add(Farm(name='Sede', location='MG', total_area=Decimal('40'), organization_id=org.id))
    db.commit()
    assert '1 fazenda' in ent.farm_limit_error(db, result, adding_farm=True, area_delta=Decimal('1'))
    assert ent.farm_limit_error(db, result, adding_farm=False, area_delta=Decimal('10')) is None
    assert 'ha' in ent.farm_limit_error(db, result, adding_farm=False, area_delta=Decimal('11'))
    assert ent.farm_limit_error(db, result, adding_farm=False, area_delta=Decimal('-5')) is None
    db.add_all([User(name='A', email='a@x.com', hashed_password='x', organization_id=org.id, is_active=True),
                User(name='B', email='b@x.com', hashed_password='x', organization_id=org.id, is_active=True)])
    db.commit()
    assert '2 usuário' in ent.user_limit_error(db, result)


# ---- middleware ----

class _Session(dict):
    pass


def _request(path, method='GET', referer=None):
    scope = {'type': 'http', 'method': method, 'path': path, 'headers': [(b'referer', referer.encode())] if referer else [], 'query_string': b'', 'server': ('app', 80), 'scheme': 'http', 'root_path': ''}
    from starlette.requests import Request
    request = Request(scope)
    request.scope['session'] = _Session(user_email='cliente@fazenda.com')
    return request


def _run(monkeypatch, entitlements, path, method='GET', referer=None):
    monkeypatch.setattr(mw, 'cached_entitlements', lambda db, org_id, today: entitlements)
    monkeypatch.setattr(mw, 'SessionLocal', lambda: SimpleNamespace(__enter__=None))

    class FakeDB:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def query(self, *a): return self
        def filter(self, *a): return self
        def first(self): return SimpleNamespace(organization_id=7)

    monkeypatch.setattr(mw, 'SessionLocal', FakeDB)
    monkeypatch.setattr(mw, 'is_super_admin_email', lambda email: False)
    request = _request(path, method, referer)

    async def call_next(req):
        return SimpleNamespace(status_code=200, headers={})

    response = asyncio.run(mw.SubscriptionEnforcementMiddleware(app=None).dispatch(request, call_next))
    return response, request.scope['session']


def test_read_only_blocks_writes_but_allows_reads_and_payment(monkeypatch):
    restricted = ent.Entitlements(organization_id=7, billing_state='restrito', days_overdue=12)
    assert _run(monkeypatch, restricted, '/fazendas')[0].status_code == 200
    response, session = _run(monkeypatch, restricted, '/fazendas', 'POST', referer='http://app/fazendas')
    assert response.status_code == 303 and response.headers['location'] == 'http://app/fazendas' and 'somente leitura' in session['flash']['message']
    assert _run(monkeypatch, restricted, '/minha-assinatura/cartao', 'POST')[0].status_code == 200
    assert _run(monkeypatch, restricted, '/meu-perfil/alterar-senha', 'POST')[0].status_code == 200
    assert _run(monkeypatch, restricted, '/insumos/estoque/exportar.pdf')[0].status_code == 200


def test_locked_module_redirects_to_upgrade_page(monkeypatch):
    plan_ent = ent.Entitlements(organization_id=7, modules=frozenset({'page.productive_unit'}))
    response, _ = _run(monkeypatch, plan_ent, '/gestao-financeira/contas')
    assert response.status_code == 303 and response.headers['location'] == '/planos-disponiveis?modulo=page.finance'
    assert _run(monkeypatch, plan_ent, '/fazendas')[0].status_code == 200
    assert _run(monkeypatch, plan_ent, '/minha-assinatura')[0].status_code == 200
    assert _run(monkeypatch, plan_ent, '/dashboard')[0].status_code == 200


def test_super_admin_and_exempt_pass(monkeypatch):
    response, _ = _run(monkeypatch, ent.Entitlements(organization_id=7, exempt=True), '/fazendas', 'POST')
    assert response.status_code == 200


# ---- e-mails ----

@pytest.mark.parametrize('days, sent, expected', [
    (0, set(), ([], [])),
    (1, set(), (['atraso'], [])),
    (5, {'atraso'}, ([], [])),
    (9, {'atraso'}, (['pre_restricao'], [])),
    (9, set(), (['pre_restricao'], ['atraso'])),
    (10, {'atraso', 'pre_restricao'}, ([], [])),
    (25, set(), (['atraso'], ['pre_restricao'])),
])
def test_notice_schedule(days, sent, expected):
    assert billing_notices.notices_due(days, 10, sent) == expected


def test_notices_are_sent_once_and_respect_grants(db):
    org = _org(db, overdue_days=1)
    outbox = []
    send = lambda to, subject, body, ctx: outbox.append((to, subject, body))
    assert billing_notices.process_organization(db, org, TODAY, send=send) == ['atraso']
    assert billing_notices.process_organization(db, org, TODAY, send=send) == []
    assert outbox[0][0] == 'financeiro@fazenda.com' and 'https://sandbox.asaas.com/i/1' in outbox[0][2] and '23/11/2026' in outbox[0][2]
    assert billing_notices.process_organization(db, org, TODAY + timedelta(days=8), send=send) == ['pre_restricao']
    assert 'modo somente leitura' in outbox[-1][2]
    other = _org(db, 'liberada', overdue_days=3)
    access_grants.create_grant(db, organization_id=other.id, reason='bonus_cortesia', justification='Mês de bônus combinado com o cliente.', ends_on=TODAY + timedelta(days=10), today=TODAY, plan_id=None, user=None)
    db.commit()
    assert billing_notices.process_organization(db, other, TODAY, send=send) == []


def test_failed_email_is_retried_later(db):
    org = _org(db, overdue_days=2)
    def boom(*args):
        raise RuntimeError('SMTP fora do ar')
    assert billing_notices.process_organization(db, org, TODAY, send=boom) == []
    assert db.query(SubscriptionNotice).count() == 0
    assert billing_notices.process_organization(db, org, TODAY, send=lambda *a: None) == ['atraso']


# ---- rotas e telas ----

def test_create_farm_route_respects_limit(db, monkeypatch):
    org = _org(db, plan=_plan(db))
    db.add(Farm(name='Sede', location='MG', total_area=Decimal('40'), organization_id=org.id))
    db.commit()
    monkeypatch.setattr(routes, 'validate_csrf', lambda *a: None)
    flashes = []
    monkeypatch.setattr(routes, '_flash', lambda request, kind, message: flashes.append((kind, message)))
    monkeypatch.setattr(routes, 'today_in_app_timezone', lambda: TODAY)
    monkeypatch.setattr(routes, 'create_farm', lambda *a, **k: pytest.fail('não deve cadastrar'))
    user = SimpleNamespace(id=3, email='gestor@fazenda.com', organization_id=org.id)
    response = asyncio.run(routes.create_farm_action(request=SimpleNamespace(), csrf_token='x', name='Nova', location='MG', total_area=5.0, car_registration_number=None, ccir_number=None,
                                                     itr_registration_number=None, property_registration_number=None, registry_office=None, boundary_geojson=None, boundary_geojson_file=None,
                                                     notes=None, redirect_to=None, db=db, user=user))
    assert response.headers['location'] == '/fazendas' and 'até 1 fazenda' in flashes[-1][1]


def _render(template, context):
    env = routes.templates.env.overlay(loader=ChoiceLoader([DictLoader({'base.html': '{% block content %}{% endblock %}'}), routes.templates.env.loader]))
    return env.get_template(template).render(**context, request=None, csrf_token='t')


def test_upgrade_page_highlights_plans_with_module(db, monkeypatch):
    basic = _plan(db)
    full = _plan(db, name='Gestão Completa', modules=['page.productive_unit', 'page.rainfall', 'page.finance'], max_farms='', max_users='', max_hectares='')
    org = _org(db, plan=basic)
    monkeypatch.setattr(routes, '_base_context', lambda *a, **k: k)
    monkeypatch.setattr(routes, '_repository', lambda _db: None)
    monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: (name, context))
    monkeypatch.setattr(routes, 'permission_codes_for_user', lambda db, user: frozenset())
    user = SimpleNamespace(id=3, email='gestor@fazenda.com', organization_id=org.id)
    _, ctx = routes.plan_upgrade_page(request=SimpleNamespace(), modulo='page.finance', db=db, user=user, csrf_token='t')
    html = _render('plan_upgrade.html', ctx)
    assert ctx['plans_catalog'][0].id == full.id and ctx['current_plan_id'] == basic.id
    assert 'Gestão financeira' in html and 'Inclui Gestão financeira' in html and 'Quero o plano Gestão Completa' in html and 'Seu plano' in html


def test_upgrade_request_is_audited(db, monkeypatch):
    import json
    from app.models import AuditLog
    plan = _plan(db)
    org = _org(db)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *a: None)
    monkeypatch.setattr(routes, '_flash', lambda *a: None)
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(support_email='vendas@sisfarm.test'))
    mails = []
    monkeypatch.setattr(routes, 'send_plain_email', lambda *a: mails.append(a))
    user = SimpleNamespace(id=3, email='gestor@fazenda.com', organization_id=org.id, organization=org)
    routes.plan_upgrade_request_action(request=SimpleNamespace(), csrf_token='x', plan_id=str(plan.id), modulo='page.finance', db=db, user=user)
    event = json.loads(db.query(AuditLog).one().metadata_json)
    assert event['acao'] == 'solicitar_mudanca' and event['modulo'] == 'page.finance'
    assert mails and mails[0][0] == 'vendas@sisfarm.test' and 'Fazenda' in mails[0][2]


def test_banner_and_menu_lock_render(db):
    from markupsafe import Markup
    restricted = ent.Entitlements(organization_id=7, billing_state='restrito', days_overdue=12, overdue_amount=Decimal('79.90'), modules=frozenset({'page.productive_unit'}))
    html = routes.templates.env.get_template('partials/billing_banner.html').render(plan_entitlements=restricted, page='dashboard', menu_visibility={'finance': True})
    assert 'Modo somente leitura' in html and 'Regularizar pagamento' in html
    warn = ent.Entitlements(organization_id=7, billing_state='aviso', days_overdue=3, restrict_on=date(2026, 11, 22))
    html = routes.templates.env.get_template('partials/billing_banner.html').render(plan_entitlements=warn, page='dashboard', menu_visibility={})
    assert '21/11/2026' in html and 'responsável financeiro' in html
    tpl = routes.templates.env.from_string("{{ module_href('/gestao-financeira') }}|{{ module_lock('/gestao-financeira') }}|{{ module_href('/fazendas') }}|{{ module_lock('/minha-assinatura') }}")
    rendered = tpl.render(plan_entitlements=restricted)
    href, lock, farms, own = rendered.split('|')
    assert href == '/planos-disponiveis?modulo=page.finance' and '<svg' in lock and farms == '/fazendas' and own == ''


# ---- API JSON e reativacao de usuario ----

def _api_request(path, method='GET'):
    from starlette.requests import Request
    return Request({'type': 'http', 'method': method, 'path': path, 'headers': [], 'query_string': b'', 'server': ('app', 80), 'scheme': 'http', 'root_path': ''})


def test_api_respects_plan_modules_and_read_only(db, monkeypatch):
    from fastapi import HTTPException
    from app.routers import api
    org = _org(db, plan=_plan(db), overdue_days=20)
    user = SimpleNamespace(email='gestor@fazenda.com', organization_id=org.id)
    monkeypatch.setattr(api, 'today_in_app_timezone', lambda: TODAY)
    api.enforce_plan(_api_request('/api/v1/plots'), db, user)  # setores fazem parte do plano
    api.enforce_plan(_api_request('/api/v1/dashboard'), db, user)
    with pytest.raises(HTTPException) as locked:
        api.enforce_plan(_api_request('/api/v1/harvests'), db, user)
    assert locked.value.status_code == 403
    with pytest.raises(HTTPException) as read_only:
        api.enforce_plan(_api_request('/api/v1/plots', 'POST'), db, user)
    assert read_only.value.status_code == 402
    monkeypatch.setattr(api, 'is_super_admin_email', lambda email: True)
    api.enforce_plan(_api_request('/api/v1/harvests'), db, user)


def test_api_router_applies_guard_to_every_route():
    from app.routers import api
    assert any(dep.dependency is api.enforce_plan for dep in api.router.dependencies)


def test_reactivating_user_respects_limit(db, monkeypatch):
    org = _org(db, plan=_plan(db))
    admin = User(name='Admin', email='admin@fazenda.com', hashed_password='x', organization_id=org.id, is_active=True)
    second = User(name='Ativo', email='ativo@fazenda.com', hashed_password='x', organization_id=org.id, is_active=True)
    inactive = User(name='Inativo', email='inativo@fazenda.com', hashed_password='x', organization_id=org.id, is_active=False)
    db.add_all([admin, second, inactive])
    db.commit()
    monkeypatch.setattr(routes, '_require_admin', lambda *a: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *a: None)
    monkeypatch.setattr(routes, 'today_in_app_timezone', lambda: TODAY)
    flashes = []
    monkeypatch.setattr(routes, '_flash', lambda request, kind, message: flashes.append((kind, message)))
    monkeypatch.setattr(routes, 'update_user', lambda *a, **k: pytest.fail('não deve reativar'))

    async def form():
        from starlette.datastructures import FormData
        return FormData([])

    request = SimpleNamespace(form=form)
    response = asyncio.run(routes.update_user_action(user_id=inactive.id, request=request, csrf_token='x', name='Inativo', email='inativo@fazenda.com', password=None,
                                                     is_active='on', is_admin=None, is_two_factor_enabled=None, db=db, user=admin))
    assert response.status_code == 303 and 'até 2 usuário' in flashes[-1][1]
    db.refresh(inactive)
    assert inactive.is_active is False


# ---- mudanca de plano pelo cliente ----

def _self_service_env(db, monkeypatch, *, codes=None):
    from app.core.permissions_catalog import PAGE_FINANCE, WRITE_FINANCE
    monkeypatch.setattr(routes, 'validate_csrf', lambda *a: None)
    monkeypatch.setattr(routes, 'permission_codes_for_user', lambda db, user: codes if codes is not None else frozenset({PAGE_FINANCE, WRITE_FINANCE}))
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(asaas_api_base_url='https://api-sandbox.asaas.com', asaas_api_key='k', support_email=None))
    flashes = []
    monkeypatch.setattr(routes, '_flash', lambda request, kind, message: flashes.append((kind, message)))
    return flashes


def test_customer_changes_plan_with_catalog_price(db, monkeypatch):
    import json
    from app.models import AuditLog
    basic = _plan(db)
    pro = _plan(db, name='Profissional', price_monthly='149,90', modules=['page.productive_unit', 'page.rainfall', 'page.finance'], max_farms='3', max_users='', max_hectares='300')
    org = _org(db, plan=basic)
    sub = db.query(OrganizationSubscription).one()
    sub.next_due_date = date(2099, 1, 10)
    db.commit()
    flashes = _self_service_env(db, monkeypatch)
    sent = {}
    def update(**kw):
        sent.update(kw['payload'])
        return {'id': sub.asaas_subscription_id, 'customer': 'cus_fazenda', 'value': kw['payload']['value'], 'cycle': kw['payload']['cycle'], 'description': kw['payload']['description'], 'billingType': 'BOLETO', 'nextDueDate': '2099-01-10'}, None
    monkeypatch.setattr(routes, 'update_sandbox_subscription', update)
    user = SimpleNamespace(id=3, email='gestor@fazenda.com', organization_id=org.id)
    response = routes.self_service_plan_change_action(request=SimpleNamespace(), csrf_token='x', plan_id=str(pro.id), cycle='MONTHLY', db=db, user=user)
    db.refresh(sub)
    assert response.headers['location'] == '/minha-assinatura' and flashes[-1][0] == 'success' and '10/01/2099' in flashes[-1][1]
    assert sent == {'description': 'Profissional', 'value': 149.9, 'cycle': 'MONTHLY', 'billingType': 'BOLETO'}
    assert (sub.plan_id, sub.amount, sub.status) == (pro.id, Decimal('149.90'), 'criada')
    event = json.loads(db.query(AuditLog).filter(AuditLog.event_type == 'comercial.assinatura.mudar_plano_cliente').one().metadata_json)
    assert event['de'] == 'Essencial' and event['para'] == 'Profissional'
    assert ent.compute_entitlements(db, org.id, TODAY).module_allowed('page.finance')


def test_downgrade_blocked_when_usage_exceeds_limits(db, monkeypatch):
    big = _plan(db, name='Grande', max_farms='', max_users='', max_hectares='')
    small = _plan(db, name='Pequeno', max_farms='1', max_hectares='50')
    org = _org(db, plan=big)
    db.add_all([Farm(name='A', location='MG', total_area=Decimal('40'), organization_id=org.id), Farm(name='B', location='MG', total_area=Decimal('30'), organization_id=org.id)])
    db.commit()
    flashes = _self_service_env(db, monkeypatch)
    monkeypatch.setattr(routes, 'update_sandbox_subscription', lambda **kw: pytest.fail('não deve chamar o Asaas'))
    routes.self_service_plan_change_action(request=SimpleNamespace(), csrf_token='x', plan_id=str(small.id), cycle='MONTHLY', db=db, user=SimpleNamespace(id=3, email='g@f.com', organization_id=org.id))
    assert flashes[-1][0] == 'error' and '2 fazendas' in flashes[-1][1] and '70,00 ha' in flashes[-1][1]


def test_plan_change_requires_finance_write(db, monkeypatch):
    basic, pro = _plan(db), _plan(db, name='Pro')
    org = _org(db, plan=basic)
    flashes = _self_service_env(db, monkeypatch, codes=frozenset())
    monkeypatch.setattr(routes, 'update_sandbox_subscription', lambda **kw: pytest.fail('não deve chamar o Asaas'))
    routes.self_service_plan_change_action(request=SimpleNamespace(), csrf_token='x', plan_id=str(pro.id), cycle='MONTHLY', db=db, user=SimpleNamespace(id=3, email='g@f.com', organization_id=org.id))
    assert 'responsável financeiro' in flashes[-1][1]


def test_change_dialog_shows_gains_losses_prices_and_date(db, monkeypatch):
    basic = _plan(db)
    pro = _plan(db, name='Profissional', price_monthly='149,90', modules=['page.productive_unit', 'page.finance'], max_farms='3', max_users='', max_hectares='300')
    org = _org(db, plan=basic)
    sub = db.query(OrganizationSubscription).one()
    sub.next_due_date = date(2099, 1, 10)
    db.commit()
    _self_service_env(db, monkeypatch)
    monkeypatch.setattr(routes, '_base_context', lambda *a, **k: k)
    monkeypatch.setattr(routes, '_repository', lambda _db: None)
    monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: (name, context))
    _, ctx = routes.plan_upgrade_page(request=SimpleNamespace(), modulo=None, db=db, user=SimpleNamespace(id=3, email='g@f.com', organization_id=org.id), csrf_token='t')
    html = _render('plan_upgrade.html', ctx)
    dialog = html[html.index(f'id="change-plan-{pro.id}"'):]
    dialog = dialog[:dialog.index('</dialog>')]
    assert 'Mudar para Profissional' in html and 'Alteração de plano' in dialog
    assert 'R$ 79,90 / mês' in dialog and 'data-price="149.90"' in dialog
    assert 'Você passa a ter' in dialog and 'Gestão financeira' in dialog
    assert 'Você deixa de ter' in dialog and 'Pluviometria' in dialog
    assert '10/01/2099' in dialog and '1 → 3' in dialog


# ---- reducao de plano (downgrade) ----

def _downgrade_setup(db, monkeypatch, *, paid=True, open_charge=False, changed_at=None):
    from datetime import datetime
    pro = _plan(db, name='Profissional', price_monthly='149,90', modules=['page.productive_unit', 'page.rainfall', 'page.finance'], max_farms='', max_users='', max_hectares='')
    basic = _plan(db, max_farms='', max_users='', max_hectares='')
    org = _org(db, plan=pro)
    sub = db.query(OrganizationSubscription).one()
    sub.amount, sub.next_due_date, sub.plan_changed_at = Decimal('149.90'), date(2099, 3, 10), changed_at
    if paid:
        db.add(OrganizationSubscriptionPayment(subscription_id=sub.id, asaas_payment_id='pay_ok', status='RECEIVED', amount=Decimal('149.90'), due_date=date(2026, 11, 10), paid_date=date(2026, 11, 10)))
    if open_charge:
        db.add(OrganizationSubscriptionPayment(subscription_id=sub.id, asaas_payment_id='pay_open', status='PENDING', amount=Decimal('149.90'), due_date=date(2099, 3, 10)))
    db.commit()
    flashes = _self_service_env(db, monkeypatch)
    sent = []
    def update(**kw):
        sent.append(kw['payload'])
        p = kw['payload']
        return {'id': sub.asaas_subscription_id, 'customer': 'cus_fazenda', 'value': p.get('value', 149.9), 'cycle': p.get('cycle', 'MONTHLY'), 'description': p.get('description'), 'billingType': 'BOLETO', 'nextDueDate': '2099-03-10'}, None
    monkeypatch.setattr(routes, 'update_sandbox_subscription', update)
    user = SimpleNamespace(id=3, email='g@f.com', organization_id=org.id)
    return org, sub, basic, pro, user, flashes, sent


def _ask(db, user, plan):
    return routes.self_service_plan_change_action(request=SimpleNamespace(), csrf_token='x', plan_id=str(plan.id), cycle='MONTHLY', db=db, user=user)


@pytest.mark.parametrize('kwargs, message', [
    (dict(open_charge=True), 'fatura em aberto'),
    (dict(paid=False), 'Aguarde a confirmação'),
])
def test_downgrade_requires_paid_invoice(db, monkeypatch, kwargs, message):
    org, sub, basic, pro, user, flashes, sent = _downgrade_setup(db, monkeypatch, **kwargs)
    _ask(db, user, basic)
    assert message in flashes[-1][1] and sent == [] and sub.pending_plan_id is None


def test_downgrade_requires_payment_after_last_upgrade(db, monkeypatch):
    from datetime import datetime
    org, sub, basic, pro, user, flashes, sent = _downgrade_setup(db, monkeypatch, changed_at=datetime(2026, 11, 12, 9, 0))
    _ask(db, user, basic)
    assert 'Aguarde a confirmação' in flashes[-1][1] and sent == []


def test_downgrade_is_scheduled_for_end_of_paid_cycle_and_can_be_cancelled(db, monkeypatch):
    org, sub, basic, pro, user, flashes, sent = _downgrade_setup(db, monkeypatch)
    _ask(db, user, basic)
    db.refresh(sub)
    assert sent[-1]['description'] == pro.name and sent[-1]['value'] == 79.9
    assert (sub.plan_id, sub.pending_plan_id, sub.pending_effective_on) == (pro.id, basic.id, date(2099, 3, 10))
    assert 'agendada para 10/03/2099' in flashes[-1][1]
    ent.invalidate_entitlements()
    assert ent.compute_entitlements(db, org.id, TODAY).module_allowed('page.finance')
    assert not ent.compute_entitlements(db, org.id, date(2099, 3, 10)).module_allowed('page.finance')
    _ask(db, user, basic)
    assert 'agendada' in flashes[-1][1] and 'Cancele' in flashes[-1][1]
    routes.cancel_scheduled_plan_change_action(request=SimpleNamespace(), csrf_token='x', db=db, user=user)
    db.refresh(sub)
    assert sub.pending_plan_id is None and sent[-1]['value'] == 149.9 and 'cancelada' in flashes[-1][1]


def test_scheduled_downgrade_takes_effect_on_due_date(db, monkeypatch):
    from datetime import datetime
    from app.services.plan_changes import apply_due_plan_changes
    org, sub, basic, pro, user, flashes, sent = _downgrade_setup(db, monkeypatch)
    _ask(db, user, basic)
    renamed = []
    assert apply_due_plan_changes(db, date(2099, 3, 9), datetime(2099, 3, 9), lambda s, n: renamed.append(n) or True) == []
    assert apply_due_plan_changes(db, date(2099, 3, 10), datetime(2099, 3, 10), lambda s, n: renamed.append(n) or True) == [org.id]
    assert (sub.plan_id, sub.plan_name, sub.pending_plan_id) == (basic.id, basic.name, None) and renamed == [basic.name]


def test_upgrade_stays_immediate_and_marks_change(db, monkeypatch):
    basic = _plan(db, max_farms='', max_users='', max_hectares='')
    pro = _plan(db, name='Profissional', price_monthly='149,90', modules=['page.productive_unit', 'page.rainfall', 'page.finance'], max_farms='', max_users='', max_hectares='')
    org = _org(db, plan=basic)
    sub = db.query(OrganizationSubscription).one()
    sub.next_due_date = date(2099, 1, 10)
    db.commit()
    _self_service_env(db, monkeypatch)
    monkeypatch.setattr(routes, 'update_sandbox_subscription', lambda **kw: ({'id': sub.asaas_subscription_id, 'customer': 'cus_fazenda', 'value': 149.9, 'cycle': 'MONTHLY', 'description': 'Profissional', 'billingType': 'BOLETO'}, None))
    _ask(db, SimpleNamespace(id=3, email='g@f.com', organization_id=org.id), pro)
    db.refresh(sub)
    assert sub.plan_id == pro.id and sub.plan_changed_at is not None and sub.pending_plan_id is None


def test_session_flyout_shows_plan_name_clickable_only_with_permission():
    from jinja2 import Environment
    html = open('app/templates/base.html').read()
    start = html.index('{% if plan_entitlements and plan_entitlements.plan_name %}')
    end = html.index('<a href="/logout"', start)
    tpl = Environment().from_string(html[start:end])
    plan = SimpleNamespace(plan_name='Essencial')
    can = tpl.render(plan_entitlements=plan, permission_codes=frozenset({'write.finance'}))
    cannot = tpl.render(plan_entitlements=plan, permission_codes=frozenset({'page.finance'}))
    assert 'href="/minha-assinatura"' in can and 'Essencial' in can
    assert 'href=' not in cannot and 'Essencial' in cannot and 'Gerenciar plano' not in html
