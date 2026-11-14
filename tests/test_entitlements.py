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
