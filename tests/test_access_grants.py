import json
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from jinja2 import ChoiceLoader, DictLoader
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import AuditLog, Organization, OrganizationAccessGrant, OrganizationBilling, OrganizationSubscription, OrganizationSubscriptionPayment
from app.services import access_grants as grants
from app.services import subscription_plans as plans
from app.services.subscription_portfolio import build_subscription_portfolio
from app.web import routes

TODAY = date(2026, 11, 14)
ADMIN = SimpleNamespace(id=1, email='equipe@sisfarm.test')
JUSTIFICATION = 'Cobrança gerada em duplicidade pela SiSFarm; mês de bônus combinado.'


def _db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return Session(engine)


def _org(db, slug='fazenda', *, with_subscription=True, overdue=False):
    org = Organization(name=slug.title(), slug=slug, is_active=True)
    db.add(org)
    db.flush()
    db.add(OrganizationBilling(organization_id=org.id, contact_name='P', contact_email=f'{slug}@example.com', asaas_customer_id=f'cus_{slug}', asaas_environment='sandbox'))
    sub = None
    if with_subscription:
        sub = OrganizationSubscription(organization_id=org.id, plan_name='Plano', amount=Decimal('100'), cycle='MONTHLY', billing_type='BOLETO', first_due_date=date(2026, 1, 1),
                                       status='criada', asaas_status='ACTIVE', asaas_subscription_id=f'sub_{slug}', environment='sandbox', is_current=True)
        db.add(sub)
        db.flush()
        if overdue:
            db.add(OrganizationSubscriptionPayment(subscription_id=sub.id, asaas_payment_id=f'pay_{slug}', status='OVERDUE', amount=Decimal('100'), due_date=date(2026, 10, 1)))
    db.commit()
    return org, sub


def _plan(db):
    plan = plans.create_plan(db, plans.parse_plan_form(name='Profissional', description=None, price_monthly='100,00', price_quarterly='', price_semiannual='', price_yearly='',
                                                       max_farms='', max_users='', max_hectares='', modules=[], trial_days='', display_order='', is_featured=None))
    plans.activate_plan(db, plan)
    db.commit()
    return plan


@pytest.mark.parametrize('kwargs, message', [
    (dict(reason='qualquer'), 'motivo'),
    (dict(justification='curta'), 'justificativa'),
    (dict(ends_on=TODAY - timedelta(days=1)), 'data de fim'),
    (dict(ends_on=TODAY + timedelta(days=91)), '90 dias'),
    (dict(ends_on=None), 'data de fim'),
])
def test_grant_validation(kwargs, message):
    with _db() as db:
        org, _ = _org(db)
        values = dict(organization_id=org.id, reason='erro_interno', justification=JUSTIFICATION, ends_on=TODAY + timedelta(days=30), today=TODAY, plan_id=None, user=ADMIN)
        values.update(kwargs)
        with pytest.raises(grants.GrantValidationError, match=f'(?i){message}'):
            grants.create_grant(db, **values)


def test_grant_without_subscription_requires_plan_and_only_one_active():
    with _db() as db:
        org, _ = _org(db, with_subscription=False)
        with pytest.raises(grants.GrantValidationError, match='plano'):
            grants.create_grant(db, organization_id=org.id, reason='instabilidade_asaas', justification=JUSTIFICATION, ends_on=TODAY + timedelta(days=7), today=TODAY, plan_id=None, user=ADMIN)
        plan = _plan(db)
        grant = grants.create_grant(db, organization_id=org.id, reason='instabilidade_asaas', justification=JUSTIFICATION, ends_on=TODAY + timedelta(days=7), today=TODAY, plan_id=plan.id, user=ADMIN)
        assert grant.plan_id == plan.id and grant.created_by_email == ADMIN.email
        with pytest.raises(grants.GrantValidationError, match='Já existe'):
            grants.create_grant(db, organization_id=org.id, reason='outro', justification=JUSTIFICATION, ends_on=TODAY + timedelta(days=7), today=TODAY, plan_id=plan.id, user=ADMIN)


def test_access_status_follows_grant_period_and_revocation():
    with _db() as db:
        org, sub = _org(db)
        sub.asaas_status = 'INACTIVE'
        db.commit()
        assert grants.organization_access(db, org.id, TODAY).has_access is False
        grant = grants.create_grant(db, organization_id=org.id, reason='bonus_cortesia', justification=JUSTIFICATION, ends_on=TODAY + timedelta(days=10), today=TODAY, plan_id=None, user=ADMIN)
        status = grants.organization_access(db, org.id, TODAY)
        assert status.has_access and status.manual and status.grant.id == grant.id
        assert grants.organization_access(db, org.id, TODAY + timedelta(days=11)).has_access is False
        grants.revoke_grant(grant, reason='pagamento regularizado', user=ADMIN)
        assert grants.organization_access(db, org.id, TODAY).has_access is False
        with pytest.raises(grants.GrantValidationError):
            grants.revoke_grant(grant, reason='de novo', user=ADMIN)


def test_extension_rules():
    with _db() as db:
        org, _ = _org(db)
        grant = grants.create_grant(db, organization_id=org.id, reason='negociacao_comercial', justification=JUSTIFICATION, ends_on=TODAY + timedelta(days=10), today=TODAY, plan_id=None, user=ADMIN)
        with pytest.raises(grants.GrantValidationError, match='posterior'):
            grants.extend_grant(grant, ends_on=TODAY + timedelta(days=5), justification=JUSTIFICATION, today=TODAY)
        with pytest.raises(grants.GrantValidationError, match='90 dias'):
            grants.extend_grant(grant, ends_on=TODAY + timedelta(days=120), justification=JUSTIFICATION, today=TODAY)
        grants.extend_grant(grant, ends_on=TODAY + timedelta(days=40), justification='Asaas ainda sem resposta do suporte técnico.', today=TODAY)
        assert grant.ends_on == TODAY + timedelta(days=40) and grant.extension_count == 1 and grant.justification == JUSTIFICATION


def test_portfolio_shows_manual_grant_without_hiding_debt_or_adding_revenue():
    with _db() as db:
        org, sub = _org(db, overdue=True)
        before = build_subscription_portfolio(db, [org], TODAY)
        assert before.rows[0].situation == 'inadimplente'
        grants.create_grant(db, organization_id=org.id, reason='erro_interno', justification=JUSTIFICATION, ends_on=TODAY + timedelta(days=2), today=TODAY, plan_id=None, user=ADMIN)
        db.commit()
        after = build_subscription_portfolio(db, [org], TODAY)
        row = after.rows[0]
        assert row.situation == 'liberada' and row.situation_label == 'Liberada manualmente' and row.grant_expiring
        assert after.overdue_total == before.overdue_total == Decimal('100')
        assert after.monthly_recurring_revenue == before.monthly_recurring_revenue and after.manual_grant_count == 1


def _route_env(monkeypatch):
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, 'today_in_app_timezone', lambda: TODAY)
    flashes = []
    monkeypatch.setattr(routes, '_flash', lambda request, kind, message: flashes.append((kind, message)))
    return flashes


def test_routes_create_extend_and_revoke_with_audit(monkeypatch):
    flashes = _route_env(monkeypatch)
    with _db() as db:
        org, _ = _org(db)
        routes.create_access_grant_action(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', reason='erro_interno', justification=JUSTIFICATION, ends_on='2026-12-14', plan_id='', confirm='', db=db, user=ADMIN)
        assert db.query(OrganizationAccessGrant).count() == 0 and flashes[-1][0] == 'error'
        routes.create_access_grant_action(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', reason='erro_interno', justification=JUSTIFICATION, ends_on='2026-12-14', plan_id='', confirm='1', db=db, user=ADMIN)
        grant = db.query(OrganizationAccessGrant).one()
        assert flashes[-1][0] == 'success' and '14/12/2026' in flashes[-1][1]
        routes.extend_access_grant_action(organization_id=org.id, grant_id=grant.id, request=SimpleNamespace(), csrf_token='x', ends_on='2027-01-10', justification='Cliente pediu mais prazo para regularizar.', db=db, user=ADMIN)
        routes.revoke_access_grant_action(organization_id=org.id, grant_id=grant.id, request=SimpleNamespace(), csrf_token='x', revoke_reason='Pagamento confirmado', db=db, user=ADMIN)
        db.refresh(grant)
        assert grant.ends_on == date(2027, 1, 10) and grant.revoked_at is not None and grant.revoked_by_email == ADMIN.email
        events = [(row.event_type, json.loads(row.metadata_json)) for row in db.query(AuditLog).order_by(AuditLog.id).all()]
        assert [event for event, _ in events] == ['comercial.liberacao.criar', 'comercial.liberacao.prorrogar', 'comercial.liberacao.encerrar']
        assert events[0][1]['justificativa'] == JUSTIFICATION and events[1][1]['justificativa'].startswith('Cliente pediu')


def test_route_cannot_touch_grant_of_another_organization(monkeypatch):
    from fastapi import HTTPException
    _route_env(monkeypatch)
    with _db() as db:
        mine, _ = _org(db, 'minha')
        other, _ = _org(db, 'outra')
        grant = grants.create_grant(db, organization_id=other.id, reason='outro', justification=JUSTIFICATION, ends_on=TODAY + timedelta(days=5), today=TODAY, plan_id=None, user=ADMIN)
        db.commit()
        with pytest.raises(HTTPException):
            routes.revoke_access_grant_action(organization_id=mine.id, grant_id=grant.id, request=SimpleNamespace(), csrf_token='x', revoke_reason='tentativa', db=db, user=ADMIN)


def _render(template, context):
    env = routes.templates.env.overlay(loader=ChoiceLoader([DictLoader({'base.html': '{% block content %}{% endblock %}'}), routes.templates.env.loader]))
    return env.get_template(template).render(**context, request=None, csrf_token='t')


def test_admin_and_customer_pages_show_grant(monkeypatch):
    from app.core.permissions_catalog import PAGE_FINANCE
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, '_base_context', lambda *args, **kwargs: kwargs)
    monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: (name, context))
    monkeypatch.setattr(routes, 'today_in_app_timezone', lambda: TODAY)
    monkeypatch.setattr(routes, 'permission_codes_for_user', lambda db, user: frozenset({PAGE_FINANCE}))
    with _db() as db:
        org, _ = _org(db, overdue=True)
        grants.create_grant(db, organization_id=org.id, reason='instabilidade_asaas', justification=JUSTIFICATION, ends_on=TODAY + timedelta(days=20), today=TODAY, plan_id=None, user=ADMIN)
        db.commit()
        monkeypatch.setattr(routes, '_repository', lambda _db: SimpleNamespace(list_organizations=lambda: [org]))
        _, admin_ctx = routes.subscriptions_page(request=SimpleNamespace(), organizacao=org.id, db=db, user=SimpleNamespace(), csrf_token='t')
        admin_html = routes.templates.env.get_template('_subscription_management.html').render(**admin_ctx)
        _, customer_ctx = routes.my_subscription_page(request=SimpleNamespace(), db=db, user=SimpleNamespace(organization=org, id=5, email='c@x'), csrf_token='t')
        customer_html = _render('my_subscription.html', customer_ctx)
    assert 'Liberação manual ativa' in admin_html and 'Acesso liberado até 04/12/2026' in admin_html and JUSTIFICATION in admin_html
    assert 'Prorrogar' in admin_html and 'Encerrar liberação' in admin_html
    assert 'liberado pela equipe SiSFarm até 04/12/2026' in customer_html and 'Acesso liberado' in customer_html
    idx = customer_html.find(JUSTIFICATION)
    assert idx == -1, customer_html[max(0, idx - 300):idx + 100]
    assert 'Faturas em aberto' in customer_html


def test_admin_page_offers_grant_without_subscription(monkeypatch):
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, '_base_context', lambda *args, **kwargs: kwargs)
    monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: (name, context))
    with _db() as db:
        org, _ = _org(db, with_subscription=False)
        _plan(db)
        monkeypatch.setattr(routes, '_repository', lambda _db: SimpleNamespace(list_organizations=lambda: [org]))
        _, ctx = routes.subscriptions_page(request=SimpleNamespace(), organizacao=org.id, db=db, user=SimpleNamespace(), csrf_token='t')
        html = routes.templates.env.get_template('_subscription_management.html').render(**ctx)
    assert 'Liberar acesso manualmente' in html and 'name="plan_id" required' in html and 'Erro ou falha da SiSFarm' in html
