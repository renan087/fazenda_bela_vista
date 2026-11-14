from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import AuditLog, Organization, OrganizationBilling, OrganizationSubscription, SubscriptionPlan
from app.services import subscription_plans as plans
from app.services.subscription_portfolio import monthly_amount
from app.web import routes


def _db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return Session(engine)


def _form(**overrides):
    fields = dict(
        name='Profissional', description='Para médias propriedades', price_monthly='149,90', price_quarterly='',
        price_semiannual='799,00', price_yearly='1.499,00', max_farms='3', max_users='', max_hectares='300',
        modules=['page.finance', 'page.productive_unit'], trial_days='14', display_order='2', is_featured='1',
    )
    fields.update(overrides)
    return plans.parse_plan_form(**fields)


def _subscribe(db, plan, cycle='MONTHLY', is_current=True, org_slug='org'):
    org = Organization(name=org_slug, slug=org_slug, is_active=True)
    db.add(org)
    db.flush()
    sub = OrganizationSubscription(
        organization_id=org.id, plan_id=plan.id, plan_name=plan.name, amount=plans.plan_price(plan, cycle), cycle=cycle,
        billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', is_current=is_current, status='criada', asaas_status='ACTIVE',
    )
    db.add(sub)
    db.flush()
    return sub


def test_parse_form_reads_brazilian_values_and_orders_modules():
    form = _form()
    assert form.prices == {'MONTHLY': Decimal('149.90'), 'QUARTERLY': None, 'SEMIANNUALLY': Decimal('799.00'), 'YEARLY': Decimal('1499.00')}
    assert form.max_farms == 3 and form.max_users is None and form.max_hectares == Decimal('300.00')
    assert form.modules == ['page.productive_unit', 'page.finance']
    assert form.trial_days == 14 and form.is_featured


@pytest.mark.parametrize('overrides, message', [
    (dict(name=' '), 'nome'),
    (dict(price_monthly='', price_semiannual='', price_yearly=''), 'pelo menos uma'),
    (dict(price_monthly='-5'), 'preços'),
    (dict(price_monthly='10,999'), 'preços'),
    (dict(max_farms='0'), 'fazendas'),
    (dict(trial_days='400'), 'teste'),
    (dict(modules=['users.manage']), 'Módulo'),
])
def test_parse_form_rejects_invalid_input(overrides, message):
    with pytest.raises(plans.PlanValidationError, match=f'(?i){message}'):
        _form(**overrides)


def test_create_plan_starts_as_draft_with_unique_code():
    with _db() as db:
        first = plans.create_plan(db, _form(name='Gestão Completa'))
        second = plans.create_plan(db, _form(name='Gestão completa'))
        assert first.status == 'rascunho' and first.version == 1
        assert first.code == 'gestao-completa' and second.code == 'gestao-completa-2'


def test_activation_requires_price_and_archives_previous_version():
    with _db() as db:
        v1 = plans.create_plan(db, _form())
        plans.activate_plan(db, v1)
        v2 = plans.create_new_version(db, v1)
        assert v2.code == v1.code and v2.version == 2 and v2.status == 'rascunho'
        with pytest.raises(plans.PlanValidationError):
            plans.create_new_version(db, v1)
        archived = plans.activate_plan(db, v2)
        assert archived == [v1] and v1.status == 'arquivado' and v2.status == 'ativo'
        assert [item.id for item in plans.active_plans(db)] == [v2.id]
        empty = SubscriptionPlan(code='vazio', version=1, name='Vazio', status='rascunho', modules_json='[]')
        db.add(empty)
        db.flush()
        with pytest.raises(plans.PlanValidationError, match='preço'):
            plans.activate_plan(db, empty)


def test_plan_with_subscriptions_locks_commercial_terms_but_allows_presentation():
    with _db() as db:
        plan = plans.create_plan(db, _form())
        plans.activate_plan(db, plan)
        _subscribe(db, plan)
        with pytest.raises(plans.PlanValidationError, match='nova versão'):
            plans.update_plan(db, plan, _form(price_monthly='199,90'))
        plans.update_plan(db, plan, _form(description='Novo texto', display_order='9', is_featured=None))
        assert plan.description == 'Novo texto' and plan.display_order == 9 and not plan.is_featured
        assert plan.price_monthly == Decimal('149.90')


def test_unused_draft_can_be_edited_and_deleted_but_used_plan_cannot():
    with _db() as db:
        draft = plans.create_plan(db, _form())
        plans.update_plan(db, draft, _form(price_monthly='99,90', modules=[]))
        assert draft.price_monthly == Decimal('99.90') and plans.plan_modules(draft) == []
        plans.delete_plan(db, draft)
        db.flush()
        assert db.query(SubscriptionPlan).count() == 0
        used = plans.create_plan(db, _form())
        plans.activate_plan(db, used)
        _subscribe(db, used, is_current=False)
        plans.archive_plan(used)
        with pytest.raises(plans.PlanValidationError):
            plans.delete_plan(db, used)
        with pytest.raises(plans.PlanValidationError, match='arquivados'):
            plans.update_plan(db, used, _form())


def test_resolve_selection_uses_catalog_price_and_rejects_unavailable():
    with _db() as db:
        plan = plans.create_plan(db, _form())
        with pytest.raises(plans.PlanValidationError):
            plans.resolve_plan_selection(db, plan.id, 'MONTHLY')
        plans.activate_plan(db, plan)
        assert plans.resolve_plan_selection(db, plan.id, 'SEMIANNUALLY') == (plan, Decimal('799.00'))
        with pytest.raises(plans.PlanValidationError, match='periodicidade'):
            plans.resolve_plan_selection(db, plan.id, 'QUARTERLY')


def test_monthly_amount_normalizes_every_cycle():
    for cycle, amount, expected in [('MONTHLY', '90', '90.00'), ('QUARTERLY', '270', '90.00'), ('SEMIANNUALLY', '540', '90.00'), ('YEARLY', '1080', '90.00')]:
        assert monthly_amount(SimpleNamespace(amount=Decimal(amount), cycle=cycle)) == Decimal(expected)


def _route_env(monkeypatch, db):
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_flash', lambda *args: None)
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(asaas_api_base_url='https://api-sandbox.asaas.com', asaas_api_key='sandbox-key'))
    monkeypatch.setattr(routes, 'list_customer_sandbox_subscriptions', lambda **kwargs: ([], None))
    calls = []
    def create(**kwargs):
        calls.append(kwargs['payload'])
        return {'id': f'sub_{len(calls)}', 'status': 'ACTIVE'}, None
    monkeypatch.setattr(routes, 'create_sandbox_subscription', create)
    org = Organization(name='Cliente', slug='cliente', is_active=True)
    db.add(org)
    db.flush()
    db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_test', asaas_environment='sandbox'))
    db.commit()
    return org, calls


def test_subscription_from_plan_ignores_submitted_name_and_amount(monkeypatch):
    with _db() as db:
        org, calls = _route_env(monkeypatch, db)
        plan = plans.create_plan(db, _form())
        plans.activate_plan(db, plan)
        db.commit()
        routes.create_organization_sandbox_subscription_action(
            organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_id=str(plan.id), plan_name='Hack', amount='1,00',
            cycle='SEMIANNUALLY', billing_type='BOLETO', first_due_date='2099-01-01', db=db, user=SimpleNamespace(),
        )
        sub = db.query(OrganizationSubscription).one()
        assert sub.plan_id == plan.id and sub.plan_name == 'Profissional' and sub.amount == Decimal('799.00') and sub.cycle == 'SEMIANNUALLY'
        assert calls[0]['value'] == 799.0 and calls[0]['cycle'] == 'SEMIANNUALLY' and calls[0]['description'] == 'Profissional'
        assert db.query(AuditLog).filter(AuditLog.event_type == 'comercial.assinatura.criar').count() == 2


def test_subscription_with_unavailable_plan_cycle_is_refused(monkeypatch):
    with _db() as db:
        org, calls = _route_env(monkeypatch, db)
        plan = plans.create_plan(db, _form())
        plans.activate_plan(db, plan)
        db.commit()
        routes.create_organization_sandbox_subscription_action(
            organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_id=str(plan.id),
            cycle='QUARTERLY', billing_type='PIX', first_due_date='2099-01-01', db=db, user=SimpleNamespace(),
        )
        assert calls == [] and db.query(OrganizationSubscription).count() == 0


def test_custom_subscription_still_works_without_plan(monkeypatch):
    with _db() as db:
        org, calls = _route_env(monkeypatch, db)
        routes.create_organization_sandbox_subscription_action(
            organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_id='', plan_name='Negociado', amount='59,90',
            cycle='QUARTERLY', billing_type='PIX', first_due_date='2099-01-01', db=db, user=SimpleNamespace(),
        )
        sub = db.query(OrganizationSubscription).one()
        assert sub.plan_id is None and sub.amount == Decimal('59.90') and sub.cycle == 'QUARTERLY'


def test_plans_page_groups_versions_and_counts_subscribers(monkeypatch):
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, '_base_context', lambda *args, **kwargs: kwargs)
    monkeypatch.setattr(routes, '_repository', lambda _db: None)
    monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: (name, context))
    with _db() as db:
        v1 = plans.create_plan(db, _form())
        plans.activate_plan(db, v1)
        _subscribe(db, v1, cycle='YEARLY')
        v2 = plans.create_new_version(db, v1)
        other = plans.create_plan(db, _form(name='Essencial', display_order='1'))
        db.commit()
        name, context = routes.subscription_plans_page(request=SimpleNamespace(), db=db, user=SimpleNamespace(), csrf_token='t')
        assert name == 'subscription_plans.html'
        assert [[item.id for item in family] for family in context['plan_families']] == [[v2.id, v1.id], [other.id]]
        assert context['plan_stats'][v1.id]['current'] == 1 and context['plan_stats'][v1.id]['mrr'] == Decimal('124.92')


def test_plan_form_routes_create_and_edit(monkeypatch):
    import asyncio

    from starlette.datastructures import FormData

    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    messages = []
    monkeypatch.setattr(routes, '_flash', lambda request, kind, message: messages.append((kind, message)))
    fields = [('csrf_token', 't'), ('name', 'Essencial'), ('price_monthly', '79,90'), ('price_yearly', '799,00'),
              ('max_farms', '1'), ('modules', 'page.rainfall'), ('modules', 'page.productive_unit'), ('trial_days', '30')]

    def request(items):
        async def form():
            return FormData(items)
        return SimpleNamespace(form=form)

    with _db() as db:
        response = asyncio.run(routes.create_subscription_plan_action(request=request(fields), db=db, user=SimpleNamespace()))
        plan = db.query(SubscriptionPlan).one()
        assert response.headers['location'] == f'/planos#plano-{plan.id}'
        assert plan.status == 'rascunho' and plan.price_yearly == Decimal('799.00')
        assert plans.plan_modules(plan) == ['page.productive_unit', 'page.rainfall']
        edited = [item for item in fields if item[0] != 'price_yearly'] + [('price_yearly', '699,00')]
        asyncio.run(routes.edit_subscription_plan_action(plan_id=plan.id, request=request(edited), db=db, user=SimpleNamespace()))
        assert plan.price_yearly == Decimal('699.00')
        asyncio.run(routes.edit_subscription_plan_action(plan_id=plan.id, request=request([('csrf_token', 't'), ('name', '')]), db=db, user=SimpleNamespace()))
        assert messages[-1][0] == 'error' and plan.name == 'Essencial'
        assert db.query(AuditLog).filter(AuditLog.event_type.like('comercial.plano.%')).count() == 2
