import asyncio
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import Organization, OrganizationBilling, OrganizationSubscription
from app.services import asaas_environment as env
from app.services import asaas_subscriptions
from app.web import routes


def _settings(**overrides):
    values = dict(asaas_environment='sandbox', asaas_api_base_url='https://api-sandbox.asaas.com', asaas_api_key='key', asaas_webhook_token='tok', app_public_url='https://app.sisfarm.com.br')
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.parametrize('overrides, error', [
    ({}, None),
    ({'asaas_api_base_url': 'https://api-sandbox.asaas.com/v3/'}, None),
    ({'asaas_environment': 'production', 'asaas_api_base_url': 'https://api.asaas.com'}, None),
    ({'asaas_environment': 'production'}, 'não corresponde'),
    ({'asaas_api_base_url': 'https://api.asaas.com'}, 'não corresponde'),
    ({'asaas_api_base_url': 'https://evil.example.com'}, 'não corresponde'),
    ({'asaas_environment': 'prod'}, "deve ser"),
    ({'asaas_api_key': ' '}, 'ASAAS_API_KEY'),
])
def test_production_requires_both_environment_and_url(overrides, error):
    message = env.asaas_configuration_error(_settings(**overrides))
    assert (message is None) if error is None else (error in message)


def test_services_call_the_configured_environment(monkeypatch):
    monkeypatch.setattr(env, 'get_settings', lambda: _settings(asaas_environment='production', asaas_api_base_url='https://api.asaas.com'))
    urls = []

    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get(self, url, **kwargs):
            urls.append(url)
            return SimpleNamespace(status_code=200, json=lambda: {'id': 'sub_1', 'customer': 'cus_1', 'status': 'ACTIVE'})

    monkeypatch.setattr(asaas_subscriptions.httpx, 'Client', Client)
    data, error = asaas_subscriptions.get_sandbox_subscription(api_key='k', subscription_id='sub_1')
    assert error is None and urls == ['https://api.asaas.com/v3/subscriptions/sub_1']
    assert env.current_asaas_environment().web_base == 'https://www.asaas.com'


def test_webhook_rejects_wrong_token_and_accepts_right_one(monkeypatch):
    from app.routers import asaas_webhook
    monkeypatch.setattr(asaas_webhook, 'get_settings', lambda: SimpleNamespace(asaas_webhook_token='segredo-123'))

    class Request:
        def __init__(self, token): self.headers = {'asaas-access-token': token}
        async def json(self): return {'event': 'PAYMENT_CREATED'}

    with pytest.raises(HTTPException) as exc:
        asyncio.run(asaas_webhook.asaas_webhook(Request('segredo-124'), SimpleNamespace(rollback=lambda: None, commit=lambda: None)))
    assert exc.value.status_code == 401
    monkeypatch.setattr(asaas_webhook, 'persist_organization_webhook', lambda db, body: True)
    assert asyncio.run(asaas_webhook.asaas_webhook(Request('segredo-123'), SimpleNamespace(rollback=lambda: None, commit=lambda: None))) == {'received': True}


def test_neutral_paths_are_registered_and_old_ones_still_work():
    paths = {route.path for route in routes.router.routes}
    for suffix in ['', '/editar', '/gerenciar', '/cartao', '/cartao-salvo', '/atualizar', '/checkout/encerrar']:
        assert f'/organizacoes/{{organization_id}}/assinatura{suffix}' in paths
        assert f'/organizacoes/{{organization_id}}/assinatura-sandbox{suffix}' in paths
    assert '/organizacoes/{organization_id}/cliente-asaas' in paths


def _db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return Session(engine)


def test_production_does_not_reuse_sandbox_subscription(monkeypatch):
    prod = _settings(asaas_environment='production', asaas_api_base_url='https://api.asaas.com')
    monkeypatch.setattr(env, 'get_settings', lambda: prod)
    monkeypatch.setattr(routes, 'get_settings', lambda: prod)
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    flashes = []
    monkeypatch.setattr(routes, '_flash', lambda request, kind, message: flashes.append((kind, message)))
    monkeypatch.setattr(routes, 'create_sandbox_subscription', lambda **kw: pytest.fail('não deve criar'))
    with _db() as db:
        org = Organization(name='F', slug='f', is_active=True)
        db.add(org)
        db.flush()
        db.add(OrganizationBilling(organization_id=org.id, contact_name='P', contact_email='a@example.com', asaas_customer_id='cus_prod', asaas_environment='production'))
        db.add(OrganizationSubscription(organization_id=org.id, plan_name='Teste', amount=Decimal('1'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', is_current=True))
        db.commit()
        routes.create_organization_sandbox_subscription_action(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', plan_id='', plan_name='Real', amount='10,00', cycle='MONTHLY', billing_type='PIX', first_due_date='2099-01-01', db=db, user=SimpleNamespace())
    assert 'encerrar_assinaturas_sandbox' in flashes[-1][1]


def test_page_warns_when_integration_is_incomplete(monkeypatch):
    broken = _settings(asaas_api_key='', asaas_webhook_token='')
    monkeypatch.setattr(routes, 'get_settings', lambda: broken)
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, '_base_context', lambda *args, **kwargs: kwargs)
    monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: (name, context))
    with _db() as db:
        monkeypatch.setattr(routes, '_repository', lambda _db: SimpleNamespace(list_organizations=lambda: []))
        _, context = routes.subscriptions_page(request=SimpleNamespace(), organizacao=None, db=db, user=SimpleNamespace(), csrf_token='t')
    assert 'ASAAS_API_KEY' in context['asaas_config_error'] and context['asaas_webhook_ready'] is False


def test_go_live_script_closes_only_sandbox_data(monkeypatch):
    import importlib.util
    import sys
    spec = importlib.util.spec_from_file_location('encerrar', 'scripts/encerrar_assinaturas_sandbox.py')
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setattr(script, 'SessionLocal', lambda: Session(engine))
    with Session(engine) as db:
        a, b = Organization(name='A', slug='a', is_active=True), Organization(name='B', slug='b', is_active=True)
        db.add_all([a, b])
        db.flush()
        db.add_all([
            OrganizationBilling(organization_id=a.id, contact_name='P', contact_email='a@example.com', asaas_customer_id='cus_sb', asaas_environment='sandbox'),
            OrganizationBilling(organization_id=b.id, contact_name='P', contact_email='b@example.com', asaas_customer_id='cus_pr', asaas_environment='production'),
            OrganizationSubscription(organization_id=a.id, plan_name='T', amount=Decimal('1'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', is_current=True),
            OrganizationSubscription(organization_id=b.id, plan_name='R', amount=Decimal('1'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='production', is_current=True),
        ])
        db.commit()
    monkeypatch.setattr(sys, 'argv', ['x'])
    script.main()
    with Session(engine) as db:
        assert db.query(OrganizationSubscription).filter_by(is_current=True).count() == 2
    monkeypatch.setattr(sys, 'argv', ['x', '--confirmar'])
    script.main()
    with Session(engine) as db:
        current = db.query(OrganizationSubscription).filter_by(is_current=True).all()
        assert [row.environment for row in current] == ['production']
        billings = {row.asaas_environment: row.asaas_customer_id for row in db.query(OrganizationBilling).all()}
        assert billings == {None: None, 'production': 'cus_pr'}
