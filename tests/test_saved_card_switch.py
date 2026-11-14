import json
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import AuditLog, Organization, OrganizationBilling, OrganizationSubscription, OrganizationSubscriptionPayment
from app.services import asaas_subscriptions as service
from app.services.organization_subscription_sync import persist_organization_webhook
from app.web import routes

TOKEN = 'tok_secret_123'


def _db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return Session(engine)


def _setup(db, *, tokenized=True):
    org = Organization(name='Fazenda', slug='fazenda', is_active=True)
    db.add(org)
    db.flush()
    db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_1', asaas_environment='sandbox'))
    old = OrganizationSubscription(organization_id=org.id, plan_name='Plano', amount=Decimal('99.90'), cycle='MONTHLY', billing_type='CREDIT_CARD', first_due_date=date(2026, 1, 1), environment='sandbox', status='criada', asaas_status='INACTIVE', asaas_subscription_id='sub_old', is_current=False)
    current = OrganizationSubscription(organization_id=org.id, plan_name='Plano', amount=Decimal('99.90'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), next_due_date=date(2099, 3, 10), environment='sandbox', status='criada', asaas_status='ACTIVE', asaas_subscription_id='sub_cur', is_current=True)
    db.add_all([old, current])
    db.flush()
    db.add(OrganizationSubscriptionPayment(subscription_id=old.id, asaas_payment_id='pay_card', status='CONFIRMED', billing_type='CREDIT_CARD', due_date=date(2026, 2, 1), card_last4='4242', card_brand='VISA', card_tokenized=tokenized))
    db.commit()
    return org, current


def test_webhook_keeps_card_digits_but_never_the_token():
    with _db() as db:
        org, current = _setup(db)
        persist_organization_webhook(db, {'id': 'evt', 'event': 'PAYMENT_CONFIRMED', 'dateCreated': '2026-11-14 10:00:00', 'payment': {
            'id': 'pay_new', 'subscription': 'sub_cur', 'customer': 'cus_1', 'status': 'CONFIRMED', 'billingType': 'CREDIT_CARD', 'value': 99.9,
            'creditCard': {'creditCardNumber': '1111', 'creditCardBrand': 'MASTERCARD', 'creditCardToken': TOKEN}}})
        row = db.query(OrganizationSubscriptionPayment).filter_by(asaas_payment_id='pay_new').one()
        assert (row.card_last4, row.card_brand, row.card_tokenized) == ('1111', 'MASTERCARD', True)
        assert TOKEN not in json.dumps({c.name: str(getattr(row, c.name)) for c in row.__table__.columns})


class _Response:
    def __init__(self, status, data=None):
        self.status_code = status
        self._data = data or {}

    def json(self):
        return self._data


def _fake_client(monkeypatch, *, card_status=200, card_path_missing=False, token=TOKEN, revert_status=200):
    calls = []
    remote = {'id': 'sub_cur', 'customer': 'cus_1', 'status': 'ACTIVE', 'billingType': 'PIX'}

    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass

        def get(self, url, **kwargs):
            calls.append(('GET', url, None))
            card = {'creditCardNumber': '4242', 'creditCardToken': token} if token else {'creditCardNumber': '4242'}
            return _Response(200, {'id': 'pay_card', 'customer': 'cus_1', 'billingType': 'CREDIT_CARD', 'status': 'CONFIRMED', 'creditCard': card})

        def put(self, url, json=None, **kwargs):
            calls.append(('PUT', url, json))
            if url.endswith('/creditCard'):
                return _Response(card_status, {**remote, 'billingType': 'CREDIT_CARD', 'nextDueDate': '2099-03-10'})
            if json.get('billingType') == 'PIX':
                return _Response(revert_status, remote)
            return _Response(200, {**remote, 'billingType': json['billingType']})

        def post(self, url, json=None, **kwargs):
            calls.append(('POST', url, json))
            if card_path_missing:
                return _Response(404)
            return _Response(card_status, {**remote, 'billingType': 'CREDIT_CARD', 'nextDueDate': '2099-03-10'})

    monkeypatch.setattr(service, 'get_sandbox_subscription', lambda **kw: (remote, None))
    monkeypatch.setattr(service.httpx, 'Client', Client)
    return calls


def _switch():
    return service.switch_sandbox_subscription_to_saved_card(api_key='k', subscription_id='sub_cur', customer_id='cus_1', card_payment_id='pay_card', remote_ip='10.0.0.1')


def test_switch_uses_token_and_keeps_pending_payments(monkeypatch):
    calls = _fake_client(monkeypatch)
    result, error = _switch()
    assert error is None and result['billingType'] == 'CREDIT_CARD'
    assert calls[1] == ('PUT', 'https://api-sandbox.asaas.com/v3/subscriptions/sub_cur', {'billingType': 'CREDIT_CARD', 'updatePendingPayments': False})
    assert calls[2] == ('POST', 'https://api-sandbox.asaas.com/v3/subscriptions/sub_cur/updateCreditCard', {'creditCardToken': TOKEN, 'remoteIp': '10.0.0.1'})
    assert 'nextDueDate' not in calls[1][2]


def test_switch_falls_back_to_alternate_card_endpoint(monkeypatch):
    calls = _fake_client(monkeypatch, card_path_missing=True)
    result, error = _switch()
    assert error is None and calls[-1][0] == 'PUT' and calls[-1][1].endswith('/creditCard')


def test_switch_reverts_payment_method_when_card_is_refused(monkeypatch):
    calls = _fake_client(monkeypatch, card_status=400)
    result, error = _switch()
    assert result is None and 'forma anterior foi mantida' in error
    assert calls[-1] == ('PUT', 'https://api-sandbox.asaas.com/v3/subscriptions/sub_cur', {'billingType': 'PIX', 'updatePendingPayments': False})


def test_switch_without_token_changes_nothing(monkeypatch):
    calls = _fake_client(monkeypatch, token=None)
    result, error = _switch()
    assert result is None and 'tokenização' in error
    assert all(method == 'GET' for method, _, _ in calls)


def _route_env(monkeypatch):
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    flashes = []
    monkeypatch.setattr(routes, '_flash', lambda request, kind, message: flashes.append((kind, message)))
    monkeypatch.setattr(routes, 'get_settings', lambda: SimpleNamespace(asaas_api_base_url='https://api-sandbox.asaas.com', asaas_api_key='k'))
    monkeypatch.setattr(routes, 'client_ip', lambda request: '10.0.0.1')
    return flashes


def test_route_requires_customer_authorization(monkeypatch):
    flashes = _route_env(monkeypatch)
    monkeypatch.setattr(routes, 'switch_sandbox_subscription_to_saved_card', lambda **kw: pytest.fail('não deve chamar o Asaas'))
    with _db() as db:
        org, current = _setup(db)
        routes.switch_to_saved_card_action(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', customer_authorized='', db=db, user=SimpleNamespace())
        assert flashes[-1][0] == 'error' and current.billing_type == 'PIX'


def test_route_without_reusable_card_suggests_link(monkeypatch):
    flashes = _route_env(monkeypatch)
    monkeypatch.setattr(routes, 'switch_sandbox_subscription_to_saved_card', lambda **kw: pytest.fail('não deve chamar o Asaas'))
    with _db() as db:
        org, current = _setup(db, tokenized=False)
        routes.switch_to_saved_card_action(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', customer_authorized='1', db=db, user=SimpleNamespace())
        assert 'link' in flashes[-1][1]


def test_route_switches_same_subscription_keeping_next_charge(monkeypatch):
    flashes = _route_env(monkeypatch)
    sent = {}
    def switch(**kw):
        sent.update(kw)
        return {'id': 'sub_cur', 'customer': 'cus_1', 'status': 'ACTIVE', 'billingType': 'CREDIT_CARD', 'nextDueDate': '2099-03-10', 'value': 99.9, 'cycle': 'MONTHLY'}, None
    monkeypatch.setattr(routes, 'switch_sandbox_subscription_to_saved_card', switch)
    with _db() as db:
        org, current = _setup(db)
        routes.switch_to_saved_card_action(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', customer_authorized='1', db=db, user=SimpleNamespace())
        db.refresh(current)
        assert (current.billing_type, current.status, current.next_due_date, current.is_current) == ('CREDIT_CARD', 'criada', date(2099, 3, 10), True)
        assert db.query(OrganizationSubscription).count() == 2
        assert sent['card_payment_id'] == 'pay_card' and sent['remote_ip'] == '10.0.0.1'
        assert flashes[-1][0] == 'success' and '4242' in flashes[-1][1] and '10/03/2099' in flashes[-1][1]
        audit = [json.loads(row.metadata_json) for row in db.query(AuditLog).all()]
        assert any(item.get('autorizado_pelo_cliente') for item in audit)
        assert all(TOKEN not in json.dumps(item) for item in audit)


def test_route_failure_keeps_previous_method(monkeypatch):
    flashes = _route_env(monkeypatch)
    monkeypatch.setattr(routes, 'switch_sandbox_subscription_to_saved_card', lambda **kw: (None, 'O Asaas não confirmou o cartão salvo (HTTP 400). A forma anterior foi mantida.'))
    with _db() as db:
        org, current = _setup(db)
        routes.switch_to_saved_card_action(organization_id=org.id, request=SimpleNamespace(), csrf_token='x', customer_authorized='1', db=db, user=SimpleNamespace())
        db.refresh(current)
        assert current.billing_type == 'PIX' and flashes[-1][0] == 'error'


def test_card_dialog_offers_saved_card_first(monkeypatch):
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, '_base_context', lambda *args, **kwargs: kwargs)
    monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: (name, context))
    with _db() as db:
        org, current = _setup(db)
        monkeypatch.setattr(routes, '_repository', lambda _db: SimpleNamespace(list_organizations=lambda: [org]))
        _, context = routes.subscriptions_page(request=SimpleNamespace(), organizacao=org.id, db=db, user=SimpleNamespace(), csrf_token='t')
        html = routes.templates.env.get_template('_subscription_management.html').render(**context)
    assert 'Voltar para o cartão final 4242' in html and 'O cliente autorizou voltar a cobrar neste cartão.' in html
    assert 'Usar outro cartão (enviar link ao cliente)' in html and 'value="2099-03-10"' in html
