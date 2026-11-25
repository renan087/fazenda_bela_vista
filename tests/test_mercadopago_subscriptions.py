import hashlib
import hmac
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models import MercadoPagoEvent, Organization, OrganizationSubscription, OrganizationSubscriptionPayment, SubscriptionPlan
from app.routers import mercadopago_webhook
from app.services import billing_environment
from app.services import mercadopago_subscriptions as mp
from app.services.access_grants import organization_access
from app.web import routes

TARGET = "/api/webhooks/mercadopago/test"
LINK = "https://www.mercadopago.com.br/subscriptions/checkout?preapproval_id=TESTID"
TODAY = date(2026, 11, 24)


@pytest.fixture()
def settings(monkeypatch):
    value = SimpleNamespace(mercadopago_sandbox=True, mercadopago_access_token="TEST-token",
                            mercadopago_public_key="TEST-public", mercadopago_webhook_secret="signing-secret")
    for module in (mp, mercadopago_webhook, billing_environment, routes):
        monkeypatch.setattr(module, "get_settings", lambda: value)
    return value


@pytest.fixture()
def database(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'mercadopago.db'}")
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine, autoflush=False)
    monkeypatch.setattr(mp, "SessionLocal", sessions)
    try:
        yield sessions
    finally:
        engine.dispose()


def prepare(db, cycle="MONTHLY", due=TODAY):
    organization = Organization(name="Fazenda Teste", slug="fazenda-teste", is_active=True)
    plan = SubscriptionPlan(code="teste", name="Essencial", status="ativo", price_monthly=99, price_yearly=990)
    db.add_all([organization, plan])
    db.flush()
    row = mp.reserve_checkout(db, organization=organization, plan=plan,
                              amount=Decimal("990" if cycle == "YEARLY" else "99"),
                              cycle=cycle, due=due, email="test@example.com")
    db.commit()
    return row


def remote_subscription(row, status, **overrides):
    return {
        "id": "TESTID", "external_reference": row.mercadopago_reference, "status": status, "version": 1,
        "auto_recurring": {"currency_id": "BRL", "transaction_amount": str(row.amount),
                           "frequency": mp.CYCLES[row.cycle], "frequency_type": "months"},
        "init_point": LINK, "next_payment_date": "2026-12-24T07:00:00-03:00", **overrides,
    }


class Gateway:
    def __init__(self, row, status="pending"):
        self.subscription = remote_subscription(row, status)
        self.invoices = {}
        self.payments = {}
        self.calls = []

    def request(self, method, path, *, payload):
        self.calls.append((method, path, payload))
        if method == "PUT":
            self.subscription["version"] += 1
            self.subscription["status"] = payload["status"]
        return self.subscription.copy()

    def get(self, topic, identifier):
        self.calls.append((topic, identifier))
        if topic == "subscription_preapproval":
            return self.subscription.copy()
        if topic == "subscription_authorized_payment":
            return self.invoices[identifier].copy()
        if topic == "payment":
            return self.payments[identifier].copy()
        return {"id": identifier, "status": "active"}

    def search(self, path, **filters):
        self.calls.append((path, filters))
        if path == "/preapproval/search":
            return [self.subscription.copy()]
        return [invoice.copy() for invoice in self.invoices.values()
                if "payment_id" not in filters or str(invoice["payment"]["id"]) == filters["payment_id"]]

    def charge(self, invoice_id, transaction, amount="99", due="2026-11-24", **fields):
        self.invoices[str(invoice_id)] = {
            "id": invoice_id, "preapproval_id": "TESTID", "currency_id": "BRL",
            "transaction_amount": amount, "debit_date": due, "status": "processed",
            "payment": {"id": transaction, "status": "approved"},
        }
        self.payments[str(transaction)] = {
            "id": transaction, "status": "approved", "currency_id": "BRL", "transaction_amount": amount,
            "live_mode": False, "payment_type_id": "credit_card", "date_approved": due, **fields,
        }


@pytest.mark.parametrize(("cycle", "frequency"), [("MONTHLY", 1), ("YEARLY", 12)])
def test_checkout_drafts_and_reuses_reference(database, settings, monkeypatch, cycle, frequency):
    monkeypatch.setattr(mp, "today_in_app_timezone", lambda: TODAY)
    with database() as db:
        row = prepare(db, cycle, date(2026, 12, 1))
        gateway = Gateway(row)
        assert mp.create_checkout(db, gateway, row) == LINK
        assert mp.create_checkout(db, gateway, row) == LINK
        [(method, endpoint, payload)] = gateway.calls
        assert (method, endpoint, payload["status"]) == ("POST", "/preapproval", "pending")
        assert payload["auto_recurring"]["frequency"] == frequency
        assert payload["auto_recurring"]["start_date"].startswith("2026-12-01")
        assert payload["external_reference"] == row.mercadopago_reference
        assert payload["back_url"].endswith("/minha-assinatura")
        assert not {"preapproval_plan_id", "card_token_id"} & payload.keys()
        assert not organization_access(db, row.organization_id, TODAY, environment=mp.MERCADOPAGO_TEST).has_access


def test_timeout_recovers_by_reference_without_second_post(database, settings):
    with database() as db:
        row = prepare(db)
        gateway = Gateway(row)
        gateway.request = lambda *a, **kw: (_ for _ in ()).throw(mp.MercadoPagoError("falha_de_conexao"))
        with pytest.raises(mp.MercadoPagoError, match="falha_de_conexao"):
            mp.create_checkout(db, gateway, row)
        assert row.is_current and row.status == "revisao" and not row.mercadopago_id
        with pytest.raises(mp.MercadoPagoError, match="assinatura_existente"):
            mp.reserve_checkout(db, organization=row.organization, plan=row.plan, amount=row.amount,
                                cycle=row.cycle, due=TODAY, email="test@example.com")
        mp.reconcile(db, gateway, row)
        db.commit()
        assert row.mercadopago_id == "TESTID" and row.mercadopago_checkout_url == LINK


def test_events_track_renewal_and_revocation_without_duplicate_invoices(database, settings):
    with database() as db:
        row = prepare(db)
        gateway = Gateway(row, "authorized")
        assert mp.process_event(db, gateway, "subscription_preapproval", "TESTID") == "authorized"
        db.commit()
        assert not organization_access(db, row.organization_id, TODAY, environment=mp.MERCADOPAGO_TEST).has_access
        gateway.charge(11, 101)
        mp.process_event(db, gateway, "subscription_authorized_payment", "11")
        db.commit()
        assert organization_access(db, row.organization_id, TODAY, environment=mp.MERCADOPAGO_TEST).has_access

        gateway.charge(12, 102, due="2026-12-24")
        mp.process_event(db, gateway, "payment", "102")
        db.commit()
        mp.process_event(db, gateway, "subscription_authorized_payment", "12")
        db.commit()
        assert db.query(OrganizationSubscriptionPayment).count() == 2
        gateway.payments["102"]["status"] = "refunded"
        mp.process_event(db, gateway, "payment", "102")
        db.commit()
        assert db.query(OrganizationSubscriptionPayment).filter_by(mercadopago_invoice_id="12").one().status == "REFUNDED"
        assert not organization_access(db, row.organization_id, TODAY, environment=mp.MERCADOPAGO_TEST).has_access

        assert mp.process_event(db, gateway, "subscription_preapproval_plan", "RP1") == "plano_consultado:active"
        mp.change_status(gateway, row, "paused")
        assert row.billing_status == "INACTIVE"
        mp.change_status(gateway, row, "authorized")
        mp.change_status(gateway, row, "canceled")
        assert ("PUT", "/preapproval/TESTID", {"status": "cancelled"}) in gateway.calls
        db.commit()
        assert not row.is_current and row.next_due_date is None
        assert db.query(OrganizationSubscriptionPayment).count() == 2


def _notify(client, *, event_id=71, request_id="request-1", identifier="TESTID", live=False, bad_signature=False, body_id=None):
    timestamp = "1795521600"
    manifest = f"id:{identifier.lower()};request-id:{request_id};ts:{timestamp};"
    signature = hmac.new(b"signing-secret", manifest.encode(), hashlib.sha256).hexdigest()
    headers = {"x-request-id": request_id, "x-signature": f"ts={timestamp},v1={signature if not bad_signature else '0' * 64}"}
    return client.post(TARGET, params={"data.id": identifier}, headers=headers, json={
        "id": event_id, "type": "subscription_preapproval", "action": "updated",
        "live_mode": live, "data": {"id": body_id or identifier},
    })


def test_signed_endpoint_deduplicates_and_queues_retriable_processing(database, settings, monkeypatch):
    app = FastAPI()
    app.include_router(mercadopago_webhook.router)

    def override_db():
        with database() as db:
            yield db

    app.dependency_overrides[mercadopago_webhook.get_db] = override_db
    with TestClient(app) as client:
        monkeypatch.setattr(client, "send", lambda request, **kwargs: client._transport.handle_request(request))
        assert _notify(client, bad_signature=True).status_code == 401
        assert _notify(client, body_id="OTHER").status_code == 400
        assert _notify(client, live=True).status_code == 200
        with database() as db:
            assert db.query(MercadoPagoEvent).count() == 1
        assert _notify(client).status_code == 200
        assert _notify(client, request_id="request-2").status_code == 200
        assert _notify(client, event_id=72).status_code == 200
        with database() as db:
            row = prepare(db)
            gateway = Gateway(row, "authorized")
            event = db.query(MercadoPagoEvent).one()
            assert event.state == "pending"
        gateway.get = lambda *a: (_ for _ in ()).throw(mp.MercadoPagoError("http_503", 503))
        mp._process_events(gateway)
        with database() as db:
            event = db.query(MercadoPagoEvent).one()
            assert event.state == "pending" and event.attempts == 1 and event.result == "http_503"
            event.retry_at = mp._now()
            gateway = Gateway(db.get(OrganizationSubscription, 1), "authorized")
            db.commit()
        mp._process_events(gateway)
        with database() as db:
            event = db.query(MercadoPagoEvent).one()
            assert event.state == "processed" and event.attempts == 2 and event.result == "authorized"
            assert db.get(OrganizationSubscription, 1).mercadopago_status == "authorized"
        settings.mercadopago_sandbox = False
        assert _notify(client).status_code == 503


def test_gateway_never_uses_production_or_accepts_live_payments(database, settings, monkeypatch):
    def response(request):
        assert request.url.host == "api.mercadopago.com"
        assert request.headers["Authorization"] == "Bearer TEST-token"
        if request.url.path == "/users/me":
            assert "X-scope" not in request.headers
            return httpx.Response(200, json={"id": 22, "site_id": "MLB", "tags": []})
        assert request.headers["X-scope"] == "stage"
        return httpx.Response(200, json={"id": 101, "live_mode": True})

    monkeypatch.setattr(httpx.Client, "send", lambda self, request, **kwargs: response(request))
    with pytest.raises(mp.MercadoPagoError, match="resposta_fora_do_sandbox"):
        mp.MercadoPagoAPI().get("payment", 101)
    assert mp.safe_link("https://mercadopago.com.br.attacker.example/checkout") is None
    settings.mercadopago_sandbox = False
    with pytest.raises(mp.MercadoPagoError, match="sandbox_indisponivel"):
        mp.MercadoPagoAPI()


def test_verified_test_seller_creates_pending_subscription_without_stage(settings, monkeypatch):
    calls = []

    def send(client, request, **kwargs):
        assert request.url.host == "api.mercadopago.com"
        assert request.headers["Authorization"] == "Bearer TEST-token"
        assert "X-scope" not in request.headers
        calls.append((request.method, request.url.path))
        if request.url.path == "/users/me":
            return httpx.Response(200, json={"id": 22, "site_id": "MLB", "tags": ["test_user"],
                                             "email": "test_user_22@testuser.com"})
        return httpx.Response(201, json={"id": "subscription-test", "status": "pending"})

    monkeypatch.setattr(httpx.Client, "send", send)
    api = mp.MercadoPagoAPI()
    for email, reason in (("customer@example.com", "email_do_comprador"),
                          ("test_user_22@testuser.com", "comprador_igual_ao_vendedor")):
        with pytest.raises(mp.MercadoPagoError, match="comprador_de_teste_invalido") as raised:
            api.request("POST", "/preapproval", payload={"payer_email": email, "status": "pending"})
        assert raised.value.reason == reason
    result = api.request("POST", "/preapproval", payload={"payer_email": "test_user_33@testuser.com", "status": "pending"})
    assert result["status"] == "pending"
    assert calls == [("GET", "/users/me"), ("POST", "/preapproval")]


def test_reconciliation_applies_payment_from_verified_test_seller(database, settings, monkeypatch):
    with database() as db:
        row = prepare(db)
        row.mercadopago_id = "TESTID"
        row.mercadopago_status = "pending"
        db.commit()
        gateway = Gateway(row, "authorized")
        gateway.charge(11, 101, collector_id=22)
        gateway.payments["101"]["live_mode"] = True

        def send(client, request, **kwargs):
            path = request.url.path
            if path == "/users/me":
                data = {"id": 22, "site_id": "MLB", "tags": ["test_user"], "email": "test_user_22@testuser.com"}
            elif path == "/preapproval/TESTID":
                data = gateway.subscription
            elif path == "/authorized_payments/search":
                assert "limit" not in request.url.params
                assert request.url.params["preapproval_id"] == "TESTID"
                data = {"results": [gateway.invoices["11"]], "paging": {"offset": 0, "limit": 12, "total": 1}}
            elif path == "/authorized_payments/11":
                data = gateway.invoices["11"]
            else:
                assert path == "/v1/payments/101"
                data = gateway.payments["101"]
            return httpx.Response(200, json=data)

        monkeypatch.setattr(httpx.Client, "send", send)
        api = mp.MercadoPagoAPI()
        mp.reconcile(db, api, row)
        db.commit()
        assert row.mercadopago_status == "authorized"
        assert db.query(OrganizationSubscriptionPayment).one().status == "CONFIRMED"
        assert organization_access(db, row.organization_id, TODAY, environment=mp.MERCADOPAGO_TEST).has_access

        gateway.payments["101"]["collector_id"] = 23
        with pytest.raises(mp.MercadoPagoError, match="resposta_fora_do_sandbox"):
            api.get("payment", 101)


def test_invoice_search_follows_provider_pagination(settings, monkeypatch):
    offsets = []

    def page(method, path, *, params):
        assert "limit" not in params
        offset = params.get("offset", 0)
        offsets.append(offset)
        invoices = [{"id": i} for i in range(offset, min(offset + 12, 13))]
        return {"results": invoices, "paging": {"offset": offset, "limit": 12, "total": 13}}

    api = mp.MercadoPagoAPI()
    monkeypatch.setattr(api, "request", page)
    results = list(api.search("/authorized_payments/search", preapproval_id="TESTID"))
    assert [row["id"] for row in results] == list(range(13))
    assert offsets == [0, 12]


def test_customer_checkout_uses_the_sisfarm_screen_and_redirects_to_the_sandbox(database, settings, monkeypatch):
    from jinja2 import ChoiceLoader, DictLoader

    monkeypatch.setattr(routes, "validate_csrf", lambda request, token: None)
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda *args: {routes.WRITE_FINANCE})
    monkeypatch.setattr(routes, "record_commercial_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(routes, "_base_context", lambda *args, **kwargs: {
        **kwargs, "subscription_cycles": routes.SUBSCRIPTION_CYCLES, "cycle_unit_labels": routes.CYCLE_UNIT_LABELS,
        "csrf_token": "token",
    })
    monkeypatch.setattr(routes, "_repository", lambda db: None)
    monkeypatch.setattr(routes, "_entitlements_for", lambda *args: SimpleNamespace(trial_ends_on=None, trial_ended=False))
    monkeypatch.setattr(routes, "today_in_app_timezone", lambda: TODAY)
    monkeypatch.setattr(routes, "_plan_limit_issues", lambda *args: [])
    env = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({"base.html": "{% block content %}{% endblock %}"}), routes.templates.env.loader,
    ]))
    monkeypatch.setattr(routes.templates, "TemplateResponse", lambda name, context: SimpleNamespace(
        text=env.get_template(name).render(context),
    ))
    with database() as db:
        org = Organization(name="Fazenda Teste", slug="fazenda", is_active=True)
        plan = SubscriptionPlan(code="teste", name="Essencial", status="ativo", price_monthly=99,
                                price_quarterly=250, price_yearly=990)
        db.add_all([org, plan])
        db.commit()
        user = SimpleNamespace(organization=org, organization_id=org.id, email="financeiro@example.com", phone=None)
        page = routes.subscribe_page(SimpleNamespace(), db, user, "token", str(plan.id))
        assert "Mercado Pago" in page.text and 'value="YEARLY"' in page.text
        assert 'value="QUARTERLY"' not in page.text and 'name="cpf_cnpj"' not in page.text

        def request(method, path, *, payload):
            assert payload["payer_email"] == "test@example.com"
            assert payload["auto_recurring"]["frequency"] == 12
            row = db.query(OrganizationSubscription).one()
            return remote_subscription(row, "pending")

        monkeypatch.setattr(mp, "MercadoPagoAPI", lambda settings: SimpleNamespace(request=request))
        response = routes.subscribe_action(
            SimpleNamespace(), csrf_token="token", plan_id=str(plan.id), cycle="YEARLY", billing_type="",
            person_type="FISICA", name="", cpf_cnpj="", email="test@example.com", mobile_phone="",
            postal_code="", address_number="", accepted_terms="1", db=db, user=user,
        )
        assert response.status_code == 303 and response.headers["location"] == LINK
        row = db.query(OrganizationSubscription).one()
        assert row.cycle == "YEARLY" and row.environment == mp.MERCADOPAGO_TEST
        assert routes._customer_subscription_display(row, None, [], TODAY)["payment_url"] == LINK


@pytest.mark.parametrize(("response_body", "status_code", "expected_reason", "message_fragment"), [
    ({"message": "Both payer and collector must be real or test users"}, 400,
     "ambiente_do_comprador", "mesmo ambiente de teste"),
    ({"cause": [{"description": "Invalid payer_email"}]}, 422,
     "email_do_comprador", "e-mail informado"),
    ({"message": "The payer and collector must be different"}, 400,
     "comprador_igual_ao_vendedor", "contas diferentes"),
    ({"message": "Restricted token test@example.com"}, 403,
     None, "recusou a credencial"),
    ({"code": "PA_UNAUTHORIZED_RESULT_FROM_POLICIES"}, 403,
     "politica_do_mercado_pago", "política de acesso"),
    ({"error": "invalid_credentials"}, 403,
     "credencial_invalida", "recusou o Access Token"),
])
def test_checkout_reports_provider_rejections_without_exposing_response(
    database, settings, monkeypatch, caplog, response_body, status_code, expected_reason, message_fragment,
):
    def send(client, request, **kwargs):
        return httpx.Response(status_code, headers={"x-request-id": "request-123"},
                              json={**response_body, "context": "secret-context test@example.com"})

    monkeypatch.setattr(httpx.Client, "send", send)
    with database() as db:
        row = prepare(db)
        with pytest.raises(mp.MercadoPagoError) as raised:
            mp.create_checkout(db, mp.MercadoPagoAPI(), row)

        error = raised.value
        assert error.reason == expected_reason
        message = mp.checkout_error_message(error)
        assert message_fragment in message
        assert f"http_{status_code}" in message and "request-123" in message
        assert row.status == "falhou" and not row.is_current
        assert f"http_{status_code}" in row.sync_issue and "request-123" in row.sync_issue
        if expected_reason:
            assert expected_reason in row.sync_issue
        for output in (message, row.sync_issue, caplog.text):
            assert "test@example.com" not in output
            assert "secret-context" not in output
            assert "TEST-token" not in output
