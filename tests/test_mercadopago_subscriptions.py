import hashlib
import hmac
from datetime import date, timedelta
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


@pytest.mark.parametrize("environment", [mp.MERCADOPAGO_TEST, mp.MERCADOPAGO_LIVE])
def test_customer_checkout_uses_the_sisfarm_screen_and_redirects_to_the_provider(database, settings, monkeypatch, request, environment):
    from jinja2 import ChoiceLoader, DictLoader

    monkeypatch.setattr(routes, "validate_csrf", lambda request, token: None)
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda *args: {routes.WRITE_FINANCE})
    monkeypatch.setattr(routes, "record_commercial_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(routes, "_base_context", lambda *args, **kwargs: {
        **kwargs, "subscription_cycles": routes.SUBSCRIPTION_CYCLES, "cycle_unit_labels": routes.CYCLE_UNIT_LABELS,
        "csrf_token": "token",
    })
    monkeypatch.setattr(routes, "_repository", lambda db: None)
    monkeypatch.setattr(routes, "_entitlements_for", lambda *args: SimpleNamespace(trial_ends_on=TODAY, trial_ended=False))
    monkeypatch.setattr(routes, "today_in_app_timezone", lambda: TODAY)
    monkeypatch.setattr(mp, "today_in_app_timezone", lambda: TODAY)
    monkeypatch.setattr(routes, "_plan_limit_issues", lambda *args: [])
    env = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({"base.html": "{% block content %}{% endblock %}"}), routes.templates.env.loader,
    ]))
    monkeypatch.setattr(routes.templates, "TemplateResponse", lambda name, context: SimpleNamespace(
        text=env.get_template(name).render(context),
    ))
    if environment == mp.MERCADOPAGO_LIVE:
        request.getfixturevalue("live_settings")
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

        def create(method, path, *, payload):
            assert payload["payer_email"] == "test@example.com"
            assert payload["auto_recurring"]["frequency"] == 12
            assert payload["auto_recurring"]["start_date"].startswith("2026-11-25T")
            row = db.query(OrganizationSubscription).one()
            return remote_subscription(row, "pending")

        monkeypatch.setattr(mp, "MercadoPagoAPI", lambda settings: SimpleNamespace(request=create))
        form = dict(csrf_token="token", plan_id=str(plan.id), cycle="YEARLY", billing_type="",
            person_type="FISICA", name="", cpf_cnpj="", email="test@example.com", mobile_phone="",
            postal_code="", address_number="", accepted_terms="1", db=db, user=user,
        )
        if environment == mp.MERCADOPAGO_LIVE:
            blocked = routes.subscribe_action(SimpleNamespace(), **form)
            assert blocked.status_code == 400
            assert db.query(OrganizationSubscription).count() == 0
            settings.mercadopago_live_checkout_enabled = True
        response = routes.subscribe_action(SimpleNamespace(), **form)
        assert response.status_code == 303 and response.headers["location"] == LINK
        row = db.query(OrganizationSubscription).one()
        assert row.cycle == "YEARLY" and row.environment == environment and row.first_due_date == date(2026, 11, 25)
        assert routes._customer_subscription_display(row, None, [], TODAY)["payment_url"] == LINK


@pytest.mark.parametrize(("response_body", "status_code", "expected_reason", "message_fragment"), [
    ({"message": "Both payer and collector must be real or test users"}, 400,
     "ambiente_do_comprador", "mesmo ambiente"),
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


@pytest.fixture
def live_settings(settings):
    settings.mercadopago_sandbox = False
    settings.mercadopago_live_enabled = True
    settings.mercadopago_live_checkout_enabled = False
    settings.mercadopago_live_seller_id = 9
    settings.mercadopago_live_access_token = "APP_USR-live-token-for-mocked-tests"
    settings.mercadopago_live_webhook_secret = "signing-live-secret-for-mocked-tests-only"
    return settings


def test_production_read_only_mode_and_payment_ownership(live_settings, monkeypatch):
    from app.services.billing_environment import MERCADOPAGO_LIVE

    payments = {"id": 88, "collector_id": 9, "live_mode": True}
    seller = {"id": 9, "site_id": "MLB", "tags": [], "email": "merchant@example.com"}
    sent = []

    def send(client, request, **kwargs):
        assert request.headers["Authorization"] == f"Bearer {live_settings.mercadopago_live_access_token}"
        assert "X-scope" not in request.headers
        sent.append((request.method, request.url.path))
        if request.url.path == "/users/me":
            return httpx.Response(200, json=seller)
        if request.url.path == "/v1/payments/88":
            return httpx.Response(200, json=payments)
        return httpx.Response(201, json={"id": "real-sub", "collector_id": 9, "status": "pending"})

    monkeypatch.setattr(httpx.Client, "send", send)
    api = mp.MercadoPagoAPI()
    assert api.environment == MERCADOPAGO_LIVE
    assert api.get("payment", 88)["id"] == 88
    with pytest.raises(mp.MercadoPagoError, match="novas_contratacoes_desativadas"):
        api.request("POST", "/preapproval", payload={"payer_email": "buyer@example.com"})
    assert all(method == "GET" for method, path in sent)
    for changes in ({"live_mode": False}, {"live_mode": True, "collector_id": 7}):
        payments.update(changes)
        with pytest.raises(mp.MercadoPagoError, match="pagamento_fora_da_conta_de_producao"):
            api.get("payment", 88)
    live_settings.mercadopago_live_checkout_enabled = True
    live_settings.app_public_url = "http://app.sisfarm.com.br"
    assert "HTTPS" in mp.checkout_configuration_error(live_settings)
    live_settings.app_public_url = "https://app.sisfarm.com.br"
    api = mp.MercadoPagoAPI()
    with pytest.raises(mp.MercadoPagoError, match="comprador_de_teste_invalido"):
        api.request("POST", "/preapproval", payload={"payer_email": "test_user_42@testuser.com"})
    assert api.request("POST", "/preapproval", payload={"payer_email": "buyer@example.com"})["id"] == "real-sub"
    live_settings.mercadopago_live_seller_id = 7
    with pytest.raises(mp.MercadoPagoError, match="vendedor_de_producao_nao_confirmado"):
        mp.MercadoPagoAPI().get("payment", 88)
    live_settings.mercadopago_live_seller_id = 9
    seller.update(tags=["test_user"])
    with pytest.raises(mp.MercadoPagoError, match="vendedor_de_producao_nao_confirmado"):
        mp.MercadoPagoAPI().request("POST", "/preapproval", payload={"payer_email": "buyer@example.com"})
    live_settings.mercadopago_sandbox = True
    assert "somente um ambiente" in mp.configuration_error(live_settings)


def test_production_cancellation_keeps_paid_period_and_reconciles_later_refunds(database, live_settings, monkeypatch):
    from app.services.billing_environment import MERCADOPAGO_LIVE
    from app.services import entitlements

    monkeypatch.setattr(entitlements, "billing_environment", lambda: MERCADOPAGO_LIVE)
    monkeypatch.setattr(mp, "today_in_app_timezone", lambda: TODAY)
    monkeypatch.setattr(routes, "today_in_app_timezone", lambda: TODAY)
    monkeypatch.setattr(live_settings, "support_email", None, raising=False)
    with database() as db:
        row = prepare(db)
        row.environment = MERCADOPAGO_LIVE
        row.mercadopago_reference = "sisfarm-live-cancel-test"
        row.mercadopago_id = "TESTID"
        row.mercadopago_status = "authorized"
        db.commit()
        gateway = Gateway(row, "authorized")
        gateway.environment = MERCADOPAGO_LIVE
        gateway.charge(11, 101)
        mp.reconcile(db, gateway, row)
        db.commit()
        assert entitlements.compute_entitlements(db, row.organization_id, TODAY).billing_state == "ok"
        confirmation = routes._customer_subscription_context(db, row.organization)
        assert confirmation["cancellation_access_through"] == date(2026, 12, 23)
        assert confirmation["cancellation_period_end"] == date(2026, 12, 24)
        mp.change_status(gateway, row, "canceled")
        db.commit()
        assert not row.is_current
        cancellation = routes._customer_subscription_context(db, row.organization)
        assert cancellation["paid_last_day"] == confirmation["cancellation_access_through"]
        assert cancellation["paid_until"] == confirmation["cancellation_period_end"]
        assert organization_access(db, row.organization_id, date(2026, 12, 23), environment=MERCADOPAGO_LIVE).has_access
        assert not organization_access(db, row.organization_id, date(2026, 12, 24), environment=MERCADOPAGO_LIVE).has_access

        gateway.payments["101"]["status_detail"] = "partially_refunded"
        gateway.payments["101"]["transaction_amount_refunded"] = 10
        mp.reconcile(db, gateway, row)
        db.commit()
        charge = db.query(OrganizationSubscriptionPayment).one()
        assert charge.status == "CONFIRMED" and charge.refunded_amount == Decimal("10")
        assert organization_access(db, row.organization_id, TODAY, environment=MERCADOPAGO_LIVE).has_access

        gateway.payments["101"]["status"] = "refunded"
        row.last_synced_at -= timedelta(hours=7)
        db.commit()
        monkeypatch.setattr(mp, "MercadoPagoAPI", lambda: gateway)
        mp._tick()
        db.refresh(charge)
        assert charge.status == "REFUNDED" and charge.refunded_amount == Decimal("99") and charge.paid_date == TODAY
        assert not organization_access(db, row.organization_id, TODAY, environment=MERCADOPAGO_LIVE).has_access


def test_external_cancellation_is_kept_when_invoice_api_fails(database, settings):
    with database() as db:
        row = prepare(db)
        gateway = Gateway(row, "authorized")
        mp.process_event(db, gateway, "subscription_preapproval", "TESTID")
        db.commit()
        gateway.subscription.update(status="cancelled", version=2)

        def broken_invoice_search(*args, **kwargs):
            raise mp.MercadoPagoError("http_503", 503)

        gateway.search = broken_invoice_search
        mp.reconcile(db, gateway, row)
        db.commit()
        assert not row.is_current and row.mercadopago_status == "cancelled"
        assert "http_503" in row.sync_issue


def test_overdue_in_process_renewal_starts_grace(database, settings, monkeypatch):
    monkeypatch.setattr(mp, "today_in_app_timezone", lambda: TODAY)
    with database() as db:
        row = prepare(db)
        gateway = Gateway(row, "authorized")
        mp.process_event(db, gateway, "subscription_preapproval", "TESTID")
        db.commit()
        gateway.charge(12, 102, due="2026-11-23")
        gateway.payments["102"]["status"] = "in_process"
        assert mp.process_event(db, gateway, "payment", "102") == "OVERDUE"
        db.commit()
        assert db.query(OrganizationSubscriptionPayment).one().due_date == date(2026, 11, 23)


def test_production_unconfirmed_renewal_cannot_extend_access_indefinitely(database, live_settings, monkeypatch):
    from app.services import entitlements

    monkeypatch.setattr(entitlements, "grace_days", lambda: 10)
    with database() as db:
        row = prepare(db, cycle="YEARLY")
        row.environment = mp.MERCADOPAGO_LIVE
        db.commit()
        gateway = Gateway(row, "authorized")
        gateway.environment = mp.MERCADOPAGO_LIVE
        gateway.charge(11, 101, amount="990")
        mp.reconcile(db, gateway, row)
        db.commit()
        assert entitlements.compute_entitlements(db, row.organization_id, date(2027, 11, 24)).billing_state == "ok"
        assert entitlements.compute_entitlements(db, row.organization_id, date(2027, 11, 25)).billing_state == "aviso"
        overdue = entitlements.compute_entitlements(db, row.organization_id, date(2027, 12, 5))
        assert overdue.billing_state == "restrito" and overdue.overdue_amount == 0

        gateway.charge(12, 102, amount="990", due="2027-11-24")
        mp.reconcile(db, gateway, row)
        db.commit()
        assert entitlements.compute_entitlements(db, row.organization_id, date(2027, 12, 5)).billing_state == "ok"


def test_webhook_routes_keep_environments_separate(database, live_settings, monkeypatch):
    app = FastAPI()
    app.include_router(mercadopago_webhook.router)

    def override_db():
        with database() as db:
            yield db

    app.dependency_overrides[mercadopago_webhook.get_db] = override_db
    body = {"type": "payment", "id": 41, "data": {"id": "123"}, "live_mode": True}
    manifest = b"id:123;request-id:request-prod;ts:1795521600;"
    with TestClient(app) as client:
        monkeypatch.setattr(client, "send", lambda request, **kwargs: client._transport.handle_request(request))
        def post(path, secret):
            digest = hmac.new(secret.encode(), manifest, hashlib.sha256).hexdigest()
            return client.post(path, json=body, params={"data.id": "123"},
                               headers={"x-signature": f"ts=1795521600,v1={digest}", "x-request-id": "request-prod"})
        prod_path = "/api/webhooks/mercadopago"
        assert post(TARGET, live_settings.mercadopago_webhook_secret).status_code == 503
        assert post(prod_path, live_settings.mercadopago_webhook_secret).status_code == 401
        assert post(prod_path, live_settings.mercadopago_live_webhook_secret).status_code == 200
        assert post(prod_path, live_settings.mercadopago_live_webhook_secret).status_code == 200
        with database() as db:
            event = db.query(MercadoPagoEvent).one()
            assert event.environment == mp.MERCADOPAGO_LIVE and event.state == "pending"


def test_verified_seller_does_not_poison_cache_on_transient_error(live_settings, monkeypatch):
    calls = []

    def send(client, request, **kwargs):
        calls.append((request.method, request.url.path))
        if request.url.path == "/users/me":
            if len(calls) == 1:
                raise httpx.ConnectTimeout("timeout")
            return httpx.Response(200, json={"id": 9, "site_id": "MLB", "tags": None, "email": "merchant@example.com"})
        return httpx.Response(200, json={"id": 88, "collector_id": 9, "live_mode": True})

    monkeypatch.setattr(httpx.Client, "send", send)
    api = mp.MercadoPagoAPI()
    with pytest.raises(mp.MercadoPagoError, match="falha_de_conexao"):
        api.get("payment", 88)
    assert api.get("payment", 88)["id"] == 88
    assert calls == [("GET", "/users/me"), ("GET", "/users/me"), ("GET", "/v1/payments/88")]


def test_reconcile_isolates_broken_invoice_from_other_invoices(database, live_settings, monkeypatch):
    monkeypatch.setattr(mp, "today_in_app_timezone", lambda: TODAY)
    with database() as db:
        row = prepare(db)
        row.environment = mp.MERCADOPAGO_LIVE
        row.mercadopago_reference = "sisfarm-live-iso"
        row.mercadopago_id = "TESTID"
        row.mercadopago_status = "authorized"
        db.commit()
        gateway = Gateway(row, "authorized")
        gateway.environment = mp.MERCADOPAGO_LIVE
        gateway.charge(11, 101, amount="50")  # valor divergente do plano (99)
        gateway.charge(12, 102, amount="99", due="2026-11-24")
        mp.reconcile(db, gateway, row)
        db.commit()
        payments = db.query(OrganizationSubscriptionPayment).all()
        assert len(payments) == 1 and payments[0].mercadopago_invoice_id == "12" and payments[0].status == "CONFIRMED"
        assert "valor_da_fatura_divergente" in (row.sync_issue or "")
        assert organization_access(db, row.organization_id, TODAY, environment=mp.MERCADOPAGO_LIVE).has_access


def test_cancelled_paid_period_survives_other_cancelled_attempt_and_blocks_paused_bypass(database, live_settings, monkeypatch):
    from datetime import datetime
    from app.services.access_grants import cancelled_paid_period

    monkeypatch.setattr(mp, "today_in_app_timezone", lambda: TODAY)
    with database() as db:
        row = prepare(db)
        row.environment = mp.MERCADOPAGO_LIVE
        row.mercadopago_reference = "sisfarm-live-keep"
        row.mercadopago_id = "TESTID"
        row.mercadopago_status = "authorized"
        db.commit()
        gateway = Gateway(row, "authorized")
        gateway.environment = mp.MERCADOPAGO_LIVE
        # Pagamento aprovado hoje com debit_date D+1
        gateway.charge(11, 101, due="2026-11-25", date_approved="2026-11-24")
        mp.reconcile(db, gateway, row)
        mp.change_status(gateway, row, "canceled")
        # Outra tentativa pendente sem pagamento cancelada depois (e uma com ended_at=None)
        unpaid_recent = OrganizationSubscription(
            organization_id=row.organization_id, plan_id=row.plan_id, plan_name=row.plan_name,
            amount=row.amount, cycle=row.cycle, billing_type="UNDEFINED", first_due_date=TODAY,
            environment=mp.MERCADOPAGO_LIVE, status="encerrada", is_current=False,
            mercadopago_reference="sisfarm-live-unpaid-1", mercadopago_ended_at=datetime(2099, 1, 1),
        )
        unpaid_null = OrganizationSubscription(
            organization_id=row.organization_id, plan_id=row.plan_id, plan_name=row.plan_name,
            amount=row.amount, cycle=row.cycle, billing_type="UNDEFINED", first_due_date=TODAY,
            environment=mp.MERCADOPAGO_LIVE, status="encerrada", is_current=False,
            mercadopago_reference="sisfarm-live-unpaid-2", mercadopago_ended_at=None,
        )
        db.add_all([unpaid_recent, unpaid_null])
        db.commit()
        kept, until = cancelled_paid_period(db, row.organization_id, TODAY)
        assert kept is not None and kept.id == row.id and until == date(2026, 12, 24)
        assert organization_access(db, row.organization_id, TODAY, environment=mp.MERCADOPAGO_LIVE).has_access

    # Pausa seguida de cancelamento não pode restaurar acesso
    with database() as db:
        org = Organization(name="Fazenda Pausada", slug="fazenda-pausada", is_active=True)
        plan = SubscriptionPlan(code="pausa", name="Essencial", status="ativo", price_monthly=99)
        db.add_all([org, plan])
        db.flush()
        paused_row = mp.reserve_checkout(db, organization=org, plan=plan, amount=Decimal("99"),
                                         cycle="MONTHLY", due=TODAY, email="pausa@example.com",
                                         environment=mp.MERCADOPAGO_LIVE)
        paused_row.mercadopago_id = "PAUSED1"
        paused_row.mercadopago_status = "authorized"
        db.commit()
        gw = Gateway(paused_row, "authorized")
        gw.environment = mp.MERCADOPAGO_LIVE
        gw.subscription["id"] = "PAUSED1"
        gw.charge(21, 201)
        gw.invoices["21"]["preapproval_id"] = "PAUSED1"
        mp.reconcile(db, gw, paused_row)
        mp.change_status(gw, paused_row, "paused")
        db.commit()
        assert not organization_access(db, org.id, TODAY, environment=mp.MERCADOPAGO_LIVE).has_access
        mp.change_status(gw, paused_row, "canceled")
        db.commit()
        assert paused_row.status == "encerrada_pausada"
        assert not organization_access(db, org.id, TODAY, environment=mp.MERCADOPAGO_LIVE).has_access


def test_release_unconfirmed_checkout_unblocks_and_recovers(database, live_settings):
    with database() as db:
        row = prepare(db)
        row.environment = mp.MERCADOPAGO_LIVE
        row.mercadopago_reference = "sisfarm-live-stuck"
        row.mercadopago_payer_email = "Cliente@Example.com"
        row.status = "revisao"
        db.commit()
        gateway = Gateway(row, "pending")
        gateway.environment = mp.MERCADOPAGO_LIVE
        gateway.search = lambda path, **filters: []
        assert mp.release_unconfirmed_checkout(db, gateway, row) == "liberada"
        db.commit()
        assert not row.is_current and row.status == "falhou"

        second = mp.reserve_checkout(db, organization=row.organization, plan=row.plan, amount=row.amount,
                                     cycle=row.cycle, due=TODAY, email="cliente@example.com",
                                     environment=mp.MERCADOPAGO_LIVE)
        second.status = "revisao"
        db.commit()
        gw2 = Gateway(second, "pending")
        gw2.environment = mp.MERCADOPAGO_LIVE
        assert mp.release_unconfirmed_checkout(db, gw2, second) == "recuperada"
        db.commit()
        assert second.is_current and second.mercadopago_id == "TESTID"


def test_trial_to_live_transition_month_length_notices_and_portfolio_disputes(database, live_settings, monkeypatch):
    from app.models import OrganizationAccessGrant
    from app.services import billing_notices, entitlements, subscription_portfolio

    monkeypatch.setattr(entitlements, "billing_environment", lambda: mp.MERCADOPAGO_LIVE)
    monkeypatch.setattr(entitlements, "grace_days", lambda: 10)
    monkeypatch.setattr(billing_notices, "billing_environment", lambda: mp.MERCADOPAGO_LIVE)
    monkeypatch.setattr(subscription_portfolio, "billing_environment", lambda: mp.MERCADOPAGO_LIVE)
    monkeypatch.setattr(mp, "today_in_app_timezone", lambda: TODAY)

    with database() as db:
        row = prepare(db, due=date(2026, 12, 1))
        row.environment = mp.MERCADOPAGO_LIVE
        row.mercadopago_reference = "sisfarm-live-trial"
        row.mercadopago_id = "TESTID"
        row.mercadopago_status = "authorized"
        db.add(OrganizationAccessGrant(
            organization_id=row.organization_id, plan_id=row.plan_id, reason="teste_gratis",
            justification="Teste grátis inicial.", starts_on=date(2026, 11, 15), ends_on=date(2026, 11, 30),
        ))
        db.commit()
        # No 1º dia após o teste grátis (data agendada da 1ª cobrança), não bloqueia imediatamente
        assert entitlements.compute_entitlements(db, row.organization_id, date(2026, 12, 1)).billing_state == "ok"
        # No dia seguinte sem confirmação, entra em restrito
        assert entitlements.compute_entitlements(db, row.organization_id, date(2026, 12, 2)).billing_state == "restrito"

        # Mês com 28 dias (28/02 -> 31/03) não gera falso aviso antes da next_due_date
        gateway = Gateway(row, "authorized")
        gateway.environment = mp.MERCADOPAGO_LIVE
        gateway.subscription["next_payment_date"] = "2026-03-31T07:00:00-03:00"
        gateway.charge(31, 301, due="2026-02-28")
        mp.reconcile(db, gateway, row)
        db.commit()
        assert entitlements.compute_entitlements(db, row.organization_id, date(2026, 3, 30)).billing_state == "ok"

        # Renovação atrasada sem nova fatura: envia e-mail e marca inadimplente no portfolio
        sent_emails = []
        assert billing_notices.process_organization(
            db, row.organization, date(2026, 4, 2), send=lambda *a: sent_emails.append(a),
        ) == ["renovacao_atraso"]
        assert len(sent_emails) == 1 and sent_emails[0][0] == row.mercadopago_payer_email
        assert billing_notices.process_organization(
            db, row.organization, date(2026, 4, 3), send=lambda *a: sent_emails.append(a),
        ) == []
        portfolio = subscription_portfolio.build_subscription_portfolio(db, [row.organization], date(2026, 4, 2))
        assert portfolio.rows[0].situation == "inadimplente" and portfolio.delinquent_count == 1

        # Contestação em parcela anterior (mesmo com parcela mais nova paga) gera alerta na carteira
        gateway.charge(32, 302, due="2026-03-31")
        gateway.payments["301"]["status"] = "in_mediation"
        mp.reconcile(db, gateway, row)
        db.commit()
        portfolio_dispute = subscription_portfolio.build_subscription_portfolio(db, [row.organization], date(2026, 4, 2))
        assert portfolio_dispute.rows[0].situation == "verificar"
        assert "contestação" in (portfolio_dispute.rows[0].attention_reason or "").lower()


def test_premature_payment_webhook_retries_before_marking_sem_fatura(database, settings):
    with database() as db:
        row = prepare(db)
        row.mercadopago_id = "TESTID"
        row.mercadopago_status = "authorized"
        db.add(MercadoPagoEvent(
            environment=mp.MERCADOPAGO_TEST, topic="payment", resource_id="101",
            delivery_key="dk-1", request_key="rk-1",
        ))
        db.commit()
        gateway = Gateway(row, "authorized")
    gateway.payments["101"] = {
        "id": 101, "status": "approved", "currency_id": "BRL", "transaction_amount": "99",
        "live_mode": False, "payment_type_id": "credit_card", "date_approved": "2026-11-24",
    }
    mp._process_events(gateway)
    with database() as db:
        ev = db.query(MercadoPagoEvent).one()
        assert ev.state == "pending" and ev.attempts == 1 and ev.result == "fatura_ainda_nao_vinculada"
        ev.retry_at = mp._now()
        db.commit()
    gateway.charge(11, 101)
    mp._process_events(gateway)
    with database() as db:
        ev = db.query(MercadoPagoEvent).one()
        assert ev.state == "processed" and ev.attempts == 2 and ev.result == "CONFIRMED"


def test_admin_subscriptions_page_renders_grant_modal_handlers_in_live_mode(database, live_settings, monkeypatch):
    from jinja2 import ChoiceLoader, DictLoader

    monkeypatch.setattr(routes, "_require_super_admin", lambda *args: None)
    live_settings.asaas_webhook_token = ""
    monkeypatch.setattr(routes, "_base_context", lambda *args, **kwargs: {
        **kwargs, "subscription_cycles": routes.SUBSCRIPTION_CYCLES, "cycle_unit_labels": routes.CYCLE_UNIT_LABELS,
        "csrf_token": "token",
    })
    monkeypatch.setattr(routes, "today_in_app_timezone", lambda: TODAY)
    env = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({"base.html": "{% block content %}{% endblock %}"}), routes.templates.env.loader,
    ]))
    monkeypatch.setattr(routes.templates, "TemplateResponse", lambda name, context: SimpleNamespace(
        text=env.get_template(name).render(context),
    ))
    with database() as db:
        row = prepare(db)
        row.environment = mp.MERCADOPAGO_LIVE
        row.status = "revisao"
        db.commit()
        monkeypatch.setattr(routes, "_repository", lambda _db: SimpleNamespace(list_organizations=lambda: [row.organization]))
        page = routes.subscriptions_page(SimpleNamespace(), organizacao=row.organization_id, db=db, user=SimpleNamespace(), csrf_token="token")
        assert "data-dialog-open" in page.text and "data-grant-days" in page.text
        assert "Liberar acesso manualmente" in page.text
        assert "Liberar tentativa" in page.text
        assert "Mercado Pago · Produção" in page.text and "bg-emerald-50" in page.text


def test_start_date_uses_utc_z_format_and_asaas_stays_blocked_in_production(database, settings, monkeypatch):
    from app.services.asaas_environment import asaas_configuration_error

    monkeypatch.setattr(mp, "today_in_app_timezone", lambda: TODAY)
    with database() as db:
        row = prepare(db, due=date(2026, 12, 1))
        gateway = Gateway(row)
        mp.create_checkout(db, gateway, row)
        start_date = gateway.calls[0][2]["auto_recurring"]["start_date"]
        assert start_date.endswith(".000Z") and start_date.startswith("2026-12-01T")
    prod_fallback = SimpleNamespace(
        is_production=True, mercadopago_sandbox=False, mercadopago_live_enabled=False,
        asaas_environment="sandbox", asaas_api_base_url="https://api-sandbox.asaas.com",
        asaas_api_key="key", asaas_live_enabled=False,
    )
    assert "ASAAS_LIVE_ENABLED" in (asaas_configuration_error(prod_fallback) or "")
