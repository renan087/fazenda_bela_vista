import base64
import hashlib
import hmac
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.path_access import required_permissions_for_web_path
from app.core.permissions_catalog import PAGE_FINANCE, WRITE_FINANCE
from app.db.base import Base
from app.models import (
    MercadoPagoEvent,
    Organization,
    OrganizationSubscription,
    OrganizationSubscriptionPayment,
    SubscriptionNotice,
    SubscriptionPlan,
    User,
)
from app.routers import mercadopago_webhook
from app.services import asaas_environment, billing_environment, billing_notices
from app.services import mercadopago_pix_orders as mp_pix
from app.services import mercadopago_subscriptions as mp
from app.services.access_grants import organization_access
from app.services.entitlements import compute_entitlements
from app.services.plan_usage import confirmed_plan_for_usage
from app.web import routes

PIX_WEBHOOK_TEST = "/api/webhooks/mercadopago/pix/test"
PIX_WEBHOOK_LIVE = "/api/webhooks/mercadopago/pix"
TODAY = date(2026, 11, 26)
SAMPLE_QR_CODE = "00020126580014br.gov.bcb.pix0136f8a9c0d1-1234-4567-89ab-cdef012345675204000053039865802BR5910SISFARM6009SAO PAULO62070503***6304ABCD"
# PNG 1x1 válido em base64 para validar decodificação e MIME
SAMPLE_PNG_B64 = base64.b64encode(
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
).decode("ascii")


def _settings(**overrides):
    defaults = dict(
        app_public_url="https://app.sisfarm.com.br",
        support_email="suporte@sisfarm.com.br",
        mercadopago_sandbox=True,
        mercadopago_live_enabled=False,
        mercadopago_live_checkout_enabled=False,
        mercadopago_live_seller_id=82986976,
        mercadopago_access_token="TEST-card-token",
        mercadopago_public_key="TEST-card-public",
        mercadopago_webhook_secret="card-test-secret",
        mercadopago_live_access_token="APP_USR-card-live-token",
        mercadopago_live_public_key="APP_USR-card-live-public",
        mercadopago_live_webhook_secret="card-live-secret-32-chars-minimum-0001",
        mercadopago_pix_enabled=False,
        mercadopago_pix_application_id="7441327382083069",
        mercadopago_pix_seller_id=82986976,
        mercadopago_pix_access_token="APP_USR-pix-live-token-0001",
        mercadopago_pix_public_key="APP_USR-pix-live-public-0001",
        mercadopago_pix_webhook_secret="pix-live-webhook-secret-32-chars-min-01",
        mercadopago_pix_test_access_token="TEST-pix-sandbox-token-0001",
        mercadopago_pix_test_public_key="TEST-pix-sandbox-public-0001",
        mercadopago_pix_test_webhook_secret="pix-test-webhook-secret-32-chars-min-01",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


@pytest.fixture()
def pix_settings(monkeypatch):
    value = _settings()
    for module in (mp_pix, mp, mercadopago_webhook, billing_environment, asaas_environment, routes):
        monkeypatch.setattr(module, "get_settings", lambda: value)
    return value


@pytest.fixture()
def database(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'mercadopago_pix.db'}")
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine, autoflush=False)
    monkeypatch.setattr(mp_pix, "SessionLocal", sessions)
    monkeypatch.setattr(mp, "SessionLocal", sessions)
    try:
        yield sessions
    finally:
        engine.dispose()


def _sign(secret: str, order_id: str, request_id: str, ts: str = "1732620000") -> str:
    manifest = f"id:{order_id.lower()};request-id:{request_id};ts:{ts};"
    digest = hmac.new(secret.encode(), manifest.encode(), hashlib.sha256).hexdigest()
    return f"ts={ts},v1={digest}"


def _make_org_and_plans(db):
    org = Organization(name="Fazenda Bela Vista", slug="fazenda-bela-vista", is_active=True)
    basic = SubscriptionPlan(
        code="essencial",
        name="Essencial",
        status="ativo",
        price_monthly=Decimal("99.00"),
        price_yearly=Decimal("990.00"),
    )
    pro = SubscriptionPlan(
        code="profissional",
        name="Profissional",
        status="ativo",
        price_monthly=Decimal("199.00"),
        price_yearly=Decimal("1990.00"),
    )
    db.add_all([org, basic, pro])
    db.flush()
    admin = User(
        name="Produtor Admin",
        email="produtor@fazenda.com.br",
        hashed_password="x",
        organization_id=org.id,
        is_admin=True,
        is_active=True,
    )
    db.add(admin)
    db.flush()
    return org, basic, pro, admin


class FakePixOrdersAPI:
    """Gateway simulado da API de Orders (/v1/orders) para testes determinísticos."""

    def __init__(self, environment=billing_environment.MERCADOPAGO_TEST):
        self.environment = environment
        self.orders: dict[str, dict] = {}
        self.calls: list[tuple] = []
        self._seq = 0
        self.fail_create_with: Exception | None = None

    def _build_order(
        self,
        *,
        order_id: str,
        amount: Decimal,
        external_reference: str,
        status: str = "action_required",
        status_detail: str = "waiting_payment",
        payment_status: str = "action_required",
        refunds: list[dict] | None = None,
        qr_b64: str | None = SAMPLE_PNG_B64,
        date_approved: str | None = None,
        expires_at: str | None = None,
    ) -> dict:
        created = datetime.now(timezone.utc)
        exp = expires_at or (created + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
        return {
            "id": order_id,
            "type": "online",
            "processing_mode": "automatic",
            "external_reference": external_reference,
            "total_amount": f"{Decimal(amount):.2f}",
            "country_code": "BRA",
            "user_id": "82986976",
            "status": status,
            "status_detail": status_detail,
            "created_date": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "last_updated_date": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "integration_data": {"application_id": "7441327382083069"},
            "transactions": {
                "payments": [
                    {
                        "id": f"PAY{order_id}",
                        "amount": f"{Decimal(amount):.2f}",
                        "status": payment_status,
                        "date_approved": date_approved,
                        "date_of_expiration": exp,
                        "expiration_time": "PT24H",
                        "payment_method": {
                            "id": "pix",
                            "type": "bank_transfer",
                            "qr_code": SAMPLE_QR_CODE,
                            "qr_code_base64": qr_b64,
                            "ticket_url": f"https://www.mercadopago.com.br/payments/{order_id}/ticket",
                        },
                    }
                ],
                "refunds": refunds or [],
            },
        }

    def create_pix_order(
        self,
        *,
        amount: Decimal,
        external_reference: str,
        payer_email: str,
        description: str,
        idempotency_key: str,
        expiration_time: str = "PT24H",
    ) -> dict:
        self.calls.append(("POST", "/v1/orders", external_reference, idempotency_key, str(amount)))
        if self.fail_create_with is not None:
            raise self.fail_create_with
        self._seq += 1
        order_id = f"ORD01KPIX{self._seq:04d}ABCDEF"
        order = self._build_order(
            order_id=order_id,
            amount=amount,
            external_reference=external_reference,
        )
        self.orders[order_id] = order
        return order.copy()

    def get_order(self, order_id: str) -> dict:
        self.calls.append(("GET", f"/v1/orders/{order_id}"))
        if order_id not in self.orders:
            raise mp.MercadoPagoError("http_404", 404)
        return self.orders[order_id].copy()

    def search_orders_by_reference(self, external_reference: str, *, since: datetime, until: datetime) -> list[dict]:
        self.calls.append(("GET", "/v1/orders", external_reference))
        return [o.copy() for o in self.orders.values() if o.get("external_reference") == external_reference]

    def cancel_order(self, order_id: str, *, idempotency_key: str) -> dict:
        self.calls.append(("POST", f"/v1/orders/{order_id}/cancel", idempotency_key))
        order = self.orders[order_id]
        order["status"] = "canceled"
        order["status_detail"] = "canceled"
        order["transactions"]["payments"][0]["status"] = "canceled"
        return order.copy()

    def refund_order(
        self,
        order_id: str,
        *,
        idempotency_key: str,
        payment_id: str | None = None,
        amount: Decimal | None = None,
    ) -> dict:
        self.calls.append(("POST", f"/v1/orders/{order_id}/refund", idempotency_key, payment_id, str(amount) if amount else "total"))
        order = self.orders[order_id]
        total = Decimal(order["total_amount"])
        if amount is None or Decimal(amount) >= total:
            order["status"] = "refunded"
            order["status_detail"] = "refunded"
            order["transactions"]["payments"][0]["status"] = "refunded"
            order["transactions"]["refunds"].append({"id": "REF1", "status": "processed", "amount": f"{total:.2f}"})
        else:
            order["status"] = "processed"
            order["status_detail"] = "partially_refunded"
            order["transactions"]["refunds"].append({"id": "REF2", "status": "processed", "amount": f"{Decimal(amount):.2f}"})
        return order.copy()

    def mark_paid(self, order_id: str, paid_on: date = TODAY):
        order = self.orders[order_id]
        order["status"] = "processed"
        order["status_detail"] = "accredited"
        order["transactions"]["payments"][0]["status"] = "processed"
        order["transactions"]["payments"][0]["date_approved"] = f"{paid_on.isoformat()}T12:00:00-03:00"
        return order

    def mark_expired(self, order_id: str):
        order = self.orders[order_id]
        order["status"] = "expired"
        order["status_detail"] = "expired"
        order["transactions"]["payments"][0]["status"] = "expired"
        return order


def test_production_pix_disabled_by_default_and_isolated_from_test(pix_settings):
    # Em modo teste (MERCADOPAGO_SANDBOX=True), Pix teste funciona e Pix produção está bloqueado.
    assert mp_pix.pix_checkout_available(pix_settings) is True
    assert mp_pix.pix_webhook_available(billing_environment.MERCADOPAGO_TEST, pix_settings) is True
    assert mp_pix.pix_webhook_available(billing_environment.MERCADOPAGO_LIVE, pix_settings) is False

    # Em produção (MERCADOPAGO_SANDBOX=False, MERCADOPAGO_LIVE_ENABLED=True), mas com MERCADOPAGO_PIX_ENABLED=False (padrão seguro):
    # checkout e webhook de produção permanecem desligados.
    pix_settings.mercadopago_sandbox = False
    pix_settings.mercadopago_live_enabled = True
    pix_settings.mercadopago_live_checkout_enabled = True
    assert mp_pix.pix_checkout_available(pix_settings) is False
    assert mp_pix.pix_webhook_available(billing_environment.MERCADOPAGO_LIVE, pix_settings) is False
    assert "desativado" in (mp_pix.pix_configuration_error(billing_environment.MERCADOPAGO_LIVE, pix_settings) or "").lower()

    # Somente quando MERCADOPAGO_PIX_ENABLED=True em produção, libera produção e fecha webhook de teste.
    pix_settings.mercadopago_pix_enabled = True
    assert mp_pix.pix_checkout_available(pix_settings) is True
    assert mp_pix.pix_webhook_available(billing_environment.MERCADOPAGO_LIVE, pix_settings) is True
    assert mp_pix.pix_webhook_available(billing_environment.MERCADOPAGO_TEST, pix_settings) is False


def test_api_client_validates_seller_application_and_uses_orders_endpoint_only(pix_settings, monkeypatch):
    pix_settings.mercadopago_sandbox = False
    pix_settings.mercadopago_live_enabled = True
    pix_settings.mercadopago_pix_enabled = True
    captured = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append((request.method, request.url.path, dict(request.headers)))
        if request.url.path == "/users/me":
            return httpx.Response(200, json={"id": 82986976, "site_id": "MLB", "email": "financeiro@sisfarm.com.br", "tags": []})
        if request.method == "POST" and request.url.path == "/v1/orders":
            return httpx.Response(201, json={
                "id": "ORD01LIVE001",
                "external_reference": "sisfarm-pix-live-abc",
                "total_amount": "99.00",
                "country_code": "BRA",
                "user_id": "82986976",
                "status": "action_required",
                "integration_data": {"application_id": "7441327382083069"},
                "transactions": {"payments": [{"id": "PAY1", "status": "action_required", "payment_method": {"id": "pix", "qr_code": SAMPLE_QR_CODE}}]},
            })
        raise AssertionError(f"Endpoint inesperado: {request.method} {request.url.path}")

    monkeypatch.setattr(httpx.Client, "send", lambda self, request, **kw: handler(request))

    api = mp_pix.MercadoPagoPixOrdersAPI(billing_environment.MERCADOPAGO_LIVE, pix_settings)
    order = api.create_pix_order(
        amount=Decimal("99.00"),
        external_reference="sisfarm-pix-live-abc",
        payer_email="produtor@fazenda.com.br",
        description="SiSFarm - Assinatura Essencial",
        idempotency_key="sisfarm-pix-live-abc",
    )
    assert order["id"] == "ORD01LIVE001"
    paths = [item[1] for item in captured]
    assert paths == ["/users/me", "/v1/orders"]
    assert captured[1][2]["x-idempotency-key"] == "sisfarm-pix-live-abc"


def test_api_client_rejects_wrong_seller_or_application_in_production(pix_settings, monkeypatch):
    pix_settings.mercadopago_sandbox = False
    pix_settings.mercadopago_live_enabled = True
    pix_settings.mercadopago_pix_enabled = True

    def wrong_seller_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"id": 99999999, "site_id": "MLB", "email": "outro@empresa.com", "tags": []})

    monkeypatch.setattr(httpx.Client, "send", lambda self, request, **kw: wrong_seller_handler(request))
    api = mp_pix.MercadoPagoPixOrdersAPI(billing_environment.MERCADOPAGO_LIVE, pix_settings)
    with pytest.raises(mp.MercadoPagoError, match="vendedor_pix_nao_confirmado"):
        api.get_order("ORD01XYZ")

    def wrong_app_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/users/me":
            return httpx.Response(200, json={"id": 82986976, "site_id": "MLB", "email": "financeiro@sisfarm.com.br", "tags": []})
        return httpx.Response(200, json={
            "id": "ORD01XYZ",
            "user_id": "82986976",
            "country_code": "BRA",
            "integration_data": {"application_id": "1111111111111111"},
        })

    monkeypatch.setattr(httpx.Client, "send", lambda self, request, **kw: wrong_app_handler(request))
    api2 = mp_pix.MercadoPagoPixOrdersAPI(billing_environment.MERCADOPAGO_LIVE, pix_settings)
    with pytest.raises(mp.MercadoPagoError, match="order_fora_da_conta"):
        api2.get_order("ORD01XYZ")


def test_pix_webhook_signature_lowercase_id_dedup_and_env_isolation(database, pix_settings, monkeypatch):
    app = FastAPI()
    app.include_router(mercadopago_webhook.router)

    def override_db():
        with database() as session:
            yield session

    app.dependency_overrides[mercadopago_webhook.get_db] = override_db
    with TestClient(app) as client:
        monkeypatch.setattr(client, "send", lambda request, **kwargs: client._transport.handle_request(request))

        order_id = "ORD01UPPERCASE123"
        req_id = "req-pix-001"
        sig = _sign(pix_settings.mercadopago_pix_test_webhook_secret, order_id, req_id)
        payload = {"id": "evt-1", "type": "order", "action": "order.processed", "date_created": "2026-11-26T10:00:00Z", "data": {"id": order_id}}

        # Webhook de produção bloqueado (503) enquanto MERCADOPAGO_PIX_ENABLED=False
        live_res = client.post(
            f"{PIX_WEBHOOK_LIVE}?data.id={order_id}",
            json=payload,
            headers={"x-request-id": req_id, "x-signature": _sign(pix_settings.mercadopago_pix_webhook_secret, order_id, req_id)},
        )
        assert live_res.status_code == 503

        # Segredo errado (ex.: segredo de produção no endpoint de teste) -> 401
        wrong_sig = client.post(
            f"{PIX_WEBHOOK_TEST}?data.id={order_id}",
            json=payload,
            headers={"x-request-id": req_id, "x-signature": _sign(pix_settings.mercadopago_pix_webhook_secret, order_id, req_id)},
        )
        assert wrong_sig.status_code == 401

        # Assinatura válida (data.id em minúsculas no manifest HMAC) -> 200 e enfileira 1 vez
        ok1 = client.post(
            f"{PIX_WEBHOOK_TEST}?data.id={order_id}",
            json=payload,
            headers={"x-request-id": req_id, "x-signature": sig},
        )
        ok2 = client.post(
            f"{PIX_WEBHOOK_TEST}?data.id={order_id}",
            json=payload,
            headers={"x-request-id": req_id, "x-signature": sig},
        )
        assert ok1.status_code == 200 and ok2.status_code == 200

        with database() as db:
            events = db.query(MercadoPagoEvent).filter_by(environment=mp_pix.PIX_EVENT_TEST).all()
            assert len(events) == 1
            assert events[0].topic == "order"
            assert events[0].resource_id == order_id


def test_full_pix_lifecycle_contract_confirm_renew_change_plan_and_refund(database, pix_settings, monkeypatch):
    monkeypatch.setattr(mp_pix, "today_in_app_timezone", lambda: TODAY)
    api = FakePixOrdersAPI(billing_environment.MERCADOPAGO_TEST)

    with database() as db:
        org, basic, pro, _admin = _make_org_and_plans(db)
        sub, pay1, reused = mp_pix.reserve_pix_checkout(
            db,
            organization=org,
            plan=basic,
            amount=Decimal("99.00"),
            cycle="MONTHLY",
            due=TODAY,
            email="produtor@fazenda.com.br",
            environment=billing_environment.MERCADOPAGO_TEST,
        )
        assert not reused
        db.commit()

        mp_pix.create_pix_order_for_payment(db, api, pay1)
        assert pay1.mercadopago_order_id == "ORD01KPIX0001ABCDEF"
        assert pay1.status == "PENDING"
        assert pay1.pix_qr_code == SAMPLE_QR_CODE
        assert mp_pix.pix_qr_data_uri(pay1).startswith("data:image/png;base64,")

        # Sem confirmação real na API, organização ainda NÃO tem acesso
        assert not organization_access(db, org.id, TODAY, environment=billing_environment.MERCADOPAGO_TEST).has_access
        assert confirmed_plan_for_usage(db, org.id, TODAY) is None

        # Prevenção de duplicidade: segundo clique no mesmo plano/ciclo reaproveita a order pendente
        sub_dup, pay_dup, reused2 = mp_pix.reserve_pix_checkout(
            db,
            organization=org,
            plan=basic,
            amount=Decimal("99.00"),
            cycle="MONTHLY",
            due=TODAY,
            email="produtor@fazenda.com.br",
            environment=billing_environment.MERCADOPAGO_TEST,
        )
        assert reused2 is True
        assert sub_dup.id == sub.id and pay_dup.id == pay1.id

        # Confirmação via webhook worker (consulta GET /v1/orders/{id} antes de liberar)
        api.mark_paid(pay1.mercadopago_order_id, paid_on=TODAY)
        result = mp_pix.process_order_event(db, api, pay1.mercadopago_order_id)
        db.commit()
        assert result == "CONFIRMED"
        assert sub.mercadopago_status == "authorized"
        assert sub.next_due_date == date(2026, 12, 26)
        assert organization_access(db, org.id, TODAY, environment=billing_environment.MERCADOPAGO_TEST).has_access
        assert confirmed_plan_for_usage(db, org.id, TODAY).id == basic.id
        ent = compute_entitlements(db, org.id, TODAY)
        assert ent.billing_state == "ok" and not ent.read_only

        # Renovação antecipada (dentro da janela de 7 dias): preserva dias já pagos somando a partir de next_due_date
        renew_day = date(2026, 12, 22)
        monkeypatch.setattr(mp_pix, "today_in_app_timezone", lambda: renew_day)
        assert mp_pix.can_renew_pix_now(sub, renew_day) is True
        pay2, reused_r, to_cancel = mp_pix.reserve_pix_charge_for_subscription(
            db, subscription=sub, plan=basic, amount=Decimal("99.00"), cycle="MONTHLY", charge_kind="renovacao",
        )
        assert not reused_r and to_cancel == []
        db.commit()
        mp_pix.create_pix_order_for_payment(db, api, pay2)
        api.mark_paid(pay2.mercadopago_order_id, paid_on=renew_day)
        mp_pix.process_order_event(db, api, pay2.mercadopago_order_id)
        db.commit()
        assert sub.next_due_date == date(2027, 1, 26)

        # Alteração de plano para Profissional anual via Pix: mantém Essencial até confirmação
        pay3, reused_u, _ = mp_pix.reserve_pix_charge_for_subscription(
            db, subscription=sub, plan=pro, amount=Decimal("1990.00"), cycle="YEARLY", charge_kind="alteracao_plano",
        )
        assert not reused_u
        db.commit()
        mp_pix.create_pix_order_for_payment(db, api, pay3)
        assert sub.plan_id == basic.id  # Ainda não mudou antes do pagamento

        api.mark_paid(pay3.mercadopago_order_id, paid_on=renew_day)
        mp_pix.process_order_event(db, api, pay3.mercadopago_order_id)
        db.commit()
        assert sub.plan_id == pro.id
        assert sub.cycle == "YEARLY"
        assert sub.next_due_date == date(2027, 12, 22)

        # Estorno parcial mantém acesso e registra refunded_amount
        status_partial = mp_pix.refund_pix_payment(db, api, pay3, amount=Decimal("100.00"))
        db.commit()
        assert status_partial == "CONFIRMED"
        assert pay3.refunded_amount == Decimal("100.00")
        assert organization_access(db, org.id, renew_day, environment=billing_environment.MERCADOPAGO_TEST).has_access

        # Estorno total marca REFUNDED
        status_full = mp_pix.refund_pix_payment(db, api, pay3)
        db.commit()
        assert status_full == "REFUNDED"
        assert pay3.refunded_amount == Decimal("1990.00")


def test_timeout_recovery_expiration_and_qr_svg_fallback(database, pix_settings, monkeypatch):
    monkeypatch.setattr(mp_pix, "today_in_app_timezone", lambda: TODAY)
    api = FakePixOrdersAPI(billing_environment.MERCADOPAGO_TEST)

    with database() as db:
        org, basic, _pro, _admin = _make_org_and_plans(db)
        sub, pay, _ = mp_pix.reserve_pix_checkout(
            db, organization=org, plan=basic, amount=Decimal("99.00"), cycle="MONTHLY",
            due=TODAY, email="produtor@fazenda.com.br", environment=billing_environment.MERCADOPAGO_TEST,
        )
        db.commit()

        # Simula timeout após o Mercado Pago criar a order remotamente
        remote_order = api._build_order(
            order_id="ORD01TIMEOUT999",
            amount=Decimal("99.00"),
            external_reference=pay.mercadopago_order_reference,
            qr_b64=None,  # sem base64 para exercitar fallback SVG com segno
        )
        api.orders["ORD01TIMEOUT999"] = remote_order
        api.fail_create_with = mp.MercadoPagoError("falha_de_conexao")

        with pytest.raises(mp.MercadoPagoError, match="falha_de_conexao"):
            mp_pix.create_pix_order_for_payment(db, api, pay)

        assert pay.operation_state == "verificar"
        assert pay.mercadopago_order_id is None

        # Novo clique enquanto "verificar" reaproveita o registro em vez de gerar cobrança duplicada
        _, pay_same, reused = mp_pix.reserve_pix_checkout(
            db, organization=org, plan=basic, amount=Decimal("99.00"), cycle="MONTHLY",
            due=TODAY, email="produtor@fazenda.com.br", environment=billing_environment.MERCADOPAGO_TEST,
        )
        assert reused is True and pay_same.id == pay.id

        # Conciliação encontra pela external_reference via GET /v1/orders?external_reference=...
        status = mp_pix.reconcile_pix_payment(db, api, pay)
        db.commit()
        assert status == "PENDING"
        assert pay.mercadopago_order_id == "ORD01TIMEOUT999"
        assert pay.operation_state is None
        assert mp_pix.pix_qr_data_uri(pay).startswith("data:image/svg+xml;base64,")

        # Expiração permite gerar novo Pix limpo
        api.mark_expired("ORD01TIMEOUT999")
        assert mp_pix.reconcile_pix_payment(db, api, pay) == "EXPIRED"
        db.commit()

        api.fail_create_with = None
        sub2, pay_new, reused_after_exp = mp_pix.reserve_pix_checkout(
            db, organization=org, plan=basic, amount=Decimal("99.00"), cycle="MONTHLY",
            due=TODAY, email="produtor@fazenda.com.br", environment=billing_environment.MERCADOPAGO_TEST,
        )
        assert not reused_after_exp and sub2.id == sub.id and pay_new.id != pay.id


def test_switching_plan_before_first_payment_cancels_previous_pix_order(database, pix_settings, monkeypatch):
    monkeypatch.setattr(mp_pix, "today_in_app_timezone", lambda: TODAY)
    api = FakePixOrdersAPI(billing_environment.MERCADOPAGO_TEST)

    with database() as db:
        org, basic, pro, _admin = _make_org_and_plans(db)
        _, pay1, _ = mp_pix.reserve_pix_checkout(
            db, organization=org, plan=basic, amount=Decimal("99.00"), cycle="MONTHLY",
            due=TODAY, email="produtor@fazenda.com.br", environment=billing_environment.MERCADOPAGO_TEST,
        )
        db.commit()
        mp_pix.create_pix_order_for_payment(db, api, pay1)
        assert pay1.status == "PENDING"

        # Usuário muda para o plano Profissional antes de pagar o 1º Pix
        _, pay2, reused = mp_pix.reserve_pix_checkout(
            db, organization=org, plan=pro, amount=Decimal("199.00"), cycle="MONTHLY",
            due=TODAY, email="produtor@fazenda.com.br", environment=billing_environment.MERCADOPAGO_TEST,
        )
        assert not reused and pay2.id != pay1.id
        db.commit()
        mp_pix.create_pix_order_for_payment(db, api, pay2)

        db.refresh(pay1)
        assert pay1.status == "CANCELLED"
        assert pay2.status == "PENDING"
        assert any(c[0] == "POST" and c[1].endswith("/cancel") for c in api.calls)


def test_card_reconciler_ignores_pix_subscriptions(database, pix_settings, monkeypatch):
    monkeypatch.setattr(mp_pix, "today_in_app_timezone", lambda: TODAY)
    api = FakePixOrdersAPI(billing_environment.MERCADOPAGO_TEST)
    touched = []

    with database() as db:
        org, basic, _pro, _admin = _make_org_and_plans(db)
        sub, pay, _ = mp_pix.reserve_pix_checkout(
            db, organization=org, plan=basic, amount=Decimal("99.00"), cycle="MONTHLY",
            due=TODAY, email="produtor@fazenda.com.br", environment=billing_environment.MERCADOPAGO_TEST,
        )
        db.commit()
        mp_pix.create_pix_order_for_payment(db, api, pay)
        # Força last_synced_at antigo para entrar na janela de conciliação se não fosse filtrado
        sub.last_synced_at = datetime(2025, 1, 1)
        db.commit()

    monkeypatch.setattr(mp, "MercadoPagoAPI", lambda _s=None: SimpleNamespace())
    monkeypatch.setattr(mp, "reconcile", lambda _db, _api, s: touched.append(s.id))
    mp._tick()
    assert touched == []


def test_pix_renewal_reminders_send_7d_3d_and_due_day_without_duplicates(database, pix_settings):
    pix_settings.mercadopago_sandbox = False
    pix_settings.mercadopago_live_enabled = True
    pix_settings.mercadopago_pix_enabled = True
    sent_emails = []

    def fake_send(to, subject, body, purpose):
        sent_emails.append((to, subject, body, purpose))

    with database() as db:
        org, basic, _pro, _admin = _make_org_and_plans(db)
        sub = OrganizationSubscription(
            organization_id=org.id,
            plan_id=basic.id,
            plan_name=basic.name,
            amount=Decimal("99.00"),
            cycle="MONTHLY",
            first_due_date=date(2026, 11, 1),
            next_due_date=date(2026, 12, 1),
            billing_type="PIX",
            environment=billing_environment.MERCADOPAGO_LIVE,
            status="criada",
            mercadopago_status="authorized",
            mercadopago_payer_email="produtor@fazenda.com.br",
            mercadopago_reference="sisfarm-pix-live-reminder-001",
            is_current=True,
        )
        db.add(sub)
        db.flush()
        paid = OrganizationSubscriptionPayment(
            subscription_id=sub.id,
            mercadopago_order_id="ORD01REMINDER1",
            mercadopago_order_reference="sisfarm-pix-live-order-001",
            status="CONFIRMED",
            billing_type="PIX",
            amount=Decimal("99.00"),
            due_date=date(2026, 11, 1),
            paid_date=date(2026, 11, 1),
            charge_kind="contratacao",
        )
        db.add(paid)
        db.commit()

        # 7 dias antes -> envia 1 vez e não repete
        assert billing_notices.process_pix_renewal_reminders(db, date(2026, 11, 24), send=fake_send) == [(org.id, "pix_renovacao_7d")]
        assert billing_notices.process_pix_renewal_reminders(db, date(2026, 11, 25), send=fake_send) == []

        # 3 dias antes -> envia 1 vez
        assert billing_notices.process_pix_renewal_reminders(db, date(2026, 11, 28), send=fake_send) == [(org.id, "pix_renovacao_3d")]
        assert billing_notices.process_pix_renewal_reminders(db, date(2026, 11, 29), send=fake_send) == []

        # No dia do vencimento -> envia 1 vez
        assert billing_notices.process_pix_renewal_reminders(db, date(2026, 12, 1), send=fake_send) == [(org.id, "pix_renovacao_hoje")]
        assert billing_notices.process_pix_renewal_reminders(db, date(2026, 12, 1), send=fake_send) == []

        assert len(sent_emails) == 3
        notices = db.query(SubscriptionNotice).filter_by(payment_id=paid.id).all()
        assert {n.kind for n in notices} == {"pix_renovacao_7d", "pix_renovacao_3d", "pix_renovacao_hoje"}


def _render_template(template_name: str, context: dict) -> str:
    from jinja2 import ChoiceLoader, DictLoader

    env = routes.templates.env.overlay(
        loader=ChoiceLoader([DictLoader({"base.html": "{% block content %}{% endblock %}"}), routes.templates.env.loader])
    )
    return env.get_template(template_name).render(**context, request=None, csrf_token="tok")


def test_pix_web_routes_permissions_rendering_and_org_isolation(database, pix_settings, monkeypatch):
    # Verifica proteção de permissões das rotas Pix
    assert required_permissions_for_web_path("/minha-assinatura/pix/1/status", "GET") == (PAGE_FINANCE,)
    assert required_permissions_for_web_path("/minha-assinatura/pix/gerar", "POST") == (PAGE_FINANCE, WRITE_FINANCE)
    assert required_permissions_for_web_path("/minha-assinatura/pix/alterar-plano", "POST") == (PAGE_FINANCE, WRITE_FINANCE)

    monkeypatch.setattr(routes, "_base_context", lambda *args, **kwargs: kwargs)
    monkeypatch.setattr(routes, "_repository", lambda _db: None)
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda db, user: frozenset({PAGE_FINANCE, WRITE_FINANCE}))
    monkeypatch.setattr(routes.templates, "TemplateResponse", lambda name, context: (name, context))
    monkeypatch.setattr(mp_pix, "today_in_app_timezone", lambda: TODAY)

    api = FakePixOrdersAPI(billing_environment.MERCADOPAGO_TEST)

    with database() as db:
        org, basic, _pro, admin = _make_org_and_plans(db)
        other_org = Organization(name="Outra Fazenda", slug="outra-fazenda", is_active=True)
        db.add(other_org)
        db.flush()
        other_user = User(
            name="Outro",
            email="outro@fazenda.com.br",
            hashed_password="x",
            organization_id=other_org.id,
            is_admin=True,
            is_active=True,
        )
        db.add(other_user)
        db.flush()

        sub, pay, _ = mp_pix.reserve_pix_checkout(
            db, organization=org, plan=basic, amount=Decimal("99.00"), cycle="MONTHLY",
            due=TODAY, email=admin.email, environment=billing_environment.MERCADOPAGO_TEST,
        )
        db.commit()
        mp_pix.create_pix_order_for_payment(db, api, pay)

        # Página /minha-assinatura renderiza QR Code, Pix copia e cola e validade
        name, ctx = routes.my_subscription_page(request=SimpleNamespace(session={}), db=db, user=admin, csrf_token="tok")
        assert name == "my_subscription.html"
        assert ctx["is_pix_subscription"] is True
        assert ctx["active_pix_payment"].id == pay.id
        html = _render_template("my_subscription.html", ctx)
        assert "Conclua o pagamento via Pix" in html
        assert SAMPLE_QR_CODE in html
        assert "Copiar código Pix" in html
        assert f"/minha-assinatura/pix/{pay.id}/status" in html

        # Endpoint JSON de status retorna pendente para o dono e 404 para outra organização
        resp = routes.my_subscription_pix_status(payment_id=pay.id, db=db, user=admin)
        assert resp.status_code == 200
        assert b'"confirmed":false' in resp.body

        with pytest.raises(Exception) as exc_info:
            routes.my_subscription_pix_status(payment_id=pay.id, db=db, user=other_user)
        assert getattr(exc_info.value, "status_code", None) == 404

        # Após pagamento confirmado, endpoint JSON sinaliza confirmed=true
        api.mark_paid(pay.mercadopago_order_id, paid_on=TODAY)
        mp_pix.process_order_event(db, api, pay.mercadopago_order_id)
        db.commit()
        resp2 = routes.my_subscription_pix_status(payment_id=pay.id, db=db, user=admin)
        assert b'"confirmed":true' in resp2.body

