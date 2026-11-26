"""Cliente, fluxo de cobrança e conciliação de Pix via API de Orders do Mercado Pago.

Usa exclusivamente a aplicação independente "SiSFarm Pix" (/v1/orders), preservando
integralmente a integração de assinaturas recorrentes por cartão (/preapproval).
"""

from __future__ import annotations

import asyncio
import base64
import io
import logging
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import segno
from sqlalchemy import or_
from starlette.concurrency import run_in_threadpool

from app.core.config import get_settings
from app.core.timezone import app_now, today_in_app_timezone
from app.db.session import SessionLocal
from app.models import (
    MercadoPagoEvent,
    Organization,
    OrganizationSubscription,
    OrganizationSubscriptionPayment,
    SubscriptionPlan,
)
from app.services.access_grants import _cycle_end
from app.services.asaas_environment import app_public_base
from app.services.billing_environment import MERCADOPAGO_LIVE, MERCADOPAGO_TEST, billing_environment
from app.services.mercadopago_subscriptions import (
    CYCLES,
    MercadoPagoError,
    can_replace_test_subscription,
    resource_id,
    safe_link,
)

logger = logging.getLogger(__name__)
API_URL = "https://api.mercadopago.com"
PIX_EVENT_LIVE = "mp_pix_live"
PIX_EVENT_TEST = "mp_pix_test"
PIX_SUB_PREFIX_LIVE = "sisfarm-pix-live-"
PIX_SUB_PREFIX_TEST = "sisfarm-pix-test-"
PIX_SUB_PREFIXES = (PIX_SUB_PREFIX_LIVE, PIX_SUB_PREFIX_TEST)
PIX_EXPIRATION_ISO = "PT24H"
PIX_EXPIRATION_HOURS = 24
PIX_RENEWAL_WINDOW_DAYS = 7
PIX_REJECTION_STATUSES = {400, 401, 403, 404, 422}

_ISO_DURATION_RE = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$"
)


def is_pix_order_subscription(subscription: OrganizationSubscription | None) -> bool:
    """Retorna True se a assinatura utiliza o fluxo independente de Pix via API de Orders."""
    if not subscription:
        return False
    ref = subscription.mercadopago_reference or ""
    return ref.startswith(PIX_SUB_PREFIXES)


def pix_event_environment(environment: str) -> str:
    return PIX_EVENT_LIVE if environment == MERCADOPAGO_LIVE else PIX_EVENT_TEST


def _reference_prefix(environment: str) -> str:
    return PIX_SUB_PREFIX_LIVE if environment == MERCADOPAGO_LIVE else PIX_SUB_PREFIX_TEST


def _new_reference(environment: str) -> str:
    # A API de Orders limita external_reference a 64 caracteres (17 + 32 = 49).
    return f"{_reference_prefix(environment)}{uuid4().hex}"


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if dt.tzinfo:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        return None


def _parse_iso_date_local(value: str | None) -> date | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        return dt.astimezone(app_now().tzinfo).date() if dt.tzinfo else dt.date()
    except ValueError:
        return None


def _parse_iso_duration(value: str | None) -> timedelta | None:
    if not isinstance(value, str):
        return None
    match = _ISO_DURATION_RE.fullmatch(value.strip())
    if not match:
        return None
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = int(match.group("seconds") or 0)
    delta = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    return delta if delta > timedelta(0) else None


def _amount(value) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
        if parsed.is_finite() and Decimal("0") <= parsed < Decimal("10000000000"):
            return parsed.quantize(Decimal("0.01"))
    except InvalidOperation:
        return None
    return None


def _clean_base64(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = "".join(value.split())
    if not cleaned or len(cleaned) > 200_000:
        return None
    if not re.fullmatch(r"[A-Za-z0-9+/=]+", cleaned):
        return None
    try:
        base64.b64decode(cleaned, validate=True)
    except Exception:
        return None
    return cleaned


def pix_qr_data_uri(payment: OrganizationSubscriptionPayment | None) -> str | None:
    """Retorna data URI segura para exibir o QR Code Pix (base64 da API ou SVG gerado via segno)."""
    if not payment:
        return None
    if payment.pix_qr_code_base64:
        cleaned = _clean_base64(payment.pix_qr_code_base64)
        if cleaned:
            raw = base64.b64decode(cleaned)
            mime = "image/png" if raw.startswith(b"\x89PNG") else "image/jpeg"
            return f"data:{mime};base64,{cleaned}"
    if payment.pix_qr_code:
        try:
            qr = segno.make(payment.pix_qr_code, error="m")
            buffer = io.BytesIO()
            qr.save(buffer, kind="svg", scale=6, border=2, xmldecl=False)
            encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
            return f"data:image/svg+xml;base64,{encoded}"
        except Exception:
            logger.warning("Falha ao renderizar QR Code Pix localmente para payment_id=%s", getattr(payment, "id", None))
    return None


def pix_configuration_error(environment: str | None = None, settings=None) -> str | None:
    """Valida a configuração da aplicação independente SiSFarm Pix para o ambiente informado."""
    settings = settings or get_settings()
    env = environment or billing_environment()
    if env == MERCADOPAGO_LIVE:
        if not getattr(settings, "mercadopago_pix_enabled", False):
            return "O pagamento via Pix em produção está desativado."
        if getattr(settings, "mercadopago_sandbox", False):
            return "Desative MERCADOPAGO_SANDBOX para usar Pix em produção."
        seller_id = getattr(settings, "mercadopago_pix_seller_id", None) or getattr(settings, "mercadopago_live_seller_id", None)
        if not seller_id or seller_id <= 0:
            return "Configure o Seller ID da conta recebedora Pix."
        token = (getattr(settings, "mercadopago_pix_access_token", None) or "").strip()
        secret = (getattr(settings, "mercadopago_pix_webhook_secret", None) or "").strip()
        test_token = (getattr(settings, "mercadopago_pix_test_access_token", None) or "").strip()
        test_secret = (getattr(settings, "mercadopago_pix_test_webhook_secret", None) or "").strip()
        if not token.startswith("APP_USR-") or (test_token and token == test_token):
            return "Configure o Access Token de produção exclusivo da aplicação SiSFarm Pix."
        if len(secret) < 32 or secret in {token, test_secret}:
            return "Configure o segredo exclusivo do webhook de produção da aplicação SiSFarm Pix."
        try:
            url = urlsplit(app_public_base(settings))
            if not (
                url.scheme == "https"
                and url.hostname
                and url.port in {None, 443}
                and not (url.path or url.username or url.password or url.query or url.fragment)
            ):
                return "Configure o endereço HTTPS público do SiSFarm em APP_PUBLIC_URL."
        except ValueError:
            return "Configure o endereço HTTPS público do SiSFarm em APP_PUBLIC_URL."
        return None
    if env == MERCADOPAGO_TEST:
        token = (getattr(settings, "mercadopago_pix_test_access_token", None) or "").strip()
        secret = (getattr(settings, "mercadopago_pix_test_webhook_secret", None) or "").strip()
        live_token = (getattr(settings, "mercadopago_pix_access_token", None) or "").strip()
        live_secret = (getattr(settings, "mercadopago_pix_webhook_secret", None) or "").strip()
        if not token or not secret:
            return "Configure MERCADOPAGO_PIX_TEST_ACCESS_TOKEN e MERCADOPAGO_PIX_TEST_WEBHOOK_SECRET para testar o Pix."
        if live_token and token == live_token:
            return "O token de teste do Pix deve ser diferente do token de produção."
        if live_secret and secret == live_secret:
            return "O segredo de teste do webhook Pix deve ser diferente do segredo de produção."
        return None
    return "O ambiente atual não utiliza Mercado Pago Pix."


def pix_checkout_available(settings=None) -> bool:
    """Indica se o botão/opção de contratar via Pix deve aparecer no checkout do cliente."""
    settings = settings or get_settings()
    env = billing_environment()
    if env == MERCADOPAGO_LIVE:
        if not getattr(settings, "mercadopago_live_checkout_enabled", False):
            return False
        return pix_configuration_error(MERCADOPAGO_LIVE, settings) is None
    if env == MERCADOPAGO_TEST:
        return pix_configuration_error(MERCADOPAGO_TEST, settings) is None
    return False


def pix_active_for_environment(environment: str | None = None, settings=None) -> bool:
    settings = settings or get_settings()
    env = environment or billing_environment()
    return pix_configuration_error(env, settings) is None


def pix_webhook_available(environment: str, settings=None) -> bool:
    """Garante isolamento estrito entre webhook Pix de teste e de produção."""
    settings = settings or get_settings()
    if environment == MERCADOPAGO_LIVE:
        if billing_environment() != MERCADOPAGO_LIVE:
            return False
        return pix_configuration_error(MERCADOPAGO_LIVE, settings) is None
    if environment == MERCADOPAGO_TEST:
        if billing_environment() == MERCADOPAGO_LIVE and getattr(settings, "mercadopago_pix_enabled", False):
            return False
        return pix_configuration_error(MERCADOPAGO_TEST, settings) is None
    return False


def pix_error_message(error: MercadoPagoError) -> str:
    code = str(error)
    if code == "organizacao_indisponivel":
        return "A organização está indisponível para esta operação."
    if code == "assinatura_existente":
        return "Sua organização já possui uma assinatura ativa. Gerencie por Minha assinatura."
    if code == "pix_indisponivel":
        return "O pagamento via Pix está indisponível no momento."
    if code == "vendedor_pix_nao_confirmado":
        return "Não foi possível confirmar a conta recebedora do Pix. Avise a equipe SiSFarm."
    if code == "order_fora_da_conta":
        return "A cobrança retornada não pertence à conta recebedora configurada."
    if error.http_status in {401, 403}:
        return "O Mercado Pago recusou a credencial da aplicação SiSFarm Pix. Confira a configuração com a equipe."
    if error.http_status in PIX_REJECTION_STATUSES:
        ref = f" Referência {error.request_id}." if error.request_id else ""
        return f"O Mercado Pago não aceitou os dados da cobrança Pix. Código {code}.{ref}"
    return "Estamos verificando a geração do Pix com o Mercado Pago. Atualize em instantes."


class MercadoPagoPixOrdersAPI:
    """Cliente HTTP exclusivo da API de Orders (/v1/orders) para a aplicação SiSFarm Pix."""

    def __init__(self, environment: str | None = None, settings=None):
        settings = settings or get_settings()
        self.environment = environment or billing_environment()
        error = pix_configuration_error(self.environment, settings)
        if error:
            raise MercadoPagoError("pix_indisponivel")
        self.seller_id = getattr(settings, "mercadopago_pix_seller_id", 82986976)
        self.application_id = str(getattr(settings, "mercadopago_pix_application_id", "7441327382083069") or "").strip()
        if self.environment == MERCADOPAGO_LIVE:
            self.token = (settings.mercadopago_pix_access_token or "").strip()
        else:
            self.token = (settings.mercadopago_pix_test_access_token or "").strip()
        self._seller: dict | None = None

    def _verified_seller(self, client: httpx.Client) -> dict:
        if self._seller is None:
            response = client.get("/users/me")
            if response.status_code >= 500 or response.status_code == 429:
                raise MercadoPagoError(f"http_{response.status_code}", response.status_code)
            try:
                account = response.json() if response.is_success else {}
            except ValueError:
                raise MercadoPagoError("resposta_invalida") from None
            self._seller = {}
            tags = account.get("tags") if isinstance(account, dict) else None
            if tags is None:
                tags = []
            if (
                isinstance(account, dict)
                and isinstance(account.get("id"), int)
                and account.get("site_id") == "MLB"
                and isinstance(tags, list)
            ):
                email = str(account.get("email") or "").lower()
                fictitious = "test_user" in tags or email.endswith("@testuser.com")
                if self.environment == MERCADOPAGO_LIVE:
                    if account["id"] == self.seller_id and email and not fictitious:
                        self._seller = {"id": account["id"], "email": email}
                else:
                    # Em teste, aceita credencial TEST- da conta do desenvolvedor ou usuário fictício de teste.
                    if self.token.startswith("TEST-") or fictitious:
                        self._seller = {"id": account["id"], "email": email, "test_token": self.token.startswith("TEST-")}
        return self._seller

    def _validate_order_ownership(self, order: dict, seller: dict) -> None:
        user_id = order.get("user_id")
        if self.environment == MERCADOPAGO_LIVE:
            if str(user_id) != str(seller["id"]):
                raise MercadoPagoError("order_fora_da_conta")
            integration = order.get("integration_data")
            app_id = integration.get("application_id") if isinstance(integration, dict) else None
            if app_id is not None and self.application_id and str(app_id) != self.application_id:
                raise MercadoPagoError("order_fora_da_conta")
            country = order.get("country_code")
            if country and country != "BRA":
                raise MercadoPagoError("order_fora_da_conta")
        else:
            if user_id is not None and str(user_id) != str(seller["id"]):
                raise MercadoPagoError("order_fora_da_conta")

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        payload: dict | None = None,
        idempotency_key: str | None = None,
        extra_headers: dict | None = None,
    ) -> dict:
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if idempotency_key:
            headers["X-Idempotency-Key"] = idempotency_key
        if extra_headers:
            headers.update(extra_headers)
        try:
            with httpx.Client(base_url=API_URL, headers=headers, timeout=10.0) as client:
                seller = self._verified_seller(client)
                if not seller:
                    raise MercadoPagoError("vendedor_pix_nao_confirmado")
                response = client.request(method, path, params=params, json=payload)
        except httpx.HTTPError:
            logger.warning("Mercado Pago Pix: falha de conexão ambiente=%s method=%s endpoint=%s", self.environment, method, path)
            raise MercadoPagoError("falha_de_conexao") from None

        if not response.is_success:
            response_id = resource_id(response.headers.get("x-request-id"))
            logger.warning(
                "Mercado Pago Pix: ambiente=%s method=%s endpoint=%s status=%s request_id=%s",
                self.environment, method, path, response.status_code, response_id or "-",
            )
            raise MercadoPagoError(f"http_{response.status_code}", response.status_code, request_id=response_id)
        try:
            data = response.json()
        except ValueError:
            raise MercadoPagoError("resposta_invalida") from None
        if not isinstance(data, dict):
            raise MercadoPagoError("resposta_invalida")
        if path == "/v1/orders" and method == "POST":
            self._validate_order_ownership(data, seller)
        elif path.startswith("/v1/orders/") and method == "GET":
            self._validate_order_ownership(data, seller)
        return data

    def create_pix_order(
        self,
        *,
        amount: Decimal,
        external_reference: str,
        payer_email: str,
        description: str,
        idempotency_key: str,
        expiration_time: str = PIX_EXPIRATION_ISO,
    ) -> dict:
        formatted = f"{Decimal(amount):.2f}"
        payload = {
            "type": "online",
            "processing_mode": "automatic",
            "total_amount": formatted,
            "external_reference": external_reference,
            "description": description[:120],
            "payer": {"email": payer_email.strip().lower()},
            "transactions": {
                "payments": [
                    {
                        "amount": formatted,
                        "payment_method": {"id": "pix", "type": "bank_transfer"},
                        "expiration_time": expiration_time,
                    }
                ]
            },
        }
        return self.request("POST", "/v1/orders", payload=payload, idempotency_key=idempotency_key)

    def get_order(self, order_id: str) -> dict:
        identifier = resource_id(order_id)
        if not identifier:
            raise MercadoPagoError("identificador_invalido")
        data = self.request("GET", f"/v1/orders/{identifier}")
        if resource_id(data.get("id")) != identifier:
            raise MercadoPagoError("recurso_divergente")
        return data

    def search_orders_by_reference(self, external_reference: str, *, since: datetime, until: datetime) -> list[dict]:
        params = {
            "begin_date": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date": until.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "external_reference": external_reference[:64],
        }
        data = self.request("GET", "/v1/orders", params=params)
        rows = data.get("data") if isinstance(data.get("data"), list) else data.get("results")
        if rows is None:
            return []
        if not isinstance(rows, list):
            raise MercadoPagoError("lista_invalida")
        return [row for row in rows if isinstance(row, dict) and row.get("external_reference") == external_reference]

    def cancel_order(self, order_id: str, *, idempotency_key: str) -> dict:
        identifier = resource_id(order_id)
        if not identifier:
            raise MercadoPagoError("identificador_invalido")
        return self.request("POST", f"/v1/orders/{identifier}/cancel", idempotency_key=idempotency_key)

    def refund_order(
        self,
        order_id: str,
        *,
        idempotency_key: str,
        payment_id: str | None = None,
        amount: Decimal | None = None,
    ) -> dict:
        identifier = resource_id(order_id)
        if not identifier:
            raise MercadoPagoError("identificador_invalido")
        payload = None
        if payment_id and amount is not None:
            payload = {"transactions": [{"id": payment_id, "amount": f"{Decimal(amount):.2f}"}]}
        return self.request(
            "POST",
            f"/v1/orders/{identifier}/refund",
            payload=payload,
            idempotency_key=idempotency_key,
            extra_headers={"X-Render-In-Process-Refunds": "true"},
        )


def _extract_payment_and_refunds(order: dict) -> tuple[dict, Decimal]:
    transactions = order.get("transactions")
    transactions = transactions if isinstance(transactions, dict) else {}
    payments = transactions.get("payments")
    payment = payments[0] if isinstance(payments, list) and payments and isinstance(payments[0], dict) else {}
    refunds = transactions.get("refunds")
    refunded_total = Decimal("0.00")
    if isinstance(refunds, list):
        for item in refunds:
            if not isinstance(item, dict):
                continue
            status = str(item.get("status") or "").lower()
            if status in {"failed", "rejected", "canceled", "cancelled"}:
                continue
            val = _amount(item.get("amount"))
            if val:
                refunded_total += val
    return payment, refunded_total


def _compute_expiration(order: dict, payment: dict, existing: datetime | None) -> datetime | None:
    for raw in (payment.get("date_of_expiration"), order.get("date_of_expiration")):
        parsed = _parse_iso_datetime(raw)
        if parsed:
            return parsed
    created = _parse_iso_datetime(order.get("created_date"))
    duration = _parse_iso_duration(payment.get("expiration_time") or order.get("expiration_time"))
    if created and duration:
        return created + duration
    return existing or (_now() + timedelta(hours=PIX_EXPIRATION_HOURS))


def apply_order(db, payment_row: OrganizationSubscriptionPayment, order: dict) -> str:
    """Aplica o estado retornado por GET/POST /v1/orders à cobrança e à assinatura local."""
    order_id = resource_id(order.get("id"))
    if not order_id:
        raise MercadoPagoError("order_invalida")
    if payment_row.mercadopago_order_id and payment_row.mercadopago_order_id != order_id:
        raise MercadoPagoError("order_divergente")
    remote_ref = order.get("external_reference")
    if payment_row.mercadopago_order_reference and remote_ref and remote_ref != payment_row.mercadopago_order_reference:
        raise MercadoPagoError("order_divergente")
    total_amount = _amount(order.get("total_amount"))
    if total_amount is None or (payment_row.amount is not None and total_amount != Decimal(payment_row.amount).quantize(Decimal("0.01"))):
        raise MercadoPagoError("valor_da_order_divergente")

    tx_payment, refunded_total = _extract_payment_and_refunds(order)
    pm = tx_payment.get("payment_method") if isinstance(tx_payment.get("payment_method"), dict) else {}
    if pm.get("id") and pm.get("id") != "pix":
        raise MercadoPagoError("meio_de_pagamento_divergente")

    order_status = str(order.get("status") or "").lower()
    order_detail = str(order.get("status_detail") or "").lower()
    tx_status = str(tx_payment.get("status") or "").lower()
    was_confirmed = payment_row.status == "CONFIRMED"

    payment_row.mercadopago_order_id = order_id
    payment_row.mercadopago_order_status = order_status[:40] if order_status else payment_row.mercadopago_order_status
    tx_payment_id = resource_id(tx_payment.get("id"))
    if tx_payment_id:
        payment_row.mercadopago_payment_id = tx_payment_id
    payment_row.billing_type = "PIX"
    payment_row.amount = total_amount
    if isinstance(pm.get("qr_code"), str) and pm["qr_code"].strip():
        payment_row.pix_qr_code = pm["qr_code"].strip()
    cleaned_b64 = _clean_base64(pm.get("qr_code_base64"))
    if cleaned_b64:
        payment_row.pix_qr_code_base64 = cleaned_b64
    ticket = safe_link(pm.get("ticket_url"))
    if ticket:
        payment_row.invoice_url = ticket[:255]
    payment_row.pix_expires_at = _compute_expiration(order, tx_payment, payment_row.pix_expires_at)
    payment_row.operation_state = None
    payment_row.last_event_at = _now()

    if order_status == "refunded" or tx_status == "refunded" or (refunded_total >= total_amount and total_amount > 0):
        payment_row.status = "REFUNDED"
        payment_row.refunded_amount = total_amount
        if not payment_row.paid_date:
            payment_row.paid_date = _parse_iso_date_local(order.get("last_updated_date")) or today_in_app_timezone()
    elif order_status == "processed" or tx_status == "processed":
        payment_row.status = "CONFIRMED"
        payment_row.refunded_amount = refunded_total if refunded_total > 0 else Decimal("0.00")
        if not payment_row.paid_date:
            payment_row.paid_date = (
                _parse_iso_date_local(tx_payment.get("date_approved"))
                or _parse_iso_date_local(order.get("last_updated_date"))
                or today_in_app_timezone()
            )
        if order_detail == "partially_refunded" and (payment_row.refunded_amount is None or payment_row.refunded_amount == 0):
            # Caso a API informe partially_refunded antes de popular transactions.refunds
            paid = _amount(order.get("total_paid_amount"))
            if paid is not None and paid < total_amount:
                payment_row.refunded_amount = total_amount - paid
    elif order_status == "expired" or tx_status == "expired":
        payment_row.status = "EXPIRED"
        payment_row.paid_date = None
    elif order_status in {"canceled", "cancelled", "failed"} or tx_status in {"canceled", "cancelled", "failed"}:
        payment_row.status = "CANCELLED"
        payment_row.paid_date = None
    else:
        payment_row.status = "PENDING"
        payment_row.paid_date = None

    subscription = payment_row.subscription
    if subscription:
        subscription.last_synced_at = _now()
        subscription.sync_issue = None
        if payment_row.status == "CONFIRMED" and not was_confirmed:
            _activate_or_extend_subscription(db, subscription, payment_row)
        elif payment_row.status == "PENDING" and subscription.status == "criando":
            subscription.status = "criada"
            subscription.mercadopago_status = subscription.mercadopago_status or "pending"

    return payment_row.status


def _activate_or_extend_subscription(
    db,
    subscription: OrganizationSubscription,
    payment_row: OrganizationSubscriptionPayment,
) -> None:
    """Ativa, renova ou efetiva troca de plano somente após confirmação real da Order Pix."""
    paid_on = payment_row.paid_date or today_in_app_timezone()
    kind = payment_row.charge_kind or "contratacao"

    if kind == "alteracao_plano" and payment_row.target_plan_id:
        new_plan = db.get(SubscriptionPlan, payment_row.target_plan_id)
        if new_plan:
            subscription.plan_id = new_plan.id
            subscription.plan_name = new_plan.name
            subscription.plan_changed_at = _now()
        if payment_row.target_cycle in CYCLES:
            subscription.cycle = payment_row.target_cycle
        if payment_row.amount is not None:
            subscription.amount = payment_row.amount
        base_start = paid_on
        if payment_row.due_date is None or payment_row.due_date < paid_on:
            payment_row.due_date = paid_on
        subscription.next_due_date = _cycle_end(base_start, subscription.cycle)
    elif kind == "renovacao":
        if payment_row.target_plan_id:
            target_plan = db.get(SubscriptionPlan, payment_row.target_plan_id)
            if target_plan:
                subscription.plan_id = target_plan.id
                subscription.plan_name = target_plan.name
        if payment_row.target_cycle in CYCLES:
            subscription.cycle = payment_row.target_cycle
        if payment_row.amount is not None:
            subscription.amount = payment_row.amount
        # Se renovou antecipado, preserva os dias já pagos somando a partir do vencimento atual.
        base_start = subscription.next_due_date if (subscription.next_due_date and subscription.next_due_date > paid_on) else paid_on
        payment_row.due_date = base_start
        subscription.next_due_date = _cycle_end(base_start, subscription.cycle)
    else:
        # Primeira contratação: se pagou durante teste grátis, ciclo pago começa em first_due_date.
        base_start = max(paid_on, subscription.first_due_date) if subscription.first_due_date else paid_on
        payment_row.due_date = paid_on
        subscription.next_due_date = _cycle_end(base_start, subscription.cycle)

    subscription.billing_type = "PIX"
    subscription.mercadopago_status = "authorized"
    subscription.status = "criada"
    subscription.is_current = True


def _reusable_pending_pix(
    db,
    subscription_id: int,
    *,
    charge_kind: str,
    amount: Decimal,
    target_plan_id: int | None,
    target_cycle: str | None,
) -> OrganizationSubscriptionPayment | None:
    now = _now()
    pending = (
        db.query(OrganizationSubscriptionPayment)
        .filter(
            OrganizationSubscriptionPayment.subscription_id == subscription_id,
            OrganizationSubscriptionPayment.status == "PENDING",
            OrganizationSubscriptionPayment.mercadopago_order_reference.isnot(None),
        )
        .order_by(OrganizationSubscriptionPayment.id.desc())
        .all()
    )
    for row in pending:
        if row.pix_expires_at and row.pix_expires_at <= now:
            continue
        same_amount = row.amount is not None and Decimal(row.amount).quantize(Decimal("0.01")) == Decimal(amount).quantize(Decimal("0.01"))
        if (
            (row.charge_kind or "contratacao") == charge_kind
            and same_amount
            and row.target_plan_id == target_plan_id
            and row.target_cycle == target_cycle
            and (row.mercadopago_order_id or row.operation_state == "verificar")
        ):
            return row
    return None


def reserve_pix_checkout(
    db,
    *,
    organization: Organization,
    plan: SubscriptionPlan,
    amount: Decimal,
    cycle: str,
    due: date,
    email: str,
    environment: str,
) -> tuple[OrganizationSubscription, OrganizationSubscriptionPayment, bool]:
    """Reserva assinatura e cobrança Pix sob bloqueio da organização.

    Retorna ``(subscription, payment, reused)``. O chamador faz commit antes do POST remoto.
    """
    if not db.query(Organization.id).filter_by(id=organization.id, archived_at=None, is_active=True).with_for_update().first():
        raise MercadoPagoError("organizacao_indisponivel")

    current = db.query(OrganizationSubscription).filter_by(organization_id=organization.id, is_current=True).first()
    if current:
        # Se já existe assinatura Pix pendente de 1º pagamento neste ambiente, reaproveita ou substitui cobrança.
        if (
            current.environment == environment
            and is_pix_order_subscription(current)
            and current.mercadopago_status in {None, "pending"}
        ):
            if current.plan_id == plan.id and current.cycle == cycle:
                existing = _reusable_pending_pix(
                    db, current.id, charge_kind="contratacao", amount=amount, target_plan_id=None, target_cycle=None,
                )
                if existing:
                    return current, existing, True
            current.plan_id = plan.id
            current.plan_name = plan.name
            current.amount = amount
            current.cycle = cycle
            current.first_due_date = due
            current.mercadopago_payer_email = email.strip().lower()
            current.status = "criando"
            current.mercadopago_status = "pending"
            current.last_synced_at = _now()
            payment = OrganizationSubscriptionPayment(
                subscription_id=current.id,
                mercadopago_order_reference=_new_reference(environment),
                status="PENDING",
                billing_type="PIX",
                amount=amount,
                due_date=due,
                charge_kind="contratacao",
            )
            db.add(payment)
            db.flush()
            return current, payment, False
        if not can_replace_test_subscription(current, environment):
            raise MercadoPagoError("assinatura_existente")
        current.is_current = False
        db.flush()

    subscription = OrganizationSubscription(
        organization_id=organization.id,
        plan_id=plan.id,
        plan_name=plan.name,
        amount=amount,
        cycle=cycle,
        first_due_date=due,
        billing_type="PIX",
        environment=environment,
        status="criando",
        mercadopago_status="pending",
        mercadopago_payer_email=email.strip().lower(),
        mercadopago_reference=_new_reference(environment),
        last_synced_at=_now(),
    )
    db.add(subscription)
    db.flush()
    payment = OrganizationSubscriptionPayment(
        subscription_id=subscription.id,
        mercadopago_order_reference=_new_reference(environment),
        status="PENDING",
        billing_type="PIX",
        amount=amount,
        due_date=due,
        charge_kind="contratacao",
    )
    db.add(payment)
    db.flush()
    return subscription, payment, False


def reserve_pix_charge_for_subscription(
    db,
    *,
    subscription: OrganizationSubscription,
    plan: SubscriptionPlan,
    amount: Decimal,
    cycle: str,
    charge_kind: str,
) -> tuple[OrganizationSubscriptionPayment, bool, list[OrganizationSubscriptionPayment]]:
    """Reserva uma cobrança Pix de renovação, 2ª via ou alteração de plano sob lock da assinatura."""
    locked = db.query(OrganizationSubscription).filter_by(id=subscription.id).with_for_update().one()
    if not is_pix_order_subscription(locked) or not locked.is_current:
        raise MercadoPagoError("assinatura_invalida")
    target_plan_id = plan.id if charge_kind in {"alteracao_plano", "renovacao"} else None
    target_cycle = cycle if charge_kind in {"alteracao_plano", "renovacao"} else None

    existing = _reusable_pending_pix(
        db,
        locked.id,
        charge_kind=charge_kind,
        amount=amount,
        target_plan_id=target_plan_id,
        target_cycle=target_cycle,
    )
    if existing:
        return existing, True, []

    # Coleta outras orders Pix pendentes abertas para cancelar na API (evitar 2 QR Codes ativos simultâneos).
    to_cancel = (
        db.query(OrganizationSubscriptionPayment)
        .filter(
            OrganizationSubscriptionPayment.subscription_id == locked.id,
            OrganizationSubscriptionPayment.status == "PENDING",
            OrganizationSubscriptionPayment.mercadopago_order_id.isnot(None),
        )
        .all()
    )
    today = today_in_app_timezone()
    due = locked.next_due_date if (charge_kind == "renovacao" and locked.next_due_date) else today
    payment = OrganizationSubscriptionPayment(
        subscription_id=locked.id,
        mercadopago_order_reference=_new_reference(locked.environment),
        status="PENDING",
        billing_type="PIX",
        amount=amount,
        due_date=due,
        charge_kind=charge_kind,
        target_plan_id=target_plan_id,
        target_cycle=target_cycle,
    )
    db.add(payment)
    db.flush()
    return payment, False, to_cancel


def create_pix_order_for_payment(
    db,
    api: MercadoPagoPixOrdersAPI,
    payment: OrganizationSubscriptionPayment,
    *,
    cancel_previous: list[OrganizationSubscriptionPayment] | None = None,
) -> OrganizationSubscriptionPayment:
    """Envia POST /v1/orders para a cobrança reservada (idempotente via X-Idempotency-Key)."""
    payment = db.query(OrganizationSubscriptionPayment).filter_by(id=payment.id).with_for_update().one()
    subscription = db.query(OrganizationSubscription).filter_by(id=payment.subscription_id).with_for_update().one()
    if payment.mercadopago_order_id:
        return payment

    if cancel_previous is None:
        cancel_previous = (
            db.query(OrganizationSubscriptionPayment)
            .filter(
                OrganizationSubscriptionPayment.subscription_id == subscription.id,
                OrganizationSubscriptionPayment.id != payment.id,
                OrganizationSubscriptionPayment.status == "PENDING",
                OrganizationSubscriptionPayment.mercadopago_order_id.isnot(None),
            )
            .all()
        )
    for old in cancel_previous:
        if old.id != payment.id and old.mercadopago_order_id and old.status == "PENDING":
            try:
                canceled = api.cancel_order(
                    old.mercadopago_order_id,
                    idempotency_key=f"cancel-{old.mercadopago_order_reference or old.id}",
                )
                apply_order(db, old, canceled)
            except MercadoPagoError:
                logger.warning("Mercado Pago Pix: não foi possível cancelar order anterior=%s", old.mercadopago_order_id)

    plan_name = subscription.plan_name
    if payment.target_plan_id:
        target = db.get(SubscriptionPlan, payment.target_plan_id)
        if target:
            plan_name = target.name
    kind_label = {
        "contratacao": "Assinatura",
        "renovacao": "Renovação",
        "alteracao_plano": "Alteração de plano",
    }.get(payment.charge_kind or "contratacao", "Assinatura")
    description = f"SiSFarm - {kind_label} {plan_name}"

    try:
        order = api.create_pix_order(
            amount=Decimal(payment.amount),
            external_reference=payment.mercadopago_order_reference,
            payer_email=subscription.mercadopago_payer_email or "",
            description=description,
            idempotency_key=payment.mercadopago_order_reference,
            expiration_time=PIX_EXPIRATION_ISO,
        )
        apply_order(db, payment, order)
    except MercadoPagoError as error:
        subscription.last_synced_at = _now()
        diagnostic = [str(error), error.request_id]
        if error.http_status in PIX_REJECTION_STATUSES or str(error) in {"vendedor_pix_nao_confirmado", "pix_indisponivel"}:
            payment.status = "CANCELLED"
            payment.operation_state = None
            has_paid = db.query(OrganizationSubscriptionPayment.id).filter(
                OrganizationSubscriptionPayment.subscription_id == subscription.id,
                OrganizationSubscriptionPayment.status == "CONFIRMED",
            ).first()
            if not has_paid and subscription.mercadopago_status in {None, "pending"}:
                subscription.is_current = False
                subscription.status = "falhou"
                subscription.sync_issue = f"Criação do Pix: {' · '.join(filter(None, diagnostic))}."
        else:
            # Falha inconclusiva (timeout/5xx): conserva a referência para conciliar sem duplicar cobrança.
            payment.operation_state = "verificar"
            subscription.sync_issue = f"Verificando Pix no Mercado Pago: {' · '.join(filter(None, diagnostic))}."
        db.commit()
        logger.warning(
            "Mercado Pago Pix: criação ambiente=%s payment_id=%s erro=%s",
            subscription.environment, payment.id, error,
        )
        raise
    db.commit()
    return payment


def cancel_pix_payment(db, api: MercadoPagoPixOrdersAPI, payment: OrganizationSubscriptionPayment) -> str:
    """Cancela uma cobrança Pix pendente via POST /v1/orders/{id}/cancel."""
    payment = db.query(OrganizationSubscriptionPayment).filter_by(id=payment.id).with_for_update().one()
    if payment.status != "PENDING" or not payment.mercadopago_order_id:
        raise MercadoPagoError("acao_indisponivel")
    remote = api.get_order(payment.mercadopago_order_id)
    status = apply_order(db, payment, remote)
    if status != "PENDING":
        return status
    canceled = api.cancel_order(
        payment.mercadopago_order_id,
        idempotency_key=f"cancel-{payment.mercadopago_order_reference or payment.id}",
    )
    return apply_order(db, payment, canceled)


def refund_pix_payment(
    db,
    api: MercadoPagoPixOrdersAPI,
    payment: OrganizationSubscriptionPayment,
    *,
    amount: Decimal | None = None,
) -> str:
    """Estorna uma cobrança Pix confirmada via POST /v1/orders/{id}/refund."""
    payment = db.query(OrganizationSubscriptionPayment).filter_by(id=payment.id).with_for_update().one()
    if payment.status != "CONFIRMED" or not payment.mercadopago_order_id:
        raise MercadoPagoError("acao_indisponivel")
    key_suffix = f"{amount:.2f}" if amount is not None else "total"
    api.refund_order(
        payment.mercadopago_order_id,
        idempotency_key=f"refund-{payment.mercadopago_order_reference or payment.id}-{key_suffix}",
        payment_id=payment.mercadopago_payment_id if amount is not None else None,
        amount=amount,
    )
    refreshed = api.get_order(payment.mercadopago_order_id)
    return apply_order(db, payment, refreshed)


def cancel_pix_subscription(db, api: MercadoPagoPixOrdersAPI | None, subscription: OrganizationSubscription) -> None:
    """Cancela a assinatura Pix (mantendo o período já pago, cancelando eventuais Pix pendentes)."""
    sub = db.query(OrganizationSubscription).filter_by(id=subscription.id).with_for_update().one()
    if not is_pix_order_subscription(sub) or not sub.is_current:
        raise MercadoPagoError("acao_indisponivel")
    if api:
        pending = (
            db.query(OrganizationSubscriptionPayment)
            .filter_by(subscription_id=sub.id, status="PENDING")
            .all()
        )
        for row in pending:
            if row.mercadopago_order_id:
                try:
                    cancel_pix_payment(db, api, row)
                except MercadoPagoError:
                    logger.warning("Mercado Pago Pix: falha ao cancelar order pendente=%s ao encerrar assinatura", row.mercadopago_order_id)
    was_paused = sub.mercadopago_status == "paused"
    sub.mercadopago_status = "cancelled"
    sub.status = "encerrada_pausada" if was_paused else "encerrada"
    sub.is_current = False
    sub.next_due_date = None
    sub.last_synced_at = _now()
    if not sub.mercadopago_ended_at:
        sub.mercadopago_ended_at = _now()


def pause_or_resume_pix_subscription(db, subscription: OrganizationSubscription, target: str) -> None:
    """Pausa ou reativa localmente a assinatura Pix (operação administrativa)."""
    sub = db.query(OrganizationSubscription).filter_by(id=subscription.id).with_for_update().one()
    if not is_pix_order_subscription(sub) or not sub.is_current:
        raise MercadoPagoError("acao_indisponivel")
    if target == "paused" and sub.mercadopago_status == "authorized":
        sub.mercadopago_status = "paused"
        sub.last_synced_at = _now()
    elif target == "authorized" and sub.mercadopago_status == "paused":
        sub.mercadopago_status = "authorized"
        sub.last_synced_at = _now()
    else:
        raise MercadoPagoError("acao_indisponivel")


def reconcile_pix_payment(db, api: MercadoPagoPixOrdersAPI, payment: OrganizationSubscriptionPayment) -> str:
    """Concilia uma cobrança Pix específica consultando a API de Orders."""
    payment = db.query(OrganizationSubscriptionPayment).filter_by(id=payment.id).with_for_update().one()
    subscription = db.query(OrganizationSubscription).filter_by(id=payment.subscription_id).with_for_update().one()
    if subscription.environment != api.environment:
        raise MercadoPagoError("ambiente_invalido")
    if payment.mercadopago_order_id:
        order = api.get_order(payment.mercadopago_order_id)
        return apply_order(db, payment, order)
    if not payment.mercadopago_order_reference:
        return payment.status
    now = _now()
    matches = api.search_orders_by_reference(
        payment.mercadopago_order_reference,
        since=now - timedelta(days=7),
        until=now + timedelta(minutes=5),
    )
    if not matches:
        payment.last_event_at = now
        return "nao_encontrada"
    if len(matches) != 1:
        raise MercadoPagoError("referencia_duplicada")
    order_id = resource_id(matches[0].get("id"))
    if not order_id:
        raise MercadoPagoError("order_invalida")
    order = api.get_order(order_id)
    return apply_order(db, payment, order)


def reconcile_pix_subscription(db, api: MercadoPagoPixOrdersAPI, subscription: OrganizationSubscription) -> None:
    """Concilia todas as cobranças Pix relevantes de uma assinatura (isoladas em savepoints individuais)."""
    sub = db.query(OrganizationSubscription).filter_by(id=subscription.id).with_for_update().one()
    sub.last_synced_at = _now()
    payments = (
        db.query(OrganizationSubscriptionPayment)
        .filter(
            OrganizationSubscriptionPayment.subscription_id == sub.id,
            OrganizationSubscriptionPayment.mercadopago_order_reference.isnot(None),
            OrganizationSubscriptionPayment.status.in_(["PENDING", "CONFIRMED"]),
        )
        .order_by(OrganizationSubscriptionPayment.id.desc())
        .limit(12)
        .all()
    )
    issues = []
    for payment in payments:
        try:
            with db.begin_nested():
                reconcile_pix_payment(db, api, payment)
        except MercadoPagoError as error:
            issues.append(str(error))
            logger.warning("Mercado Pago Pix: conciliação payment_id=%s erro=%s", payment.id, error)
    sub.sync_issue = f"Conciliação Pix: {issues[-1]}." if issues else None


def process_order_event(db, api: MercadoPagoPixOrdersAPI, order_id: str) -> str:
    """Processa um evento da fila de webhook `order`, sempre consultando GET /v1/orders/{id}."""
    order = api.get_order(order_id)
    remote_id = resource_id(order.get("id"))
    remote_ref = order.get("external_reference")
    match = OrganizationSubscriptionPayment.mercadopago_order_id == remote_id
    prefix = _reference_prefix(api.environment)
    if isinstance(remote_ref, str) and remote_ref.startswith(prefix):
        match = or_(match, OrganizationSubscriptionPayment.mercadopago_order_reference == remote_ref)
    payment = (
        db.query(OrganizationSubscriptionPayment)
        .join(OrganizationSubscription, OrganizationSubscription.id == OrganizationSubscriptionPayment.subscription_id)
        .filter(OrganizationSubscription.environment == api.environment, match)
        .with_for_update()
        .one_or_none()
    )
    if not payment:
        return "sem_vinculo"
    # Lock da assinatura na mesma transação para serializar atualização de ciclo/acesso.
    db.query(OrganizationSubscription).filter_by(id=payment.subscription_id).with_for_update().one()
    return apply_order(db, payment, order)


def latest_active_pix_payment(db, subscription_id: int) -> OrganizationSubscriptionPayment | None:
    """Retorna a cobrança Pix pendente ou recém-expirada mais relevante para exibição ao cliente."""
    return (
        db.query(OrganizationSubscriptionPayment)
        .filter(
            OrganizationSubscriptionPayment.subscription_id == subscription_id,
            OrganizationSubscriptionPayment.mercadopago_order_reference.isnot(None),
            OrganizationSubscriptionPayment.status.in_(["PENDING", "EXPIRED"]),
        )
        .order_by(OrganizationSubscriptionPayment.id.desc())
        .first()
    )


def can_renew_pix_now(subscription: OrganizationSubscription | None, today: date) -> bool:
    """Permite renovar antecipadamente a partir de 7 dias antes do vencimento (ou já vencido)."""
    if not subscription or not is_pix_order_subscription(subscription):
        return False
    if subscription.mercadopago_status != "authorized" or not subscription.next_due_date:
        return False
    return today >= subscription.next_due_date - timedelta(days=PIX_RENEWAL_WINDOW_DAYS)


def _process_pix_events(api: MercadoPagoPixOrdersAPI) -> None:
    event_env = pix_event_environment(api.environment)
    for _ in range(20):
        with SessionLocal() as db:
            event = (
                db.query(MercadoPagoEvent)
                .filter(
                    MercadoPagoEvent.environment == event_env,
                    MercadoPagoEvent.state == "pending",
                    MercadoPagoEvent.retry_at <= _now(),
                )
                .order_by(MercadoPagoEvent.id)
                .with_for_update(skip_locked=True)
                .first()
            )
            if not event:
                return
            event.attempts += 1
            try:
                with db.begin_nested():
                    event.result = process_order_event(db, api, event.resource_id)
                event.state = "processed"
            except MercadoPagoError as error:
                event.result = str(error)
                event.retry_at = _now() + timedelta(seconds=min(1800, 30 * (2 ** event.attempts)))
                if event.attempts >= 8:
                    event.state = "failed"
                logger.warning(
                    "Mercado Pago Pix: evento=%s ambiente=%s order=%s tentativa=%s erro=%s",
                    event.id, event.environment, event.resource_id, event.attempts, error,
                )
            db.commit()


def _reconcile_due_pix_payments(api: MercadoPagoPixOrdersAPI) -> None:
    for _ in range(10):
        with SessionLocal() as db:
            now = _now()
            payment = (
                db.query(OrganizationSubscriptionPayment)
                .join(OrganizationSubscription, OrganizationSubscription.id == OrganizationSubscriptionPayment.subscription_id)
                .filter(
                    OrganizationSubscription.environment == api.environment,
                    OrganizationSubscriptionPayment.mercadopago_order_reference.isnot(None),
                    or_(
                        (OrganizationSubscriptionPayment.status == "PENDING")
                        & (
                            OrganizationSubscriptionPayment.last_event_at.is_(None)
                            | (OrganizationSubscriptionPayment.last_event_at < now - timedelta(seconds=60))
                        ),
                        (OrganizationSubscriptionPayment.status == "CONFIRMED")
                        & (OrganizationSubscriptionPayment.paid_date >= today_in_app_timezone() - timedelta(days=180))
                        & (
                            OrganizationSubscriptionPayment.last_event_at.is_(None)
                            | (OrganizationSubscriptionPayment.last_event_at < now - timedelta(hours=6))
                        ),
                    ),
                )
                .order_by(
                    (OrganizationSubscriptionPayment.status == "PENDING").desc(),
                    OrganizationSubscriptionPayment.last_event_at.asc().nullsfirst(),
                )
                .with_for_update(skip_locked=True)
                .first()
            )
            if not payment:
                return
            try:
                with db.begin_nested():
                    reconcile_pix_payment(db, api, payment)
            except MercadoPagoError as error:
                payment.last_event_at = _now()
                if payment.subscription:
                    payment.subscription.sync_issue = f"Conciliação Pix: {error}."
                logger.warning(
                    "Mercado Pago Pix: conciliação ambiente=%s payment_id=%s erro=%s",
                    api.environment, payment.id, error,
                )
            db.commit()


def _tick_pix() -> None:
    settings = get_settings()
    for env in (MERCADOPAGO_LIVE, MERCADOPAGO_TEST):
        if not pix_webhook_available(env, settings):
            continue
        api = MercadoPagoPixOrdersAPI(env, settings)
        _process_pix_events(api)
        _reconcile_due_pix_payments(api)


async def run_mercadopago_pix_loop() -> None:
    while True:
        try:
            await run_in_threadpool(_tick_pix)
        except Exception:
            logger.exception("Mercado Pago Pix: falha na fila ou conciliação")
        await asyncio.sleep(10)
