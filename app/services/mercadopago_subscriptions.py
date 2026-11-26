"""Cliente e conciliação da API de Assinaturas do Mercado Pago."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from sqlalchemy import or_
from starlette.concurrency import run_in_threadpool

from app.core.config import get_settings
from app.core.timezone import app_now, today_in_app_timezone
from app.db.session import SessionLocal
from app.models import MercadoPagoEvent, Organization, OrganizationSubscription, OrganizationSubscriptionPayment
from app.services.billing_environment import MERCADOPAGO_LIVE, MERCADOPAGO_TEST, mercadopago_environment
from app.services.asaas_environment import app_public_base

logger = logging.getLogger(__name__)
API_URL = "https://api.mercadopago.com"
CYCLES = {"MONTHLY": 1, "YEARLY": 12}
TOPICS = {
    "subscription_preapproval": "/preapproval/",
    "subscription_authorized_payment": "/authorized_payments/",
    "subscription_preapproval_plan": "/preapproval_plan/",
    "payment": "/v1/payments/",
}
PAYMENT_TYPES = {"credit_card": "CREDIT_CARD", "debit_card": "DEBIT_CARD",
                 "bank_transfer": "PIX", "ticket": "BOLETO", "account_money": "ACCOUNT_MONEY"}
CONFIRMATION_PENDING = {"in_process", "authorized"}


class MercadoPagoError(Exception):
    def __init__(self, code: str, http_status: int | None = None, *, reason: str | None = None,
                 request_id: str | None = None):
        super().__init__(code)
        self.http_status = http_status
        self.reason = reason
        self.request_id = request_id


CHECKOUT_REJECTIONS = {400, 401, 403, 404, 422}
CHECKOUT_ERROR_MESSAGES = {
    "ambiente_do_comprador": "O Mercado Pago recusou a combinação das contas. Confira se o vendedor e o comprador pertencem ao mesmo ambiente.",
    "comprador_igual_ao_vendedor": "O Mercado Pago exige contas diferentes para vendedor e comprador.",
    "email_do_comprador": "O Mercado Pago recusou o e-mail informado. Confira o e-mail da conta do comprador.",
    "email_de_teste_em_producao": "Informe o e-mail da sua conta real do Mercado Pago.",
    "data_da_cobranca": "O Mercado Pago recusou a data de cobrança. Informe à equipe SiSFarm a data que aparece no resumo do plano.",
    "credencial_invalida": "O Mercado Pago recusou o Access Token. Confira se ele pertence à aplicação de assinaturas.",
    "politica_do_mercado_pago": "O Mercado Pago bloqueou a solicitação por uma política de acesso. Informe ao suporte do Mercado Pago o código e a referência desta tentativa.",
}


def _preapproval_error_reason(response: httpx.Response) -> str | None:
    """Classifica mensagens do provedor sem repassar seu conteúdo aos logs ou à interface."""
    if response.status_code not in CHECKOUT_REJECTIONS:
        return None
    try:
        data = response.json()
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    if response.status_code in {401, 403}:
        # Retorne somente classificações conhecidas. A resposta pode conter outros dados da conta.
        codes = {str(data.get(field, "")).casefold() for field in ("error", "code")}
        if "pa_unauthorized_result_from_policies" in codes:
            return "politica_do_mercado_pago"
        if codes & {"invalid_credentials", "invalid_token"}:
            return "credencial_invalida"
        return None
    if response.status_code not in {400, 422}:
        return None
    cause = data.get("cause")
    entries = cause if isinstance(cause, list) else [cause]
    messages = [data.get("message"), data.get("error")]
    for entry in entries:
        if isinstance(entry, dict):
            messages.extend(entry.get(field) for field in ("code", "description", "message"))
    detail = " ".join(value[:512] for value in messages if isinstance(value, str)).casefold()
    if "real or test users" in detail or "invalid users involved" in detail:
        return "ambiente_do_comprador"
    if ("payer" in detail and "collector" in detail
            and any(word in detail for word in ("different", "the same", "same user", "iguais"))):
        return "comprador_igual_ao_vendedor"
    if any(marker in detail for marker in ("payer_email", "payer email", "email of the payer", "invalid email", "payer not found")):
        return "email_do_comprador"
    if any(marker in detail for marker in ("start_date", "end_date", "billing date")):
        return "data_da_cobranca"
    return None


def checkout_error_message(error: MercadoPagoError) -> str:
    if error.reason in CHECKOUT_ERROR_MESSAGES:
        message = CHECKOUT_ERROR_MESSAGES[error.reason]
    elif error.http_status in {401, 403}:
        message = "O Mercado Pago recusou a credencial ou a permissão da aplicação. Confira a configuração com a equipe SiSFarm."
    elif error.http_status in CHECKOUT_REJECTIONS:
        message = "O Mercado Pago recusou a criação da assinatura. Informe o código da tentativa à equipe SiSFarm."
    elif str(error) == "organizacao_indisponivel":
        return "A organização está indisponível para uma nova contratação."
    else:
        message = "A tentativa está em verificação. Acompanhe a situação em Minha assinatura."
    reference = f" Referência do Mercado Pago {error.request_id}." if error.request_id else ""
    return f"{message} Código {error}.{reference}"


def configuration_error(settings=None) -> str | None:
    settings = settings or get_settings()
    environment = mercadopago_environment(settings)
    if not environment:
        return "O Mercado Pago está desativado."
    if environment == MERCADOPAGO_LIVE:
        if settings.mercadopago_sandbox:
            return "Selecione somente um ambiente do Mercado Pago."
        if not settings.mercadopago_live_seller_id or settings.mercadopago_live_seller_id <= 0:
            return "Configure o ID da conta brasileira que receberá as assinaturas reais."
        token = (settings.mercadopago_live_access_token or "").strip()
        secret = (settings.mercadopago_live_webhook_secret or "").strip()
        if not token.startswith("APP_USR-") or token == (settings.mercadopago_access_token or "").strip():
            return "Configure um Access Token próprio da aplicação de produção do Mercado Pago."
        if len(secret) < 32 or secret in {token, (settings.mercadopago_webhook_secret or "").strip()}:
            return "Configure o segredo exclusivo do webhook de produção do Mercado Pago."
        return None
    missing = [name.upper() for name in ("mercadopago_access_token", "mercadopago_public_key", "mercadopago_webhook_secret")
               if not (getattr(settings, name) or "").strip()]
    if missing:
        return "Configure " + ", ".join(missing) + " com os dados da aplicação usada para homologação."
    return None


def checkout_configuration_error(settings=None) -> str | None:
    settings = settings or get_settings()
    error = configuration_error(settings)
    if error:
        return error
    if mercadopago_environment(settings) == MERCADOPAGO_LIVE and not settings.mercadopago_live_checkout_enabled:
        return "Novas contratações estão temporariamente indisponíveis."
    if mercadopago_environment(settings) == MERCADOPAGO_LIVE:
        try:
            url = urlsplit(app_public_base(settings))
            if (url.scheme == "https" and url.hostname and url.port in {None, 443}
                    and not (url.path or url.username or url.password or url.query or url.fragment)):
                return None
        except ValueError:
            pass
        return "Configure o endereço HTTPS público do SiSFarm em APP_PUBLIC_URL."
    return None


def first_due_date(trial_ends_on: date | None, today: date) -> date:
    return max(today, trial_ends_on + timedelta(days=1)) if trial_ends_on else today


def resource_id(value) -> str | None:
    return str(value) if re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", str(value or "")) else None


def safe_link(value) -> str | None:
    if not isinstance(value, str) or len(value) > 512 or "\\" in value or any(ord(c) <= 32 for c in value):
        return None
    try:
        url = urlsplit(value)
        if url.scheme == "https" and url.hostname in {"mercadopago.com.br", "www.mercadopago.com.br",
                                                      "sandbox.mercadopago.com.br"} and not url.username and not url.password and url.port in {None, 443}:
            return value
    except ValueError:
        return None
    return None


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _date(value):
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return stamp.astimezone(app_now().tzinfo).date() if stamp.tzinfo else stamp.date()
    except ValueError:
        return None


def _amount(value):
    try:
        amount = Decimal(str(value))
        if amount.is_finite() and 0 <= amount < Decimal("10000000000"):
            return amount
    except InvalidOperation:
        pass
    return None


class MercadoPagoAPI:
    def __init__(self, settings=None):
        settings = settings or get_settings()
        self.environment = mercadopago_environment(settings)
        if configuration_error(settings):
            raise MercadoPagoError("producao_indisponivel" if self.environment == MERCADOPAGO_LIVE else "sandbox_indisponivel")
        self.checkout_enabled = checkout_configuration_error(settings) is None
        self.seller_id = settings.mercadopago_live_seller_id if self.environment == MERCADOPAGO_LIVE else None
        self.token = (settings.mercadopago_live_access_token if self.environment == MERCADOPAGO_LIVE
                      else settings.mercadopago_access_token).strip()
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
            if (isinstance(account, dict) and isinstance(account.get("id"), int)
                    and account.get("site_id") == "MLB" and isinstance(tags, list)):
                email = str(account.get("email") or "").lower()
                fictitious = "test_user" in tags or email.endswith("@testuser.com")
                if ((self.environment == MERCADOPAGO_TEST and "test_user" in tags and email.endswith("@testuser.com"))
                        or (self.environment == MERCADOPAGO_LIVE and account["id"] == self.seller_id and email and not fictitious)):
                    self._seller = {"id": account["id"], "email": email}
        return self._seller

    def request(self, method, path, *, params=None, payload=None):
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}
        try:
            with httpx.Client(base_url=API_URL, headers=headers, timeout=8.0) as client:
                seller = self._verified_seller(client)
                if self.environment == MERCADOPAGO_LIVE and not seller:
                    raise MercadoPagoError("vendedor_de_producao_nao_confirmado")
                # O endpoint de assinaturas recusa X-scope: stage para a aplicação do vendedor fictício.
                if method == "POST" and path == "/preapproval":
                    if not self.checkout_enabled:
                        raise MercadoPagoError("novas_contratacoes_desativadas")
                    email = str((payload or {}).get("payer_email") or "").strip().lower()
                    test_email = bool(re.fullmatch(r"[a-z0-9._+-]+@testuser\.com", email))
                    if self.environment == MERCADOPAGO_TEST and seller and not test_email:
                        raise MercadoPagoError("comprador_de_teste_invalido", reason="email_do_comprador")
                    if self.environment == MERCADOPAGO_LIVE and test_email:
                        raise MercadoPagoError("comprador_de_teste_invalido", reason="email_de_teste_em_producao")
                    if email == seller.get("email"):
                        raise MercadoPagoError("comprador_de_teste_invalido", reason="comprador_igual_ao_vendedor")
                scope = {} if seller else {"X-scope": "stage"}
                response = client.request(method, path, params=params, json=payload, headers=scope)
        except httpx.HTTPError:
            logger.warning("Mercado Pago: falha de conexão ambiente=%s method=%s endpoint=%s", self.environment, method, path)
            raise MercadoPagoError("falha_de_conexao") from None
        if not response.is_success:
            response_id = resource_id(response.headers.get("x-request-id"))
            reason = _preapproval_error_reason(response) if method == "POST" and path == "/preapproval" else None
            logger.warning("Mercado Pago: ambiente=%s method=%s endpoint=%s status=%s request_id=%s motivo=%s",
                           self.environment, method, path, response.status_code, response_id or "-", reason or "-")
            raise MercadoPagoError(f"http_{response.status_code}", response.status_code, reason=reason, request_id=response_id)
        try:
            data = response.json()
        except ValueError:
            raise MercadoPagoError("resposta_invalida") from None
        if not isinstance(data, dict):
            raise MercadoPagoError("resposta_invalida")
        live_mode = data.get("live_mode")
        owned = bool(seller) and str(data.get("collector_id")) == str(seller["id"])
        subscription = path == "/preapproval" or (
            method == "GET" and path.startswith("/preapproval/") and path != "/preapproval/search"
        )
        if path.startswith("/v1/payments/"):
            if self.environment == MERCADOPAGO_LIVE:
                if live_mode is not True or not owned:
                    raise MercadoPagoError("pagamento_fora_da_conta_de_producao")
            elif live_mode is not False and not (live_mode is True and owned):
                raise MercadoPagoError("resposta_fora_do_sandbox")
        elif self.environment == MERCADOPAGO_TEST and live_mode is True:
            raise MercadoPagoError("resposta_fora_do_sandbox")
        elif self.environment == MERCADOPAGO_LIVE and (live_mode is False or (subscription and not owned)):
            raise MercadoPagoError("recurso_fora_da_conta_de_producao")
        return data

    def get(self, topic, identifier):
        identifier = resource_id(identifier)
        if not identifier:
            raise MercadoPagoError("identificador_invalido")
        data = self.request("GET", TOPICS[topic] + identifier)
        if resource_id(data.get("id")) != identifier:
            raise MercadoPagoError("recurso_divergente")
        return data

    def search(self, path, **filters):
        offset = 0
        invoice_search = path == "/authorized_payments/search"
        params = dict(filters)
        if not invoice_search:
            params["limit"] = 25
        while True:
            if offset:
                params["offset"] = offset
            page = self.request("GET", path, params=params)
            rows, paging = page.get("results"), page.get("paging", {})
            if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
                raise MercadoPagoError("lista_invalida")
            yield from rows
            offset += len(rows)
            paging = paging if isinstance(paging, dict) else {}
            total = paging.get("total")
            limit = paging.get("limit", 12 if invoice_search else 25)
            if not rows or (offset >= total if isinstance(total, int) else len(rows) < limit):
                break


def _apply_subscription(subscription, remote):
    identifier = resource_id(remote.get("id"))
    if (not identifier or remote.get("external_reference") != subscription.mercadopago_reference
            or subscription.mercadopago_id not in (None, identifier)):
        raise MercadoPagoError("assinatura_divergente")
    status = remote.get("status")
    if status not in ("pending", "authorized", "paused", "cancelled", "canceled"):
        raise MercadoPagoError("status_da_assinatura_desconhecido")
    if status in ("pending", "authorized"):
        terms = remote.get("auto_recurring")
        if (not isinstance(terms, dict) or terms.get("currency_id") != "BRL" or _amount(terms.get("transaction_amount")) != subscription.amount
                or terms.get("frequency_type") != "months" or terms.get("frequency") != CYCLES[subscription.cycle]):
            raise MercadoPagoError("condicoes_da_assinatura_divergentes")
    version = remote.get("version")
    if isinstance(version, int) and subscription.mercadopago_version is not None and version < subscription.mercadopago_version:
        return
    if subscription.mercadopago_status in {"canceled", "cancelled"} and status not in {"canceled", "cancelled"}:
        return
    previous_status = subscription.mercadopago_status
    subscription.mercadopago_id = identifier
    if isinstance(version, int):
        subscription.mercadopago_version = version
    subscription.mercadopago_status = status
    subscription.next_due_date = _date(remote.get("next_payment_date")) if status in {"pending", "authorized"} else None
    subscription.mercadopago_checkout_url = safe_link(remote.get("init_point")) if status == "pending" else None
    subscription.last_synced_at = _now()
    subscription.sync_issue = None
    subscription.status = "criada"
    if status == "pending" and not subscription.mercadopago_checkout_url:
        subscription.status = "revisao"
        subscription.sync_issue = "Aguardando link de autorização do Mercado Pago."
    if status in {"cancelled", "canceled"}:
        # Assinatura cancelada enquanto pausada não restaura acesso pelo período pago restante.
        subscription.status = "encerrada_pausada" if previous_status == "paused" else "encerrada"
        subscription.is_current = False
        if not subscription.mercadopago_ended_at:
            subscription.mercadopago_ended_at = _now()


def reserve_checkout(db, *, organization, plan, amount, cycle, due, email, environment=MERCADOPAGO_TEST):
    """Reserva local sob o bloqueio da organização. O chamador confirma antes do POST remoto."""
    if not db.query(Organization.id).filter_by(id=organization.id, archived_at=None, is_active=True).with_for_update().first():
        raise MercadoPagoError("organizacao_indisponivel")
    current = db.query(OrganizationSubscription).filter_by(organization_id=organization.id, is_current=True).first()
    if current:
        if not can_replace_test_subscription(current, environment):
            raise MercadoPagoError("assinatura_existente")
        current.is_current = False
        db.flush()
    row = OrganizationSubscription(
        organization_id=organization.id, plan_id=plan.id, plan_name=plan.name, amount=amount, cycle=cycle,
        first_due_date=due, billing_type="UNDEFINED", environment=environment,
        mercadopago_payer_email=email,
        mercadopago_reference=f"sisfarm-{'live' if environment == MERCADOPAGO_LIVE else 'test'}-{uuid4().hex}",
        last_synced_at=_now(),
    )
    db.add(row)
    db.flush()
    return row


def can_replace_test_subscription(current, environment):
    return current.environment == "sandbox" or (
        environment == MERCADOPAGO_LIVE and current.environment == MERCADOPAGO_TEST
    )


def create_checkout(db, api, subscription):
    subscription = db.query(OrganizationSubscription).filter_by(id=subscription.id).with_for_update().one()
    if subscription.mercadopago_id:
        return subscription.mercadopago_checkout_url
    if subscription.status != "criando":
        raise MercadoPagoError("criacao_ja_iniciada")
    recurring = {"frequency": CYCLES[subscription.cycle], "frequency_type": "months",
                 "transaction_amount": float(subscription.amount), "currency_id": "BRL"}
    if subscription.first_due_date > today_in_app_timezone():
        start_utc = datetime.combine(subscription.first_due_date, time.min, app_now().tzinfo).astimezone(timezone.utc)
        recurring["start_date"] = start_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    payload = {"reason": f"SiSFarm - {subscription.plan_name}", "external_reference": subscription.mercadopago_reference,
               "payer_email": subscription.mercadopago_payer_email, "back_url": app_public_base() + "/minha-assinatura", "status": "pending",
               "auto_recurring": recurring}
    try:
        _apply_subscription(subscription, api.request("POST", "/preapproval", payload=payload))
    except MercadoPagoError as error:
        subscription.last_synced_at = _now()
        diagnostic = [str(error), error.reason, error.request_id]
        stage = " em teste" if subscription.environment == MERCADOPAGO_TEST else ""
        subscription.sync_issue = f"Criação da assinatura{stage}: {' · '.join(filter(None, diagnostic))}."
        subscription.status = "revisao"
        # Falhas inconclusivas são recuperadas por referência, sem repetir o POST.
        if error.http_status in CHECKOUT_REJECTIONS or str(error) in {
            "comprador_de_teste_invalido", "vendedor_de_producao_nao_confirmado", "novas_contratacoes_desativadas",
        }:
            subscription.is_current = False
            subscription.status = "falhou"
        db.commit()
        logger.warning("Mercado Pago: criação ambiente=%s subscription_id=%s resultado=%s",
                       subscription.environment, subscription.id, error)
        raise
    db.commit()
    return subscription.mercadopago_checkout_url


def _find_subscription(db, environment, remote_id, reference=None):
    if not resource_id(remote_id):
        return None
    match = OrganizationSubscription.mercadopago_id == str(remote_id)
    prefix = "sisfarm-live-" if environment == MERCADOPAGO_LIVE else "sisfarm-test-"
    if isinstance(reference, str) and reference.startswith(prefix):
        match = or_(match, OrganizationSubscription.mercadopago_reference == reference)
    return db.query(OrganizationSubscription).filter(
        OrganizationSubscription.environment == environment, match,
    ).with_for_update().one_or_none()


def _apply_invoice(db, api, invoice):
    identifier = resource_id(invoice.get("id"))
    remote_id = resource_id(invoice.get("preapproval_id"))
    if not identifier or not remote_id:
        raise MercadoPagoError("fatura_invalida")
    subscription = _find_subscription(db, getattr(api, "environment", MERCADOPAGO_TEST), remote_id, invoice.get("external_reference"))
    if not subscription:
        return "sem_vinculo"
    if not subscription.mercadopago_id:
        _apply_subscription(subscription, api.get("subscription_preapproval", remote_id))
    # Releitura sob bloqueio garante que eventos em paralelo usem a tentativa atual da fatura.
    invoice = api.get("subscription_authorized_payment", identifier)
    if resource_id(invoice.get("preapproval_id")) != subscription.mercadopago_id:
        raise MercadoPagoError("fatura_divergente")
    amount = _amount(invoice.get("transaction_amount"))
    if invoice.get("currency_id") != "BRL" or amount != subscription.amount:
        raise MercadoPagoError("valor_da_fatura_divergente")
    payment_id = _invoice_payment_id(invoice)
    payment = api.get("payment", payment_id) if payment_id else {}
    if payment and (payment.get("currency_id") != "BRL" or _amount(payment.get("transaction_amount")) != amount):
        raise MercadoPagoError("pagamento_divergente")
    remote_status = payment.get("status")
    due = _date(invoice.get("debit_date"))
    existing = db.query(OrganizationSubscriptionPayment).filter_by(mercadopago_invoice_id=identifier).one_or_none()
    if existing and existing.subscription_id != subscription.id:
        raise MercadoPagoError("fatura_ja_vinculada")
    if existing and existing.due_date:
        due = min(due, existing.due_date) if due else existing.due_date
    refunded = _amount(payment.get("transaction_amount_refunded"))
    status = "PENDING"
    if remote_status == "approved":
        status = "CONFIRMED"
    elif remote_status in {"refunded", "charged_back"}:
        status = "REFUNDED" if remote_status == "refunded" else "CHARGEBACK_REQUESTED"
    elif remote_status == "in_mediation":
        status = "CHARGEBACK_DISPUTE"
    elif (
        (remote_status in {"rejected", "cancelled", "canceled"}
         and (invoice.get("status") == "processed" or (due is not None and due <= today_in_app_timezone())))
        or (due and due < today_in_app_timezone())
    ):
        status = "OVERDUE"
    elif remote_status in CONFIRMATION_PENDING:
        status = "AWAITING_RISK_ANALYSIS"
    row = existing or OrganizationSubscriptionPayment(subscription_id=subscription.id, mercadopago_invoice_id=identifier)
    if not existing:
        db.add(row)
    row.mercadopago_payment_id, row.amount, row.status = payment_id, amount, status
    row.due_date = due
    row.refunded_amount = amount if status == "REFUNDED" else refunded
    if status in {"CONFIRMED", "REFUNDED", "CHARGEBACK_REQUESTED", "CHARGEBACK_DISPUTE"}:
        row.paid_date = _date(payment.get("date_approved")) or row.paid_date
    else:
        row.paid_date = None
    method = PAYMENT_TYPES.get(payment.get("payment_type_id"))
    if payment.get("payment_method_id") == "pix":
        method = "PIX"
    row.billing_type = method or row.billing_type
    if method:
        subscription.billing_type = method
    # Links legados são renderizados na mesma tela. Só persista URLs validadas.
    transaction = payment.get("transaction_details")
    transaction = transaction if isinstance(transaction, dict) else {}
    row.invoice_url = (safe_link(transaction.get("external_resource_url")) or "")[:255] or None
    row.bank_slip_url = row.invoice_url if method == "BOLETO" else None
    row.last_event_at = _now()
    return status


def _invoice_payment_id(invoice):
    payment = invoice.get("payment")
    return resource_id(payment.get("id")) if isinstance(payment, dict) else None


def process_event(db, api, topic, identifier):
    remote = api.get(topic, identifier)
    if topic == "subscription_preapproval_plan":
        # O catálogo é local. Registra o evento sem importar regras de um plano externo.
        status = remote.get("status")
        return f"plano_consultado:{status}" if status in {"active", "inactive", "paused", "cancelled", "canceled"} else "plano_consultado"
    if topic == "subscription_preapproval":
        row = _find_subscription(db, getattr(api, "environment", MERCADOPAGO_TEST), identifier, remote.get("external_reference"))
        if not row:
            return "sem_vinculo"
        _apply_subscription(row, api.get(topic, identifier))
        return row.mercadopago_status
    if topic == "payment":
        for invoice in api.search("/authorized_payments/search", payment_id=identifier):
            if _invoice_payment_id(invoice) == identifier:
                return _apply_invoice(db, api, invoice)
        # Se o pagamento chegar antes da fatura, a conciliação periódica fecha o vínculo.
        return "sem_fatura"
    return _apply_invoice(db, api, remote)


def reconcile(db, api, row):
    row.last_synced_at = _now()
    if not row.mercadopago_id:
        email = (row.mercadopago_payer_email or "").strip().lower()
        if not email:
            return
        matches = [item for item in api.search("/preapproval/search", payer_email=email)
                   if item.get("external_reference") == row.mercadopago_reference]
        if not matches:
            return
        if len(matches) != 1:
            raise MercadoPagoError("referencia_duplicada")
        remote = api.get("subscription_preapproval", matches[0].get("id"))
    else:
        remote = api.get("subscription_preapproval", row.mercadopago_id)
    _apply_subscription(row, remote)
    if row.mercadopago_status != "pending":
        try:
            invoices = list(api.search("/authorized_payments/search", preapproval_id=row.mercadopago_id))
        except MercadoPagoError as error:
            row.sync_issue = f"Conciliação de cobranças: {error}."
            logger.warning("Mercado Pago: conciliação de cobranças subscription_id=%s erro=%s", row.id, error)
            return
        issues = []
        for invoice in invoices:
            if resource_id(invoice.get("preapproval_id")) != row.mercadopago_id:
                continue
            try:
                # Isola cada fatura: falha em uma cobrança não reverte as demais da assinatura.
                with db.begin_nested():
                    _apply_invoice(db, api, invoice)
            except MercadoPagoError as error:
                invoice_id = resource_id(invoice.get("id")) or "-"
                issues.append(str(error))
                logger.warning("Mercado Pago: conciliação da fatura=%s subscription_id=%s erro=%s",
                               invoice_id, row.id, error)
        if issues:
            row.sync_issue = f"Conciliação de cobranças: {issues[-1]}."


def release_unconfirmed_checkout(db, api, row) -> str:
    """Libera uma reserva em revisão que não chegou a ser criada no Mercado Pago."""
    if row.environment != getattr(api, "environment", MERCADOPAGO_TEST):
        raise MercadoPagoError("ambiente_invalido")
    if row.mercadopago_id or not row.is_current or row.status not in {"criando", "revisao"}:
        raise MercadoPagoError("acao_indisponivel")
    email = (row.mercadopago_payer_email or "").strip().lower()
    if email:
        matches = [item for item in api.search("/preapproval/search", payer_email=email)
                   if item.get("external_reference") == row.mercadopago_reference]
        if len(matches) > 1:
            raise MercadoPagoError("referencia_duplicada")
        if len(matches) == 1:
            remote = api.get("subscription_preapproval", matches[0].get("id"))
            _apply_subscription(row, remote)
            return "recuperada"
    row.is_current = False
    row.status = "falhou"
    row.last_synced_at = _now()
    row.sync_issue = "Tentativa liberada pela equipe após confirmar ausência no Mercado Pago."
    return "liberada"


def change_status(api, row, target):
    if not row.mercadopago_id or row.environment != getattr(api, "environment", MERCADOPAGO_TEST):
        raise MercadoPagoError("aguardando_vinculo")
    allowed = {"canceled": {"pending", "authorized", "paused"}, "paused": {"authorized"}, "authorized": {"paused"}}
    remote = api.get("subscription_preapproval", row.mercadopago_id)
    _apply_subscription(row, remote)
    expected = {"cancelled", "canceled"} if target == "canceled" else {target}
    if row.mercadopago_status in expected:
        return
    if row.mercadopago_status not in allowed[target]:
        raise MercadoPagoError("acao_indisponivel")
    remote_target = "cancelled" if target == "canceled" else target
    api.request("PUT", f"/preapproval/{row.mercadopago_id}", payload={"status": remote_target})
    _apply_subscription(row, api.get("subscription_preapproval", row.mercadopago_id))
    if row.mercadopago_status not in expected:
        raise MercadoPagoError("acao_nao_confirmada")


def _process_events(api):
    for _ in range(20):
        with SessionLocal() as db:
            event = db.query(MercadoPagoEvent).filter(
                MercadoPagoEvent.environment == getattr(api, "environment", MERCADOPAGO_TEST),
                MercadoPagoEvent.state == "pending", MercadoPagoEvent.retry_at <= _now(),
            ).order_by(MercadoPagoEvent.id).with_for_update(skip_locked=True).first()
            if not event:
                return
            event.attempts += 1
            try:
                with db.begin_nested():
                    result = process_event(db, api, event.topic, event.resource_id)
                    # Pagamento pode chegar segundos antes da fatura aparecer em /authorized_payments/search.
                    if event.topic == "payment" and result == "sem_fatura" and event.attempts < 3:
                        raise MercadoPagoError("fatura_ainda_nao_vinculada")
                    event.result = result
                event.state = "processed"
            except MercadoPagoError as error:
                event.result = str(error)
                event.retry_at = _now() + timedelta(seconds=min(1800, 30 * (2 ** event.attempts)))
                if event.attempts >= 8:
                    event.state = "failed"
                logger.warning("Mercado Pago: evento=%s ambiente=%s tópico=%s recurso=%s tentativa=%s erro=%s",
                               event.id, event.environment, event.topic, event.resource_id, event.attempts, error)
            db.commit()


def _tick():
    if configuration_error():
        return
    api = MercadoPagoAPI()
    _process_events(api)
    for _ in range(10):
        with SessionLocal() as db:
            now = _now()
            row = db.query(OrganizationSubscription).filter(
                OrganizationSubscription.environment == getattr(api, "environment", MERCADOPAGO_TEST),
                or_(
                    OrganizationSubscription.mercadopago_reference.is_(None),
                    ~OrganizationSubscription.mercadopago_reference.like("sisfarm-pix-%"),
                ),
                or_(
                    OrganizationSubscription.is_current.is_(True) & (
                        OrganizationSubscription.last_synced_at.is_(None)
                        | (OrganizationSubscription.last_synced_at < now - timedelta(minutes=5))
                    ),
                    (OrganizationSubscription.mercadopago_ended_at >= now - timedelta(days=180))
                    & (
                        (OrganizationSubscription.last_synced_at <= OrganizationSubscription.mercadopago_ended_at)
                        | (OrganizationSubscription.last_synced_at < now - timedelta(hours=6))
                    ),
                ),
            ).order_by(OrganizationSubscription.is_current.desc(),
                       OrganizationSubscription.last_synced_at.asc().nullsfirst()).with_for_update(skip_locked=True).first()
            if not row:
                return
            try:
                with db.begin_nested():
                    reconcile(db, api, row)
            except MercadoPagoError as error:
                row.last_synced_at = _now()
                row.sync_issue = f"Conciliação da assinatura: {error}."
                logger.warning("Mercado Pago: conciliação ambiente=%s subscription_id=%s erro=%s",
                               row.environment, row.id, error)
            db.commit()


async def run_mercadopago_loop():
    while True:
        try:
            await run_in_threadpool(_tick)
        except Exception:
            logger.exception("Mercado Pago: falha na fila ou conciliação")
        await asyncio.sleep(10)
