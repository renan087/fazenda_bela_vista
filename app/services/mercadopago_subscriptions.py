"""Cliente e conciliação da API de Assinaturas do Mercado Pago em modo de teste."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, time, timedelta, timezone
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
from app.services.billing_environment import MERCADOPAGO_TEST
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
CONFIRMATION_PENDING = {"in_process", "authorized", "in_mediation"}


class MercadoPagoError(Exception):
    def __init__(self, code: str, http_status: int | None = None, *, reason: str | None = None,
                 request_id: str | None = None):
        super().__init__(code)
        self.http_status = http_status
        self.reason = reason
        self.request_id = request_id


CHECKOUT_REJECTIONS = {400, 401, 403, 404, 422}
CHECKOUT_ERROR_MESSAGES = {
    "ambiente_do_comprador": "O Mercado Pago recusou a combinação das contas. Confira se o vendedor e o comprador pertencem ao mesmo ambiente de teste.",
    "comprador_igual_ao_vendedor": "O Mercado Pago exige contas diferentes para vendedor e comprador. Use o e-mail do comprador de teste.",
    "email_do_comprador": "O Mercado Pago recusou o e-mail informado. Confira o e-mail da conta de comprador de teste.",
    "data_da_cobranca": "O Mercado Pago recusou a data de cobrança. Informe à equipe SiSFarm a data que aparece no resumo do plano.",
    "credencial_invalida": "O Mercado Pago recusou o Access Token. Confira se ele pertence à aplicação usada neste teste de assinaturas.",
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
        message = "O Mercado Pago recusou a criação da assinatura de teste. Informe o código da tentativa à equipe SiSFarm."
    elif str(error) == "organizacao_indisponivel":
        return "A organização está indisponível para uma nova contratação."
    else:
        message = "A tentativa está em verificação. Acompanhe a situação em Minha assinatura."
    reference = f" Referência do Mercado Pago {error.request_id}." if error.request_id else ""
    return f"{message} Código {error}.{reference}"


def configuration_error(settings=None) -> str | None:
    settings = settings or get_settings()
    if not settings.mercadopago_sandbox:
        return "O sandbox do Mercado Pago está desativado."
    missing = [name.upper() for name in ("mercadopago_access_token", "mercadopago_public_key", "mercadopago_webhook_secret")
               if not (getattr(settings, name) or "").strip()]
    if missing:
        return "Configure " + ", ".join(missing) + " com os dados da aplicação usada para homologação."
    return None


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
        if configuration_error(settings):
            raise MercadoPagoError("sandbox_indisponivel")
        self.token = settings.mercadopago_access_token.strip()
        self._test_seller: dict | None = None

    def _verified_test_seller(self, client: httpx.Client) -> dict:
        if self._test_seller is None:
            self._test_seller = {}
            try:
                response = client.get("/users/me")
                account = response.json() if response.is_success else {}
            except (httpx.HTTPError, ValueError):
                return self._test_seller
            if (isinstance(account, dict) and isinstance(account.get("id"), int)
                    and account.get("site_id") == "MLB" and isinstance(account.get("tags"), list)
                    and "test_user" in account["tags"] and str(account.get("email") or "").lower().endswith("@testuser.com")):
                self._test_seller = {"id": account["id"], "email": account["email"].lower()}
        return self._test_seller

    def request(self, method, path, *, params=None, payload=None):
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}
        try:
            with httpx.Client(base_url=API_URL, headers=headers, timeout=8.0) as client:
                seller = self._verified_test_seller(client)
                # O endpoint de assinaturas recusa X-scope: stage para a aplicação do vendedor fictício.
                if seller and method == "POST" and path == "/preapproval":
                    email = str((payload or {}).get("payer_email") or "").strip().lower()
                    if not re.fullmatch(r"[a-z0-9._+-]+@testuser\.com", email):
                        raise MercadoPagoError("comprador_de_teste_invalido", reason="email_do_comprador")
                    if email == seller["email"]:
                        raise MercadoPagoError("comprador_de_teste_invalido", reason="comprador_igual_ao_vendedor")
                scope = {} if seller else {"X-scope": "stage"}
                response = client.request(method, path, params=params, json=payload, headers=scope)
        except httpx.HTTPError:
            logger.warning("Mercado Pago sandbox: falha de conexão method=%s endpoint=%s", method, path)
            raise MercadoPagoError("falha_de_conexao") from None
        if not response.is_success:
            response_id = resource_id(response.headers.get("x-request-id"))
            reason = _preapproval_error_reason(response) if method == "POST" and path == "/preapproval" else None
            logger.warning("Mercado Pago sandbox: method=%s endpoint=%s status=%s request_id=%s motivo=%s",
                           method, path, response.status_code, response_id or "-", reason or "-")
            raise MercadoPagoError(f"http_{response.status_code}", response.status_code, reason=reason, request_id=response_id)
        try:
            data = response.json()
        except ValueError:
            raise MercadoPagoError("resposta_invalida") from None
        if not isinstance(data, dict) or data.get("live_mode") is True:
            raise MercadoPagoError("resposta_fora_do_sandbox")
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
        while True:
            page = self.request("GET", path, params={**filters, "offset": offset, "limit": 25})
            rows, paging = page.get("results"), page.get("paging", {})
            if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
                raise MercadoPagoError("lista_invalida")
            yield from rows
            offset += len(rows)
            total = paging.get("total") if isinstance(paging, dict) else None
            if not rows or (offset >= total if isinstance(total, int) else len(rows) < 25):
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
        subscription.status = "encerrada"
        subscription.is_current = False


def reserve_checkout(db, *, organization, plan, amount, cycle, due, email):
    """Reserva local sob o bloqueio da organização. O chamador confirma antes do POST remoto."""
    if not db.query(Organization.id).filter_by(id=organization.id, archived_at=None, is_active=True).with_for_update().first():
        raise MercadoPagoError("organizacao_indisponivel")
    current = db.query(OrganizationSubscription).filter_by(organization_id=organization.id, is_current=True).first()
    if current:
        if current.environment != "sandbox":
            raise MercadoPagoError("assinatura_existente")
        current.is_current = False
        db.flush()
    row = OrganizationSubscription(
        organization_id=organization.id, plan_id=plan.id, plan_name=plan.name, amount=amount, cycle=cycle,
        first_due_date=due, billing_type="UNDEFINED", environment=MERCADOPAGO_TEST,
        mercadopago_payer_email=email,
        mercadopago_reference=f"sisfarm-test-{uuid4().hex}", last_synced_at=_now(),
    )
    db.add(row)
    db.flush()
    return row


def create_checkout(db, api, subscription):
    subscription = db.query(OrganizationSubscription).filter_by(id=subscription.id).with_for_update().one()
    if subscription.mercadopago_id:
        return subscription.mercadopago_checkout_url
    if subscription.status != "criando":
        raise MercadoPagoError("criacao_ja_iniciada")
    recurring = {"frequency": CYCLES[subscription.cycle], "frequency_type": "months",
                 "transaction_amount": float(subscription.amount), "currency_id": "BRL"}
    if subscription.first_due_date > today_in_app_timezone():
        recurring["start_date"] = datetime.combine(subscription.first_due_date, time.min, app_now().tzinfo).isoformat()
    payload = {"reason": f"SiSFarm - {subscription.plan_name}", "external_reference": subscription.mercadopago_reference,
               "payer_email": subscription.mercadopago_payer_email, "back_url": app_public_base() + "/minha-assinatura", "status": "pending",
               "auto_recurring": recurring}
    try:
        _apply_subscription(subscription, api.request("POST", "/preapproval", payload=payload))
    except MercadoPagoError as error:
        subscription.last_synced_at = _now()
        diagnostic = [str(error), error.reason, error.request_id]
        subscription.sync_issue = f"Criação da assinatura em teste: {' · '.join(filter(None, diagnostic))}."
        subscription.status = "revisao"
        # Falhas inconclusivas são recuperadas por referência, sem repetir o POST.
        if error.http_status in CHECKOUT_REJECTIONS or str(error) == "comprador_de_teste_invalido":
            subscription.is_current = False
            subscription.status = "falhou"
        db.commit()
        logger.warning("Mercado Pago sandbox: criação subscription_id=%s resultado=%s", subscription.id, error)
        raise
    db.commit()
    return subscription.mercadopago_checkout_url


def _find_subscription(db, remote_id, reference=None):
    if not resource_id(remote_id):
        return None
    match = OrganizationSubscription.mercadopago_id == str(remote_id)
    if isinstance(reference, str) and reference.startswith("sisfarm-test-"):
        match = or_(match, OrganizationSubscription.mercadopago_reference == reference)
    return db.query(OrganizationSubscription).filter(
        OrganizationSubscription.environment == MERCADOPAGO_TEST, match,
    ).with_for_update().one_or_none()


def _apply_invoice(db, api, invoice):
    identifier = resource_id(invoice.get("id"))
    remote_id = resource_id(invoice.get("preapproval_id"))
    if not identifier or not remote_id:
        raise MercadoPagoError("fatura_invalida")
    subscription = _find_subscription(db, remote_id, invoice.get("external_reference"))
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
    if payment and (payment.get("live_mode") is not False or payment.get("currency_id") != "BRL"
                    or _amount(payment.get("transaction_amount")) != amount):
        raise MercadoPagoError("pagamento_divergente_ou_fora_do_sandbox")
    remote_status = payment.get("status")
    due = _date(invoice.get("debit_date"))
    status = "PENDING"
    if remote_status == "approved":
        status = "CONFIRMED"
    elif remote_status in {"refunded", "charged_back"}:
        status = "REFUNDED" if remote_status == "refunded" else "CHARGEBACK_REQUESTED"
    elif remote_status in CONFIRMATION_PENDING:
        status = "AWAITING_RISK_ANALYSIS"
    elif (remote_status in {"rejected", "cancelled", "canceled"} and invoice.get("status") == "processed") or (due and due < today_in_app_timezone()):
        status = "OVERDUE"
    existing = db.query(OrganizationSubscriptionPayment).filter_by(mercadopago_invoice_id=identifier).one_or_none()
    if existing and existing.subscription_id != subscription.id:
        raise MercadoPagoError("fatura_ja_vinculada")
    row = existing or OrganizationSubscriptionPayment(subscription_id=subscription.id, mercadopago_invoice_id=identifier)
    if not existing:
        db.add(row)
    row.mercadopago_payment_id, row.amount, row.status = payment_id, amount, status
    row.due_date = min(row.due_date, due) if row.due_date and due else due or row.due_date
    row.paid_date = _date(payment.get("date_approved")) if status == "CONFIRMED" else None
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
        row = _find_subscription(db, identifier, remote.get("external_reference"))
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
        if not row.mercadopago_payer_email:
            return
        matches = [item for item in api.search("/preapproval/search", payer_email=row.mercadopago_payer_email)
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
        for invoice in api.search("/authorized_payments/search", preapproval_id=row.mercadopago_id):
            if resource_id(invoice.get("preapproval_id")) == row.mercadopago_id:
                _apply_invoice(db, api, invoice)


def change_status(api, row, target):
    if not row.mercadopago_id or row.environment != MERCADOPAGO_TEST:
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
                MercadoPagoEvent.state == "pending", MercadoPagoEvent.retry_at <= _now(),
            ).order_by(MercadoPagoEvent.id).with_for_update(skip_locked=True).first()
            if not event:
                return
            event.attempts += 1
            try:
                with db.begin_nested():
                    event.result = process_event(db, api, event.topic, event.resource_id)
                event.state = "processed"
            except MercadoPagoError as error:
                event.result = str(error)
                event.retry_at = _now() + timedelta(seconds=min(1800, 30 * (2 ** event.attempts)))
                if event.attempts >= 8:
                    event.state = "failed"
                logger.warning("Mercado Pago sandbox: evento=%s tópico=%s recurso=%s tentativa=%s erro=%s",
                               event.id, event.topic, event.resource_id, event.attempts, error)
            db.commit()


def _tick():
    if configuration_error():
        return
    api = MercadoPagoAPI()
    _process_events(api)
    for _ in range(10):
        with SessionLocal() as db:
            row = db.query(OrganizationSubscription).filter(
                OrganizationSubscription.environment == MERCADOPAGO_TEST,
                OrganizationSubscription.is_current.is_(True),
                or_(OrganizationSubscription.last_synced_at.is_(None),
                    OrganizationSubscription.last_synced_at < _now() - timedelta(minutes=5)),
            ).order_by(OrganizationSubscription.last_synced_at.asc().nullsfirst()).with_for_update(skip_locked=True).first()
            if not row:
                return
            try:
                with db.begin_nested():
                    reconcile(db, api, row)
            except MercadoPagoError as error:
                row.last_synced_at = _now()
                row.sync_issue = f"Conciliação em teste: {error}."
                logger.warning("Mercado Pago sandbox: conciliação subscription_id=%s erro=%s", row.id, error)
            db.commit()


async def run_mercadopago_loop():
    while True:
        try:
            await run_in_threadpool(_tick)
        except Exception:
            logger.exception("Mercado Pago sandbox: falha na fila ou conciliação")
        await asyncio.sleep(10)
