"""Conciliacao automatica das assinaturas das organizacoes com o Asaas.

O usuario do SiSFarm nao acessa o Asaas: toda situacao incerta (resposta inconclusiva, operacao
enviada, checkout expirado, evento fora de ordem) e resolvida aqui, consultando a API do Asaas,
que e a fonte da verdade. Nunca reenvia uma operacao; apenas consulta e registra o resultado.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timedelta

from sqlalchemy import or_

from app.core.config import get_settings
from app.core.timezone import app_now
from app.db.session import SessionLocal
from app.models import (
    OrganizationBilling,
    OrganizationCardMigration,
    OrganizationSubscription,
    OrganizationSubscriptionPayment,
)
from app.services.asaas_card_checkout import cancel_sandbox_checkout
from app.services.asaas_subscriptions import (
    get_sandbox_payment,
    get_sandbox_subscription,
    list_customer_sandbox_subscriptions,
    list_sandbox_subscription_payments,
    sandbox_subscription_is_missing,
)
from app.services.commercial_audit import record_commercial_event
from app.services.entitlements import invalidate_entitlements
from app.services.organization_subscription_sync import apply_subscription_details, persist_organization_webhook
from app.services.asaas_environment import asaas_api_key, asaas_env_name, asaas_ready

logger = logging.getLogger(__name__)

PENDING_SUBSCRIPTION_STATUSES = {"criando", "revisao"}
CREATION_GRACE = timedelta(minutes=10)
FULL_SYNC_INTERVAL = timedelta(minutes=60)
STALE_OPERATION = timedelta(minutes=2)
OPEN_CHARGE_STATUSES = {"PENDING", "OVERDUE"}


class ReconciliationError(ValueError):
    pass


def _now() -> datetime:
    return app_now().replace(tzinfo=None, microsecond=0)


def _naive(value: datetime | None) -> datetime | None:
    """Converte para o horario local da aplicacao sem fuso (mesma base de ``_now``)."""
    if value is None:
        return None
    if value.tzinfo:
        return value.astimezone(app_now().tzinfo).replace(tzinfo=None)
    return value


def _sync_event(db, *, key: str, payload_name: str, payload: dict, now: datetime) -> bool:
    event_id = hashlib.sha256(f"{key}:{now.isoformat()}".encode()).hexdigest()
    return persist_organization_webhook(db, {"id": f"auto-{event_id}", "event": "AUTO_SYNC", "dateCreated": now.isoformat(), payload_name: payload})


def reconcile_payment(db, charge: OrganizationSubscriptionPayment, subscription: OrganizationSubscription, billing: OrganizationBilling, api_key: str, now: datetime) -> None:
    """Atualiza uma cobranca pela consulta direta ao Asaas e libera a cobranca para novas acoes."""
    remote, error = get_sandbox_payment(api_key=api_key, payment_id=charge.asaas_payment_id)
    if error:
        raise ReconciliationError(error)
    if remote.get("deleted") is True and not remote.get("subscription"):
        charge.status = "DELETED"
        charge.paid_date = None
    else:
        if remote.get("subscription") != subscription.asaas_subscription_id or remote.get("customer") != billing.asaas_customer_id:
            raise ReconciliationError("Cobrança retornada pelo Asaas não corresponde à assinatura")
        _sync_event(db, key=f"pay:{charge.asaas_payment_id}", payload_name="payment", payload=remote, now=now)
    charge.operation_state = None


def _sync_subscription_and_payments(db, subscription: OrganizationSubscription, billing: OrganizationBilling, api_key: str, now: datetime) -> None:
    remote, error = get_sandbox_subscription(api_key=api_key, subscription_id=subscription.asaas_subscription_id)
    if error:
        missing, missing_error = sandbox_subscription_is_missing(api_key=api_key, subscription_id=subscription.asaas_subscription_id)
        if missing is True:
            subscription.asaas_status = "NOT_FOUND"
            subscription.status = "encerrada"
            subscription.is_current = False
            record_commercial_event(db, user=None, organization_id=subscription.organization_id, resource="assinatura", resource_id=subscription.id, action="conciliar", outcome="success", details={"resultado": "nao_encontrada_no_asaas"})
            return
        raise ReconciliationError(missing_error or error)
    if remote.get("customer") != billing.asaas_customer_id:
        raise ReconciliationError("Cliente da assinatura no Asaas diverge do cadastro")
    apply_subscription_details(subscription, remote)
    _sync_event(db, key=f"sub:{subscription.asaas_subscription_id}", payload_name="subscription", payload=remote, now=now)
    if remote.get("deleted") is True or subscription.asaas_status in {"DELETED", "EXPIRED"}:
        subscription.asaas_status = "DELETED" if remote.get("deleted") is True else subscription.asaas_status
        subscription.status = "encerrada" if subscription.is_current else subscription.status
        subscription.is_current = False
    elif subscription.status in PENDING_SUBSCRIPTION_STATUSES:
        subscription.status = "criada"
    rows, error = list_sandbox_subscription_payments(api_key=api_key, subscription_id=subscription.asaas_subscription_id)
    if error:
        raise ReconciliationError(error)
    seen = set()
    for payment in rows:
        if not isinstance(payment, dict) or payment.get("subscription") != subscription.asaas_subscription_id or not payment.get("id"):
            raise ReconciliationError("Cobrança retornada não corresponde à assinatura")
        seen.add(payment["id"])
        _sync_event(db, key=f"pay:{payment['id']}", payload_name="payment", payload=payment, now=now)
    db.flush()
    # Cobrancas que sumiram da lista (canceladas/excluidas) ou com estado incerto: consulta individual.
    local_rows = db.query(OrganizationSubscriptionPayment).filter_by(subscription_id=subscription.id).all()
    for charge in local_rows:
        if charge.asaas_payment_id in seen:
            # A lista do Asaas acabou de atualizar esta cobranca: o estado e confiavel.
            charge.operation_state = None
        elif charge.operation_state is not None or charge.status in OPEN_CHARGE_STATUSES | {"VERIFICAR", "DESCONHECIDO"}:
            reconcile_payment(db, charge, subscription, billing, api_key, now)


def _link_pending_creation(db, subscription: OrganizationSubscription, billing: OrganizationBilling, api_key: str, now: datetime) -> bool:
    """Criacao com resposta inconclusiva: procura no Asaas pela referencia externa. Retorna True se vinculou."""
    rows, error = list_customer_sandbox_subscriptions(api_key=api_key, customer_id=billing.asaas_customer_id)
    if error:
        raise ReconciliationError(error)
    reference = f"sisfarm-sub-{subscription.id}"
    matches = [item for item in rows if item.get("externalReference") == reference]
    if len(matches) > 1:
        raise ReconciliationError("Mais de uma assinatura no Asaas com a mesma referência")
    if matches:
        remote_id = str(matches[0]["id"])
        if db.query(OrganizationSubscription.id).filter(OrganizationSubscription.asaas_subscription_id == remote_id, OrganizationSubscription.id != subscription.id).first():
            raise ReconciliationError("Assinatura do Asaas já vinculada a outro registro")
        subscription.asaas_subscription_id = remote_id
        subscription.asaas_status = str(matches[0].get("status") or "")[:30] or None
        record_commercial_event(db, user=None, organization_id=subscription.organization_id, resource="assinatura", resource_id=subscription.id, action="conciliar", outcome="success", details={"resultado": "criacao_confirmada"})
        return True
    created_at = _naive(subscription.created_at) or now
    if now - created_at >= CREATION_GRACE:
        # A criacao nunca chegou ao Asaas: encerra a tentativa e libera nova contratacao.
        subscription.status = "falhou"
        subscription.is_current = False
        record_commercial_event(db, user=None, organization_id=subscription.organization_id, resource="assinatura", resource_id=subscription.id, action="conciliar", outcome="success", details={"resultado": "criacao_nao_registrada_no_asaas"})
    return False


def reconcile_migration(db, migration: OrganizationCardMigration, api_key: str) -> None:
    """Checkout de cartao que nao sera mais pago: encerra automaticamente e restaura a assinatura anterior."""
    if migration.status != "revisao":
        return
    if migration.checkout_id:
        status, error = cancel_sandbox_checkout(api_key=api_key, checkout_id=migration.checkout_id)
        if error:
            raise ReconciliationError(error)
        if status == "PAID":
            migration.status = "pago"
            migration.last_check_error = None
            return
        if status not in {"CANCELED", "EXPIRED"}:
            raise ReconciliationError("Checkout ainda ativo no Asaas")
    close_card_checkout(db, migration, remote_status=None if not migration.checkout_id else "ENCERRADO")


def close_card_checkout(db, migration: OrganizationCardMigration, *, remote_status: str | None, user=None) -> None:
    if migration.old_subscription_id:
        old = db.query(OrganizationSubscription).filter_by(id=migration.old_subscription_id, organization_id=migration.organization_id).first()
        if old and old.status == "migrando":
            old.status = "criada"
    migration.status = "encerrado"
    migration.is_open = False
    migration.last_check_error = None
    record_commercial_event(db, user=user, organization_id=migration.organization_id, resource="assinatura", resource_id=migration.old_subscription_id, action="encerrar_checkout", outcome="success", details={"checkout_local": migration.id, "situacao_asaas": remote_status or "SEM_CHECKOUT"})


def reconcile_organization(db, organization_id: int, api_key: str, *, full: bool = True) -> list[str]:
    """Concilia tudo da organizacao. Retorna a lista de pendencias que nao puderam ser resolvidas agora."""
    now = _now()
    issues: list[str] = []
    billing = db.query(OrganizationBilling).filter_by(organization_id=organization_id, asaas_environment=asaas_env_name()).first()
    migration = db.query(OrganizationCardMigration).filter_by(organization_id=organization_id, is_open=True).with_for_update().first()
    if migration and migration.status == "revisao":
        try:
            reconcile_migration(db, migration, api_key)
        except ReconciliationError as exc:
            issues.append(f"Checkout de cartão: {exc}")
    if not billing or not billing.asaas_customer_id:
        return issues
    subscriptions = db.query(OrganizationSubscription).filter(
        OrganizationSubscription.organization_id == organization_id,
        OrganizationSubscription.environment == asaas_env_name(),
        or_(OrganizationSubscription.is_current.is_(True), OrganizationSubscription.id.in_(
            db.query(OrganizationSubscriptionPayment.subscription_id).filter(or_(
                OrganizationSubscriptionPayment.operation_state.isnot(None),
                OrganizationSubscriptionPayment.status == "VERIFICAR",
            ))
        )),
    ).with_for_update().all()
    for subscription in subscriptions:
        savepoint = db.begin_nested()
        try:
            if not subscription.asaas_subscription_id:
                if subscription.status in PENDING_SUBSCRIPTION_STATUSES and not _link_pending_creation(db, subscription, billing, api_key, now):
                    savepoint.commit()
                    continue
            needs_attention = (
                subscription.status in PENDING_SUBSCRIPTION_STATUSES
                or db.query(OrganizationSubscriptionPayment.id).filter(
                    OrganizationSubscriptionPayment.subscription_id == subscription.id,
                    or_(OrganizationSubscriptionPayment.operation_state.isnot(None), OrganizationSubscriptionPayment.status == "VERIFICAR"),
                ).first() is not None
            )
            if full or needs_attention:
                _sync_subscription_and_payments(db, subscription, billing, api_key, now)
                subscription.last_synced_at = now
            subscription.sync_issue = None
            savepoint.commit()
        except ReconciliationError as exc:
            savepoint.rollback()
            subscription.sync_issue = str(exc)[:255]
            issues.append(str(exc))
    return issues


def organizations_needing_reconciliation(db, now: datetime) -> list[int]:
    stale_before = now - FULL_SYNC_INTERVAL
    ids = set()
    ids.update(row[0] for row in db.query(OrganizationSubscription.organization_id).filter(
        OrganizationSubscription.environment == asaas_env_name(),
        or_(
            OrganizationSubscription.status.in_(PENDING_SUBSCRIPTION_STATUSES),
            (OrganizationSubscription.is_current.is_(True)) & (OrganizationSubscription.asaas_subscription_id.isnot(None)) & (
                OrganizationSubscription.last_synced_at.is_(None) | (OrganizationSubscription.last_synced_at < stale_before)
            ),
        ),
    ).all())
    ids.update(row[0] for row in db.query(OrganizationSubscription.organization_id).join(
        OrganizationSubscriptionPayment, OrganizationSubscriptionPayment.subscription_id == OrganizationSubscription.id
    ).filter(or_(OrganizationSubscriptionPayment.operation_state.isnot(None), OrganizationSubscriptionPayment.status == "VERIFICAR")).all())
    ids.update(row[0] for row in db.query(OrganizationCardMigration.organization_id).filter(
        OrganizationCardMigration.is_open.is_(True), OrganizationCardMigration.status == "revisao"
    ).all())
    return sorted(ids)


def _tick(limit: int = 20) -> None:
    settings = get_settings()
    if not asaas_ready(settings):
        return
    api_key = asaas_api_key(settings)
    with SessionLocal() as db:
        organization_ids = organizations_needing_reconciliation(db, _now())[:limit]
    for organization_id in organization_ids:
        with SessionLocal() as db:
            try:
                reconcile_organization(db, organization_id, api_key)
                db.commit()
                invalidate_entitlements(organization_id)
            except Exception:
                db.rollback()
                logger.exception("Falha na conciliação automática da organização %s", organization_id)


async def run_subscription_reconciliation_loop(interval_seconds: int = 300) -> None:
    while True:
        try:
            await asyncio.to_thread(_tick)
        except Exception:
            logger.exception("Falha no ciclo de conciliação de assinaturas")
        await asyncio.sleep(interval_seconds)
