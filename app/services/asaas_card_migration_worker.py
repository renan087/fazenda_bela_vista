"""Processa migracoes confirmadas; nunca usa o retorno do navegador como pagamento."""

import asyncio
import logging
from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib

from app.core.config import get_settings
from app.core.timezone import app_now
from app.db.session import SessionLocal
from app.models import OrganizationBilling, OrganizationCardMigration, OrganizationSubscription
from app.services.asaas_checkout_reconciliation import find_paid_checkout_subscription
from app.services.asaas_subscriptions import get_sandbox_subscription, list_sandbox_subscription_payments, manage_sandbox_subscription
from app.services.commercial_audit import record_commercial_event
from app.services.organization_subscription_sync import persist_organization_webhook

logger = logging.getLogger(__name__)


def process_paid_card_migration(db, migration, api_key):
    old = db.query(OrganizationSubscription).filter_by(id=migration.old_subscription_id, organization_id=migration.organization_id, is_current=True, environment='sandbox').with_for_update().first()
    billing = db.query(OrganizationBilling).filter_by(organization_id=migration.organization_id, asaas_environment='sandbox').first()
    if not old or not billing or not billing.asaas_customer_id or not old.asaas_subscription_id:
        raise ValueError('Assinatura antiga ou cliente não identificado')
    new_remote, error = find_paid_checkout_subscription(api_key=api_key, checkout_id=migration.checkout_id, customer_id=billing.asaas_customer_id)
    if error:
        raise ValueError(error)
    try:
        if Decimal(str(new_remote.get('value'))) != old.amount or new_remote.get('cycle') != old.cycle or new_remote['id'] == old.asaas_subscription_id:
            raise ValueError('Dados da nova assinatura não correspondem ao plano')
        next_due = date.fromisoformat(str(new_remote['nextDueDate'])[:10])
    except (KeyError, InvalidOperation, ValueError):
        raise ValueError('Dados da nova assinatura são inválidos')
    current_remote, error = get_sandbox_subscription(api_key=api_key, subscription_id=old.asaas_subscription_id)
    if error or current_remote.get('customer') != billing.asaas_customer_id or current_remote.get('status') not in {'ACTIVE', 'INACTIVE'}:
        raise ValueError('Não foi possível verificar a assinatura anterior')
    if current_remote['status'] == 'ACTIVE':
        result, error = manage_sandbox_subscription(api_key=api_key, subscription_id=old.asaas_subscription_id, action='suspender')
        if error or result.get('customer') != billing.asaas_customer_id:
            raise ValueError('Suspensão anterior não confirmada; será verificada novamente')
    existing = db.query(OrganizationSubscription).filter_by(asaas_subscription_id=new_remote['id']).first()
    if existing:
        raise ValueError('Nova assinatura já vinculada; verificação manual necessária')
    old.asaas_status = 'INACTIVE'
    old.status = 'criada'
    old.is_current = False
    db.flush()
    new = OrganizationSubscription(organization_id=old.organization_id, is_current=True, plan_name=old.plan_name, amount=old.amount, cycle=old.cycle, billing_type='CREDIT_CARD', first_due_date=migration.first_due_date or next_due, next_due_date=next_due, status='criada', environment='sandbox', asaas_subscription_id=new_remote['id'], asaas_status='ACTIVE')
    db.add(new)
    db.flush()
    payments, error = list_sandbox_subscription_payments(api_key=api_key, subscription_id=new.asaas_subscription_id)
    if error:
        raise ValueError('Não foi possível conciliar as cobranças da nova assinatura')
    timestamp = app_now().replace(tzinfo=None, microsecond=0).isoformat()
    for payment in payments:
        if not isinstance(payment, dict) or payment.get('subscription') != new.asaas_subscription_id or payment.get('customer') != billing.asaas_customer_id or not payment.get('id'):
            raise ValueError('Cobrança da nova assinatura inválida')
        event_id = hashlib.sha256(f"migracao:{migration.id}:{payment['id']}".encode()).hexdigest()
        if not persist_organization_webhook(db, {'id': f'migracao-{event_id}', 'event': 'MANUAL_SYNC', 'dateCreated': timestamp, 'payment': payment}):
            raise ValueError('Não foi possível vincular cobrança da assinatura')
    migration.new_subscription_id = new.id
    migration.status = 'concluido'
    migration.is_open = False
    record_commercial_event(db, user=None, organization_id=old.organization_id, resource='assinatura', resource_id=new.id, action='migrar_cartao', outcome='success', details={'assinatura_anterior': old.id})


def _tick():
    settings = get_settings()
    if (settings.asaas_api_base_url or '').rstrip('/') != 'https://api-sandbox.asaas.com' or not (settings.asaas_api_key or '').strip():
        return
    with SessionLocal() as db:
        migration = db.query(OrganizationCardMigration).filter_by(status='pago', is_open=True).with_for_update(skip_locked=True).first()
        if not migration:
            return
        try:
            process_paid_card_migration(db, migration, settings.asaas_api_key.strip())
            db.commit()
        except Exception:
            db.rollback()
            logger.exception('Migração de cartão pendente de verificação; será novamente consultada')


async def run_card_migration_loop():
    while True:
        try:
            await asyncio.to_thread(_tick)
        except Exception:
            logger.exception('Falha no processamento de migração de cartão')
        await asyncio.sleep(120)
