"""Processa migracoes confirmadas; nunca usa o retorno do navegador como pagamento."""

import asyncio
import logging
from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib

from app.core.config import get_settings
from app.core.timezone import app_now
from app.services.asaas_environment import asaas_env_name, asaas_ready
from app.db.session import SessionLocal
from app.models import OrganizationBilling, OrganizationCardMigration, OrganizationSubscription
from app.services.asaas_checkout_reconciliation import find_paid_checkout_subscription
from app.services.asaas_subscriptions import get_sandbox_subscription, list_sandbox_subscription_payments, manage_sandbox_subscription
from app.services.commercial_audit import record_commercial_event
from app.services.organization_subscription_sync import persist_organization_webhook

logger = logging.getLogger(__name__)


def _validated_checkout_subscription(api_key, migration, customer_id, *, amount, cycle):
    new_remote, error = find_paid_checkout_subscription(api_key=api_key, checkout_id=migration.checkout_id, customer_id=customer_id)
    if error:
        raise ValueError(error)
    try:
        remote_value = Decimal(str(new_remote.get('value')))
        next_due = date.fromisoformat(str(new_remote['nextDueDate'])[:10])
    except (KeyError, InvalidOperation, ValueError):
        raise ValueError('Dados da nova assinatura são inválidos')
    if remote_value != amount or new_remote.get('cycle') != cycle:
        raise ValueError('Dados da nova assinatura não correspondem ao plano')
    return new_remote, next_due


def _process_new_card_subscription(db, migration, api_key, billing):
    """Contratacao direta no cartao: a assinatura local so nasce apos o pagamento confirmado."""
    if migration.amount is None or not migration.cycle or not migration.plan_name:
        raise ValueError('Checkout sem dados do plano contratado')
    if db.query(OrganizationSubscription.id).filter_by(organization_id=migration.organization_id, is_current=True).with_for_update().first():
        raise ValueError('A organização já possui assinatura atual; verificação manual necessária')
    new_remote, next_due = _validated_checkout_subscription(api_key, migration, billing.asaas_customer_id, amount=migration.amount, cycle=migration.cycle)
    if db.query(OrganizationSubscription.id).filter_by(asaas_subscription_id=new_remote['id']).first():
        raise ValueError('Nova assinatura já vinculada; verificação manual necessária')
    new = OrganizationSubscription(organization_id=migration.organization_id, plan_id=migration.plan_id, is_current=True, plan_name=migration.plan_name, amount=migration.amount, cycle=migration.cycle, billing_type='CREDIT_CARD', first_due_date=migration.first_due_date or next_due, next_due_date=next_due, status='criada', environment=asaas_env_name(), asaas_subscription_id=new_remote['id'], asaas_status='ACTIVE')
    db.add(new)
    db.flush()
    return new, None


def _process_card_migration(db, migration, api_key, billing):
    old = db.query(OrganizationSubscription).filter_by(id=migration.old_subscription_id, organization_id=migration.organization_id, is_current=True, environment=asaas_env_name()).with_for_update().first()
    if not old or not old.asaas_subscription_id:
        raise ValueError('Assinatura antiga ou cliente não identificado')
    new_remote, next_due = _validated_checkout_subscription(api_key, migration, billing.asaas_customer_id, amount=old.amount, cycle=old.cycle)
    if new_remote['id'] == old.asaas_subscription_id:
        raise ValueError('Dados da nova assinatura não correspondem ao plano')
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
    new = OrganizationSubscription(organization_id=old.organization_id, plan_id=old.plan_id, is_current=True, plan_name=old.plan_name, amount=old.amount, cycle=old.cycle, billing_type='CREDIT_CARD', first_due_date=migration.first_due_date or next_due, next_due_date=next_due, status='criada', environment=asaas_env_name(), asaas_subscription_id=new_remote['id'], asaas_status='ACTIVE')
    db.add(new)
    db.flush()
    return new, old


def process_paid_card_migration(db, migration, api_key):
    billing = db.query(OrganizationBilling).filter_by(organization_id=migration.organization_id, asaas_environment=asaas_env_name()).first()
    if not billing or not billing.asaas_customer_id:
        raise ValueError('Assinatura antiga ou cliente não identificado')
    if migration.old_subscription_id is None:
        new, old = _process_new_card_subscription(db, migration, api_key, billing)
    else:
        new, old = _process_card_migration(db, migration, api_key, billing)
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
    migration.last_check_error = None
    migration.is_open = False
    if old is None:
        record_commercial_event(db, user=None, organization_id=new.organization_id, resource='assinatura', resource_id=new.id, action='criar_cartao', outcome='success', details={'plano_id': new.plan_id, 'checkout': migration.checkout_id})
    else:
        record_commercial_event(db, user=None, organization_id=old.organization_id, resource='assinatura', resource_id=new.id, action='migrar_cartao', outcome='success', details={'assinatura_anterior': old.id})


def _tick():
    settings = get_settings()
    if not asaas_ready(settings):
        return
    with SessionLocal() as db:
        migration = (db.query(OrganizationCardMigration)
                     .filter(OrganizationCardMigration.status.in_(['pago', 'aguardando']),
                             OrganizationCardMigration.is_open.is_(True),
                             OrganizationCardMigration.checkout_id.isnot(None))
                     .order_by(OrganizationCardMigration.updated_at, OrganizationCardMigration.id)
                     .with_for_update(skip_locked=True).first())
        if not migration:
            return
        migration_id = migration.id
        try:
            process_paid_card_migration(db, migration, settings.asaas_api_key.strip())
            db.commit()
        except Exception as exc:
            db.rollback()
            migration = db.query(OrganizationCardMigration).filter_by(id=migration_id, is_open=True).with_for_update().first()
            if migration and migration.status in {'pago', 'aguardando'}:
                migration.last_check_error = str(exc)[:255] if isinstance(exc, ValueError) else 'Falha ao consultar o Asaas; verifique os logs.'
                migration.updated_at = app_now()
                db.commit()
            if not isinstance(exc, ValueError):
                logger.exception('Falha ao conciliar migração de cartão')


async def run_card_migration_loop():
    while True:
        try:
            await asyncio.to_thread(_tick)
        except Exception:
            logger.exception('Falha no processamento de migração de cartão')
        await asyncio.sleep(120)
