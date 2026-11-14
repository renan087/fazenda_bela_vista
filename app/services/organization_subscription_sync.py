"""Persistencia idempotente de eventos de assinatura e cobrança de organizações."""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from app.models import AsaasWebhookEvent, OrganizationBilling, OrganizationSubscription, OrganizationSubscriptionPayment


def _event_time(raw):
    try:
        return datetime.fromisoformat(str(raw or '').replace('Z', '+00:00')).replace(tzinfo=None)
    except ValueError:
        return None


def _date(raw):
    try:
        return date.fromisoformat(str(raw)[:10]) if raw else None
    except ValueError:
        return None


def persist_organization_webhook(db, body: dict) -> bool:
    payment = body.get('payment') if isinstance(body.get('payment'), dict) else None
    subscription_data = body.get('subscription') if isinstance(body.get('subscription'), dict) else None
    remote_id = (payment or {}).get('subscription') if payment else (subscription_data or {}).get('id')
    if not remote_id:
        return False
    subscription = db.query(OrganizationSubscription).filter_by(asaas_subscription_id=str(remote_id), environment='sandbox').with_for_update().first()
    if not subscription:
        return False
    billing = db.query(OrganizationBilling).filter_by(organization_id=subscription.organization_id).first()
    resource = payment or subscription_data
    if not billing or not resource or resource.get('customer') != billing.asaas_customer_id:
        raise ValueError('Cliente do evento não corresponde à organização')
    event_id = str(body.get('id') or '').strip()
    event_type = str(body.get('event') or '').strip()
    resource_id = str(resource.get('id') or '').strip()
    timestamp = _event_time(body.get('dateCreated'))
    if not event_id or len(event_id) > 200 or not event_type or len(event_type) > 80 or not resource_id or len(resource_id) > 80 or not timestamp:
        raise ValueError('Evento sem identificação ou data válida')
    if db.query(AsaasWebhookEvent.id).filter_by(event_id=event_id).first():
        return True
    db.add(AsaasWebhookEvent(event_id=event_id, event_type=event_type, resource_id=resource_id, event_at=timestamp))
    if payment:
        row = db.query(OrganizationSubscriptionPayment).filter_by(asaas_payment_id=resource_id).first()
        if row and row.subscription_id != subscription.id:
            raise ValueError('Cobrança já vinculada a outra assinatura')
        if not row:
            row = OrganizationSubscriptionPayment(subscription_id=subscription.id, asaas_payment_id=resource_id, status='DESCONHECIDO')
            db.add(row)
        status = 'DELETED' if payment.get('deleted') is True or event_type == 'PAYMENT_DELETED' else str(payment.get('status') or '').strip().upper()
        if row.last_event_at == timestamp and status and row.status != status:
            row.status = 'VERIFICAR'
            row.paid_date = None
        if row.last_event_at is None or timestamp > row.last_event_at:
            if status:
                row.status = status[:40]
            try:
                amount = Decimal(str(payment['value'])) if payment.get('value') is not None else None
                if amount is not None and (not amount.is_finite() or amount < 0 or amount >= Decimal('10000000000')):
                    raise ValueError('Valor de cobrança inválido')
                row.amount = amount
            except (InvalidOperation, ValueError):
                raise ValueError('Valor de cobrança inválido')
            row.due_date = _date(payment.get('dueDate'))
            row.paid_date = _date(payment.get('clientPaymentDate') or payment.get('paymentDate')) if row.status in {'RECEIVED', 'CONFIRMED', 'RECEIVED_IN_CASH'} else None
            row.last_event_at = timestamp
    elif subscription.asaas_last_event_at is None or timestamp > subscription.asaas_last_event_at:
        status = str(subscription_data.get('status') or '').strip()
        if status:
            subscription.asaas_status = status[:30]
        subscription.asaas_last_event_at = timestamp
    return True
