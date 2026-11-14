"""Persistencia idempotente de eventos de assinatura e cobrança de organizações."""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from app.services.asaas_card_checkout import safe_asaas_link
from app.services.asaas_environment import asaas_env_name
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


def apply_subscription_details(subscription, data):
    """Apply only valid fields actually returned by the authoritative API."""
    if data.get('description'):
        subscription.plan_name = str(data['description'])[:120]
    if data.get('value') is not None:
        try:
            amount = Decimal(str(data['value']))
            if not amount.is_finite() or amount <= 0 or amount >= Decimal('10000000000'):
                raise ValueError('Valor inválido')
            subscription.amount = amount.quantize(Decimal('0.01'))
        except (InvalidOperation, ValueError):
            raise ValueError('Valor da assinatura inválido')
    if data.get('cycle') in {'MONTHLY', 'QUARTERLY', 'SEMIANNUALLY', 'YEARLY'}:
        subscription.cycle = data['cycle']
    if data.get('billingType') in {'PIX', 'BOLETO', 'CREDIT_CARD'}:
        subscription.billing_type = data['billingType']
    if data.get('nextDueDate'):
        due = _date(data['nextDueDate'])
        if not due:
            raise ValueError('Próximo vencimento inválido')
        subscription.next_due_date = due


def persist_organization_webhook(db, body: dict) -> bool:
    payment = body.get('payment') if isinstance(body.get('payment'), dict) else None
    subscription_data = body.get('subscription') if isinstance(body.get('subscription'), dict) else None
    remote_id = (payment or {}).get('subscription') if payment else (subscription_data or {}).get('id')
    if not remote_id:
        return False
    subscription = db.query(OrganizationSubscription).filter_by(asaas_subscription_id=str(remote_id), environment=asaas_env_name()).with_for_update().first()
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
        # Links de pagamento nao dependem da ordem dos eventos: guarda o mais recente valido.
        invoice_url = safe_asaas_link(payment.get('invoiceUrl'))
        bank_slip_url = safe_asaas_link(payment.get('bankSlipUrl'))
        if invoice_url:
            row.invoice_url = invoice_url
        if bank_slip_url:
            row.bank_slip_url = bank_slip_url
        card = payment.get('creditCard') if isinstance(payment.get('creditCard'), dict) else None
        if card:
            digits = ''.join(ch for ch in str(card.get('creditCardNumber') or '') if ch.isdigit())[-4:]
            if len(digits) == 4:
                row.card_last4 = digits
            if card.get('creditCardBrand'):
                row.card_brand = str(card['creditCardBrand'])[:30]
            if card.get('creditCardToken'):
                row.card_tokenized = True
        if row.last_event_at is None or timestamp > row.last_event_at:
            if status:
                row.status = status[:40]
            if payment.get('billingType'):
                row.billing_type = str(payment['billingType'])[:30]
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
        status = 'DELETED' if subscription_data.get('deleted') is True or event_type == 'SUBSCRIPTION_DELETED' else str(subscription_data.get('status') or '').strip()
        if status:
            subscription.asaas_status = status[:30]
        subscription.asaas_last_event_at = timestamp
    return True
