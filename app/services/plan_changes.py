"""Regras de troca de plano feita pelo proprio cliente.

- Upgrade (plano maior): vale na hora.
- Downgrade (valor mensal menor ou perda de modulo): so pode ser pedido com todas as faturas quitadas e
  com ao menos uma fatura paga depois da ultima troca de plano. O plano atual continua ate o fim do ciclo
  pago (proxima cobranca); o novo valor ja vai para a proxima fatura. Pode ser cancelado antes de valer.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy.orm import Session

from app.models import OrganizationSubscription, OrganizationSubscriptionPayment, SubscriptionPlan
from app.services.subscription_plans import cycle_months, plan_modules

PAID = {"CONFIRMED", "RECEIVED", "RECEIVED_IN_CASH"}
OPEN = {"PENDING", "OVERDUE"}


def monthly(amount, cycle: str) -> Decimal:
    return (Decimal(amount or 0) / cycle_months(cycle)).quantize(Decimal("0.01"))


def is_downgrade(subscription: OrganizationSubscription, plan: SubscriptionPlan, price: Decimal, cycle: str) -> bool:
    if monthly(price, cycle) < monthly(subscription.amount, subscription.cycle):
        return True
    current = set(plan_modules(subscription.plan)) if subscription.plan else set()
    return bool(current - set(plan_modules(plan)))


def downgrade_block_reason(db: Session, subscription: OrganizationSubscription) -> str | None:
    """Motivo que impede pedir o downgrade agora, ou None se pode."""
    charges = db.query(OrganizationSubscriptionPayment).filter_by(subscription_id=subscription.id).all()
    if any(charge.status in OPEN for charge in charges):
        return "Há fatura em aberto. A redução de plano fica disponível depois que ela for paga."
    since = subscription.plan_changed_at.date() if subscription.plan_changed_at else None
    paid_after_change = [c for c in charges if c.status in PAID and (since is None or (c.paid_date or c.due_date or date.min) >= since)]
    if not paid_after_change:
        return "Aguarde a confirmação do pagamento da fatura do plano atual para reduzir o plano."
    if not subscription.next_due_date:
        return "A data do próximo ciclo ainda não foi confirmada. Tente novamente em alguns minutos."
    return None


def schedule_downgrade(subscription: OrganizationSubscription, plan: SubscriptionPlan, price: Decimal, cycle: str, *, previous_amount, previous_cycle) -> date:
    subscription.pending_previous_amount = previous_amount
    subscription.pending_previous_cycle = previous_cycle
    subscription.pending_plan_id = plan.id
    subscription.pending_plan_name = plan.name
    subscription.pending_amount = price
    subscription.pending_cycle = cycle
    subscription.pending_effective_on = subscription.next_due_date
    return subscription.next_due_date


def clear_pending(subscription: OrganizationSubscription) -> None:
    subscription.pending_plan_id = None
    subscription.pending_plan_name = None
    subscription.pending_amount = None
    subscription.pending_cycle = None
    subscription.pending_effective_on = None
    subscription.pending_previous_amount = None
    subscription.pending_previous_cycle = None


def effective_plan_id(subscription: OrganizationSubscription | None, today: date) -> int | None:
    """Plano que vale hoje para modulos e limites (considera downgrade agendado ja vencido)."""
    if not subscription:
        return None
    if subscription.pending_plan_id and subscription.pending_effective_on and subscription.pending_effective_on <= today:
        return subscription.pending_plan_id
    return subscription.plan_id


def apply_due_plan_changes(db: Session, today: date, now: datetime, rename_in_asaas) -> list[int]:
    """Efetiva downgrades cuja data chegou. ``rename_in_asaas(subscription, name) -> bool`` atualiza a descricao
    no Asaas (o valor ja foi ajustado no agendamento); se falhar, tenta de novo no proximo ciclo."""
    due = db.query(OrganizationSubscription).filter(
        OrganizationSubscription.pending_plan_id.isnot(None),
        OrganizationSubscription.pending_effective_on <= today,
    ).all()
    organizations = []
    for subscription in due:
        if not rename_in_asaas(subscription, subscription.pending_plan_name):
            continue
        subscription.plan_id = subscription.pending_plan_id
        subscription.plan_name = subscription.pending_plan_name
        subscription.plan_changed_at = now
        clear_pending(subscription)
        organizations.append(subscription.organization_id)
    return organizations
