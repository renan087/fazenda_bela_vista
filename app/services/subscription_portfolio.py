"""Panorama comercial das assinaturas de todas as organizacoes.

Somente leitura sobre os dados locais ja sincronizados com o Asaas: nao faz chamadas externas.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from sqlalchemy.orm import Session

from app.services.subscription_plans import cycle_months
from app.models import (
    Organization,
    OrganizationBilling,
    OrganizationCardMigration,
    OrganizationSubscription,
    OrganizationSubscriptionPayment,
)

# Ordem de exibicao: o que exige atencao primeiro.
SITUATION_ORDER = ("verificar", "inadimplente", "aguardando_cartao", "suspensa", "sem_assinatura", "sem_cliente", "ativa", "outra")
SITUATION_LABELS = {
    "verificar": "Sincronizando",
    "inadimplente": "Inadimplente",
    "aguardando_cartao": "Aguardando cartão",
    "suspensa": "Suspensa",
    "sem_assinatura": "Sem assinatura",
    "sem_cliente": "Sem cliente Asaas",
    "ativa": "Em dia",
    "outra": "Outra situação",
}
_PENDING_REVIEW_STATUSES = {"criando", "revisao"}
_ZERO = Decimal("0.00")


@dataclass
class PortfolioRow:
    organization: Organization
    situation: str
    subscription: OrganizationSubscription | None = None
    has_customer: bool = False
    overdue_count: int = 0
    overdue_amount: Decimal = _ZERO
    oldest_overdue_due_date: date | None = None
    days_overdue: int = 0
    attention_reason: str | None = None

    @property
    def situation_label(self) -> str:
        return SITUATION_LABELS[self.situation]


@dataclass
class SubscriptionPortfolio:
    rows: list[PortfolioRow] = field(default_factory=list)
    monthly_recurring_revenue: Decimal = _ZERO
    active_count: int = 0
    delinquent_count: int = 0
    overdue_total: Decimal = _ZERO
    without_subscription_count: int = 0
    review_count: int = 0


def monthly_amount(subscription: OrganizationSubscription) -> Decimal:
    amount = Decimal(subscription.amount or 0)
    return (amount / cycle_months(subscription.cycle)).quantize(Decimal("0.01"))


def _is_overdue(payment: OrganizationSubscriptionPayment, today: date) -> bool:
    # O webhook OVERDUE pode chegar com atraso; uma cobranca pendente com vencimento passado tambem esta em atraso.
    return payment.status == "OVERDUE" or (payment.status == "PENDING" and payment.due_date is not None and payment.due_date < today)


def build_subscription_portfolio(db: Session, organizations: list[Organization], today: date) -> SubscriptionPortfolio:
    org_ids = [item.id for item in organizations]
    portfolio = SubscriptionPortfolio()
    if not org_ids:
        return portfolio

    billing_by_org = {
        item.organization_id: item
        for item in db.query(OrganizationBilling).filter(OrganizationBilling.organization_id.in_(org_ids)).all()
    }
    subscriptions = db.query(OrganizationSubscription).filter(OrganizationSubscription.organization_id.in_(org_ids)).all()
    current_by_org = {item.organization_id: item for item in subscriptions if item.is_current}
    org_by_subscription = {item.id: item.organization_id for item in subscriptions}
    open_migrations = db.query(OrganizationCardMigration).filter(
        OrganizationCardMigration.organization_id.in_(org_ids),
        OrganizationCardMigration.is_open.is_(True),
    ).all()
    open_migration_review = {item.organization_id for item in open_migrations if item.status == "revisao"}
    migration_errors = {item.organization_id: item.last_check_error for item in open_migrations if item.last_check_error}
    awaiting_card_contract = {item.organization_id for item in open_migrations if item.old_subscription_id is None and item.status != "revisao"}

    overdue_by_org: dict[int, list[OrganizationSubscriptionPayment]] = defaultdict(list)
    payment_review_orgs: set[int] = set()
    if org_by_subscription:
        payments = db.query(OrganizationSubscriptionPayment).filter(
            OrganizationSubscriptionPayment.subscription_id.in_(list(org_by_subscription))
        ).all()
        for payment in payments:
            org_id = org_by_subscription[payment.subscription_id]
            if payment.operation_state == "verificar":
                payment_review_orgs.add(org_id)
            # Inclui cobrancas de assinaturas anteriores: o valor continua devido.
            if _is_overdue(payment, today):
                overdue_by_org[org_id].append(payment)

    for organization in organizations:
        billing = billing_by_org.get(organization.id)
        subscription = current_by_org.get(organization.id)
        overdue = overdue_by_org.get(organization.id, [])
        row = PortfolioRow(
            organization=organization,
            situation="outra",
            subscription=subscription,
            has_customer=bool(billing and billing.asaas_customer_id),
            overdue_count=len(overdue),
            overdue_amount=sum((Decimal(item.amount or 0) for item in overdue), _ZERO),
        )
        due_dates = [item.due_date for item in overdue if item.due_date]
        if due_dates:
            row.oldest_overdue_due_date = min(due_dates)
            row.days_overdue = max((today - row.oldest_overdue_due_date).days, 0)

        if organization.id in open_migration_review or (subscription and (subscription.status in _PENDING_REVIEW_STATUSES or organization.id in payment_review_orgs)):
            row.situation = "verificar"
            row.attention_reason = (subscription.sync_issue if subscription else None) or migration_errors.get(organization.id) or "Conferência automática com o Asaas em andamento."
        elif overdue:
            row.situation = "inadimplente"
        elif not subscription and organization.id in awaiting_card_contract:
            row.situation = "aguardando_cartao"
        elif subscription and subscription.asaas_status == "INACTIVE":
            row.situation = "suspensa"
        elif subscription and subscription.asaas_status == "ACTIVE":
            row.situation = "ativa"
        elif not subscription and not row.has_customer:
            row.situation = "sem_cliente"
        elif not subscription:
            row.situation = "sem_assinatura"
        portfolio.rows.append(row)

        if subscription and subscription.asaas_status == "ACTIVE":
            portfolio.active_count += 1
            portfolio.monthly_recurring_revenue += monthly_amount(subscription)
        if overdue:
            portfolio.delinquent_count += 1
            portfolio.overdue_total += row.overdue_amount
        if not subscription:
            portfolio.without_subscription_count += 1
        if row.situation == "verificar":
            portfolio.review_count += 1

    portfolio.rows.sort(key=lambda row: (SITUATION_ORDER.index(row.situation), -row.days_overdue, (row.organization.name or "").lower()))
    return portfolio
