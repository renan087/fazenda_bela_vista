"""Liberacao manual (contingencial) do plano e situacao de acesso de uma organizacao.

Regras:
- somente a equipe SiSFarm (super administrador) cria, prorroga ou encerra;
- motivo de uma lista fechada + justificativa obrigatoria (fica na auditoria);
- sempre tem data de fim, no maximo 90 dias a partir de hoje (prorrogavel, com nova justificativa);
- uma liberacao ativa por organizacao; encerramento antecipado exige motivo;
- nao cria, altera ou quita cobrancas: receita e inadimplencia continuam refletindo o provedor.
"""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.models import OrganizationAccessGrant, OrganizationSubscription, OrganizationSubscriptionPayment, SubscriptionPlan
from app.services.plan_changes import PAID, effective_plan_id, has_confirmed_subscription_payment, latest_settled_subscription_payment
from app.services.subscription_plans import cycle_months
from app.services.billing_environment import MERCADOPAGO_LIVE, MERCADOPAGO_TEST, billing_environment

GRANT_REASONS: dict[str, str] = {
    "erro_interno": "Erro ou falha da SiSFarm",
    "instabilidade_asaas": "Instabilidade ou falha no Asaas",
    "instabilidade_mercadopago": "Instabilidade ou falha no Mercado Pago",
    "negociacao_comercial": "Negociação comercial em andamento",
    "bonus_cortesia": "Bônus ou cortesia",
    "implantacao": "Implantação / período de avaliação",
    "teste_gratis": "Teste grátis (autocadastro)",
    "outro": "Outro motivo",
}
TRIAL_GRANT_REASON = "teste_gratis"
MAX_GRANT_DAYS = 90
MIN_JUSTIFICATION = 15
EXPIRING_SOON_DAYS = 3


class GrantValidationError(ValueError):
    pass


@dataclass
class AccessStatus:
    """Situacao de acesso usada pelas telas (e pelo bloqueio da etapa de inadimplencia)."""

    has_access: bool
    source: str | None  # "assinatura", "liberacao" ou None
    grant: OrganizationAccessGrant | None = None
    plan_id: int | None = None
    renewal_due: date | None = None

    @property
    def manual(self) -> bool:
        return self.source == "liberacao"


def active_grant(db: Session, organization_id: int, today: date) -> OrganizationAccessGrant | None:
    return (
        db.query(OrganizationAccessGrant)
        .filter(
            OrganizationAccessGrant.organization_id == organization_id,
            OrganizationAccessGrant.revoked_at.is_(None),
            OrganizationAccessGrant.starts_on <= today,
            OrganizationAccessGrant.ends_on >= today,
        )
        .order_by(OrganizationAccessGrant.ends_on.desc())
        .first()
    )


def active_grants_by_organization(db: Session, organization_ids: list[int], today: date) -> dict[int, OrganizationAccessGrant]:
    if not organization_ids:
        return {}
    rows = db.query(OrganizationAccessGrant).filter(
        OrganizationAccessGrant.organization_id.in_(organization_ids),
        OrganizationAccessGrant.revoked_at.is_(None),
        OrganizationAccessGrant.starts_on <= today,
        OrganizationAccessGrant.ends_on >= today,
    ).all()
    result: dict[int, OrganizationAccessGrant] = {}
    for row in rows:
        if row.organization_id not in result or row.ends_on > result[row.organization_id].ends_on:
            result[row.organization_id] = row
    return result


def grant_status_label(grant: OrganizationAccessGrant | None, today: date) -> str | None:
    if not grant:
        return None
    if grant.revoked_at:
        return "Encerrada"
    if grant.ends_on < today:
        return "Expirada"
    if grant.starts_on > today:
        return "Agendada"
    return "Ativa"


def is_expiring_soon(grant: OrganizationAccessGrant | None, today: date) -> bool:
    return bool(grant and grant.revoked_at is None and 0 <= (grant.ends_on - today).days <= EXPIRING_SOON_DAYS)


def _validate(reason: str, justification: str, ends_on: date | None, today: date, starts_on: date) -> str:
    if reason not in GRANT_REASONS:
        raise GrantValidationError("Escolha o motivo da liberação.")
    text = (justification or "").strip()
    if len(text) < MIN_JUSTIFICATION or len(text) > 500:
        raise GrantValidationError(f"Descreva a justificativa com pelo menos {MIN_JUSTIFICATION} caracteres (máximo 500).")
    if not ends_on or ends_on < today or ends_on < starts_on:
        raise GrantValidationError("A data de fim deve ser hoje ou depois.")
    if (ends_on - today).days > MAX_GRANT_DAYS:
        raise GrantValidationError(f"A liberação pode durar no máximo {MAX_GRANT_DAYS} dias a partir de hoje. Prorrogue depois, se necessário.")
    return text


def create_grant(db: Session, *, organization_id: int, reason: str, justification: str, ends_on: date | None, today: date,
                 plan_id: int | None, user) -> OrganizationAccessGrant:
    text = _validate(reason, justification, ends_on, today, today)
    if active_grant(db, organization_id, today):
        raise GrantValidationError("Já existe uma liberação ativa. Prorrogue ou encerre a atual.")
    current = db.query(OrganizationSubscription).filter_by(
        organization_id=organization_id, environment=billing_environment(), is_current=True,
    ).first()
    resolved_plan = plan_id or (current.plan_id if current else None)
    if resolved_plan is not None and not db.get(SubscriptionPlan, resolved_plan):
        raise GrantValidationError("Plano inválido.")
    if resolved_plan is None and not current:
        raise GrantValidationError("Sem assinatura, escolha o plano que será liberado.")
    grant = OrganizationAccessGrant(
        organization_id=organization_id, plan_id=resolved_plan, reason=reason, justification=text, starts_on=today, ends_on=ends_on,
        created_by_user_id=getattr(user, "id", None), created_by_email=getattr(user, "email", None),
    )
    db.add(grant)
    db.flush()
    return grant


def extend_grant(grant: OrganizationAccessGrant, *, ends_on: date | None, justification: str, today: date) -> str:
    """Prorroga a liberacao. A justificativa da prorrogacao fica na auditoria; retorna o texto validado."""
    if grant.revoked_at or grant.ends_on < today:
        raise GrantValidationError("Somente liberações ativas podem ser prorrogadas.")
    if not ends_on or ends_on <= grant.ends_on:
        raise GrantValidationError("A nova data de fim deve ser posterior à atual.")
    text = _validate(grant.reason, justification, ends_on, today, grant.starts_on)
    grant.ends_on = ends_on
    grant.extension_count = (grant.extension_count or 0) + 1
    return text


def revoke_grant(grant: OrganizationAccessGrant, *, reason: str, user) -> None:
    if grant.revoked_at:
        raise GrantValidationError("Esta liberação já foi encerrada.")
    text = (reason or "").strip()
    if len(text) < 5:
        raise GrantValidationError("Informe o motivo do encerramento.")
    grant.revoked_at = datetime.now(timezone.utc)
    grant.revoked_by_email = getattr(user, "email", None)
    grant.revoke_reason = text[:255]


def cancelled_paid_period(db: Session, organization_id: int, today: date):
    subscriptions = db.query(OrganizationSubscription).filter_by(
        organization_id=organization_id, environment=MERCADOPAGO_LIVE, status="encerrada",
    ).order_by(OrganizationSubscription.mercadopago_ended_at.desc().nullslast(), OrganizationSubscription.id.desc()).all()
    best: tuple[OrganizationSubscription | None, date | None] = (None, None)
    for subscription in subscriptions:
        payment = latest_settled_subscription_payment(db, subscription.id)
        if not payment or payment.status not in PAID:
            continue
        candidates = [day for day in (payment.due_date, payment.paid_date) if day and day <= today]
        if not candidates:
            continue
        start = min(candidates)
        end = _cycle_end(start, subscription.cycle)
        if today < end and (best[1] is None or end > best[1]):
            best = (subscription, end)
    return best


def _cycle_end(start: date, cycle: str) -> date:
    year, month_index = divmod(start.year * 12 + start.month - 1 + cycle_months(cycle), 12)
    month = month_index + 1
    return date(year, month, min(start.day, monthrange(year, month)[1]))


def subscription_renewal_due(subscription: OrganizationSubscription, payment: OrganizationSubscriptionPayment | None) -> date | None:
    """Próxima data esperada de renovação, respeitando variações de meses (28/29/30/31 dias)."""
    if not payment:
        return None
    start = payment.due_date or payment.paid_date
    if not start:
        return None
    base_due = _cycle_end(start, subscription.cycle)
    if subscription.next_due_date and base_due < subscription.next_due_date <= base_due + timedelta(days=4):
        return subscription.next_due_date
    return base_due


def _awaiting_scheduled_first_charge(db: Session, subscription: OrganizationSubscription, today: date) -> bool:
    """Evita bloqueio no 1º dia após o teste grátis enquanto o Mercado Pago processa o débito agendado."""
    if not subscription.first_due_date or today > subscription.first_due_date:
        return False
    has_overdue_or_failed = db.query(OrganizationSubscriptionPayment.id).filter(
        OrganizationSubscriptionPayment.subscription_id == subscription.id,
        OrganizationSubscriptionPayment.status == "OVERDUE",
    ).first()
    if has_overdue_or_failed:
        return False
    trial = db.query(OrganizationAccessGrant.id).filter(
        OrganizationAccessGrant.organization_id == subscription.organization_id,
        OrganizationAccessGrant.reason == TRIAL_GRANT_REASON,
        OrganizationAccessGrant.revoked_at.is_(None),
        OrganizationAccessGrant.ends_on >= subscription.first_due_date - timedelta(days=1),
    ).first()
    return trial is not None


def organization_access(db: Session, organization_id: int, today: date, *, environment: str | None = None) -> AccessStatus:
    """Acesso pela assinatura em dia ou por liberacao manual. A etapa de inadimplencia aplica o bloqueio a partir daqui."""
    grant = active_grant(db, organization_id, today)
    if grant:
        return AccessStatus(True, "liberacao", grant, grant.plan_id)
    query = db.query(OrganizationSubscription).filter_by(organization_id=organization_id, is_current=True)
    if environment:
        query = query.filter_by(environment=environment)
    current = query.first()
    plan_id = effective_plan_id(current, today)
    if current and current.billing_status == "ACTIVE":
        if current.environment == MERCADOPAGO_LIVE:
            payment = latest_settled_subscription_payment(db, current.id)
            if not payment:
                if _awaiting_scheduled_first_charge(db, current, today):
                    return AccessStatus(True, "assinatura", None, plan_id)
                return AccessStatus(False, None, None, plan_id)
            if payment.status not in PAID:
                return AccessStatus(False, None, None, plan_id)
            renewal_due = subscription_renewal_due(current, payment)
            return AccessStatus(True, "assinatura", None, plan_id, renewal_due=renewal_due)
        if current.environment in {"production", MERCADOPAGO_TEST} and not has_confirmed_subscription_payment(db, current.id):
            return AccessStatus(False, None, None, plan_id)
        return AccessStatus(True, "assinatura", None, plan_id)
    if not current and environment == MERCADOPAGO_LIVE:
        previous, _ = cancelled_paid_period(db, organization_id, today)
        if previous:
            return AccessStatus(True, "assinatura", None, effective_plan_id(previous, today))
    return AccessStatus(False, None, None, plan_id)


def default_grant_end(today: date, days: int = 30) -> date:
    return today + timedelta(days=days)
