"""Liberacao manual (contingencial) do plano e situacao de acesso de uma organizacao.

Regras:
- somente a equipe SiSFarm (super administrador) cria, prorroga ou encerra;
- motivo de uma lista fechada + justificativa obrigatoria (fica na auditoria);
- sempre tem data de fim, no maximo 90 dias a partir de hoje (prorrogavel, com nova justificativa);
- uma liberacao ativa por organizacao; encerramento antecipado exige motivo;
- nao cria, altera ou quita cobrancas: receita e inadimplencia continuam refletindo o Asaas.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.models import OrganizationAccessGrant, OrganizationSubscription, SubscriptionPlan

GRANT_REASONS: dict[str, str] = {
    "erro_interno": "Erro ou falha da SiSFarm",
    "instabilidade_asaas": "Instabilidade ou falha no Asaas",
    "negociacao_comercial": "Negociação comercial em andamento",
    "bonus_cortesia": "Bônus ou cortesia",
    "implantacao": "Implantação / período de avaliação",
    "outro": "Outro motivo",
}
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
    current = db.query(OrganizationSubscription).filter_by(organization_id=organization_id, is_current=True).first()
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


def organization_access(db: Session, organization_id: int, today: date, *, environment: str | None = None) -> AccessStatus:
    """Acesso pela assinatura em dia ou por liberacao manual. A etapa de inadimplencia aplica o bloqueio a partir daqui."""
    grant = active_grant(db, organization_id, today)
    if grant:
        return AccessStatus(True, "liberacao", grant, grant.plan_id)
    query = db.query(OrganizationSubscription).filter_by(organization_id=organization_id, is_current=True)
    if environment:
        query = query.filter_by(environment=environment)
    current = query.first()
    if current and current.asaas_status == "ACTIVE":
        return AccessStatus(True, "assinatura", None, current.plan_id)
    return AccessStatus(False, None, None, current.plan_id if current else None)


def default_grant_end(today: date, days: int = 30) -> date:
    return today + timedelta(days=days)
