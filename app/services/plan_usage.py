"""Consumo atual da organização e cadastros que excedem os limites do plano."""

from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy.orm import Session

from app.models import Farm, Organization, OrganizationSubscription, SubscriptionPlan, User
from app.services.billing_environment import billing_environment
from app.services.entitlements import Entitlements, compute_entitlements
from app.services.plan_changes import effective_plan_id, has_confirmed_subscription_payment


@dataclass
class PlanUsage:
    farms: list
    users: list
    total_area: Decimal
    max_farms: int | None
    max_users: int | None
    max_hectares: Decimal | None
    excess_farms: dict
    excess_users: list

    @property
    def exceeds(self) -> bool:
        return bool(self.excess_farms or self.excess_users)


def ordered_active_users(db: Session, organization_id: int):
    # O responsável com acesso administrativo tem prioridade para poder regularizar a conta.
    return (db.query(User.id, User.name)
            .filter_by(organization_id=organization_id, is_active=True)
            .order_by(User.is_admin.desc(), User.id))


def plan_usage(db: Session, organization_id: int, plan) -> PlanUsage:
    farms = (db.query(Farm.id, Farm.name, Farm.total_area)
             .filter_by(organization_id=organization_id, operations_paused=False).order_by(Farm.id).all())
    users = ordered_active_users(db, organization_id).all()
    max_farms, max_users, max_hectares = plan.max_farms, plan.max_users, plan.max_hectares
    excess_farms = {}
    area = Decimal(0)
    count = 0
    for farm in farms:
        current_area = Decimal(farm.total_area or 0)
        reasons = []
        if max_farms is not None and count >= max_farms:
            reasons.append("Limite de fazendas")
        if max_hectares is not None and area + current_area > max_hectares:
            reasons.append("Limite de área")
        if reasons:
            excess_farms[farm.id] = tuple(reasons)
        else:
            count += 1
            area += current_area
    return PlanUsage(
        farms, users, sum((Decimal(farm.total_area or 0) for farm in farms), Decimal(0)),
        max_farms, max_users, max_hectares, excess_farms, users[max_users:] if max_users is not None else [],
    )


def quota_write_entitlements(db: Session, organization_id: int | None, today) -> Entitlements:
    """Serializa alterações que consomem limite e consulta o plano na mesma transação."""
    if organization_id:
        db.query(Organization.id).filter_by(id=organization_id).with_for_update().first()
    return compute_entitlements(db, organization_id, today)


def user_exceeds_plan(db: Session, entitlements: Entitlements, user_id: int) -> bool:
    if entitlements.exempt or not entitlements.organization_id or entitlements.max_users is None:
        return False
    allowed = ordered_active_users(db, entitlements.organization_id).limit(entitlements.max_users)
    return user_id not in {row.id for row in allowed}



def confirmed_plan_for_usage(db: Session, organization_id: int, today) -> SubscriptionPlan | None:
    subscription = db.query(OrganizationSubscription).filter_by(
        organization_id=organization_id, environment=billing_environment(), is_current=True,
    ).first()
    if (not subscription or subscription.billing_status != "ACTIVE"
            or not (subscription.asaas_subscription_id or subscription.mercadopago_id)
            or not has_confirmed_subscription_payment(db, subscription.id)):
        return None
    plan_id = effective_plan_id(subscription, today)
    return db.get(SubscriptionPlan, plan_id) if plan_id else None


def regularization_status(db: Session, entitlements: Entitlements, today) -> PlanUsage | None:
    if entitlements.exempt or not entitlements.organization_id:
        return None
    plan = entitlements
    if entitlements.billing_state == "teste":
        plan = confirmed_plan_for_usage(db, entitlements.organization_id, today)
    if not plan or all(limit is None for limit in (plan.max_farms, plan.max_users, plan.max_hectares)):
        return None
    return plan_usage(db, entitlements.organization_id, plan)
