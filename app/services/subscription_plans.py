"""Catalogo de planos comerciais do SiSFarm.

Regras:
- um plano e uma familia (``code``) com versoes numeradas;
- versao com assinaturas nao muda condicoes comerciais (precos, limites, modulos, teste gratis);
  para mudar, cria-se nova versao e os assinantes atuais mantem o que contrataram;
- ao ativar uma versao, a versao ativa anterior da mesma familia e arquivada;
- planos usados por assinaturas nunca sao excluidos, apenas arquivados.
"""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core import permissions_catalog as perms
from app.models import OrganizationSubscription, SubscriptionPlan

# Ciclo Asaas -> (rotulo, meses, coluna de preco)
SUBSCRIPTION_CYCLES: dict[str, tuple[str, int, str]] = {
    "MONTHLY": ("Mensal", 1, "price_monthly"),
    "QUARTERLY": ("Trimestral", 3, "price_quarterly"),
    "SEMIANNUALLY": ("Semestral", 6, "price_semiannual"),
    "YEARLY": ("Anual", 12, "price_yearly"),
}
CYCLE_UNIT_LABELS = {"MONTHLY": "mês", "QUARTERLY": "trimestre", "SEMIANNUALLY": "semestre", "YEARLY": "ano"}

PLAN_STATUSES = {"rascunho": "Rascunho", "ativo": "Ativo", "arquivado": "Arquivado"}

# Modulos comercializaveis (o dashboard e o perfil sempre fazem parte).
PLAN_MODULES: tuple[tuple[str, str], ...] = (
    (perms.PAGE_PRODUCTIVE_UNIT, "Fazendas, safras e setores"),
    (perms.PAGE_MAP, "Mapa da fazenda"),
    (perms.PAGE_OPERATIONS, "Fertilização e recomendações"),
    (perms.PAGE_AGENDA, "Agenda de fertilização"),
    (perms.PAGE_INPUTS, "Insumos e estoque"),
    (perms.PAGE_ASSETS, "Patrimônio e manutenção"),
    (perms.PAGE_FINANCE, "Gestão financeira"),
    (perms.PAGE_PRODUCTION, "Colheita e comercialização"),
    (perms.PAGE_VARIETIES, "Variedades"),
    (perms.PAGE_IRRIGATION, "Irrigação"),
    (perms.PAGE_RAINFALL, "Pluviometria"),
    (perms.PAGE_PESTS, "Pragas e doenças"),
    (perms.PAGE_SOIL, "Análise de solo"),
    (perms.PAGE_AGRONOMIC, "Perfil agronômico"),
    (perms.PAGE_MOBILE, "Campo mobile"),
)
PLAN_MODULE_CODES = frozenset(code for code, _ in PLAN_MODULES)
_MAX_PRICE = Decimal("10000000000")


class PlanValidationError(ValueError):
    pass


@dataclass
class PlanForm:
    name: str
    description: str | None
    prices: dict[str, Decimal | None]
    max_farms: int | None
    max_users: int | None
    max_hectares: Decimal | None
    modules: list[str]
    trial_days: int
    display_order: int
    is_featured: bool


def cycle_months(cycle: str) -> int:
    return SUBSCRIPTION_CYCLES.get(cycle, ("", 1, ""))[1]


def plan_price(plan: SubscriptionPlan, cycle: str) -> Decimal | None:
    entry = SUBSCRIPTION_CYCLES.get(cycle)
    return getattr(plan, entry[2]) if entry else None


def plan_prices(plan: SubscriptionPlan) -> dict[str, Decimal]:
    return {cycle: price for cycle in SUBSCRIPTION_CYCLES if (price := plan_price(plan, cycle)) is not None}


def plan_modules(plan: SubscriptionPlan) -> list[str]:
    try:
        values = json.loads(plan.modules_json or "[]")
    except ValueError:
        return []
    return [code for code, _ in PLAN_MODULES if code in values]


def _money(value: str | None) -> Decimal | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        amount = Decimal(raw.replace(".", "").replace(",", "."))
    except InvalidOperation:
        raise PlanValidationError("Informe preços em reais, com até duas casas decimais.")
    if not amount.is_finite() or amount <= 0 or amount >= _MAX_PRICE or amount.as_tuple().exponent < -2:
        raise PlanValidationError("Informe preços em reais, com até duas casas decimais.")
    return amount.quantize(Decimal("0.01"))


def _positive_int(value: str | None, label: str, *, allow_zero: bool = False, maximum: int = 1_000_000) -> int | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        number = int(raw)
    except ValueError:
        raise PlanValidationError(f"{label}: informe um número inteiro.")
    if number < (0 if allow_zero else 1) or number > maximum:
        raise PlanValidationError(f"{label}: valor fora do permitido.")
    return number


def parse_plan_form(*, name, description, price_monthly, price_quarterly, price_semiannual, price_yearly,
                    max_farms, max_users, max_hectares, modules, trial_days, display_order, is_featured) -> PlanForm:
    clean_name = (name or "").strip()
    if not clean_name or len(clean_name) > 120:
        raise PlanValidationError("Informe o nome do plano (até 120 caracteres).")
    clean_description = (description or "").strip() or None
    if clean_description and len(clean_description) > 500:
        raise PlanValidationError("A descrição deve ter até 500 caracteres.")
    prices = {
        "MONTHLY": _money(price_monthly),
        "QUARTERLY": _money(price_quarterly),
        "SEMIANNUALLY": _money(price_semiannual),
        "YEARLY": _money(price_yearly),
    }
    if not any(prices.values()):
        raise PlanValidationError("Informe o preço de pelo menos uma periodicidade.")
    hectares = None
    if (max_hectares or "").strip():
        try:
            hectares = Decimal(max_hectares.strip().replace(".", "").replace(",", "."))
        except InvalidOperation:
            raise PlanValidationError("Limite de hectares inválido.")
        if not hectares.is_finite() or hectares <= 0 or hectares >= Decimal("10000000000"):
            raise PlanValidationError("Limite de hectares inválido.")
        hectares = hectares.quantize(Decimal("0.01"))
    selected = [code for code in (modules or []) if code in PLAN_MODULE_CODES]
    if len(selected) != len(set(modules or [])):
        raise PlanValidationError("Módulo inválido selecionado.")
    return PlanForm(
        name=clean_name,
        description=clean_description,
        prices=prices,
        max_farms=_positive_int(max_farms, "Limite de fazendas"),
        max_users=_positive_int(max_users, "Limite de usuários"),
        max_hectares=hectares,
        modules=[code for code, _ in PLAN_MODULES if code in selected],
        trial_days=_positive_int(trial_days, "Dias de teste grátis", allow_zero=True, maximum=365) or 0,
        display_order=_positive_int(display_order, "Ordem de exibição", allow_zero=True, maximum=9999) or 0,
        is_featured=bool(is_featured),
    )


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    slug = re.sub(r"[^a-z0-9]+", "-", normalized.lower()).strip("-")
    return slug[:50] or "plano"


def _unique_code(db: Session, name: str) -> str:
    base = _slugify(name)
    code, suffix = base, 2
    while db.query(SubscriptionPlan.id).filter(SubscriptionPlan.code == code).first():
        code = f"{base}-{suffix}"
        suffix += 1
    return code


def plan_subscription_count(db: Session, plan_id: int) -> int:
    return db.query(func.count(OrganizationSubscription.id)).filter(OrganizationSubscription.plan_id == plan_id).scalar() or 0


def plan_is_locked(db: Session, plan: SubscriptionPlan) -> bool:
    """Condicoes comerciais ficam travadas quando alguma assinatura (atual ou historica) usa a versao."""
    return plan_subscription_count(db, plan.id) > 0


def _apply_commercial_terms(plan: SubscriptionPlan, form: PlanForm) -> None:
    plan.name = form.name
    plan.price_monthly = form.prices["MONTHLY"]
    plan.price_quarterly = form.prices["QUARTERLY"]
    plan.price_semiannual = form.prices["SEMIANNUALLY"]
    plan.price_yearly = form.prices["YEARLY"]
    plan.max_farms = form.max_farms
    plan.max_users = form.max_users
    plan.max_hectares = form.max_hectares
    plan.modules_json = json.dumps(form.modules)
    plan.trial_days = form.trial_days


def _apply_presentation(plan: SubscriptionPlan, form: PlanForm) -> None:
    plan.description = form.description
    plan.display_order = form.display_order
    plan.is_featured = form.is_featured


def create_plan(db: Session, form: PlanForm) -> SubscriptionPlan:
    plan = SubscriptionPlan(code=_unique_code(db, form.name), version=1, status="rascunho")
    _apply_commercial_terms(plan, form)
    _apply_presentation(plan, form)
    db.add(plan)
    db.flush()
    return plan


def _same_terms(plan: SubscriptionPlan, form: PlanForm) -> bool:
    return (
        plan.name == form.name
        and all(plan_price(plan, cycle) == form.prices[cycle] for cycle in SUBSCRIPTION_CYCLES)
        and plan.max_farms == form.max_farms
        and plan.max_users == form.max_users
        and (plan.max_hectares == form.max_hectares)
        and plan_modules(plan) == form.modules
        and plan.trial_days == form.trial_days
    )


def update_plan(db: Session, plan: SubscriptionPlan, form: PlanForm) -> None:
    if plan.status == "arquivado":
        raise PlanValidationError("Planos arquivados não podem ser editados. Crie uma nova versão.")
    if plan_is_locked(db, plan):
        if not _same_terms(plan, form):
            raise PlanValidationError("Esta versão já tem assinaturas. Para mudar nome, preços, limites, módulos ou teste grátis, crie uma nova versão.")
    else:
        _apply_commercial_terms(plan, form)
    _apply_presentation(plan, form)


def create_new_version(db: Session, plan: SubscriptionPlan) -> SubscriptionPlan:
    if db.query(SubscriptionPlan.id).filter(SubscriptionPlan.code == plan.code, SubscriptionPlan.status == "rascunho").first():
        raise PlanValidationError("Já existe uma versão em rascunho deste plano. Edite-a ou exclua-a antes de criar outra.")
    latest = db.query(func.max(SubscriptionPlan.version)).filter(SubscriptionPlan.code == plan.code).scalar() or plan.version
    clone = SubscriptionPlan(
        code=plan.code, version=latest + 1, status="rascunho", name=plan.name, description=plan.description,
        price_monthly=plan.price_monthly, price_quarterly=plan.price_quarterly, price_semiannual=plan.price_semiannual,
        price_yearly=plan.price_yearly, max_farms=plan.max_farms, max_users=plan.max_users, max_hectares=plan.max_hectares,
        modules_json=plan.modules_json, trial_days=plan.trial_days, display_order=plan.display_order, is_featured=plan.is_featured,
    )
    db.add(clone)
    db.flush()
    return clone


def activate_plan(db: Session, plan: SubscriptionPlan) -> list[SubscriptionPlan]:
    """Ativa a versao e arquiva a versao ativa anterior da mesma familia. Retorna as versoes arquivadas."""
    if plan.status == "ativo":
        return []
    if not plan_prices(plan):
        raise PlanValidationError("Defina ao menos um preço antes de ativar o plano.")
    archived = db.query(SubscriptionPlan).filter(
        SubscriptionPlan.code == plan.code, SubscriptionPlan.status == "ativo", SubscriptionPlan.id != plan.id
    ).all()
    for item in archived:
        item.status = "arquivado"
    plan.status = "ativo"
    return archived


def archive_plan(plan: SubscriptionPlan) -> None:
    if plan.status != "ativo":
        raise PlanValidationError("Somente planos ativos podem ser arquivados.")
    plan.status = "arquivado"


def delete_plan(db: Session, plan: SubscriptionPlan) -> None:
    if plan.status != "rascunho" or plan_is_locked(db, plan):
        raise PlanValidationError("Somente rascunhos sem assinaturas podem ser excluídos. Arquive o plano.")
    db.delete(plan)


def active_plans(db: Session) -> list[SubscriptionPlan]:
    return db.query(SubscriptionPlan).filter(SubscriptionPlan.status == "ativo").order_by(
        SubscriptionPlan.display_order, SubscriptionPlan.name
    ).all()


def resolve_plan_selection(db: Session, plan_id: int, cycle: str) -> tuple[SubscriptionPlan, Decimal]:
    """Plano e preco para uma nova assinatura; o preco vem sempre do catalogo, nunca do formulario."""
    plan = db.get(SubscriptionPlan, plan_id)
    if not plan or plan.status != "ativo":
        raise PlanValidationError("Plano indisponível para novas assinaturas.")
    price = plan_price(plan, cycle)
    if price is None:
        raise PlanValidationError("O plano não tem preço para a periodicidade escolhida.")
    return plan, price
