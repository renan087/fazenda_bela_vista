"""Direitos de uma organizacao: situacao de cobranca, modulos e limites do plano.

Fonte unica para o modo somente leitura (inadimplencia), o cadeado de modulos e os limites de cadastro.

Regras:
- a organizacao da equipe SiSFarm (slug "sisfarm") nunca e limitada;
- liberacao manual ativa garante acesso e define o plano;
- sem assinatura nem liberacao: sem limites (clientes ainda nao migrados para a cobranca);
- inadimplencia: aviso do 1o dia de atraso ate o fim da tolerancia; depois, somente leitura;
- plano personalizado (fora do catalogo): sem limites de modulos/quantidade.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models import Farm, Organization, OrganizationSubscription, OrganizationSubscriptionPayment, SubscriptionPlan, User
from app.services.access_grants import organization_access
from app.services.asaas_environment import asaas_env_name
from app.services.subscription_plans import PLAN_MODULES, plan_modules

DEFAULT_ORGANIZATION_SLUG = "sisfarm"
MODULE_LABELS = dict(PLAN_MODULES)
# Paginas sempre liberadas, mesmo com modulo bloqueado ou em somente leitura.
ALWAYS_ALLOWED_PREFIXES = ("/minha-assinatura", "/planos-disponiveis")


def grace_days() -> int:
    value = getattr(get_settings(), "subscription_grace_days", 10)
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 10


@dataclass(frozen=True)
class Entitlements:
    organization_id: int | None
    exempt: bool = False
    plan_id: int | None = None
    plan_name: str | None = None
    modules: frozenset[str] | None = None  # None = todos os modulos
    max_farms: int | None = None
    max_users: int | None = None
    max_hectares: Decimal | None = None
    billing_state: str = "ok"  # ok | aviso | restrito | liberado
    days_overdue: int = 0
    overdue_amount: Decimal = Decimal("0.00")
    restrict_on: date | None = None
    grant_until: date | None = None
    locked_modules: frozenset[str] = field(default_factory=frozenset)

    @property
    def regularize_until(self) -> date | None:
        """Ultimo dia para pagar antes do modo somente leitura."""
        return self.restrict_on - timedelta(days=1) if self.restrict_on else None

    @property
    def read_only(self) -> bool:
        return self.billing_state == "restrito"

    def module_allowed(self, code: str | None) -> bool:
        return self.exempt or code is None or self.modules is None or code not in MODULE_LABELS or code in self.modules


UNRESTRICTED = Entitlements(organization_id=None, exempt=True)


def compute_entitlements(db: Session, organization_id: int | None, today: date) -> Entitlements:
    if not organization_id:
        return UNRESTRICTED
    organization = db.get(Organization, organization_id)
    if not organization or organization.slug == DEFAULT_ORGANIZATION_SLUG:
        return Entitlements(organization_id=organization_id, exempt=True)
    env = asaas_env_name()
    access = organization_access(db, organization_id, today, environment=env)
    plan = db.get(SubscriptionPlan, access.plan_id) if access.plan_id else None
    modules = frozenset(plan_modules(plan)) if plan else None
    locked = frozenset(code for code in MODULE_LABELS if modules is not None and code not in modules)

    subscription_ids = [row[0] for row in db.query(OrganizationSubscription.id).filter_by(organization_id=organization_id, environment=env).all()]
    days_overdue, overdue_amount, oldest = 0, Decimal("0.00"), None
    if subscription_ids:
        overdue = db.query(OrganizationSubscriptionPayment).filter(
            OrganizationSubscriptionPayment.subscription_id.in_(subscription_ids),
            (OrganizationSubscriptionPayment.status == "OVERDUE")
            | ((OrganizationSubscriptionPayment.status == "PENDING") & (OrganizationSubscriptionPayment.due_date < today)),
        ).all()
        dates = [row.due_date for row in overdue if row.due_date]
        if dates:
            oldest = min(dates)
            days_overdue = max((today - oldest).days, 0)
        overdue_amount = sum((Decimal(row.amount or 0) for row in overdue), Decimal("0.00"))

    grace = grace_days()
    if access.manual:
        state = "liberado"
    elif days_overdue <= 0:
        state = "ok"
    elif days_overdue <= grace:
        state = "aviso"
    else:
        state = "restrito"
    return Entitlements(
        organization_id=organization_id,
        plan_id=plan.id if plan else None,
        plan_name=plan.name if plan else None,
        modules=modules,
        max_farms=plan.max_farms if plan else None,
        max_users=plan.max_users if plan else None,
        max_hectares=Decimal(plan.max_hectares) if plan and plan.max_hectares is not None else None,
        billing_state=state,
        days_overdue=days_overdue,
        overdue_amount=overdue_amount,
        restrict_on=(oldest + timedelta(days=grace + 1)) if oldest else None,
        grant_until=access.grant.ends_on if access.grant else None,
        locked_modules=locked,
    )


# Cache curto por organizacao: evita consultar o banco a cada requisicao.
_CACHE_TTL_SECONDS = 60
_cache: dict[int, tuple[float, Entitlements]] = {}
_cache_lock = threading.Lock()


def cached_entitlements(db: Session, organization_id: int | None, today: date) -> Entitlements:
    if not organization_id:
        return UNRESTRICTED
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(organization_id)
        if hit and hit[0] > now:
            return hit[1]
    value = compute_entitlements(db, organization_id, today)
    with _cache_lock:
        _cache[organization_id] = (now + _CACHE_TTL_SECONDS, value)
    return value


def invalidate_entitlements(organization_id: int | None = None) -> None:
    with _cache_lock:
        if organization_id is None:
            _cache.clear()
        else:
            _cache.pop(organization_id, None)


# ---- Limites de cadastro ----

def farm_limit_error(db: Session, entitlements: Entitlements, *, adding_farm: bool, area_delta: Decimal) -> str | None:
    """Mensagem de bloqueio ou None. Nunca afeta o que ja existe; so impede ultrapassar o limite."""
    if entitlements.exempt or not entitlements.organization_id:
        return None
    org_id = entitlements.organization_id
    plan = entitlements.plan_name or "atual"
    if adding_farm and entitlements.max_farms is not None:
        count = db.query(func.count(Farm.id)).filter(Farm.organization_id == org_id).scalar() or 0
        if count >= entitlements.max_farms:
            return f"Seu plano {plan} permite até {entitlements.max_farms} fazenda(s). Conheça os planos para cadastrar mais."
    if entitlements.max_hectares is not None and area_delta > 0:
        total = Decimal(str(db.query(func.coalesce(func.sum(Farm.total_area), 0)).filter(Farm.organization_id == org_id).scalar() or 0))
        if total + area_delta > entitlements.max_hectares:
            limit = f"{entitlements.max_hectares:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            return f"Seu plano {plan} permite até {limit} ha somando as fazendas. Conheça os planos para ampliar a área."
    return None


def user_limit_error(db: Session, entitlements: Entitlements) -> str | None:
    if entitlements.exempt or not entitlements.organization_id or entitlements.max_users is None:
        return None
    count = db.query(func.count(User.id)).filter(User.organization_id == entitlements.organization_id, User.is_active.is_(True)).scalar() or 0
    if count >= entitlements.max_users:
        return f"Seu plano {entitlements.plan_name or 'atual'} permite até {entitlements.max_users} usuário(s) ativo(s). Conheça os planos para adicionar mais."
    return None


# Textos da pagina de upgrade: o que cada modulo resolve para o produtor.
MODULE_PITCH: dict[str, str] = {
    "page.productive_unit": "Organize fazendas, safras e setores com mapa, área e custos por setor.",
    "page.map": "Veja a fazenda no mapa com os setores desenhados e acompanhe a lavoura de forma visual.",
    "page.operations": "Planeje e registre adubações e recomendações de insumos por setor, com histórico completo.",
    "page.agenda": "Agende as aplicações de fertilização e receba alertas do que vence na semana.",
    "page.inputs": "Controle compras, estoque, lotes e validades dos insumos, com baixa automática nas aplicações.",
    "page.assets": "Cadastre máquinas e equipamentos, planeje manutenções preventivas e controle seus custos.",
    "page.finance": "Contas a pagar e a receber, cartões e extrato da fazenda em um só lugar.",
    "page.production": "Registre a colheita e a comercialização do café e acompanhe a produtividade por hectare.",
    "page.varieties": "Cadastre as variedades de café e compare o desempenho de cada uma.",
    "page.irrigation": "Registre a irrigação por setor e acompanhe o volume aplicado.",
    "page.rainfall": "Anote as chuvas e acompanhe o histórico pluviométrico da propriedade.",
    "page.pests": "Registre pragas e doenças da lavoura e acompanhe a evolução e os tratamentos.",
    "page.soil": "Guarde as análises de solo e use os resultados nas recomendações de adubação.",
    "page.agronomic": "Monte o perfil agronômico da fazenda com sugestões automáticas.",
    "page.mobile": "Registre as atividades direto do campo pelo celular.",
}
