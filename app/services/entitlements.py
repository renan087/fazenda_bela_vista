"""Direitos de uma organizacao: situacao de cobranca, modulos e limites do plano.

Fonte unica para o modo somente leitura (inadimplencia), o cadeado de modulos e os limites de cadastro.

Regras:
- a organizacao da equipe SiSFarm (slug "sisfarm") nunca e limitada;
- liberacao manual ativa garante acesso e define o plano;
- assinatura ou teste encerrado, sem outra liberação válida: somente leitura;
- inadimplencia: aviso do 1o dia de atraso ate o fim da tolerancia; depois, somente leitura;
- plano personalizado (fora do catalogo): sem limites de modulos/quantidade.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models import Farm, Organization, OrganizationAccessGrant, OrganizationSubscription, OrganizationSubscriptionPayment, SubscriptionPlan, User
from app.services.access_grants import TRIAL_GRANT_REASON, organization_access
from app.services.billing_environment import MERCADOPAGO_TEST, billing_environment
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
    plan_icon: str | None = None
    modules: frozenset[str] | None = None  # None = todos os modulos
    max_farms: int | None = None
    max_users: int | None = None
    max_hectares: Decimal | None = None
    billing_state: str = "ok"  # ok | aviso | restrito | liberado | teste
    trial_ends_on: date | None = None
    trial_ended: bool = False
    access_missing: bool = False
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
    env = billing_environment()
    access = organization_access(db, organization_id, today, environment=env)
    is_trial = bool(access.grant and access.grant.reason == TRIAL_GRANT_REASON)
    plan_id = access.plan_id
    last_subscription = None
    last_trial = None
    if not access.has_access:
        last_trial = (db.query(OrganizationAccessGrant)
                      .filter_by(organization_id=organization_id, reason=TRIAL_GRANT_REASON)
                      .order_by(OrganizationAccessGrant.ends_on.desc()).first())
        last_subscription = (db.query(OrganizationSubscription)
                             .filter_by(organization_id=organization_id, environment=env)
                             .order_by(OrganizationSubscription.is_current.desc(), OrganizationSubscription.id.desc()).first())
        if plan_id is None and last_subscription:
            plan_id = last_subscription.plan_id
    access_missing = not access.has_access and bool(env == "production" or last_trial or last_subscription)
    plan = db.get(SubscriptionPlan, plan_id) if plan_id else None
    modules = frozenset(plan_modules(plan)) if plan and not is_trial else None
    locked = frozenset(code for code in MODULE_LABELS if modules is not None and code not in modules)

    subscription_query = db.query(OrganizationSubscription.id).filter_by(organization_id=organization_id, environment=env)
    if env in {"production", MERCADOPAGO_TEST}:
        subscription_query = subscription_query.filter_by(is_current=True)
    subscription_ids = [row[0] for row in subscription_query.all()]
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
    trial_ended = bool(last_trial and last_trial.ends_on < today)
    trial_ends_on = access.grant.ends_on if is_trial else last_trial.ends_on if trial_ended else None
    if is_trial:
        state = "teste"
    elif access_missing:
        state = "restrito"
    elif access.manual:
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
        plan_icon=plan.icon if plan else None,
        modules=modules,
        max_farms=plan.max_farms if plan and not is_trial else None,
        max_users=plan.max_users if plan and not is_trial else None,
        max_hectares=Decimal(plan.max_hectares) if plan and not is_trial and plan.max_hectares is not None else None,
        billing_state=state,
        days_overdue=days_overdue,
        overdue_amount=overdue_amount,
        restrict_on=(oldest + timedelta(days=grace + 1)) if oldest else None,
        grant_until=access.grant.ends_on if access.grant else None,
        locked_modules=locked,
        trial_ends_on=trial_ends_on,
        trial_ended=trial_ended,
        access_missing=access_missing,
    )


# ---- Limites de cadastro ----

def farm_limit_error(db: Session, entitlements: Entitlements, *, adding_farm: bool, area_delta: Decimal) -> str | None:
    """Mensagem de bloqueio ou None. Nunca afeta o que ja existe; so impede ultrapassar o limite."""
    if entitlements.exempt or not entitlements.organization_id:
        return None
    org_id = entitlements.organization_id
    plan = entitlements.plan_name or "atual"
    if adding_farm and entitlements.max_farms is not None:
        count = db.query(func.count(Farm.id)).filter(Farm.organization_id == org_id, Farm.operations_paused.is_(False)).scalar() or 0
        if count >= entitlements.max_farms:
            return f"Seu plano {plan} permite até {entitlements.max_farms} fazenda(s). Conheça os planos para cadastrar mais."
    if entitlements.max_hectares is not None and (adding_farm or area_delta > 0):
        total = Decimal(str(db.query(func.coalesce(func.sum(Farm.total_area), 0)).filter(Farm.organization_id == org_id, Farm.operations_paused.is_(False)).scalar() or 0))
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

# Detalhe de cada modulo na landing page: grupos (titulo, itens) em linguagem do produtor.
MODULE_DETAILS: dict[str, tuple[tuple[str, tuple[str, ...]], ...]] = {
    "page.productive_unit": (
        ("Fazendas", ("Cadastro de uma ou mais fazendas com localização", "Troca rápida entre fazendas")),
        ("Safras e setores", ("Safras com início e fim", "Setores (talhões) com área, variedade e mapa", "Custos por setor e por safra, com a composição detalhada")),
    ),
    "page.map": (
        ("No mapa", ("Setores desenhados sobre a imagem de satélite", "Importação do CAR ou desenho manual das áreas", "Navegação entre fazendas e setores")),
    ),
    "page.operations": (
        ("Planejamento", ("Recomendação de insumos por setor", "Doses e produtos definidos para cada área")),
        ("Execução", ("Registro das aplicações realizadas", "Operações agrícolas com seus custos", "Histórico completo por setor e safra")),
    ),
    "page.agenda": (
        ("Agenda", ("Calendário das aplicações planejadas", "Mudança de data de vários agendamentos de uma vez", "Lembretes do que vence na semana")),
    ),
    "page.inputs": (
        ("Compras", ("Registro de compras de insumos", "Lote do fabricante e validade de cada produto", "Compra lançada direto no financeiro")),
        ("Estoque", ("Saldo atual de cada insumo", "Baixa automática a cada aplicação", "Consumo avulso e suprimentos de uso geral", "Inventário físico para conferir e corrigir o saldo")),
    ),
    "page.assets": (
        ("Patrimônio", ("Cadastro de máquinas e equipamentos", "Valor e compra lançados no financeiro")),
        ("Manutenção", ("Planos de manutenção recorrente", "Registro das manutenções com despesas", "Indicadores de custo por equipamento")),
    ),
    "page.finance": (
        ("Extrato", ("Todas as receitas e despesas da fazenda", "Filtros por período, conta e categoria", "Exportação em PDF e Excel")),
        ("Contas", ("Contas bancárias com saldo atualizado", "Cartões de crédito com faturas e parcelas", "Contas a pagar, com marcação de pagamento", "Contas a receber", "Organização por categorias")),
    ),
    "page.production": (
        ("Colheita", ("Registro da colheita por setor e por lote", "Produtividade em sacas por hectare")),
        ("Comercialização", ("Lotes disponíveis para venda", "Registro das vendas por lote de origem", "Histórico de comercialização", "Cotação do café atualizada diariamente")),
    ),
    "page.varieties": (
        ("Variedades", ("Catálogo das variedades de café", "Variedade vinculada a cada setor", "Comparação de desempenho entre variedades")),
    ),
    "page.irrigation": (
        ("Irrigação", ("Registro das irrigações por setor", "Volume de água aplicado", "Histórico das últimas irrigações")),
    ),
    "page.rainfall": (
        ("Chuvas", ("Registro diário de chuvas", "Total de chuva no mês no painel", "Histórico de chuvas por período")),
    ),
    "page.pests": (
        ("Sanidade", ("Ocorrências por setor e por tipo", "Acompanhamento da evolução", "Registro dos tratamentos aplicados")),
    ),
    "page.soil": (
        ("Análises", ("Cadastro dos laudos do laboratório", "Evolução da fertilidade ao longo do tempo")),
        ("Inteligência artificial", ("Recomendação de adubação gerada por IA", "Histórico das recomendações geradas")),
    ),
    "page.agronomic": (
        ("Perfil da lavoura", ("Cultivar, sistema de produção e fase da lavoura", "Clima, tipo de solo, altitude e declividade", "Irrigação, espaçamento e meta de produtividade", "Sugestões automáticas para preencher")),
    ),
    "page.mobile": (
        ("No campo", ("Tela de registro rápido pensada para o celular", "Lançamentos feitos na lavoura entram no sistema na hora")),
    ),
}
