"""Codigos de permissao estaveis (referenciados no codigo e armazenados no banco)."""

from __future__ import annotations

# ---- Paginas / modulos (alinhado ao menu lateral) ----
PAGE_DASHBOARD = "page.dashboard"
PAGE_PRODUCTIVE_UNIT = "page.productive_unit"
PAGE_OPERATIONS = "page.operations"
PAGE_INPUTS = "page.inputs"
PAGE_AGENDA = "page.agenda"
PAGE_ASSETS = "page.assets"
PAGE_FINANCE = "page.finance"
PAGE_PRODUCTION = "page.production"
PAGE_VARIETIES = "page.varieties"
PAGE_IRRIGATION = "page.irrigation"
PAGE_RAINFALL = "page.rainfall"
PAGE_PESTS = "page.pests"
PAGE_SOIL = "page.soil"
PAGE_AGRONOMIC = "page.agronomic"
PAGE_MAP = "page.map"
PAGE_MOBILE = "page.mobile"

# Administracao / infra
USERS_MANAGE = "users.manage"
BACKUPS_MANAGE = "backups.manage"

PAGE_PERMISSION_CODES: frozenset[str] = frozenset(
    {
        PAGE_DASHBOARD,
        PAGE_PRODUCTIVE_UNIT,
        PAGE_OPERATIONS,
        PAGE_INPUTS,
        PAGE_AGENDA,
        PAGE_ASSETS,
        PAGE_FINANCE,
        PAGE_PRODUCTION,
        PAGE_VARIETIES,
        PAGE_IRRIGATION,
        PAGE_RAINFALL,
        PAGE_PESTS,
        PAGE_SOIL,
        PAGE_AGRONOMIC,
        PAGE_MAP,
        PAGE_MOBILE,
    }
)

PERMISSION_DEFINITIONS: tuple[tuple[str, str], ...] = (
    (PAGE_DASHBOARD, "Dashboard e perfil do usuario."),
    (PAGE_PRODUCTIVE_UNIT, "Unidade produtiva: fazendas, safras e setores."),
    (PAGE_OPERATIONS, "Operacoes: fertilizacao e recomendacoes de insumos."),
    (PAGE_INPUTS, "Insumos: compras, estoque e suprimentos."),
    (PAGE_AGENDA, "Agenda de fertilizacao."),
    (PAGE_ASSETS, "Patrimonio e equipamentos."),
    (PAGE_FINANCE, "Gestao financeira, contas e assinatura."),
    (PAGE_PRODUCTION, "Producao: colheita e comercializacao."),
    (PAGE_VARIETIES, "Variedades de cafe."),
    (PAGE_IRRIGATION, "Irrigacao."),
    (PAGE_RAINFALL, "Pluviometria."),
    (PAGE_PESTS, "Pragas e doencas."),
    (PAGE_SOIL, "Analise de solo."),
    (PAGE_AGRONOMIC, "Perfil agronomico."),
    (PAGE_MAP, "Mapa da fazenda."),
    (PAGE_MOBILE, "Campo mobile."),
    (USERS_MANAGE, "Gerenciar usuarios e permissoes da organizacao."),
    (BACKUPS_MANAGE, "Acessar backups (organizacao padrao SiSFarm)."),
)


def all_permission_codes() -> frozenset[str]:
    return frozenset(code for code, _ in PERMISSION_DEFINITIONS)


def operational_permission_codes() -> frozenset[str]:
    """Todas as permissoes de modulo operacional (exceto usuarios e backups)."""
    return PAGE_PERMISSION_CODES
