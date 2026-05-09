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

# Alteracoes por pagina/modulo
WRITE_PRODUCTIVE_UNIT = "write.productive_unit"
WRITE_OPERATIONS = "write.operations"
WRITE_INPUTS = "write.inputs"
WRITE_AGENDA = "write.agenda"
WRITE_ASSETS = "write.assets"
WRITE_FINANCE = "write.finance"
WRITE_PRODUCTION = "write.production"
WRITE_VARIETIES = "write.varieties"
WRITE_IRRIGATION = "write.irrigation"
WRITE_RAINFALL = "write.rainfall"
WRITE_PESTS = "write.pests"
WRITE_SOIL = "write.soil"
WRITE_AGRONOMIC = "write.agronomic"
WRITE_MAP = "write.map"
WRITE_MOBILE = "write.mobile"

# Permissao transversal de alto risco
DATA_DELETE = "data.delete"

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

PAGE_WRITE_PERMISSION_MAP: dict[str, str] = {
    PAGE_PRODUCTIVE_UNIT: WRITE_PRODUCTIVE_UNIT,
    PAGE_OPERATIONS: WRITE_OPERATIONS,
    PAGE_INPUTS: WRITE_INPUTS,
    PAGE_AGENDA: WRITE_AGENDA,
    PAGE_ASSETS: WRITE_ASSETS,
    PAGE_FINANCE: WRITE_FINANCE,
    PAGE_PRODUCTION: WRITE_PRODUCTION,
    PAGE_VARIETIES: WRITE_VARIETIES,
    PAGE_IRRIGATION: WRITE_IRRIGATION,
    PAGE_RAINFALL: WRITE_RAINFALL,
    PAGE_PESTS: WRITE_PESTS,
    PAGE_SOIL: WRITE_SOIL,
    PAGE_AGRONOMIC: WRITE_AGRONOMIC,
    PAGE_MAP: WRITE_MAP,
    PAGE_MOBILE: WRITE_MOBILE,
}

WRITE_PERMISSION_CODES: frozenset[str] = frozenset(PAGE_WRITE_PERMISSION_MAP.values())

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
    (WRITE_PRODUCTIVE_UNIT, "Alterar dados de unidade produtiva: fazendas, safras e setores."),
    (WRITE_OPERATIONS, "Alterar operacoes: fertilizacao e recomendacoes de insumos."),
    (WRITE_INPUTS, "Alterar insumos: compras, estoque e suprimentos."),
    (WRITE_AGENDA, "Alterar agendamentos de fertilizacao."),
    (WRITE_ASSETS, "Alterar patrimonio e equipamentos."),
    (WRITE_FINANCE, "Alterar gestao financeira, contas e assinatura."),
    (WRITE_PRODUCTION, "Alterar producao, colheita e comercializacao."),
    (WRITE_VARIETIES, "Alterar variedades de cafe."),
    (WRITE_IRRIGATION, "Alterar registros de irrigacao."),
    (WRITE_RAINFALL, "Alterar registros de pluviometria."),
    (WRITE_PESTS, "Alterar registros de pragas e doencas."),
    (WRITE_SOIL, "Alterar analises de solo."),
    (WRITE_AGRONOMIC, "Alterar perfil agronomico."),
    (WRITE_MAP, "Alterar dados do mapa da fazenda."),
    (WRITE_MOBILE, "Alterar dados pelo campo mobile."),
    (DATA_DELETE, "Permitir exclusoes de dados em paginas autorizadas."),
    (USERS_MANAGE, "Gerenciar usuarios e permissoes da organizacao."),
    (BACKUPS_MANAGE, "Acessar backups (organizacao padrao SiSFarm)."),
)


def all_permission_codes() -> frozenset[str]:
    return frozenset(code for code, _ in PERMISSION_DEFINITIONS)


def operational_permission_codes() -> frozenset[str]:
    """Todas as permissoes de modulo operacional (exceto usuarios e backups)."""
    return PAGE_PERMISSION_CODES | WRITE_PERMISSION_CODES


def write_permission_for_page(page_permission: str) -> str | None:
    return PAGE_WRITE_PERMISSION_MAP.get(page_permission)
