"""Mapeamento prefixo de URL -> permissao necessaria (middleware web). Mais especifico primeiro."""

from __future__ import annotations

from app.core.permissions_catalog import (
    BACKUPS_MANAGE,
    DATA_DELETE,
    PAGE_AGENDA,
    PAGE_AGRONOMIC,
    PAGE_ASSETS,
    PAGE_DASHBOARD,
    PAGE_FINANCE,
    PAGE_INPUTS,
    PAGE_IRRIGATION,
    PAGE_MAP,
    PAGE_MOBILE,
    PAGE_OPERATIONS,
    PAGE_PESTS,
    PAGE_PRODUCTION,
    PAGE_PRODUCTIVE_UNIT,
    PAGE_RAINFALL,
    PAGE_SOIL,
    PAGE_VARIETIES,
    USERS_MANAGE,
    write_permission_for_page,
)

# Ordem: primeiro match vence (prefixos mais longos antes).
WEB_PATH_PERMISSION_PREFIXES: tuple[tuple[str, str], ...] = (
    ("/gestao-financeira", PAGE_FINANCE),
    ("/fertilizacao/agendamentos", PAGE_AGENDA),
    ("/fertilizacao", PAGE_OPERATIONS),
    ("/insumos/recomendacao", PAGE_OPERATIONS),
    ("/insumos/patrimonio", PAGE_ASSETS),
    ("/insumos", PAGE_INPUTS),
    ("/producao/comercializacao", PAGE_PRODUCTION),
    ("/producao", PAGE_PRODUCTION),
    ("/fazendas", PAGE_PRODUCTIVE_UNIT),
    ("/safras", PAGE_PRODUCTIVE_UNIT),
    ("/setores", PAGE_PRODUCTIVE_UNIT),
    ("/talhoes", PAGE_PRODUCTIVE_UNIT),
    ("/variedades", PAGE_VARIETIES),
    ("/irrigacao", PAGE_IRRIGATION),
    ("/pluviometria", PAGE_RAINFALL),
    ("/pragas", PAGE_PESTS),
    ("/analise-solo", PAGE_SOIL),
    ("/perfil-agronomico", PAGE_AGRONOMIC),
    ("/mapa", PAGE_MAP),
    ("/mobile", PAGE_MOBILE),
    ("/usuarios", USERS_MANAGE),
    ("/backups", BACKUPS_MANAGE),
    ("/dashboard", PAGE_DASHBOARD),
    ("/meu-perfil", PAGE_DASHBOARD),
    ("/contexto", PAGE_DASHBOARD),
)

WEB_PATH_PUBLIC_PREFIXES: frozenset[str] = frozenset(
    {
        "/static/",
        "/health",
        "/login",
        "/logout",
        "/auth/",
        "/api/",
        "/docs",
        "/redoc",
        "/openapi",
        "/favicon",
    }
)


DELETE_PATH_MARKERS: tuple[str, ...] = (
    "/excluir",
    "/remover",
    "/delete",
    "/deletar",
)

WRITE_EXEMPT_PREFIXES: frozenset[str] = frozenset(
    {
        "/contexto",
        "/meu-perfil",
    }
)

MANAGE_ONLY_PERMISSIONS: frozenset[str] = frozenset({USERS_MANAGE, BACKUPS_MANAGE})


def is_delete_web_path(path: str) -> bool:
    normalized = (path or "").split("?")[0].rstrip("/") or "/"
    return any(marker in normalized for marker in DELETE_PATH_MARKERS)


def required_permissions_for_web_path(path: str, method: str = "GET") -> tuple[str, ...]:
    if not path:
        return ()
    path = path.split("?")[0]
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/") or "/"
    for prefix, code in WEB_PATH_PERMISSION_PREFIXES:
        if path == prefix or path.startswith(prefix + "/"):
            required = [code]
            normalized_method = (method or "GET").upper()
            if normalized_method in {"GET", "HEAD", "OPTIONS"} or code in MANAGE_ONLY_PERMISSIONS:
                return tuple(required)
            if any(path == exempt or path.startswith(exempt + "/") for exempt in WRITE_EXEMPT_PREFIXES):
                return tuple(required)
            write_code = write_permission_for_page(code)
            if write_code:
                required.append(write_code)
            if is_delete_web_path(path):
                required.append(DATA_DELETE)
            return tuple(required)
    return ()


def required_permission_for_web_path(path: str) -> str | None:
    required = required_permissions_for_web_path(path)
    return required[0] if required else None


def is_public_web_prefix(path: str) -> bool:
    if not path:
        return True
    if path in ("/",):
        return True
    for p in WEB_PATH_PUBLIC_PREFIXES:
        if path.startswith(p):
            return True
    return False
