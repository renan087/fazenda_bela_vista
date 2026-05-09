"""Codigos de permissao estaveis (referenciados no codigo e armazenados no banco)."""

from __future__ import annotations

# Modulos administrativos
USERS_MANAGE = "users.manage"
BACKUPS_MANAGE = "backups.manage"

PERMISSION_DEFINITIONS: tuple[tuple[str, str], ...] = (
    (USERS_MANAGE, "Gerenciar usuarios e permissoes da organizacao."),
    (BACKUPS_MANAGE, "Acessar backups (organizacao padrao SiSFarm)."),
)


def all_permission_codes() -> frozenset[str]:
    return frozenset(code for code, _ in PERMISSION_DEFINITIONS)
