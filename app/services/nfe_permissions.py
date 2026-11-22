"""Matriz de permissões fiscais granulares para o emissor próprio de NF-e.

Separa preparação de rascunho, gestão de perfis fiscais, gestão de certificado A1,
transmissão à SEFAZ e eventos fiscais (cancelamento/CC-e).
"""
FISCAL_PERMISSIONS = (
    'nfe.draft.prepare',
    'nfe.profile.manage',
    'nfe.certificate.manage',
    'nfe.transmit',
    'nfe.event.cancel',
)

# Mapeamento padrão por papel organizacional quando não há permissão explícita atribuída.
_ROLE_DEFAULTS = {
    'owner': frozenset(FISCAL_PERMISSIONS),
    'admin': frozenset(FISCAL_PERMISSIONS),
    'manager': frozenset({'nfe.draft.prepare', 'nfe.profile.manage', 'nfe.transmit'}),
    'operator': frozenset({'nfe.draft.prepare'}),
    'viewer': frozenset(),
}


def _is_super_admin(user) -> bool:
    email = getattr(user, 'email', None)
    if not isinstance(email, str) or not email:
        return False
    from app.core.admin_access import is_super_admin_email
    return is_super_admin_email(email)


def allowed_fiscal_permissions(user) -> frozenset[str]:
    """Retorna o conjunto de permissões fiscais efetivas para o usuário."""
    if not user:
        return frozenset()
    if getattr(user, 'is_superuser', False) or _is_super_admin(user):
        return frozenset(FISCAL_PERMISSIONS)

    explicit = getattr(user, 'fiscal_permissions', None)
    if isinstance(explicit, (set, frozenset, list, tuple)):
        return frozenset(p for p in explicit if p in FISCAL_PERMISSIONS)

    role_name = str(getattr(user, 'role_name', '') or getattr(user, 'role', '') or '').strip().lower()
    if role_name in _ROLE_DEFAULTS:
        return _ROLE_DEFAULTS[role_name]
    # No SiSFarm o administrador da organização é marcado por User.is_admin.
    if getattr(user, 'is_org_admin', False) or getattr(user, 'is_admin', False) is True:
        return _ROLE_DEFAULTS['admin']
    return frozenset()


def has_fiscal_permission(user, permission: str) -> bool:
    """Verifica se o usuário possui determinada permissão fiscal granular."""
    if permission not in FISCAL_PERMISSIONS:
        raise ValueError(f'Permissão fiscal desconhecida: {permission!r}.')
    return permission in allowed_fiscal_permissions(user)


def require_fiscal_permission(user, permission: str) -> None:
    """Bloqueia a ação caso o usuário não tenha a permissão fiscal exigida."""
    if not has_fiscal_permission(user, permission):
        raise PermissionError(f'Usuário sem permissão fiscal para esta ação ({permission}).')
