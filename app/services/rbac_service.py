"""RBAC por organizacao: papéis, permissoes e atribuicao a usuarios."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import delete, insert, select
from sqlalchemy.orm import Session

from app.core.admin_access import is_super_admin_email
from app.core.permissions_catalog import (
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
    PERMISSION_DEFINITIONS,
    all_permission_codes,
    operational_permission_codes,
)
from app.models import Organization, User
from app.models.rbac import Permission, Role, role_permissions_table, user_roles_table

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

ROLE_SLUG_ADMIN = "administrador"
ROLE_SLUG_OPERATOR = "operador"
ROLE_NAME_ADMIN = "Administrador da organizacao"
ROLE_NAME_OPERATOR = "Operador (acesso operacional completo)"

# (slug, nome exibido, conjunto de codigos de permissao)
ROLE_PROFILES: tuple[tuple[str, str, frozenset[str]], ...] = (
    (ROLE_SLUG_ADMIN, ROLE_NAME_ADMIN, all_permission_codes()),
    (
        ROLE_SLUG_OPERATOR,
        ROLE_NAME_OPERATOR,
        operational_permission_codes(),
    ),
    (
        "papel-financeiro",
        "Financeiro",
        frozenset({PAGE_DASHBOARD, PAGE_FINANCE}),
    ),
    (
        "papel-producao",
        "Producao e comercializacao",
        frozenset({PAGE_DASHBOARD, PAGE_PRODUCTION}),
    ),
    (
        "papel-agronomia",
        "Coordenacao agronomica",
        frozenset(
            {
                PAGE_DASHBOARD,
                PAGE_PRODUCTIVE_UNIT,
                PAGE_OPERATIONS,
                PAGE_INPUTS,
                PAGE_AGENDA,
                PAGE_ASSETS,
                PAGE_VARIETIES,
                PAGE_IRRIGATION,
                PAGE_RAINFALL,
                PAGE_PESTS,
                PAGE_SOIL,
                PAGE_AGRONOMIC,
                PAGE_MAP,
                PAGE_MOBILE,
            }
        ),
    ),
    (
        "papel-monitoramento",
        "Monitoramento de campo",
        frozenset(
            {
                PAGE_DASHBOARD,
                PAGE_VARIETIES,
                PAGE_IRRIGATION,
                PAGE_RAINFALL,
                PAGE_PESTS,
                PAGE_SOIL,
                PAGE_AGRONOMIC,
                PAGE_MAP,
                PAGE_MOBILE,
            }
        ),
    ),
)


def seed_permissions(db: Session) -> None:
    for code, description in PERMISSION_DEFINITIONS:
        row = db.query(Permission).filter(Permission.code == code).first()
        if not row:
            db.add(Permission(code=code, description=description))
    db.commit()


def _get_role_by_slug(db: Session, organization_id: int, slug: str) -> Role | None:
    return (
        db.query(Role)
        .filter(Role.organization_id == organization_id, Role.slug == slug)
        .first()
    )


def _sync_role_permissions(db: Session, role: Role, codes: frozenset[str]) -> None:
    rows = db.query(Permission).filter(Permission.code.in_(list(codes))).all()
    wanted_ids = {r.id for r in rows}
    current_ids = set(
        db.scalars(
            select(role_permissions_table.c.permission_id).where(role_permissions_table.c.role_id == role.id)
        ).all()
    )
    to_drop = current_ids - wanted_ids
    to_add = wanted_ids - current_ids
    for pid in to_drop:
        db.execute(
            delete(role_permissions_table).where(
                role_permissions_table.c.role_id == role.id,
                role_permissions_table.c.permission_id == pid,
            )
        )
    for pid in to_add:
        db.execute(insert(role_permissions_table).values(role_id=role.id, permission_id=pid))


def ensure_organization_roles(db: Session, organization_id: int) -> None:
    """Cria/atualiza papéis de sistema e permissoes por organizacao."""
    seed_permissions(db)
    for slug, name, codes in ROLE_PROFILES:
        role = _get_role_by_slug(db, organization_id, slug)
        if not role:
            role = Role(
                organization_id=organization_id,
                name=name,
                slug=slug,
                is_system=True,
            )
            db.add(role)
            db.flush()
        else:
            role.name = name
        _sync_role_permissions(db, role, codes)
    db.commit()


def _clear_user_roles_in_organization(db: Session, user_id: int, organization_id: int) -> None:
    role_ids = db.scalars(select(Role.id).where(Role.organization_id == organization_id)).all()
    if not role_ids:
        return
    db.execute(
        delete(user_roles_table).where(
            user_roles_table.c.user_id == user_id,
            user_roles_table.c.role_id.in_(role_ids),
        )
    )


def set_user_roles_for_organization(db: Session, user: User, role_ids: list[int]) -> None:
    """Substitui papéis do usuario na organizacao (valida FK na propria org)."""
    if not user.organization_id:
        return
    ids = role_ids or []
    if not ids:
        wanted = []
    else:
        wanted = sorted(
            set(
                db.scalars(
                    select(Role.id).where(
                        Role.organization_id == user.organization_id,
                        Role.id.in_(ids),
                    )
                ).all()
            )
        )
    _clear_user_roles_in_organization(db, user.id, user.organization_id)
    for rid in wanted:
        db.execute(insert(user_roles_table).values(user_id=user.id, role_id=rid))
    db.commit()


def sync_roles_from_admin_flag(db: Session, user: User) -> None:
    """Um papel: administrador ou operador completo (compatibilidade com flag is_admin)."""
    if not user.organization_id:
        return
    ensure_organization_roles(db, user.organization_id)
    admin_role = _get_role_by_slug(db, user.organization_id, ROLE_SLUG_ADMIN)
    operador_role = _get_role_by_slug(db, user.organization_id, ROLE_SLUG_OPERATOR)
    if not admin_role or not operador_role:
        return
    target_id = admin_role.id if (user.is_admin or is_super_admin_email(user.email)) else operador_role.id
    set_user_roles_for_organization(db, user, [target_id])


def sync_legacy_user_roles(db: Session) -> None:
    users = db.query(User).filter(User.organization_id.isnot(None)).all()
    for user in users:
        has_any = (
            db.query(Role.id)
            .join(user_roles_table, Role.id == user_roles_table.c.role_id)
            .filter(user_roles_table.c.user_id == user.id, Role.organization_id == user.organization_id)
            .first()
        )
        if has_any:
            continue
        sync_roles_from_admin_flag(db, user)


def seed_rbac_for_all_organizations(db: Session) -> None:
    seed_permissions(db)
    for org in db.query(Organization).all():
        ensure_organization_roles(db, org.id)
    sync_legacy_user_roles(db)


def permission_codes_for_user(db: Session, user: User | None) -> frozenset[str]:
    if not user:
        return frozenset()
    if is_super_admin_email(user.email):
        return all_permission_codes()
    if user.is_admin:
        return all_permission_codes()
    if not user.organization_id:
        return frozenset()

    rows = (
        db.query(Permission.code)
        .join(role_permissions_table, Permission.id == role_permissions_table.c.permission_id)
        .join(Role, Role.id == role_permissions_table.c.role_id)
        .join(user_roles_table, user_roles_table.c.role_id == Role.id)
        .filter(
            user_roles_table.c.user_id == user.id,
            Role.organization_id == user.organization_id,
        )
        .distinct()
        .all()
    )
    return frozenset(r[0] for r in rows)


def user_has_permission(db: Session, user: User | None, code: str) -> bool:
    return code in permission_codes_for_user(db, user)


def list_roles_for_organization(db: Session, organization_id: int) -> list[Role]:
    return (
        db.query(Role)
        .filter(Role.organization_id == organization_id)
        .order_by(Role.name.asc())
        .all()
    )


def assignable_roles_for_editing(db: Session, organization_id: int) -> list[Role]:
    """Papéis que podem ser combinados na edicao (exceto administrador — via flag)."""
    return (
        db.query(Role)
        .filter(Role.organization_id == organization_id, Role.slug != ROLE_SLUG_ADMIN)
        .order_by(Role.name.asc())
        .all()
    )


def role_ids_for_user_in_org(db: Session, user: User) -> list[int]:
    if not user.organization_id:
        return []
    q = (
        db.query(Role.id)
        .join(user_roles_table, Role.id == user_roles_table.c.role_id)
        .filter(user_roles_table.c.user_id == user.id, Role.organization_id == user.organization_id)
        .order_by(Role.slug.asc())
        .all()
    )
    return [row[0] for row in q]


def role_labels_for_user(db: Session, user: User) -> list[str]:
    if not user.organization_id:
        return []
    roles = (
        db.query(Role.name)
        .join(user_roles_table, Role.id == user_roles_table.c.role_id)
        .filter(user_roles_table.c.user_id == user.id, Role.organization_id == user.organization_id)
        .order_by(Role.name.asc())
        .all()
    )
    return [r[0] for r in roles]
