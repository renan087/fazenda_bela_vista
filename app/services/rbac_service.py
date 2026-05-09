"""RBAC por organizacao: seed, consulta e sincronizacao com o flag legado `User.is_admin`."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import delete, insert, select
from sqlalchemy.orm import Session

from app.core.admin_access import is_super_admin_email
from app.core.permissions_catalog import PERMISSION_DEFINITIONS, all_permission_codes
from app.models import Organization, User
from app.models.rbac import Permission, Role, role_permissions_table, user_roles_table

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

ROLE_SLUG_ADMIN = "administrador"
ROLE_SLUG_OPERATOR = "operador"
ROLE_NAME_ADMIN = "Administrador da organizacao"
ROLE_NAME_OPERATOR = "Operador"


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


def ensure_organization_roles(db: Session, organization_id: int) -> tuple[Role, Role]:
    """Garante papéis sistema e permissoes; retorna (administrador, operador)."""
    seed_permissions(db)
    admin_role = _get_role_by_slug(db, organization_id, ROLE_SLUG_ADMIN)
    op_role = _get_role_by_slug(db, organization_id, ROLE_SLUG_OPERATOR)
    all_perm_rows = db.query(Permission).all()

    if not admin_role:
        admin_role = Role(
            organization_id=organization_id,
            name=ROLE_NAME_ADMIN,
            slug=ROLE_SLUG_ADMIN,
            is_system=True,
        )
        db.add(admin_role)
        db.flush()

    if not op_role:
        op_role = Role(
            organization_id=organization_id,
            name=ROLE_NAME_OPERATOR,
            slug=ROLE_SLUG_OPERATOR,
            is_system=True,
        )
        db.add(op_role)
        db.flush()

    existing_admin_perm_ids = set(
        db.scalars(
            select(role_permissions_table.c.permission_id).where(role_permissions_table.c.role_id == admin_role.id)
        ).all()
    )
    for perm in all_perm_rows:
        if perm.id not in existing_admin_perm_ids:
            db.execute(insert(role_permissions_table).values(role_id=admin_role.id, permission_id=perm.id))
    db.commit()

    db.refresh(admin_role)
    db.refresh(op_role)
    return admin_role, op_role


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


def sync_roles_from_admin_flag(db: Session, user: User) -> None:
    """Alinha `user_roles` ao flag `is_admin` / super admin (um papel por vez nesta versao)."""
    if not user.organization_id:
        return
    admin_role, operador_role = ensure_organization_roles(db, user.organization_id)
    _clear_user_roles_in_organization(db, user.id, user.organization_id)
    target = admin_role if (user.is_admin or is_super_admin_email(user.email)) else operador_role
    db.execute(insert(user_roles_table).values(user_id=user.id, role_id=target.id))
    db.commit()


def sync_legacy_user_roles(db: Session) -> None:
    """Primeira carga: usuarios sem papel na organizacao recebem um padrao a partir de `is_admin`."""
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
