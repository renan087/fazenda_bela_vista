"""RBAC — catálogo de permissões e papéis."""

import pytest

from app.core.permissions_catalog import (
    PAGE_DASHBOARD,
    PAGE_FINANCE,
    PAGE_WRITE_PERMISSION_MAP,
    PERMISSION_DEFINITIONS,
    all_permission_codes,
    write_permission_for_page,
)
from app.services.rbac_service import ROLE_SLUG_ADMIN, ROLE_SLUG_OPERATOR

pytestmark = pytest.mark.suite_rbac


def test_permission_catalog_has_dashboard_and_finance() -> None:
    codes = all_permission_codes()
    assert PAGE_DASHBOARD in codes
    assert PAGE_FINANCE in codes


def test_each_page_has_write_permission_mapping() -> None:
    for page_code, write_code in PAGE_WRITE_PERMISSION_MAP.items():
        assert page_code in all_permission_codes()
        assert write_code in all_permission_codes()
        assert write_permission_for_page(page_code) == write_code


def test_permission_definitions_match_codes() -> None:
    defined = {code for code, _ in PERMISSION_DEFINITIONS}
    assert defined == all_permission_codes()


def test_default_role_slugs_are_stable() -> None:
    assert ROLE_SLUG_ADMIN == "administrador"
    assert ROLE_SLUG_OPERATOR == "operador"
