"""Middleware web — permissões por rota."""

import pytest

from app.core.path_access import (
    is_delete_web_path,
    is_public_web_prefix,
    required_permissions_for_web_path,
)
from app.core.permissions_catalog import PAGE_DASHBOARD, PAGE_FINANCE, WRITE_FINANCE

pytestmark = pytest.mark.suite_path_access


def test_public_prefixes_include_health_and_static() -> None:
    assert is_public_web_prefix("/health")
    assert is_public_web_prefix("/static/css/styles.css")
    assert is_public_web_prefix("/login")
    assert not is_public_web_prefix("/dashboard")


def test_finance_path_requires_finance_permission() -> None:
    assert required_permissions_for_web_path("/gestao-financeira/contas") == (PAGE_FINANCE,)


def test_finance_post_requires_write_permission() -> None:
    perms = required_permissions_for_web_path("/gestao-financeira/contas", method="POST")
    assert PAGE_FINANCE in perms
    assert WRITE_FINANCE in perms


def test_delete_path_detection() -> None:
    assert is_delete_web_path("/fazendas/1/excluir")
    assert not is_delete_web_path("/fazendas")


def test_dashboard_requires_dashboard_permission() -> None:
    assert required_permissions_for_web_path("/dashboard") == (PAGE_DASHBOARD,)
