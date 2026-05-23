"""Catálogo e utilitários de testes automatizados expostos no painel de qualidade."""

from app.testing.catalog import AUTOMATED_TEST_SUITES, AutomatedTestSuite, get_suite, list_suites

__all__ = [
    "AUTOMATED_TEST_SUITES",
    "AutomatedTestSuite",
    "get_suite",
    "list_suites",
]
