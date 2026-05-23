"""Metadados — catálogo alinhado aos arquivos de teste."""

from pathlib import Path

import pytest

from app.testing.catalog import AUTOMATED_TEST_SUITES

pytestmark = pytest.mark.suite_quality_meta

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_every_suite_has_test_files() -> None:
    for suite in AUTOMATED_TEST_SUITES:
        for relative in suite.pytest_paths:
            path = PROJECT_ROOT / relative
            assert path.is_file(), f"Arquivo ausente para {suite.id}: {relative}"


def test_every_suite_has_marker() -> None:
    for suite in AUTOMATED_TEST_SUITES:
        assert suite.marker, f"Suite {suite.id} sem marcador pytest"


def test_suite_ids_are_unique() -> None:
    ids = [suite.id for suite in AUTOMATED_TEST_SUITES]
    assert len(ids) == len(set(ids))
