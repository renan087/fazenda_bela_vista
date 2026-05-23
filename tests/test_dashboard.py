"""Utilitários do painel da fazenda."""

import pytest

from app.services.dashboard import _compact_currency_label, _float

pytestmark = pytest.mark.suite_dashboard


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (1234.56, 1234.56),
        (None, 0.0),
        ("", 0.0),
    ],
)
def test_float_normalizes_values(raw: object, expected: float) -> None:
    assert _float(raw) == expected


@pytest.mark.parametrize(
    ("value", "label"),
    [
        (1_500_000, "1M"),
        (25_000, "25k"),
        (500, "500"),
        (-2_000_000, "-2M"),
    ],
)
def test_compact_currency_label(value: float, label: str) -> None:
    assert _compact_currency_label(value) == label
