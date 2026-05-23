"""Fixtures compartilhadas — testes unitários não exigem banco nem rede por padrão."""

from __future__ import annotations

import pytest


@pytest.fixture
def sample_cepea_html() -> str:
    return """
<html><body>
INDICADOR DO CAFÉ ARÁBICA CEPEA/ESALQ
15/05/2026 1.637,88 -0,55% -7,02% 7,12
14/05/2026 1.646,95 -0,12% -6,80% 7,16
INDICADOR DO CAFÉ ROBUSTA CEPEA/ESALQ
15/05/2026 930,15 -2,40% 0,53% 4,04
14/05/2026 953,00 -1,10% 0,20% 4,14
</body></html>
"""
