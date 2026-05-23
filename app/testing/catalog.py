"""Registro das suites de teste — espelha cada implemento verificável no painel."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AutomatedTestSuite:
    id: str
    name: str
    domain: str
    description: str
    pytest_paths: tuple[str, ...]
    marker: str | None = None


AUTOMATED_TEST_SUITES: tuple[AutomatedTestSuite, ...] = (
    AutomatedTestSuite(
        id="coffee_market",
        name="Cotação do café (CEPEA)",
        domain="Mercado / Dashboard",
        description="Parse CEPEA, variação mensal vs fechamento do mês anterior, série remota e prontidão do painel.",
        pytest_paths=("tests/test_coffee_quotes.py",),
        marker="suite_coffee_market",
    ),
    AutomatedTestSuite(
        id="rbac",
        name="RBAC e permissões",
        domain="Segurança / Acesso",
        description="Catálogo de permissões, papéis operacionais e vínculo página → permissão de escrita.",
        pytest_paths=("tests/test_rbac.py",),
        marker="suite_rbac",
    ),
    AutomatedTestSuite(
        id="security",
        name="Autenticação e credenciais",
        domain="Segurança / Acesso",
        description="Hash de senha, verificação de login e tokens JWT.",
        pytest_paths=("tests/test_security.py",),
        marker="suite_security",
    ),
    AutomatedTestSuite(
        id="path_access",
        name="Rotas web e middleware",
        domain="Segurança / Acesso",
        description="Prefixos públicos, permissões por URL e detecção de exclusão/edição.",
        pytest_paths=("tests/test_path_access.py",),
        marker="suite_path_access",
    ),
    AutomatedTestSuite(
        id="dashboard",
        name="Dashboard e formatação",
        domain="Dashboard / KPIs",
        description="Utilitários numéricos e rótulos compactos usados nos cards do painel.",
        pytest_paths=("tests/test_dashboard.py",),
        marker="suite_dashboard",
    ),
    AutomatedTestSuite(
        id="quality_meta",
        name="Catálogo de qualidade",
        domain="Plataforma / QA",
        description="Garante que cada implemento registrado possui arquivo de teste e marcador pytest.",
        pytest_paths=("tests/test_quality_catalog.py",),
        marker="suite_quality_meta",
    ),
)

_SUITES_BY_ID = {suite.id: suite for suite in AUTOMATED_TEST_SUITES}


def list_suites() -> tuple[AutomatedTestSuite, ...]:
    return AUTOMATED_TEST_SUITES


def get_suite(suite_id: str) -> AutomatedTestSuite | None:
    return _SUITES_BY_ID.get(suite_id)
