from pathlib import Path

from app.core.path_access import required_permission_for_web_path
from app.core.permissions_catalog import PAGE_FINANCE

SIDEBAR = Path(__file__).resolve().parents[1] / "app/templates/partials/sidebar.html"


def _group(template: str, group_id: str) -> str:
    return template.split(f"{{'id': '{group_id}'", 1)[1].split("]},", 1)[0]


def test_sector_costs_is_in_finance_menu_with_finance_permission():
    template = SIDEBAR.read_text()
    assert "/custos-por-setor" not in _group(template, "propriedade")
    finance = _group(template, "financeiro")
    assert "'raw': '/custos-por-setor', 'page': 'sector_costs', 'show': mv.get('finance')" in finance
    assert required_permission_for_web_path("/custos-por-setor") == PAGE_FINANCE


def test_sidebar_is_included_in_base_layout():
    base = (Path(__file__).resolve().parents[1] / "app/templates/base.html").read_text()
    assert '{% include "partials/sidebar.html" %}' in base
    assert "js/sidebar.js" in base
