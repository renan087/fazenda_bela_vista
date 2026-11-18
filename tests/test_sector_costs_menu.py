from pathlib import Path

from app.core.path_access import required_permission_for_web_path
from app.core.permissions_catalog import PAGE_FINANCE


def test_sector_costs_is_in_finance_menu_with_finance_permission():
    template = (Path(__file__).resolve().parents[1] / "app/templates/base.html").read_text()
    productive = template.split('id="productive-unit-menu"', 1)[1].split('data-submenu-panel>', 1)[1].split('</div>', 1)[0]
    finance = template.split('id="finance-menu"', 1)[1].split('data-submenu-panel>', 1)[1].split('</div>', 1)[0]
    assert '/custos-por-setor' not in productive
    assert '/custos-por-setor' in finance
    assert "'my_subscription', 'sector_costs'" in template
    assert required_permission_for_web_path('/custos-por-setor') == PAGE_FINANCE
