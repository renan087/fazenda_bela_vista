from pathlib import Path
from types import SimpleNamespace

from app.web.routes import _commercialization_available_sacks


def test_lot_balance_uses_only_sales_of_that_harvest_and_allows_edit_exclusion():
    harvest = SimpleNamespace(id=1, sacks_produced=100)
    sales = [SimpleNamespace(id=1, harvest_id=1, equivalent_sacks=15),
             SimpleNamespace(id=2, harvest_id=1, equivalent_sacks=10),
             SimpleNamespace(id=3, harvest_id=2, equivalent_sacks=50)]
    assert _commercialization_available_sacks(harvest, sales) == 75
    assert _commercialization_available_sacks(harvest, sales, exclude_id=1) == 90


def test_lot_balance_does_not_claim_negative_stock():
    harvest = SimpleNamespace(id=1, sacks_produced=10)
    sales = [SimpleNamespace(id=1, harvest_id=1, equivalent_sacks=15)]
    assert _commercialization_available_sacks(harvest, sales) == 0


def test_sales_page_groups_sales_and_shows_progress():
    root = Path(__file__).resolve().parents[1]
    template = (root / "app/templates/commercialization.html").read_text()
    assert "production_tabs('sales')" in template
    assert "/producao/vendas/nova" in template and "Disponível para vender" in template
    assert "Registrar entrega" in template and "Registrar recebimento" in template
    assert 'aria-label="Andamento da venda"' in template
    assert "sf-page pd-page sl-page" in template
