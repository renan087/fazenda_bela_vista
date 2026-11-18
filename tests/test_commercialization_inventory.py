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


def test_inventory_explains_estimate_without_implying_physical_classification():
    template = (Path(__file__).resolve().parents[1] / "app/templates/commercialization.html").read_text()
    assert "saldo é uma estimativa" in template
    assert "não calcula rendimento de beneficiamento" in template
    assert "Não informada" in template
    assert "Negociado acima do registrado" in template


def test_commercialization_header_and_receipt_label():
    root = Path(__file__).resolve().parents[1]
    template = (root / "app/templates/commercialization.html").read_text()
    css = (root / "app/static/css/styles.css").read_text()
    assert "stock-page-shell sf-page commercialization-page" in template
    assert "body:has(.commercialization-page) .hd { background: #fff; }" in css
    assert "body .commercialization-page .stock-page-header { top: 69px; background: #f1f5f9; }" in css
    assert template.count("Forma de recebimento") == 2
    assert "Forma de pagamento" not in template
