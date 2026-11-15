from datetime import date
from types import SimpleNamespace as NS

from app.services.season_costs import build_cost_tree, classify_entries, cost_group_summary


def _entry(key, item, category="", text="", kind="fin", item_type="", supplier="", amount=100.0):
    return {"key": key, "kind": kind, "item": item, "base_category": category, "text": text or item.lower(),
            "item_type": item_type, "supplier": supplier, "learn_key": item.lower(), "amount": amount}


def test_classifier_uses_category_keywords_overrides_and_supplier_history():
    entries = [
        _entry("fin:1", "Ureia 46%", "Fertilizantes", "ureia 46%"),
        _entry("fin:2", "Diárias colheita junho", "Serviços Terceirizados", "diarias colheita junho"),
        _entry("fin:3", "Diesel S10", "Outros Itens", "diesel s10"),
        _entry("fin:4", "Compra diversa", "Despesas Diversas", "compra diversa"),
        _entry("stock:5", "Glifosato", "Herbicida", kind="stock"),
        _entry("stock:6", "Diesel", "Combustíveis", kind="stock", item_type="combustivel"),
        _entry("fin:7", "Item A", "Outros Itens", "item a", supplier="Oficina São José"),
        _entry("fin:8", "Revisão trator", "Manutenção de Máquinas e Equipamentos", "revisao trator", supplier="Oficina São José"),
        _entry("fin:9", "Troca de óleo", "Manutenção de Máquinas e Equipamentos", "troca", supplier="Oficina São José"),
        _entry("fin:10", "Calcário", "Outros Itens", "calcario"),
    ]
    overrides = [NS(entry_key="fin:10", learn_key="calcário", group_code="insumos", subgroup_code="corretivos")]
    classify_entries(entries, overrides)
    got = {e["key"]: (e["group"], e["sub"], e["confidence"]) for e in entries}
    assert got["fin:1"] == ("insumos", "fertilizantes", 97)
    assert got["fin:2"][:2] == ("mao_obra", "colheita")
    assert got["fin:3"][:2] == ("maquinas", "combustivel")
    assert got["fin:4"][:2] == ("outros", "nao_identificado") and got["fin:4"][2] < 70
    assert got["stock:5"][:2] == ("insumos", "defensivos")
    assert got["stock:6"][:2] == ("maquinas", "combustivel")
    assert got["fin:7"][:2] == ("maquinas", "manutencao")  # aprendido pelo fornecedor
    assert got["fin:10"] == ("insumos", "corretivos", 100)


def test_learned_correction_applies_to_similar_entries():
    entries = [_entry("fin:20", "Calcário", "Outros Itens", "xyz")]
    classify_entries(entries, [NS(entry_key="fin:99", learn_key="calcário", group_code="insumos", subgroup_code="corretivos")])
    assert entries[0]["sub"] == "corretivos" and entries[0]["confidence"] == 92


class _Repo:
    db = None

    def __init__(self, outputs, txs, seasons):
        self._o, self._t, self._s = outputs, txs, seasons

    def list_stock_outputs(self, farm_id=None):
        return self._o

    def list_finance_transactions(self, farm_id=None):
        return self._t

    def list_crop_seasons(self, farm_id=None):
        return self._s


def test_tree_totals_match_entries_and_previous_season():
    plot = NS(id=1, name="Setor 1", area_hectares=10, estimated_yield_sacks=200)
    cur = NS(id=2, name="Safra 26", start_date=date(2025, 7, 1), end_date=date(2026, 6, 30), plots=[plot])
    prev = NS(id=1, name="Safra 25", start_date=date(2024, 7, 1), end_date=date(2025, 6, 30), plots=[plot])
    cat = NS(name="Ureia", category="Fertilizantes", item_type="insumo_agricola")
    out = lambda i, s, d, v, q: NS(id=i, season_id=s, movement_date=d, origin="Fertilização", plot_id=1, total_cost=v,
                                    input_catalog=cat, purchased_input=None, quantity=q, unit="kg", notes=None, input_id=7)
    tx = NS(id=3, operation_type="despesa", source="contas", season_id=2, launch_date=date(2025, 9, 1), plot_id=1,
            is_reversed=False, product_service="Diárias de capina", description=None, counterparty_name="", document_number="",
            payment_method="PIX", category="Serviços Terceirizados", notes=None, amount=500)
    repo = _Repo([out(1, 2, date(2025, 8, 1), 1200, 100), out(2, 1, date(2024, 8, 1), 1000, 100)], [tx], [cur, prev])
    data = build_cost_tree(repo, farm_id=1, season=cur, plots=[plot], today=date(2026, 1, 10))
    root = data["tree"]
    assert data["total"] == 1700 and root["value"] == 1700
    names = [c["name"] for c in root["children"]]
    assert names == ["Insumos", "Mão de obra"]
    ins = root["children"][0]
    assert ins["prev"] == 1000
    item = ins["children"][0]["children"][0]
    assert item["entries"][0]["link"].startswith("/insumos/estoque/saidas/")
    assert any("mais caro" in text for _, text in item["insights"])
    assert [g["pct"] for g in cost_group_summary(data)] == [71, 29]
