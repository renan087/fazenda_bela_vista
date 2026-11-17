from datetime import date
from types import SimpleNamespace as N

from app.core.permissions_catalog import PAGE_FINANCE, PAGE_INPUTS, PAGE_RAINFALL
from app.services.assistant_queries import AssistantQueries

TODAY = date(2026, 11, 15)
SEASON = N(id=1, name="Café 2026", start_date=date(2026, 1, 1), end_date=date(2026, 12, 31), culture="Café", status="ativa")
FARM = N(id=1, name="Bela Vista", total_area=100, boundary_geojson=None, plots=[])
PLOTS = [N(id=7, name="Setor 1", area_hectares=5, plant_count=1000, planting_date=None),
         N(id=8, name="Setor 2", area_hectares=3, plant_count=500, planting_date=None)]
ureia = N(name="Ureia", default_unit="kg")


class Repo:
    def list_rainfalls(self, farm_id, start_date, end_date):
        return [N(rainfall_date=date(2026, 11, 10), millimeters=12), N(rainfall_date=date(2026, 11, 12), millimeters=30),
                N(rainfall_date=date(2026, 11, 12), millimeters=5)]

    def list_stock_outputs(self, farm_id):
        return [N(movement_date=date(2026, 11, 14), input_catalog=ureia, quantity=40, unit="kg", plot_id=7, plot=PLOTS[0], total_cost=100),
                N(movement_date=date(2026, 11, 1), input_catalog=ureia, quantity=10, unit="kg", plot_id=8, plot=PLOTS[1], total_cost=25),
                N(movement_date=date(2026, 8, 1), input_catalog=ureia, quantity=99, unit="kg", plot_id=7, plot=PLOTS[0], total_cost=1)]

    def list_finance_transactions(self, farm_id):
        return [N(launch_date=date(2026, 11, 5), amount=1000, operation_type="receita", category="Venda"),
                N(launch_date=date(2026, 11, 6), amount=300, operation_type="despesa", category="Diesel"),
                N(launch_date=date(2025, 1, 6), amount=999, operation_type="despesa", category="Velho")]

    def list_purchased_inputs(self):
        return [N(id=10, farm_id=1, purchase_date=date(2026, 11, 14), input_catalog=ureia, name="Ureia", quantity_purchased=3),
                N(id=11, farm_id=2, purchase_date=date(2026, 11, 14), input_catalog=ureia, name="Ureia", quantity_purchased=9)]


def _queries(allowed=lambda page: True):
    return AssistantQueries(repo=Repo(), farm=FARM, season=SEASON, plots=PLOTS, today=TODAY, allowed=allowed)


def test_rain_totals_by_day():
    data = _queries().consultar_chuva()
    assert data["total_mm"] == 47 and data["dias_com_chuva"] == 2
    assert data["maior_chuva"] == {"data": "12/11/2026", "mm": 35.0}


def test_consumption_filters_by_period_item_and_plot():
    data = _queries().consultar_consumos(item="ure", setor="Setor 1")
    assert data["totais"] == [{"item": "Ureia", "quantidade": 40.0, "unidade": "kg"}]
    data = _queries().consultar_consumos(inicio="2026-01-01", fim=None)
    assert data["totais"][0]["quantidade"] == 149.0


def test_finance_summary():
    data = _queries().consultar_financeiro()
    assert (data["receitas"], data["despesas"], data["saldo"]) == (1000, 300, 700)
    assert data["maiores_despesas_por_categoria"] == {"Diesel": 300}


def test_purchase_query_limits_records_to_active_farm():
    result = _queries().consultar_compras(item="ure")
    assert result["quantidade"] == 1
    assert result["compras"][0]["registro_id"] == 10


def test_purchase_query_requires_input_permission():
    names = {schema["function"]["name"] for schema, _ in _queries(lambda page: page == PAGE_RAINFALL).tools()}
    assert "consultar_compras" not in names


def test_agent_retains_structured_purchase_references():
    import json
    from app.services.stock_assistant import FarmAssistant
    assistant = FarmAssistant(farm_name="Bela Vista", season=SEASON, items=[], balances={}, plots=PLOTS, today=TODAY,
                              prepare_draft=lambda **kw: None, extra_tools=_queries().tools())
    assistant.call_tool("consultar_compras", json.dumps({"inicio": None, "fim": None, "item": "ureia"}))
    assert assistant.record_references == [{"kind": "compra", "id": 10, "label": "Ureia", "date": "14/11/2026"}]


def test_assistant_pagination_targets_only_records_in_scoped_collection():
    from starlette.requests import Request
    from app.web.routes import _paginate_collection
    request = Request({"type": "http", "method": "GET", "path": "/producao", "headers": [],
                       "query_string": b"assistant_kind=colheita&assistant_view=3", "server": ("localhost", 80),
                       "scheme": "http", "root_path": ""})
    scoped = [N(id=1), N(id=2), N(id=3)]
    result = _paginate_collection(request, scoped, "harvests_page", per_page=1)
    assert result["page"] == 3 and result["items"][0].id == 3
    assert _paginate_collection(request, scoped[:2], "harvests_page", per_page=1)["page"] == 1
    assert _paginate_collection(request, scoped, "other_page", per_page=1)["page"] == 1


def test_tools_respect_permissions():
    names = lambda q: {schema["function"]["name"] for schema, _ in q.tools()}
    only_rain = names(_queries(lambda page: page == PAGE_RAINFALL))
    assert only_rain == {"consultar_chuva", "consultar_previsao_tempo"}
    assert "consultar_financeiro" in names(_queries(lambda page: page in {PAGE_FINANCE, PAGE_INPUTS}))


def test_ambiguous_plot_and_bad_date():
    import pytest
    with pytest.raises(ValueError):
        _queries().consultar_consumos(setor="Setor")
    with pytest.raises(ValueError):
        _queries().consultar_chuva(inicio="ontem")


def test_agent_runs_query_tool_and_records_trace():
    from app.services.stock_assistant import FarmAssistant
    import json
    q = _queries()
    assistant = FarmAssistant(farm_name="Bela Vista", season=SEASON, items=[], balances={}, plots=PLOTS, today=TODAY,
                              prepare_draft=lambda **kw: None, extra_tools=q.tools())
    assert any(t["function"]["name"] == "consultar_chuva" for t in assistant.tools)
    result = assistant.call_tool("consultar_chuva", json.dumps({"inicio": None, "fim": None}))
    assert result["total_mm"] == 47 and "Consultou a chuva" in assistant.trace
    assert assistant.call_tool("consultar_colheitas", json.dumps({"setor": None}))["erro"]  # repo sem colheitas: erro tratado
