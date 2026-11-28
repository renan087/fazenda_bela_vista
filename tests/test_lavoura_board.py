from datetime import date
from types import SimpleNamespace as NS

from app.models.plot import Plot
from app.services.lavoura_board import build_lavoura_board
from tests.test_coffee_sales import env  # noqa: F401
from tests.test_plot_wizard import plot_env  # noqa: F401

TODAY = date(2026, 11, 28)


def _plot(pid, name, area=2.0, irrigation="gotejo"):
    return NS(id=pid, name=name, area_hectares=area, variety=NS(name="Catuaí"), irrigation_type=irrigation)


def test_board_status_por_pilar_acao_e_ordem():
    p1, p2, p3 = _plot(1, "Baixada"), _plot(2, "Morro"), _plot(3, "Sede")
    board = build_lavoura_board(
        plots=[p3, p2, p1],
        today=TODAY,
        fertilizations=[NS(id=1, plot_id=3, application_date=date(2026, 11, 16), recipe_name="NPK 20-05-20", product=None)],
        active_schedules=[NS(plot_id=1, scheduled_date=date(2026, 11, 23), recipe_name="Cobertura N")],
        operations=[NS(id=1, plot_id=2, operation_date=date(2026, 10, 2), operation_type="Pulverização preventiva")],
        active_incidents=[NS(plot_id=2, occurrence_date=date(2026, 11, 25), name="Ferrugem", severity=4)],
        irrigations=[NS(id=1, plot_id=2, irrigation_date=date(2026, 11, 20), volume_liters=300000)],
        soil_analyses=[NS(id=1, plot_id=3, analysis_date=date(2026, 8, 1), base_saturation=62)],
        water_rows=[
            {"plot_id": 1, "status": "ok", "status_label": "Há 2d", "is_irrigated": True},
            {"plot_id": 2, "status": "warn", "status_label": "6d sem água", "is_irrigated": True, "last_water_br": "22/11/2026"},
            {"plot_id": 3, "status": "ok", "status_label": "Hoje", "is_irrigated": True},
        ],
        soil_rows=[
            {"plot_id": 1, "status": "missing"},
            {"plot_id": 2, "status": "stale", "latest_date_br": "10/07/2023"},
            {"plot_id": 3, "status": "ok", "v_pct": 62.0},
        ],
        cost_by_plot={3: {"cost_per_hectare": 8420.0}},
    )
    rows = {r["name"]: r for r in board["rows"]}
    assert rows["Baixada"]["status"]["nutri"]["tone"] == "err"
    assert rows["Baixada"]["status"]["nutri"]["label"] == "Atrasada 5 dias"
    assert rows["Baixada"]["status"]["solo"]["tone"] == "err"
    assert rows["Morro"]["status"]["san"]["label"] == "Ferrugem nv.4"
    assert rows["Morro"]["need"]["action"] == "Registrar pulverização"
    assert rows["Morro"]["need"]["href"] == "/pragas/pulverizacoes?novo=1&plot_id=2"
    assert rows["Morro"]["status"]["agua"]["tone"] == "warn"
    assert rows["Morro"]["status"]["solo"]["label"] == "Laudo de 2023"
    assert rows["Sede"]["need"] is None and rows["Sede"]["status"]["solo"]["label"] == "V 62%"
    assert rows["Sede"]["status"]["nutri"]["label"] == "Adubado há 12 dias"
    # mais urgentes primeiro; Sede (tudo em dia) por último
    assert board["rows"][-1]["name"] == "Sede"
    # linha do tempo junta módulos, do mais novo para o mais antigo
    assert [e["module"] for e in rows["Morro"]["timeline"]][:2] == ["Pragas", "Clima e Água"]
    assert board["pillars"]["nutri"]["count"] == 1 and board["pillars"]["nutri"]["href"] == "/fertilizacao"
    assert board["pillars"]["san"]["line"].startswith("Morro: ferrugem")
    assert board["attention_count"] == 2


def test_pagina_lavoura_renderiza_resumo_lista_e_painel(plot_env):
    env = plot_env.env
    env.db.add_all([
        Plot(farm_id=plot_env.farm.id, name="Talhão A", area_hectares=3, plant_count=9000),
        Plot(farm_id=plot_env.farm.id, name="Talhão B", area_hectares=2, plant_count=6000),
    ])
    env.db.commit()
    resp = env.routes.lavoura_overview_page(env.request("/lavoura"), db=env.db, user=env.user, csrf_token="x")
    html = resp.body.decode()
    assert resp.status_code == 200
    assert 'class="lv-summary"' in html and html.count("data-lv-seg=") == 4
    assert "Registrar no campo" in html and "data-lv-menu" in html and "Trato cultural" in html
    assert 'data-lv-item="' in html and 'data-lv-panel="' in html
    assert "Precisa agora" in html  # sem análise de solo gera ação
    assert "Abrir Análise de solo" in html
    assert "Radar dos talhões" not in html and "Precisa de atenção agora" not in html
