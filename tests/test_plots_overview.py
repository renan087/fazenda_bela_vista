import json
from datetime import date, timedelta
from types import SimpleNamespace

from fastapi import BackgroundTasks

from app.models.coffee_variety import CoffeeVariety
from app.models.crop_season import CropSeason
from app.models.farm import Farm
from app.models.fertilization_schedule import FertilizationSchedule
from app.models.plot import Plot
from app.services.plots_overview import build_plots_overview_context
from tests.test_coffee_sales import env  # noqa: F401
from tests.test_plot_wizard import INSIDE, plot_env  # noqa: F401


def test_build_plots_overview_context_aggregates_kpis_attention_and_varieties(plot_env):
    db = plot_env.env.db
    farm = plot_env.farm
    farm.total_area = 100.0
    season = db.query(CropSeason).get(plot_env.env.scope["active_season_id"])

    v1 = CoffeeVariety(organization_id=farm.organization_id, name="Catuaí Vermelho", species="Arábica", maturation_cycle="Médio")
    v2 = CoffeeVariety(organization_id=farm.organization_id, name="Arara", species="Arábica", maturation_cycle="Tardio")
    db.add_all([v1, v2])
    db.flush()

    p1 = Plot(
        farm_id=farm.id,
        variety_id=v1.id,
        name="Talhão da Represa",
        area_hectares=30.0,
        plant_count=120000,
        planting_date=date.today() - timedelta(days=365 * 5),
        estimated_yield_sacks=1200.0,
        irrigation_type="gotejo",
        irrigation_line_count=80,
        boundary_geojson=json.dumps(INSIDE),
        map_color="#38bdf8",
        group_name="Represa",
        soil_texture="argiloso",
        field_reference="Depois da represa",
    )
    p2 = Plot(
        farm_id=farm.id,
        variety_id=v2.id,
        name="Talhão da Grota",
        area_hectares=10.0,
        plant_count=40000,
        planting_date=date.today() - timedelta(days=365 * 4),
        estimated_yield_sacks=0,
        irrigation_type="none",
        boundary_geojson=None,
        group_name="Grota",
    )
    p3 = Plot(
        farm_id=farm.id,
        variety_id=v2.id,
        name="Novo Horizonte",
        area_hectares=10.0,
        plant_count=42000,
        planting_date=date.today() - timedelta(days=200),
        estimated_yield_sacks=0,
        irrigation_type="aspersor",
        boundary_geojson=json.dumps(INSIDE),
        group_name="Chapada",
    )
    db.add_all([p1, p2, p3])
    db.flush()
    season.plots.append(p1)
    db.add(
        FertilizationSchedule(
            plot_id=p1.id,
            season_id=season.id,
            scheduled_date=date.today() - timedelta(days=2),
            status="scheduled",
            application_method="fertirrigacao",
        )
    )
    db.commit()

    overview = build_plots_overview_context(
        db,
        plots=[p1, p2, p3],
        scoped_farms=[farm],
        active_season=season,
        today=date.today(),
    )
    s = overview["summary"]
    assert s["count"] == 3
    assert s["geo_count"] == 2
    assert s["missing_geo"] == 1
    assert s["in_season_count"] == 1
    assert s["total_area"] == 50.0
    assert s["occupation_pct"] == 50
    assert s["free_area"] == 50.0
    assert s["total_plants"] == 202000
    assert s["irrigated_count"] == 2
    assert s["irrigated_area"] == 40.0
    assert s["total_sacks"] == 1200.0
    assert s["prod_count"] == 2
    assert s["form_count"] == 1

    # Variedades ordenadas por área (Catuaí 30 ha = 60%, Arara 20 ha = 40%)
    assert [v["name"] for v in overview["variety_breakdown"]] == ["Catuaí Vermelho", "Arara"]
    assert overview["variety_breakdown"][0]["pct_int"] == 60
    assert overview["variety_breakdown"][1]["pct_int"] == 40

    # Pendências: p1 tem adubação pendente; p2 sem mapa, fora da safra e sem estimativa; p3 em formação não gera pendência
    att_by_name = {a["name"]: a["reasons"] for a in overview["attention_items"]}
    assert "Talhão da Represa" in att_by_name and any("adubação" in r for r in att_by_name["Talhão da Represa"])
    assert "Talhão da Grota" in att_by_name and "sem perímetro no mapa" in att_by_name["Talhão da Grota"]
    assert "Novo Horizonte" not in att_by_name


def test_plots_page_renders_redesign_and_modern_delete_modal(plot_env):
    env = plot_env.env
    db = env.db
    farm = plot_env.farm
    plot = Plot(
        farm_id=farm.id,
        name="Chapada Alta",
        area_hectares=22.5,
        plant_count=90000,
        planting_date=date(2020, 3, 1),
        estimated_yield_sacks=900,
        irrigation_type="gotejo",
        boundary_geojson=json.dumps(INSIDE),
        map_color="#a3e635",
        group_name="Chapada",
        soil_texture="argiloso",
        field_reference="Platô superior",
    )
    db.add(plot)
    db.commit()

    resp = env.routes.plots_page(
        env.request("/setores"),
        BackgroundTasks(),
        q=None,
        sort="name",
        edit_id=None,
        plot_map=None,
        selected_farm_id=None,
        db=db,
        user=env.user,
        csrf_token="tok-123",
    )
    assert resp.status_code == 200
    html = resp.body.decode("utf-8")

    # Elementos centrais do redesign aprovado
    for expected in (
        "data-pl-page",
        "Área em setores",
        "Lavoura &amp; Irrigação",
        "Estimativa da safra",
        "Composição das lavouras por variedade",
        "data-pl-view-mode=\"cards\"",
        "data-pl-view-mode=\"table\"",
        "data-pl-view-mode=\"groups\"",
        "Chapada Alta",
        "Grupo Chapada",
        "Platô superior",
        "id=\"pl-drawer-backdrop\"",
        "id=\"pl-delete-modal\"",
        "Exclusão permanente",
        "O que será removido com o setor",
        "Vai apenas trocar a lavoura?",
        "pl-del-confirm-check",
        "plots_page.js",
    ):
        assert expected in html, f"Esperado encontrar {expected!r} na página /setores"

    # Preservação do wizard sem alterações
    assert "data-plot-wizard" in html
    assert "id=\"plots-map-preview-modal\"" in html
    assert "confirm('Excluir o setor" not in html
