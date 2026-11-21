import json
from types import SimpleNamespace

import pytest

from app.models.crop_season import CropSeason
from app.models.farm import Farm
from app.models.plot import Plot
from app.services.plot_wizard import (
    PLOT_COLORS,
    geometry_outside_farm,
    normalize_color,
    normalize_group,
    normalize_soil,
    pick_auto_color,
)
from tests.test_coffee_sales import env  # noqa: F401

FARM = {"type": "Polygon", "coordinates": [[[-40.0, -14.0], [-39.9, -14.0], [-39.9, -13.9], [-40.0, -13.9], [-40.0, -14.0]]]}
INSIDE = {"type": "Polygon", "coordinates": [[[-39.98, -13.98], [-39.95, -13.98], [-39.95, -13.95], [-39.98, -13.98]]]}
OUTSIDE = {"type": "Polygon", "coordinates": [[[-39.98, -13.98], [-39.85, -13.98], [-39.95, -13.95], [-39.98, -13.98]]]}
ON_EDGE = {"type": "Polygon", "coordinates": [[[-40.0, -13.98], [-39.95, -13.98], [-39.95, -13.95], [-40.0, -13.98]]]}


def test_geometry_must_stay_inside_farm_with_tolerance_on_the_edge():
    assert not geometry_outside_farm(json.dumps(INSIDE), json.dumps(FARM))
    assert geometry_outside_farm(json.dumps(OUTSIDE), json.dumps(FARM))
    assert not geometry_outside_farm(json.dumps(ON_EDGE), json.dumps(FARM))
    assert not geometry_outside_farm(json.dumps(OUTSIDE), None)  # fazenda sem perímetro: desenho livre


def test_auto_color_avoids_neighbors_and_spreads_usage():
    fills = [fill for _, _, fill in PLOT_COLORS]
    neighbor = SimpleNamespace(map_color=fills[0], boundary_geojson=json.dumps(INSIDE))
    far = SimpleNamespace(map_color=fills[1], boundary_geojson=json.dumps({"type": "Polygon", "coordinates": [[[-30, -10], [-29.9, -10], [-29.9, -9.9], [-30, -10]]]}))
    color = pick_auto_color([neighbor, far], json.dumps(INSIDE))
    assert color != fills[0]
    assert color == fills[2]  # menos usada entre as que não são do vizinho
    assert pick_auto_color([], None) == fills[0]


def test_field_normalization():
    assert normalize_color("#AABBCC") == "#aabbcc"
    assert normalize_color("vermelho") is None
    assert normalize_group("  Gleba   Sul ") == "Gleba Sul"
    assert normalize_group("   ") is None
    assert normalize_soil("argiloso") == "argiloso"
    assert normalize_soil("pedra") is None


@pytest.fixture
def plot_env(env, monkeypatch):
    farm = env.db.query(Farm).get(env.scope["active_farm_id"])
    farm.boundary_geojson = json.dumps(FARM)
    env.db.commit()
    scope = {**env.scope, "context_farms": [farm]}
    monkeypatch.setattr(env.routes, "_global_scope_context", lambda *args, **kwargs: scope)
    monkeypatch.setattr(env.routes, "generate_plot_preview_image", lambda *args, **kwargs: None)
    monkeypatch.setattr(env.routes, "_maybe_record_plot_boundary_attachments", lambda *args, **kwargs: None)
    return SimpleNamespace(env=env, farm=farm)


def _create(plot_env, **overrides):
    fields = dict(
        csrf_token="x", name="Represa", area_hectares=5.4, farm_id=str(plot_env.farm.id), planting_date=None, plant_count=None,
        spacing_row_meters=None, spacing_plant_centimeters=None, estimated_yield_sacks=None, variety_id=None,
        centroid_lat=None, centroid_lng=None, boundary_geojson=json.dumps(INSIDE), boundary_geojson_file=None,
        irrigation_type="none", irrigation_line_count=None, irrigation_line_length_meters=None, drip_spacing_centimeters=None,
        drip_liters_per_hour=None, sprinkler_count=None, sprinkler_liters_per_hour=None, notes=None,
        map_color=None, color_mode="auto", group_name="Gleba Sul", soil_texture="argiloso", include_in_season="1", return_to=None,
    )
    fields.update(overrides)
    return plot_env.env.routes.create_plot_action(plot_env.env.request("/setores"), db=plot_env.env.db, user=plot_env.env.user, **fields)


def test_create_plot_without_planting_gets_auto_color_group_soil_and_joins_active_season(plot_env):
    resp = _create(plot_env)
    assert resp.status_code == 303
    plot = plot_env.env.db.query(Plot).filter(Plot.name == "Represa").one()
    assert plot.plant_count == 0
    assert plot.map_color in {fill for _, _, fill in PLOT_COLORS}
    assert plot.group_name == "Gleba Sul"
    assert plot.soil_texture == "argiloso"
    season = plot_env.env.db.query(CropSeason).get(plot_env.env.scope["active_season_id"])
    assert plot in season.plots


def test_create_plot_outside_farm_is_blocked(plot_env):
    resp = _create(plot_env, name="Fora", boundary_geojson=json.dumps(OUTSIDE))
    assert resp.status_code == 303
    assert plot_env.env.db.query(Plot).filter(Plot.name == "Fora").count() == 0
    assert "perímetro da fazenda" in plot_env.env.session["flash"]["message"]


def test_manual_color_is_kept(plot_env):
    _create(plot_env, name="Colorido", color_mode="manual", map_color="#123456", include_in_season=None)
    assert plot_env.env.db.query(Plot).filter(Plot.name == "Colorido").one().map_color == "#123456"


def test_plots_page_renders_wizard_for_new_and_edit(plot_env):
    from fastapi import BackgroundTasks
    env = plot_env.env
    _create(plot_env, name="Editar", include_in_season=None)
    plot = env.db.query(Plot).filter(Plot.name == "Editar").one()
    for edit_id, label in ((None, "Criar setor"), (plot.id, "Salvar setor")):
        resp = env.routes.plots_page(env.request("/setores"), BackgroundTasks(), q=None, sort="name", edit_id=edit_id, plot_map=None,
                                     selected_farm_id=None, db=env.db, user=env.user, csrf_token="x")
        html = resp.body.decode("utf-8")
        assert resp.status_code == 200
        assert "data-plot-wizard" in html and label in html
        assert 'class="pzw' not in html.split("data-plot-wizard")[0][-400:]
