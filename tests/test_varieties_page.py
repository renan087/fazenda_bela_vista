from datetime import date

from app.models.coffee_variety import CoffeeVariety
from app.models.crop_season import CropSeason
from app.models.harvest import HarvestRecord
from app.models.plot import Plot
from app.services.varieties_overview import build_varieties_overview, cycle_key
from tests.test_coffee_sales import env  # noqa: F401
from tests.test_plot_wizard import plot_env  # noqa: F401


def _seed(plot_env):
    db, farm = plot_env.env.db, plot_env.farm
    v1 = CoffeeVariety(organization_id=farm.organization_id, name="Catuaí Vermelho IAC 144", species="Arábica", maturation_cycle="Médio-tardio")
    v2 = CoffeeVariety(organization_id=farm.organization_id, name="Arara", species="Arábica", maturation_cycle="Tardio", rust_resistance="Resistente")
    v3 = CoffeeVariety(organization_id=farm.organization_id, name="Topázio", species="Arábica", maturation_cycle="Media")
    db.add_all([v1, v2, v3])
    db.flush()
    p1 = Plot(farm_id=farm.id, variety_id=v1.id, name="Talhão 1", area_hectares=10, plant_count=40000, planting_date=date(2016, 10, 1))
    p2 = Plot(farm_id=farm.id, variety_id=v2.id, name="Talhão 2", area_hectares=5, plant_count=20000, planting_date=date(2020, 10, 1))
    p3 = Plot(farm_id=farm.id, variety_id=None, name="Talhão 3", area_hectares=4, plant_count=16000)
    db.add_all([p1, p2, p3])
    db.flush()
    season_id = plot_env.env.scope["active_season_id"]
    db.add_all([
        HarvestRecord(plot_id=p1.id, season_id=season_id, harvest_date=date(2026, 6, 1), sacks_produced=300),
        HarvestRecord(plot_id=p2.id, season_id=season_id, harvest_date=date(2026, 8, 1), sacks_produced=200),
    ])
    db.commit()
    return v1, v2, v3, p1, p2, p3


def test_cycle_key_reads_old_and_new_labels():
    assert cycle_key("Precoce") == "pre"
    assert cycle_key("Media") == "med"
    assert cycle_key("Médio-tardio") == "mtar"
    assert cycle_key("Tardia") == "tar"


def test_overview_sums_area_plants_and_yield_per_variety(plot_env):
    v1, v2, v3, p1, p2, p3 = _seed(plot_env)
    db = plot_env.env.db
    plots = db.query(Plot).filter(Plot.farm_id == plot_env.farm.id).all()
    ov = build_varieties_overview(db, [v1, v2, v3], plots, today=date(2026, 11, 21))
    items = {item["id"]: item for item in ov["items"]}
    assert items[v1.id]["area"] == 10 and items[v1.id]["plants"] == 40000
    assert items[v1.id]["last_yield"] == 30.0
    assert items[v2.id]["last_yield"] == 40.0
    assert items[v2.id]["rust_key"] == "res"
    assert items[v3.id]["used"] is False
    assert ov["used_count"] == 2 and ov["unused_count"] == 1
    assert ov["best"]["id"] == v2.id
    # Calendário em ordem de maturação: médio-tardio antes de tardio.
    assert [i["id"] for i in ov["calendar"]] == [v1.id, v2.id]
    assert items[v1.id]["window"] == "jun–jul"


def test_create_variety_with_new_fields_links_plots(plot_env):
    _, _, _, p1, _, p3 = _seed(plot_env)
    env = plot_env.env
    resp = env.routes.create_variety_action(
        env.request("/variedades"), csrf_token="x", name="  Paraíso  MG H 419-1 ", species="Arábica", maturation_cycle="Médio",
        flavor_profile="Doce, Frutado", notes=None, plant_height="Baixo", rust_resistance="Resistente", map_color="#1F8A7A",
        plot_ids=[str(p1.id), str(p3.id), "abc"], db=env.db, user=env.user,
    )
    assert resp.status_code == 303
    variety = env.db.query(CoffeeVariety).filter(CoffeeVariety.name == "Paraíso MG H 419-1").one()
    assert variety.plant_height == "Baixo" and variety.rust_resistance == "Resistente" and variety.map_color == "#1F8A7A"
    env.db.refresh(p1)
    env.db.refresh(p3)
    assert p1.variety_id == variety.id and p3.variety_id == variety.id
    assert resp.headers["location"] == f"/variedades?variedade={variety.id}"


def test_create_variety_blocks_duplicate_name_ignoring_accents(plot_env):
    _seed(plot_env)
    env = plot_env.env
    resp = env.routes.create_variety_action(
        env.request("/variedades"), csrf_token="x", name="catuai vermelho iac 144", species="Arábica", maturation_cycle="Médio",
        flavor_profile=None, notes=None, plant_height=None, rust_resistance=None, map_color="não-é-cor", plot_ids=[], db=env.db, user=env.user,
    )
    assert resp.status_code == 303
    assert env.db.query(CoffeeVariety).filter(CoffeeVariety.name.ilike("catuai%")).count() == 0
    assert env.session["flash"]["kind"] == "error"
    assert "já está cadastrada" in env.session["flash"]["message"]


def test_delete_variety_with_plots_is_blocked(plot_env):
    v1, *_ = _seed(plot_env)
    env = plot_env.env
    env.routes.delete_variety_action(v1.id, env.request("/variedades"), csrf_token="x", db=env.db, user=env.user)
    assert env.db.get(CoffeeVariety, v1.id) is not None


def test_varieties_page_renders_cards_panel_and_form(plot_env):
    v1, v2, v3, *_ = _seed(plot_env)
    env = plot_env.env
    resp = env.routes.varieties_page(env.request("/variedades"), edit_id=None, variedade=None, db=env.db, user=env.user, csrf_token="x")
    html = resp.body.decode()
    assert f'data-vr-item="{v1.id}"' in html and f'data-vr-panel="{v3.id}"' in html
    assert "Colheita prevista" in html and "vr-summary" in html
    assert "data-vr-menu-btn" in html and "Excluir variedade" in html
    assert "data-vr-view" not in html and "sf-kpis" not in html
    assert "Vincular setor" in html
    assert "Ainda não está em nenhum setor." in html
    assert 'name="plot_ids"' in html and 'role="switch"' in html
    assert "Catuaí Vermelho IAC 144" in html
    assert html.count('data-selected="' + str(v1.id) + '"') == 1  # maior área abre selecionada


def test_varieties_page_edit_locks_plots_already_linked(plot_env):
    v1, _, _, p1, *_ = _seed(plot_env)
    env = plot_env.env
    resp = env.routes.varieties_page(env.request(f"/variedades?edit_id={v1.id}"), edit_id=v1.id, variedade=None, db=env.db, user=env.user, csrf_token="x")
    html = resp.body.decode()
    assert f'/variedades/{v1.id}/editar' in html
    assert f'value="{p1.id}" data-vr-plot' in html
    assert "checked disabled" in html


def test_page_is_empty_state_without_varieties(plot_env):
    env = plot_env.env
    env.db.query(CropSeason).update({CropSeason.variety_id: None})
    env.db.query(CoffeeVariety).delete()
    env.db.commit()
    resp = env.routes.varieties_page(env.request("/variedades"), edit_id=None, variedade=None, db=env.db, user=env.user, csrf_token="x")
    assert "Nenhuma variedade cadastrada" in resp.body.decode()


def test_color_options_mark_automatic_colors_as_taken(plot_env):
    from app.services.varieties_overview import color_options

    v1, v2, v3, *_ = _seed(plot_env)
    db = plot_env.env.db
    plots = db.query(Plot).filter(Plot.farm_id == plot_env.farm.id).all()
    ov = build_varieties_overview(db, [v1, v2, v3], plots)
    options = color_options(ov["items"])
    taken = {o["value"] for o in options if o["taken_by"]}
    assert {item["color"] for item in ov["items"]} <= taken
    assert any(not o["taken_by"] for o in options)
