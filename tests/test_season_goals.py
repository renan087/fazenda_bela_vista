import json
from datetime import date
from types import SimpleNamespace

from app.services.season_goals import (
    coffee_year_options,
    common_variety_id,
    goals_summary,
    parse_goals,
    plot_stage,
    status_from_dates,
)


def test_parse_goals_keeps_only_selected_plots_known_categories_and_valid_numbers():
    raw = json.dumps({
        "yields": {"1": 36, "2": "31,5", "9": 40, "x": 10, "3": -5},
        "budget": {"adubacao": 4200, "defensivos": "1800", "inventada": 99, "outros": "abc"},
        "price": 2150,
    })
    goals = parse_goals(raw, {1, 2, 3})
    assert goals == {"yields": {"1": 36.0, "2": 31.5}, "budget": {"adubacao": 4200.0, "defensivos": 1800.0}, "price": 2150.0}


def test_parse_goals_returns_none_for_empty_or_invalid_payload():
    assert parse_goals(None, {1}) is None
    assert parse_goals("não é json", {1}) is None
    assert parse_goals(json.dumps({"yields": {}, "budget": {}, "price": None}), {1}) is None


def test_goals_summary_computes_sacks_budget_and_breakeven():
    plots = [SimpleNamespace(id=1, area_hectares=10), SimpleNamespace(id=2, area_hectares=5)]
    season = SimpleNamespace(plots=plots, goals_json=json.dumps({"yields": {"1": 30}, "budget": {"adubacao": 4000, "outros": 1000}, "price": 2000}))
    summary = goals_summary(season)
    assert summary["sacks"] == 300
    assert summary["budget_per_ha"] == 5000
    assert summary["budget_total"] == 75000
    assert summary["revenue"] == 600000
    assert summary["margin"] == 525000
    assert summary["breakeven"] == 3.8  # 75000 / 2000 / 10 ha produtivos


def test_status_comes_from_dates_and_never_reopens_closed_season():
    start, end = date(2026, 7, 1), date(2027, 6, 30)
    assert status_from_dates(start, end, date(2026, 6, 1)) == "planejada"
    assert status_from_dates(start, end, date(2026, 11, 20)) == "ativa"
    assert status_from_dates(start, end, date(2027, 8, 1)) == "encerrada"
    assert status_from_dates(start, end, date(2026, 11, 20), current="encerrada") == "encerrada"


def test_coffee_year_runs_july_to_june():
    options = coffee_year_options(date(2026, 11, 20))
    assert [option["label"] for option in options] == ["2026/27", "2027/28"]
    assert options[0]["start"] == "2026-07-01" and options[0]["end"] == "2027-06-30"
    assert coffee_year_options(date(2027, 3, 1))[0]["label"] == "2026/27"


def test_plot_stage_uses_planting_age():
    today = date(2026, 11, 20)
    def plot(planting_date):
        return SimpleNamespace(active_planting=SimpleNamespace(planting_date=planting_date), planting_date=None)
    assert plot_stage(plot(date(2018, 3, 1)), today) == "prod"
    assert plot_stage(plot(date(2025, 3, 1)), today) == "form"
    assert plot_stage(plot(None), today) == "sem"


def test_common_variety_only_when_all_plots_share_it():
    same = [SimpleNamespace(active_planting=SimpleNamespace(variety_id=4), variety_id=None), SimpleNamespace(active_planting=None, variety_id=4)]
    mixed = same + [SimpleNamespace(active_planting=SimpleNamespace(variety_id=7), variety_id=None)]
    assert common_variety_id(same) == 4
    assert common_variety_id(mixed) is None


from app.models.crop_season import CropSeason  # noqa: E402
from app.models.plot import Plot  # noqa: E402
from tests.test_coffee_sales import env  # noqa: E402,F401


def test_seasons_page_renders_wizard_with_plots(env, monkeypatch):
    from app.models.farm import Farm
    scope = {**env.scope, "context_farms": env.db.query(Farm).all()}
    monkeypatch.setattr(env.routes, "_global_scope_context", lambda *args, **kwargs: scope)
    resp = env.routes.crop_seasons_page(env.request("/safras"), db=env.db, user=env.user, csrf_token="x")
    html = resp.body.decode("utf-8")
    assert resp.status_code == 200
    assert "data-season-wizard" in html
    assert "season-wizard-data" in html
    assert "Talh\\u00e3o 07" in html or "Talhão 07" in html
    assert "Criar safra" in html


def test_create_season_from_wizard_saves_goals_status_and_active_context(env):
    plot = env.db.query(Plot).filter(Plot.name == "Talhão 07").one()
    goals = json.dumps({"yields": {str(plot.id): 35}, "budget": {"adubacao": 4000}, "price": 2000})
    resp = env.routes.create_crop_season_action(
        env.request("/safras"), csrf_token="x", name="", start_date="2030-07-01", end_date="2031-06-30",
        culture="Café", variety_id=None, plot_ids=[plot.id], cultivated_area=None, area_unit="ha", notes="",
        status_value=None, goals=goals, make_active="1", db=env.db, user=env.user,
    )
    assert resp.status_code == 303
    season = env.db.query(CropSeason).filter(CropSeason.start_date == date(2030, 7, 1)).one()
    assert season.status == "planejada"
    assert json.loads(season.goals_json)["yields"] == {str(plot.id): 35.0}
    assert env.session["active_season_id"] == season.id
