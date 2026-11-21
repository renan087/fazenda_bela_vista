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


def _scoped(env, monkeypatch):
    from app.models.farm import Farm
    farm = env.db.query(Farm).one()
    scope = {**env.scope, "context_farms": [farm], "active_farm": farm}
    monkeypatch.setattr(env.routes, "_global_scope_context", lambda *args, **kwargs: scope)
    return scope


def test_new_season_page_renders_canvas_layout(env, monkeypatch):
    _scoped(env, monkeypatch)
    resp = env.routes.season_new_page(env.request("/safras/nova"), db=env.db, user=env.user, csrf_token="x")
    html = resp.body.decode("utf-8")
    assert resp.status_code == 200
    for piece in ("data-season-wizard", "Salvar rascunho", "Dica do Pezinho", "Resumo da safra", "szw-sheet", "szw-mhead",
                  "Copiar o programa de adubação", "Avisos do Pezinho", "Criar safra"):
        assert piece in html
    assert "Em breve" not in html
    assert "Talh\\u00e3o 07" in html or "Talhão 07" in html


def test_old_query_urls_redirect_to_pages(env, monkeypatch):
    _scoped(env, monkeypatch)
    resp = env.routes.crop_seasons_page(env.request("/safras?novo=1&setor=5"), db=env.db, user=env.user, csrf_token="x")
    assert resp.status_code in (302, 303) and resp.headers["location"].startswith("/safras/nova")
    resp = env.routes.crop_seasons_page(env.request("/safras?edit_id=3"), edit_id=3, db=env.db, user=env.user, csrf_token="x")
    assert resp.headers["location"].startswith("/safras/3/editar")


def test_create_season_from_wizard_saves_goals_status_and_active_context(env):
    plot = env.db.query(Plot).filter(Plot.name == "Talhão 07").one()
    goals = json.dumps({"yields": {str(plot.id): 35}, "budget": {"adubacao": 4000}, "price": 2000})
    resp = env.routes.create_crop_season_action(
        env.request("/safras"), csrf_token="x", name="", start_date="2030-07-01", end_date="2031-06-30",
        culture="Café", variety_id=None, plot_ids=[plot.id], cultivated_area=None, area_unit="ha", notes="",
        status_value=None, goals=goals, make_active="1", integrations=json.dumps({"fin": True, "lemb": True}), db=env.db, user=env.user,
    )
    assert resp.status_code == 303
    assert "/pronta" in resp.headers["location"]
    season = env.db.query(CropSeason).filter(CropSeason.start_date == date(2030, 7, 1)).one()
    assert season.status == "planejada"
    assert json.loads(season.goals_json)["yields"] == {str(plot.id): 35.0}
    assert env.session["active_season_id"] == season.id
    assert json.loads(season.goals_json)["integrations"]["fin"] is True


def test_copy_fertilization_program_shifts_dates_as_drafts(env):
    from app.models.fertilization_schedule import FertilizationSchedule
    from app.services.season_goals import copy_fertilization_program
    plot = env.db.query(Plot).filter(Plot.name == "Talhão 07").one()
    old = env.db.query(CropSeason).get(env.scope["active_season_id"])
    if plot not in old.plots:
        old.plots.append(plot)
    env.db.add(FertilizationSchedule(plot_id=plot.id, season_id=old.id, scheduled_date=old.start_date.replace(day=10), status="completed", application_method="fertirrigacao"))
    new = CropSeason(farm_id=old.farm_id, name="Seguinte", start_date=old.start_date.replace(year=old.start_date.year + 1),
                     end_date=old.end_date.replace(year=old.end_date.year + 1), culture="Café", cultivated_area=1, status="planejada", plots=[plot])
    env.db.add(new)
    env.db.commit()
    assert copy_fertilization_program(env.repo if hasattr(env, "repo") else env.routes._repository(env.db), old, new) == 1
    copy = env.db.query(FertilizationSchedule).filter(FertilizationSchedule.season_id == new.id).one()
    assert copy.status == "scheduled" and copy.scheduled_date.year == old.start_date.year + 1
    assert "Rascunho copiado" in copy.notes


def test_close_and_reopen_season(env, monkeypatch):
    _scoped(env, monkeypatch)
    season = env.db.query(CropSeason).get(env.scope["active_season_id"])
    page = env.routes.season_close_page(season.id, env.request(f"/safras/{season.id}/encerrar"), db=env.db, user=env.user, csrf_token="x")
    html = page.body.decode("utf-8")
    assert page.status_code == 200 and "Previsto x realizado" in html and "Pendências antes de fechar" in html
    assert "colheitas sem classificação" in html  # os 2 lotes do fixture não têm classificação
    blocked = env.routes.season_close_action(season.id, env.request("/x"), csrf_token="x", closing_notes="geada", keep_unsold="1", confirm=None, db=env.db, user=env.user)
    assert blocked.status_code == 303 and env.db.query(CropSeason).get(season.id).status != "encerrada"
    env.routes.season_close_action(season.id, env.request("/x"), csrf_token="x", closing_notes="geada", keep_unsold="1", confirm="1", db=env.db, user=env.user)
    closed = env.db.query(CropSeason).get(season.id)
    assert closed.status == "encerrada"
    assert json.loads(closed.goals_json)["closing"]["snapshot"]["produced"] == 154
    env.routes.season_reopen_action(season.id, env.request("/x"), csrf_token="x", db=env.db, user=env.user)
    reopened = env.db.query(CropSeason).get(season.id)
    assert reopened.status != "encerrada" and "closing" not in json.loads(reopened.goals_json or "{}")
