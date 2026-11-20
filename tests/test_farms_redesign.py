from fastapi import BackgroundTasks

from app.models.farm import Farm
from app.models.farm_rain_settings import FarmRainSettings
from tests.test_coffee_sales import env  # noqa: F401


def test_farms_list_renders_redesign_metrics(env):
    farm = env.db.query(Farm).one()
    resp = env.routes.farms_page(env.request("/fazendas"), BackgroundTasks(), db=env.db, user=env.user, csrf_token="x")
    html = resp.body.decode("utf-8")
    assert resp.status_code == 200
    assert "Fazendas" in html
    assert "data-fm-list-page" in html
    assert farm.name in html
    assert f"/fazendas/{farm.id}" in html


def test_farms_legacy_edit_and_view_query_redirect(env):
    farm = env.db.query(Farm).one()
    r_view = env.routes.farms_page(
        env.request(f"/fazendas?edit_id={farm.id}&view=1"),
        BackgroundTasks(),
        edit_id=farm.id,
        view=1,
        db=env.db,
        user=env.user,
        csrf_token="x",
    )
    assert r_view.status_code == 303
    assert r_view.headers["location"] == f"/fazendas/{farm.id}"

    r_edit = env.routes.farms_page(
        env.request(f"/fazendas?edit_id={farm.id}"),
        BackgroundTasks(),
        edit_id=farm.id,
        db=env.db,
        user=env.user,
        csrf_token="x",
    )
    assert r_edit.status_code == 303
    assert r_edit.headers["location"] == f"/fazendas/{farm.id}/editar"


def test_new_farm_wizard_renders_approved_copy_and_steps(env):
    resp = env.routes.new_farm_page(env.request("/fazendas/nova"), db=env.db, user=env.user, csrf_token="x")
    html = resp.body.decode("utf-8")
    assert resp.status_code == 200
    assert "data-fm-wizard" in html
    assert "Número do CAR (opcional)" in html
    assert "agiliza o cadastro" not in html
    assert "Informações adicionais" in html
    assert "Dados cadastrais e registrais" in html
    assert "ITR / NIRF / CIB" in html


def test_farm_detail_all_tabs_and_edit_page(env):
    farm = env.db.query(Farm).one()
    for tab in ("visao", "setores", "safras", "documentos", "chuva"):
        resp = env.routes.farm_detail_page(
            farm.id,
            env.request(f"/fazendas/{farm.id}?aba={tab}"),
            BackgroundTasks(),
            aba=tab,
            db=env.db,
            user=env.user,
            csrf_token="x",
        )
        assert resp.status_code == 200
        assert farm.name in resp.body.decode("utf-8")

    edit_resp = env.routes.edit_farm_page(
        farm.id,
        env.request(f"/fazendas/{farm.id}/editar"),
        BackgroundTasks(),
        passo=2,
        db=env.db,
        user=env.user,
        csrf_token="x",
    )
    assert edit_resp.status_code == 200
    assert f"/fazendas/{farm.id}/editar" in edit_resp.body.decode("utf-8")


def test_farm_detail_rain_settings_save(env, monkeypatch):
    farm = env.db.query(Farm).one()
    monkeypatch.setattr(env.routes, "_require_permission", lambda *a, **k: None)
    resp = env.routes.save_farm_rain_settings_action(
        farm.id,
        env.request(f"/fazendas/{farm.id}/pluviometria-config"),
        csrf_token="x",
        heavy_rain_mm=42.5,
        rainy_day_mm=2.0,
        alert_registered="1",
        alert_forecast=None,
        notify_dashboard="1",
        notify_bell="1",
        db=env.db,
        user=env.user,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == f"/fazendas/{farm.id}?aba=chuva"
    row = env.db.query(FarmRainSettings).filter_by(farm_id=farm.id).one()
    assert float(row.heavy_rain_mm) == 42.5
    assert float(row.rainy_day_mm) == 2.0
    assert row.alert_registered is True
    assert row.alert_forecast is False
