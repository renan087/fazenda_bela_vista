import asyncio
import json
from datetime import date

from fastapi import BackgroundTasks
from sqlalchemy import inspect as sa_inspect

from app.models.farm import Farm
from app.models.farm_attachment import FarmAttachment
from app.models.farm_preview_cache import FarmPreviewCache
from app.models.farm_rain_settings import FarmRainSettings
from app.models.rainfall import RainfallRecord
from app.services.farm_preview_image import (
    farm_geojson_hash,
    farm_preview_fs_path,
    farm_preview_thumb_fs_path,
)
from tests.test_coffee_sales import env  # noqa: F401

SAMPLE_GEOJSON = json.dumps(
    {
        "type": "Polygon",
        "coordinates": [[[-40.84, -14.86], [-40.83, -14.86], [-40.83, -14.85], [-40.84, -14.86]]],
    }
)
SAMPLE_GEOJSON_2 = json.dumps(
    {
        "type": "Polygon",
        "coordinates": [[[-40.85, -14.87], [-40.82, -14.87], [-40.82, -14.84], [-40.85, -14.87]]],
    }
)


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
    assert "data-fm-map-prompt" in html


def test_farm_detail_all_tabs_and_visual_fixes(env):
    farm = env.db.query(Farm).one()
    farm.car_registration_number = "BA-2925006-E817CC3F22DB437C871E523B4E664CAA"
    farm.boundary_geojson = SAMPLE_GEOJSON
    env.db.commit()

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
        html = resp.body.decode("utf-8")
        assert farm.name in html
        if tab == "visao":
            assert "fm-summary-rows" in html
            assert "fm-car-pill" in html
            assert "fm-map-edit-pane" in html

    # Quando já existe mapa desenhado, o prompt "Carregar mapa da região" deve ser removido e o mapa expandido
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
    edit_html = edit_resp.body.decode("utf-8")
    assert "data-fm-map-prompt" not in edit_html
    assert "Carregar mapa da região" not in edit_html
    assert "fm-map-canvas is-expanded" in edit_html


def test_deferred_attachment_skips_binary_on_list_and_serves_download(env):
    farm = env.db.query(Farm).one()
    payload = b"%PDF-1.4 exemplo-anexo-fazenda-sisfarm"
    att = FarmAttachment(
        farm_id=farm.id,
        filename="car_fazenda.pdf",
        content_type="application/pdf",
        file_data=payload,
    )
    env.db.add(att)
    env.db.commit()
    att_id = att.id
    env.db.expire_all()

    # Listagem e ficha não devem carregar file_data
    resp_list = env.routes.farms_page(env.request("/fazendas"), BackgroundTasks(), db=env.db, user=env.user, csrf_token="x")
    assert resp_list.status_code == 200
    loaded_att = env.db.query(FarmAttachment).filter_by(id=att_id).one()
    assert "file_data" in sa_inspect(loaded_att).unloaded

    # Download/visualização carrega sob demanda e devolve bytes exatos
    resp_dl = env.routes.open_farm_attachment(att_id, env.request(f"/fazendas/anexos/{att_id}"), db=env.db, user=env.user)
    assert resp_dl.status_code == 200
    assert resp_dl.body == payload
    assert resp_dl.media_type == "application/pdf"


def test_edit_farm_skips_preview_regen_when_geometry_unchanged_and_queues_on_change(env, monkeypatch):
    farm = env.db.query(Farm).one()
    farm.boundary_geojson = SAMPLE_GEOJSON
    geo_hash = farm_geojson_hash(SAMPLE_GEOJSON)
    env.db.add(
        FarmPreviewCache(
            farm_id=farm.id,
            geojson_hash=geo_hash,
            preview_png=b"\x89PNG\r\n\x1a\nfull",
            thumb_jpg=b"\xff\xd8\xffthumb",
        )
    )
    env.db.commit()

    async def no_attachments(_req):
        return []

    monkeypatch.setattr(env.routes, "_request_attachments", no_attachments)

    # 1. Edição apenas de dados cadastrais (sem alterar mapa) -> NÃO agenda geração de preview
    resp_meta = asyncio.run(
        env.routes.update_farm_action(
            farm.id,
            env.request(f"/fazendas/{farm.id}/editar"),
            csrf_token="x",
            name=farm.name,
            location=farm.location,
            total_area=float(farm.total_area),
            car_registration_number=None,
            ccir_number="950.123.456.789-0",
            itr_registration_number="D.445.8ZN-M",
            property_registration_number=None,
            registry_office=None,
            boundary_geojson="",
            boundary_geojson_file=None,
            notes="Observação atualizada",
            db=env.db,
            user=env.user,
        )
    )
    assert resp_meta.status_code == 303
    assert resp_meta.headers["location"] == f"/fazendas/{farm.id}"
    assert resp_meta.background is None
    env.db.refresh(farm)
    assert farm.itr_registration_number == "D.445.8ZN-M"

    # 2. Edição alterando geometria -> agenda em background (não bloqueia a requisição)
    resp_geo = asyncio.run(
        env.routes.update_farm_action(
            farm.id,
            env.request(f"/fazendas/{farm.id}/editar"),
            csrf_token="x",
            name=farm.name,
            location=farm.location,
            total_area=float(farm.total_area),
            car_registration_number=None,
            ccir_number=None,
            itr_registration_number=None,
            property_registration_number=None,
            registry_office=None,
            boundary_geojson=SAMPLE_GEOJSON_2,
            boundary_geojson_file=None,
            notes=None,
            db=env.db,
            user=env.user,
        )
    )
    assert resp_geo.status_code == 303
    assert resp_geo.background is not None


def test_preview_persists_in_db_and_restores_after_ephemeral_disk_wipe(env):
    farm = env.db.query(Farm).one()
    farm.boundary_geojson = SAMPLE_GEOJSON
    geo_hash = farm_geojson_hash(SAMPLE_GEOJSON)
    png_bytes = b"\x89PNG\r\n\x1a\nfull-preview"
    jpg_bytes = b"\xff\xd8\xffthumb-preview"
    env.db.add(
        FarmPreviewCache(
            farm_id=farm.id,
            geojson_hash=geo_hash,
            preview_png=png_bytes,
            thumb_jpg=jpg_bytes,
        )
    )
    env.db.commit()

    # Simula redeploy no Render apagando arquivos locais
    farm_preview_fs_path(farm.id).unlink(missing_ok=True)
    farm_preview_thumb_fs_path(farm.id).unlink(missing_ok=True)

    bg = BackgroundTasks()
    resp_list = env.routes.farms_page(env.request("/fazendas"), bg, db=env.db, user=env.user, csrf_token="x")
    assert resp_list.status_code == 200
    assert len(bg.tasks) == 0  # Não dispara Pillow porque já existe no PostgreSQL!
    assert f"/fazendas/{farm.id}/preview-thumb.jpg?v={geo_hash}" in resp_list.body.decode("utf-8")

    # Endpoints servem e reidratam o disco automaticamente
    r_thumb = env.routes.farm_preview_thumb_image(farm.id, db=env.db, user=env.user)
    r_full = env.routes.farm_preview_full_image(farm.id, db=env.db, user=env.user)
    assert r_thumb.body == jpg_bytes
    assert r_full.body == png_bytes

    # Limpa arquivos de teste gerados no disco
    farm_preview_fs_path(farm.id).unlink(missing_ok=True)
    farm_preview_thumb_fs_path(farm.id).unlink(missing_ok=True)


def test_farm_detail_rainfall_uses_sql_aggregate_without_loading_history(env):
    farm = env.db.query(Farm).one()
    today = env.routes.today_in_app_timezone()
    env.db.add_all(
        [
            RainfallRecord(farm_id=farm.id, rainfall_date=date(today.year, today.month, 1), millimeters=25.0),
            RainfallRecord(farm_id=farm.id, rainfall_date=date(today.year - 1, 1, 15), millimeters=100.0),
        ]
    )
    env.db.commit()
    env.db.expire_all()

    resp = env.routes.farm_detail_page(
        farm.id,
        env.request(f"/fazendas/{farm.id}?aba=visao"),
        BackgroundTasks(),
        aba="visao",
        db=env.db,
        user=env.user,
        csrf_token="x",
    )
    assert resp.status_code == 200
    html = resp.body.decode("utf-8")
    assert "25 mm no mês" in html
    assert "rainfalls" in sa_inspect(farm).unloaded


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
