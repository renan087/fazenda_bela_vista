from datetime import date

import pytest

from app.models import HarvestRecord
from app.services.harvest_lots import estimate_sacks, initial_stage, stage_of
from tests.test_coffee_sales import env  # noqa: F401  (fixture)


def test_estimate_and_initial_stage():
    assert estimate_sacks("litros", 72000, 480) == 150
    assert estimate_sacks("sacos_roca", 160, 8) == 20
    assert estimate_sacks("sc_60", 138, None) == 138
    assert initial_stage("litros", "Terreiro") == "secagem"
    assert initial_stage("litros", "Tulha") == "tulha"
    assert initial_stage("sc_60", "Terreiro") == "beneficiado"


def test_harvest_measure_converts_and_validates(env):
    measure = env.routes._harvest_measure
    sacks, extra = measure("litros", "72.000", "480", None, "Terreiro")
    assert sacks == 150 and extra["sacks_estimated"] is True and extra["lot_stage"] == "secagem"
    assert extra["measured_quantity"] == 72000
    sacks, extra = measure("sc_60", "138", None, None, "Terreiro")
    assert sacks == 138 and extra["sacks_estimated"] is False and extra["lot_stage"] == "beneficiado"
    with pytest.raises(ValueError):
        measure("litros", "", "480", None, "Terreiro")


def _harvest(env, **kw):
    r = env.routes
    response = r.create_harvest_action(
        env.request("/producao"), csrf_token="x", plot_id=env.lots[0].plot_id, harvest_date="2026-09-02", lot_code=None,
        sacks_produced=None, harvest_type="Mecanizada", coffee_stage=None, initial_destination="Terreiro",
        responsible_name="João", work_shift=None, maturation_percentage="70", impurity_percentage=None,
        input_moisture_percentage=None, volume_count=None, notes=None, measure_unit="litros",
        measured_quantity="72.000", yield_factor="480", db=env.db, user=env.user, **kw)
    lot = env.db.query(HarvestRecord).order_by(HarvestRecord.id.desc()).first()
    return response, lot


def test_new_harvest_creates_estimated_lot_then_processing_makes_it_real(env, monkeypatch):
    r = env.routes
    response, lot = _harvest(env)
    assert response.headers["location"] == f"/producao/lotes/{lot.id}"
    assert lot.sacks_estimated and float(lot.sacks_produced) == 150 and lot.lot_stage == "secagem"

    r.lot_stage_action(lot.id, env.request("/x"), csrf_token="x", stage="armazem", event_date="2026-09-20",
                       storage_location="", warehouse_receipt=None, db=env.db, user=env.user)
    assert "Informe o nome do armazém" in env.session["flash"]["message"]
    r.lot_stage_action(lot.id, env.request("/x"), csrf_token="x", stage="tulha", event_date="2026-09-18",
                       storage_location=None, warehouse_receipt=None, db=env.db, user=env.user)
    env.db.refresh(lot)
    assert lot.lot_stage == "tulha" and stage_of(lot)[1] == "Na tulha · falta beneficiar"

    r.lot_processing_action(lot.id, env.request("/x"), csrf_token="x", real_sacks="144", processed_on="2026-09-25",
                            db=env.db, user=env.user)
    env.db.refresh(lot)
    assert not lot.sacks_estimated and float(lot.sacks_produced) == 144 and lot.lot_stage == "beneficiado"
    assert float(lot.yield_factor) == 500
    assert [e.title for e in lot.events] == ["Entrou na tulha", "Beneficiado"]

    html = r.lot_detail_page(lot.id, env.request(f"/producao/lotes/{lot.id}"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Caminho do lote" in html and "Entrou na tulha" in html and "estimado era ≈ 150" in html
    assert "Vender deste lote" in html and "Informar benefício" not in html


def test_overview_and_harvest_form_render(env):
    r = env.routes
    _, lot = _harvest(env)
    html = r.production_page(env.request("/producao"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Para onde foi o café da safra" in html and "Precisa de atenção" in html
    assert f'href="/producao/lotes/{lot.id}"' in html and "≈ 150,00 sc" in html
    html = r.production_page(env.request("/producao?filtro=vendidos"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Nenhum lote neste filtro" in html
    assert r.production_page(env.request(f"/producao?lote={lot.id}"), db=env.db, user=env.user,
                             csrf_token="x").headers["location"] == f"/producao/lotes/{lot.id}"
    form = r.harvest_form_page(env.request("/producao/colheitas/nova"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "data-harvest-wizard" in form and "Sacos da roça" in form and "Talhão 07" in form
    edit = r.harvest_form_page(env.request(f"/producao/colheitas/{lot.id}/editar"), lot_id=lot.id, db=env.db, user=env.user,
                               csrf_token="x").body.decode()
    assert f'action="/producao/{lot.id}/editar"' in edit and 'value="72.000"' in edit
