import asyncio
import json
from datetime import date, timedelta

from starlette.datastructures import FormData

from app.models import (
    FertilizationRecord,
    FertilizationSchedule,
    InputCatalog,
    IrrigationRecord,
    Plot,
    PurchasedInput,
)
from app.services.fertilization_views import build_input_stock_metrics, compute_schedule_demand
from tests.test_coffee_sales import env  # noqa: F401  (fixture)


def _post(env, handler, fields, *args, headers=None, path="/fertilizacao/agendamentos"):
    req = env.request(path)
    req.headers = {k.lower(): v for k, v in (headers or {}).items()}

    async def form():
        return FormData([("csrf_token", "x"), *fields])

    req.form = form
    return asyncio.run(handler(*args, req, env.db, env.user))


def _seed_stock_and_drip_plot(env):
    farm_id = env.scope["active_farm_id"]
    plot_a = env.db.query(Plot).filter(Plot.farm_id == farm_id).first()
    plot_a.area_hectares = 5.0
    plot_a.plant_count = 20000
    plot_a.irrigation_type = "gotejo"
    plot_a.irrigation_line_count = 20
    plot_a.irrigation_line_length_meters = 100
    plot_a.drip_spacing_meters = 0.5
    plot_a.drip_liters_per_hour = 2.0

    plot_b = Plot(
        name="Talhão B",
        area_hectares=10.0,
        plant_count=40000,
        farm_id=farm_id,
        irrigation_type="none",
    )
    env.db.add(plot_b)
    env.db.flush()
    season = env.db.get(env.routes.CropSeason, env.scope["active_season_id"])
    if season:
        for p in (plot_a, plot_b):
            if p not in season.plots:
                season.plots.append(p)
        env.scope["active_season"] = season

    farm = env.db.get(env.routes.Farm, farm_id)
    ureia = InputCatalog(
        organization_id=farm.organization_id if farm else None,
        name="Ureia 45%",
        normalized_name="ureia 45%",
        item_type="insumo_agricola",
        category="Solúvel",
        default_unit="kg",
        is_active=True,
    )
    env.db.add(ureia)
    env.db.flush()

    compra = PurchasedInput(
        input_id=ureia.id,
        farm_id=farm_id,
        name="Ureia 45%",
        normalized_name="ureia 45%",
        quantity_purchased=1,
        package_size=250,
        package_unit="kg",
        unit_price=875.0,
        purchase_date=date(2026, 10, 1),
        supplier_name="Cooxupé",
        batch_number="L-99",
        total_quantity=250,
        available_quantity=250,
        total_cost=875.0,
        entry_origin="purchase",
    )
    env.db.add(compra)
    env.db.commit()
    return plot_a, plot_b, ureia, compra


def test_schedules_and_fertilization_pages_and_wizards_render(env):
    _seed_stock_and_drip_plot(env)
    r = env.routes

    sched_html = r.fertilization_schedules_page(
        env.request("/fertilizacao/agendamentos"), db=env.db, user=env.user, csrf_token="x"
    ).body.decode()
    assert "Agendamentos" in sched_html and "Por data" in sched_html and "fertilization_schedule.js" in sched_html

    fert_html = r.fertilization_page(
        env.request("/fertilizacao"), db=env.db, user=env.user, csrf_token="x"
    ).body.decode()
    assert "Situação nutricional por talhão" in fert_html and "fertilization_page.js" in fert_html

    wiz_sched = r.new_fertilization_schedule_wizard_page(
        env.request("/fertilizacao/agendamentos/novo"), db=env.db, user=env.user, csrf_token="x"
    ).body.decode()
    assert "Novo agendamento" in wiz_sched and "Receita e insumos" in wiz_sched and "fertilization_wizard.js" in wiz_sched

    wiz_fert = r.new_fertilization_wizard_page(
        env.request("/fertilizacao/nova"), db=env.db, user=env.user, csrf_token="x"
    ).body.decode()
    assert "Registrar aplicação" in wiz_fert and "Conferir e salvar" in wiz_fert


def test_wizard_creates_proportional_schedules_and_detects_cumulative_shortage(env):
    plot_a, plot_b, ureia, compra = _seed_stock_and_drip_plot(env)
    payload = {
        "plotIds": [plot_a.id, plot_b.id],
        "dates": ["2026-11-22", "2026-11-29"],
        "method": "fertirrigacao",
        "durationMinutes": 45,
        "responsible": "Carlos",
        "recipeName": "Pegamento",
        "notes": "Aplicar de manhã",
        "doseMode": "per_ha",
        "items": [{"inputId": ureia.id, "value": 15}],
    }
    resp = _post(env, env.routes.create_fertilization_schedule_action, [("wizard_payload_json", json.dumps(payload))])
    assert resp.status_code == 303

    created = env.db.query(FertilizationSchedule).order_by(FertilizationSchedule.scheduled_date, FertilizationSchedule.id).all()
    assert len(created) == 4
    # Plot A (5 ha) -> 75 kg; Plot B (10 ha) -> 150 kg
    qtys_by_plot = {s.plot_id: float(s.items[0].quantity) for s in created[:2]}
    assert qtys_by_plot[plot_a.id] == 75.0
    assert qtys_by_plot[plot_b.id] == 150.0
    assert created[0].responsible_name == "Carlos"
    assert created[0].recipe_name == "Pegamento"

    # Estoque disponível = 250 kg; soma das 4 aplicações = 450 kg -> falta acumulada de 200 kg
    metrics = build_input_stock_metrics([ureia], [compra], farm_id=plot_a.farm_id)
    demand = compute_schedule_demand(created, metrics)
    assert demand["total_shortage_items"] == 1
    assert demand["missing_rows"][0]["missing"] == 200.0
    # Primeiros dois cabem (75 + 150 = 225 <= 250); terceiro e quarto entram em risco acumulado
    assert demand["by_schedule"][created[0].id]["cumulative_ok"] is True
    assert demand["by_schedule"][created[1].id]["cumulative_ok"] is True
    assert demand["by_schedule"][created[2].id]["direct_ok"] is True
    assert demand["by_schedule"][created[2].id]["cumulative_ok"] is False


def test_conclude_with_field_adjustments_syncs_stock_irrigation_and_revert_on_delete(env):
    plot_a, _, ureia, compra = _seed_stock_and_drip_plot(env)
    payload = {
        "plotIds": [plot_a.id],
        "dates": ["2026-11-20"],
        "method": "fertirrigacao",
        "durationMinutes": 45,
        "responsible": "Carlos",
        "recipeName": "Cobertura",
        "doseMode": "per_ha",
        "items": [{"inputId": ureia.id, "value": 20}],  # 100 kg planejados
    }
    _post(env, env.routes.create_fertilization_schedule_action, [("wizard_payload_json", json.dumps(payload))])
    sch = env.db.query(FertilizationSchedule).one()

    # Executa ajustando quantidade real para 90 kg e duração para 60 min
    actual_items = [{"input_id": ureia.id, "name": ureia.name, "unit": "kg", "quantity": 90}]
    resp = _post(
        env,
        env.routes.conclude_fertilization_schedule_action,
        [
            ("application_date", "2026-11-21"),
            ("duration_minutes", "60"),
            ("responsible_name", "Marcos"),
            ("notes", "Ajustado em campo"),
            ("actual_items_json", json.dumps(actual_items)),
        ],
        sch.id,
        path=f"/fertilizacao/agendamentos/{sch.id}/concluir",
    )
    assert resp.status_code == 303

    env.db.refresh(sch)
    env.db.refresh(compra)
    assert sch.status == "completed" and sch.fertilization_record_id is not None
    fert = env.db.query(FertilizationRecord).get(sch.fertilization_record_id)
    assert fert.application_date == date(2026, 11, 21)
    assert fert.duration_minutes == 60
    assert fert.responsible_name == "Marcos"
    assert float(fert.items[0].total_quantity) == 90.0
    assert float(compra.available_quantity) == 160.0  # 250 - 90

    # Irrigação automática gerada pelo gotejo
    irr = env.db.query(IrrigationRecord).filter_by(reference_type="fertilization_record", reference_id=fert.id).one()
    assert float(irr.volume_liters) == 8000.0  # 20 linhas * (100/0.5=200) * 2 L/h * 1h

    # Excluir a aplicação deve estornar estoque, remover irrigação e reabrir o agendamento
    env.routes.delete_fertilization_action(
        fert.id, env.request(f"/fertilizacao/{fert.id}/excluir"), csrf_token="x", db=env.db, user=env.user
    )
    env.db.refresh(sch)
    env.db.refresh(compra)
    assert float(compra.available_quantity) == 250.0
    assert sch.status == "scheduled" and sch.fertilization_record_id is None
    assert env.db.query(IrrigationRecord).filter_by(reference_type="fertilization_record", reference_id=fert.id).count() == 0


def test_batch_reschedule_shift_mode_json_and_duplicate_and_legacy_edit_redirect(env):
    plot_a, _, ureia, _ = _seed_stock_and_drip_plot(env)
    payload = {
        "plotIds": [plot_a.id],
        "dates": ["2026-11-21", "2026-11-28"],
        "method": "adubacao_solida",
        "doseMode": "fixed_plot",
        "items": [{"inputId": ureia.id, "value": 50}],
    }
    _post(env, env.routes.create_fertilization_schedule_action, [("wizard_payload_json", json.dumps(payload))])
    s1, s2 = env.db.query(FertilizationSchedule).order_by(FertilizationSchedule.scheduled_date).all()

    # Empurrar calendário em +3 dias via fetch (mantendo intervalo de 7 dias!)
    res = _post(
        env,
        env.routes.update_fertilization_schedules_date_batch_action,
        [("mode", "shift"), ("shift_days", "3"), ("schedule_ids", f"{s1.id},{s2.id}")],
        headers={"X-Requested-With": "fetch", "Accept": "application/json"},
        path="/fertilizacao/agendamentos/alterar-data-lote",
    )
    assert res.status_code == 200
    body = json.loads(res.body.decode())
    assert body["ok"] is True and body["updated"] == 2
    env.db.refresh(s1)
    env.db.refresh(s2)
    assert s1.scheduled_date == date(2026, 11, 24)
    assert s2.scheduled_date == date(2026, 12, 1)

    # Duplicar (+7 dias)
    env.routes.duplicate_fertilization_schedule_action(
        s1.id, env.request(f"/fertilizacao/agendamentos/{s1.id}/duplicar"), csrf_token="x", db=env.db, user=env.user
    )
    all_scheds = env.db.query(FertilizationSchedule).all()
    assert len(all_scheds) == 3
    assert any(x.scheduled_date == s1.scheduled_date + timedelta(days=7) for x in all_scheds)

    # Link legado ?edit_id redireciona para o novo Wizard de edição
    redir = env.routes.fertilization_schedules_page(
        env.request(f"/fertilizacao/agendamentos?edit_id={s1.id}"), edit_id=s1.id, db=env.db, user=env.user, csrf_token="x"
    )
    assert redir.status_code == 303 and redir.headers["location"] == f"/fertilizacao/agendamentos/{s1.id}/editar"
