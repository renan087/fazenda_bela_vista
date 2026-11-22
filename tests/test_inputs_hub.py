import json
from datetime import date, timedelta

import pytest

from app.models.equipment_asset import EquipmentAsset
from app.models.farm import Farm
from app.models.equipment_fueling import EquipmentFueling
from app.models.equipment_maintenance_plan import EquipmentMaintenancePlan
from app.models.input_catalog import InputCatalog
from app.models.plot import Plot
from app.models.purchased_input import PurchasedInput
from app.models.stock_output import StockOutput
from app.services import inputs_hub as ih
from app.services.forms import _normalize_input_name
from tests.test_coffee_sales import env  # noqa: F401


def _item(db, name, item_type="insumo_agricola", category="Fertilizantes", unit="sc", threshold=None):
    item = InputCatalog(name=name, normalized_name=_normalize_input_name(name), item_type=item_type, category=category, default_unit=unit,
                        low_stock_threshold=threshold, is_active=True)
    db.add(item)
    db.flush()
    return item


def _entry(db, item, farm_id, qty, unit, price, *, expires=None, batch=None, bought=None):
    entry = PurchasedInput(input_id=item.id, farm_id=farm_id, name=item.name, quantity_purchased=qty, package_size=1, package_unit=unit,
                           unit_price=price, total_quantity=qty, available_quantity=qty, total_cost=qty * price,
                           purchase_date=bought or date(2026, 11, 1), expiration_date=expires, batch_number=batch)
    db.add(entry)
    db.flush()
    return entry


@pytest.fixture
def hub(env, monkeypatch):
    db, scope = env.db, env.scope
    farm_id = scope["active_farm_id"]
    farm = db.get(Farm, farm_id)
    monkeypatch.setattr(env.routes, "_global_scope_context", lambda *a, **k: {**scope, "context_farms": [farm], "active_farm": farm})
    plot = db.query(Plot).filter(Plot.farm_id == farm_id).first()
    kcl = _item(db, "Cloreto de potássio", unit="sc", threshold=20)
    _entry(db, kcl, farm_id, 8, "sc", 190)
    fung = _item(db, "Fungicida azoxistrobina", category="Fungicida", unit="L")
    _entry(db, fung, farm_id, 12, "L", 240, expires=date.today() + timedelta(days=20), batch="2304-A")
    diesel = _item(db, "Diesel S10", item_type="combustivel", category="Combustíveis", unit="L")
    _entry(db, diesel, farm_id, 420, "L", 6)
    luva = _item(db, "Luva nitrílica", item_type="suprimento", category="EPIs", unit="par")
    _entry(db, luva, farm_id, 14, "par", 14)
    trator = EquipmentAsset(farm_id=farm_id, name="Trator 4x4", category="Máquina", subtype="trator", status="ativo")
    galpao = EquipmentAsset(farm_id=farm_id, name="Galpão", category="Benfeitoria", status="ativo")
    db.add_all([trator, galpao])
    db.flush()
    db.add(EquipmentMaintenancePlan(asset_id=trator.id, description="Revisão de 250 h", maintenance_type="preventiva",
                                    interval_hours=250, next_due_hours=1250, is_active=True))
    db.commit()
    return env, dict(plot=plot, kcl=kcl, fung=fung, diesel=diesel, luva=luva, trator=trator, galpao=galpao)


def test_stock_items_merge_supplies_and_flag_low_and_expiring(hub):
    env, x = hub
    items = {i["name"]: i for i in ih.build_stock_items(env.routes._repository(env.db), env.scope["active_farm_id"], date.today())}
    assert set(items) >= {"Cloreto de potássio", "Fungicida azoxistrobina", "Diesel S10", "Luva nitrílica"}
    assert items["Luva nitrílica"]["group"] == "sup" and items["Diesel S10"]["group"] == "comb"
    assert items["Fungicida azoxistrobina"]["group"] == "def" and items["Fungicida azoxistrobina"]["expiring_count"] == 1
    assert items["Cloreto de potássio"]["status"] == "low"
    assert items["Cloreto de potássio"]["value_label"] == "R$ 1.520"


def test_register_uses_in_one_transaction_and_rolls_back_on_error(hub):
    env, x = hub
    repo = env.routes._repository(env.db)
    plots = {x["plot"].id: x["plot"]}
    farm_id, season_id = env.scope["active_farm_id"], env.scope["active_season_id"]
    lines = [
        {"input_id": x["kcl"].id, "plot_id": str(x["plot"].id), "quantity": "2", "date": "2026-11-20"},
        {"input_id": x["luva"].id, "plot_id": "", "quantity": "4"},
    ]
    outputs = ih.register_uses(repo, lines, farm_id=farm_id, season_id=season_id, plots=plots)
    assert len(outputs) == 2
    items = {i["name"]: i for i in ih.build_stock_items(repo, farm_id, date.today())}
    assert items["Cloreto de potássio"]["available"] == 6 and items["Luva nitrílica"]["available"] == 10

    with pytest.raises(ValueError, match="Linha 2"):
        ih.register_uses(repo, [{"input_id": x["kcl"].id, "quantity": "1"}, {"input_id": x["kcl"].id, "quantity": "99"}],
                         farm_id=farm_id, season_id=season_id, plots=plots)
    items = {i["name"]: i for i in ih.build_stock_items(repo, farm_id, date.today())}
    assert items["Cloreto de potássio"]["available"] == 6  # nada da primeira linha ficou gravado


def test_register_fuelings_takes_diesel_updates_meter_and_blocks_going_back(hub):
    env, x = hub
    repo = env.routes._repository(env.db)
    farm_id, season_id = env.scope["active_farm_id"], env.scope["active_season_id"]
    assets = {x["trator"].id: x["trator"]}
    base = {"asset_id": str(x["trator"].id), "source": "estoque", "input_id": str(x["diesel"].id)}
    created = ih.register_fuelings(repo, env.db, [
        {**base, "liters": "110", "meter": "1.229", "date": "2026-11-17"},
        {**base, "liters": "120", "meter": "1.268", "date": "2026-11-21"},
    ], assets=assets, farm_id=farm_id, season_id=season_id)
    assert [float(f.meter_reading) for f in created] == [1229.0, 1268.0]
    output = env.db.get(StockOutput, created[1].stock_output_id)
    assert output.reference_type == ih.FUELING_REFERENCE and output.reference_id == created[1].id
    assert float(created[1].total_cost) == 720.0
    diesel = next(i for i in ih.build_stock_items(repo, farm_id, date.today()) if i["name"] == "Diesel S10")
    assert diesel["available"] == 190
    assert ih.fueling_rate(created[1], created[0])["label"] == "3,1 L/h"

    with pytest.raises(ValueError, match="não pode ser menor"):
        ih.register_fuelings(repo, env.db, [{**base, "liters": "10", "meter": "1.100"}], assets=assets, farm_id=farm_id, season_id=season_id)
    with pytest.raises(ValueError, match="Estoque insuficiente"):
        ih.register_fuelings(repo, env.db, [{**base, "liters": "500", "meter": "1.300"}], assets=assets, farm_id=farm_id, season_id=season_id)
    assert env.db.query(EquipmentFueling).count() == 2

    posto = ih.register_fuelings(repo, env.db, [{"asset_id": str(x["trator"].id), "source": "posto", "price": "6,10", "liters": "50", "meter": "1300"}],
                                 assets=assets, farm_id=farm_id, season_id=season_id)
    assert posto[0].stock_output_id is None and float(posto[0].total_cost) == 305.0


def test_overview_alerts_overdue_revision_low_stock_and_expiring_lot(hub):
    env, x = hub
    repo = env.routes._repository(env.db)
    ih.register_fuelings(repo, env.db, [{"asset_id": str(x["trator"].id), "source": "estoque", "input_id": str(x["diesel"].id), "liters": "100", "meter": "1268"}],
                         assets={x["trator"].id: x["trator"]}, farm_id=env.scope["active_farm_id"], season_id=env.scope["active_season_id"])
    ov = ih.build_overview(repo, env.db, farm_id=env.scope["active_farm_id"], farm_ids=[env.scope["active_farm_id"]], season=None, today=date.today())
    titles = [a["title"] for a in ov["alerts"]]
    assert titles[0] == "Revisão de 250 h · Trator 4x4"
    assert ov["alerts"][0]["detail"] == "Manutenção 18 h atrasada."
    assert "Cloreto de potássio abaixo do mínimo" in titles
    assert any(t.startswith("Fungicida azoxistrobina: lote vence em") for t in titles)
    assert any(a["title"] == "Hora de contar o estoque do galpão" for a in ov["alerts"])
    machines = {c["name"]: c for c in ov["machines"]}
    assert machines["Trator 4x4"]["meter"] == 1268 and machines["Trator 4x4"]["is_machine"]
    assert not machines["Galpão"]["is_machine"]
    assert ov["feed"][0]["kind"] in {"Abasteceu", "Entrada"}


def test_overview_page_renders_alerts_shortcuts_and_drawers(hub):
    env, x = hub
    resp = env.routes.inputs_overview_page(env.request("/insumos"), db=env.db, user=env.user, csrf_token="x")
    html = resp.body.decode()
    assert "Precisa de você" in html and "Cloreto de potássio abaixo do mínimo" in html
    assert 'data-io-open="use"' in html and 'data-io-open="fuel"' in html
    data = json.loads(html.split("data-io-data>")[1].split("</script>")[0])
    assert {i["name"] for i in data["items"]} >= {"Luva nitrílica", "Diesel S10"}
    assert [m["name"] for m in data["machines"]] == ["Trator 4x4"]
    assert any(i["fuel"] for i in data["items"])


def test_use_route_registers_lines_and_reports_errors(hub):
    env, x = hub
    lines = json.dumps([{"input_id": x["kcl"].id, "plot_id": str(x["plot"].id), "quantity": "3"}])
    resp = env.routes.register_inputs_use_action(env.request("/insumos/uso"), csrf_token="x", lines=lines, return_to="/insumos/estoque", db=env.db, user=env.user)
    assert resp.status_code == 303 and resp.headers["location"] == "/insumos/estoque"
    assert env.session["flash"]["kind"] == "success"
    resp = env.routes.register_inputs_use_action(env.request("/insumos/uso"), csrf_token="x", lines="{nao-json", return_to="https://evil.com", db=env.db, user=env.user)
    assert resp.headers["location"] == "/insumos" and env.session["flash"]["kind"] == "error"


def test_fueling_route_ignores_non_machine_assets(hub):
    env, x = hub
    lines = json.dumps([{"asset_id": str(x["galpao"].id), "liters": "10", "source": "posto"}])
    env.routes.register_fuelings_action(env.request("/insumos/patrimonio/abastecimentos"), csrf_token="x", lines=lines, return_to=None, db=env.db, user=env.user)
    assert env.session["flash"]["message"] == "escolha a máquina."


def test_asset_sheet_page_shows_timeline_plans_and_costs(hub):
    env, x = hub
    repo = env.routes._repository(env.db)
    base = {"asset_id": str(x["trator"].id), "source": "estoque", "input_id": str(x["diesel"].id)}
    ih.register_fuelings(repo, env.db, [{**base, "liters": "110", "meter": "1229", "date": "2026-11-17"}, {**base, "liters": "120", "meter": "1268", "date": "2026-11-21"}],
                         assets={x["trator"].id: x["trator"]}, farm_id=env.scope["active_farm_id"], season_id=env.scope["active_season_id"])
    resp = env.routes.equipment_asset_sheet_page(x["trator"].id, env.request(f"/insumos/patrimonio/{x['trator'].id}/ficha"), db=env.db, user=env.user, csrf_token="x")
    html = resp.body.decode()
    assert "Trator 4x4" in html and "Abasteceu 120 L" in html and "3,1 L/h" in html
    assert "Revisão de 250 h" in html and "18 h atrasada" in html
    assert "data-io-machine" in html


def test_stock_page_lists_supplies_with_item_panel_and_use_shortcut(hub):
    env, x = hub
    resp = env.routes.stock_page(env.request(f"/insumos/estoque?item={x['luva'].id}"), db=env.db, user=env.user, csrf_token="x")
    html = resp.body.decode()
    assert f'data-io-stock-row="{x["luva"].id}"' in html and 'data-io-chip="sup"' in html
    assert f'data-io-open="use" data-io-item="{x["luva"].id}"' in html
    assert 'class="io-row is-on" data-io-fitem data-group="sup"' in html
    assert "Lote vencendo" in html and "Abaixo do mínimo" in html
    assert 'href="/insumos/estoque?aba=contagens"' in html and 'data-io-drawer="use"' in html


def test_assets_page_shows_cards_agenda_and_fuel_drawer(hub):
    env, x = hub
    repo = env.routes._repository(env.db)
    ih.register_fuelings(repo, env.db, [{"asset_id": str(x["trator"].id), "source": "estoque", "input_id": str(x["diesel"].id), "liters": "100", "meter": "1268"}],
                         assets={x["trator"].id: x["trator"]}, farm_id=env.scope["active_farm_id"], season_id=env.scope["active_season_id"])
    resp = env.routes.equipment_assets_page(env.request("/insumos/patrimonio"), edit_id=None, farm_id=None, status=None, db=env.db, user=env.user, csrf_token="x")
    html = resp.body.decode()
    assert f'href="/insumos/patrimonio/{x["trator"].id}/ficha"' in html
    assert "Próximas manutenções" in html and "18 h atrasada" in html
    assert 'data-io-chip="Benfeitoria"' in html and "1.268 h" in html
    assert 'data-io-drawer="fuel"' in html


def test_sidebar_groups_inputs_in_three_items():
    from pathlib import Path
    html = Path("app/templates/partials/sidebar.html").read_text()
    block = html[html.index("'id': 'insumos'"):html.index("'id': 'financeiro'")]
    assert [label for label in ("Visão geral", "Estoque", "Máquinas e bens") if label in block] == ["Visão geral", "Estoque", "Máquinas e bens"]
    assert "'Compras'" not in block and "Suprimentos" not in block and "Manutenções" not in block
