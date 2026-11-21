import asyncio
import json
from datetime import date

from app.models.equipment_fueling import EquipmentFueling
from app.models.stock_count_draft import StockCountDraft
from app.models.stock_output import StockOutput
from app.services import inputs_hub as ih
from tests.test_inputs_hub import env, hub  # noqa: F401


def _page(env, url, **params):
    return env.routes.stock_page(env.request(url), db=env.db, user=env.user, csrf_token="x", **params).body.decode()


def _use(env, x, qty="3"):
    repo = env.routes._repository(env.db)
    return ih.register_uses(repo, [{"input_id": x["kcl"].id, "plot_id": str(x["plot"].id), "quantity": qty, "notes": "cobertura"}],
                            farm_id=env.scope["active_farm_id"], season_id=env.scope["active_season_id"], plots={x["plot"].id: x["plot"]})[0]


def _fuel(env, x):
    repo = env.routes._repository(env.db)
    return ih.register_fuelings(repo, env.db, [{"asset_id": str(x["trator"].id), "source": "estoque", "input_id": str(x["diesel"].id), "liters": "100", "meter": "1268"}],
                                assets={x["trator"].id: x["trator"]}, farm_id=env.scope["active_farm_id"], season_id=env.scope["active_season_id"])[0]


def test_movements_tab_groups_by_day_with_balance_and_filters(hub):
    env, x = hub
    _use(env, x)
    _fuel(env, x)
    html = _page(env, "/insumos/estoque?aba=movimentacoes", aba="movimentacoes", periodo="safra")
    assert "Saiu para a lavoura" in html and "Combustível nas máquinas" in html
    assert "3 sc de Cloreto de potássio" in html and "fica 5 sc" in html
    assert "100 L de Diesel S10" in html and "Trator 4x4 · horímetro 1.268 h" in html
    assert 'data-io-fix=' in html and "/desfazer" in html
    only_fuel = _page(env, "/insumos/estoque", aba="movimentacoes", periodo="safra", tipo="abast")
    assert "100 L de Diesel S10" in only_fuel and "3 sc de Cloreto de potássio" not in only_fuel
    found = _page(env, "/insumos/estoque", aba="movimentacoes", periodo="safra", busca="cobertura")
    assert "3 sc de Cloreto de potássio" in found and "100 L de Diesel" not in found


def test_day_labels_and_periods():
    today = date(2026, 11, 21)
    assert ih.day_label(today, today) == "Hoje · sábado, 21 de novembro"
    assert ih.day_label(date(2026, 11, 17), today) == "Terça, 17 de novembro"
    key, start, end, label = ih.period_range("7", None, today)
    assert (key, start, label) == ("7", date(2026, 11, 15), "últimos 7 dias")
    key, start, end, label = ih.period_range("personalizado", None, today, "2026-10-01", "2026-10-31")
    assert key == "personalizado" and label == "01/10/2026 a 31/10/2026"


def test_undo_use_and_fueling_returns_stock(hub):
    env, x = hub
    use = _use(env, x)
    fueling = _fuel(env, x)
    output_id = fueling.stock_output_id
    env.routes.undo_stock_movement_action(use.id, env.request("/x"), csrf_token="x", db=env.db, user=env.user)
    assert env.session["flash"]["kind"] == "success"
    env.routes.undo_stock_movement_action(output_id, env.request("/x"), csrf_token="x", db=env.db, user=env.user)
    assert "Abastecimento desfeito" in env.session["flash"]["message"]
    env.db.expire_all()
    items = {i["name"]: i for i in ih.build_stock_items(env.routes._repository(env.db), env.scope["active_farm_id"], date.today())}
    assert items["Cloreto de potássio"]["available"] == 8 and items["Diesel S10"]["available"] == 420
    assert env.db.query(EquipmentFueling).count() == 0
    assert env.db.get(StockOutput, output_id) is None


def test_correct_use_replaces_old_output_in_one_step(hub):
    env, x = hub
    use = _use(env, x, qty="3")
    lines = json.dumps([{"input_id": x["kcl"].id, "plot_id": str(x["plot"].id), "quantity": "5"}])
    env.routes.register_inputs_use_action(env.request("/insumos/uso"), csrf_token="x", lines=lines, return_to="/insumos/estoque?aba=movimentacoes",
                                          replace_output_id=str(use.id), db=env.db, user=env.user)
    assert env.session["flash"]["message"].startswith("Uso corrigido")
    env.db.expire_all()
    manual = env.db.query(StockOutput).filter(StockOutput.reference_type == "manual_stock_output").all()
    assert [float(o.quantity) for o in manual] == [5.0]
    kcl = next(i for i in ih.build_stock_items(env.routes._repository(env.db), env.scope["active_farm_id"], date.today()) if i["name"] == "Cloreto de potássio")
    assert kcl["available"] == 3
    # corrigir para mais do que existe não perde o uso original
    bad = json.dumps([{"input_id": x["kcl"].id, "quantity": "50"}])
    new_use = env.db.query(StockOutput).filter(StockOutput.reference_type == "manual_stock_output").one()
    env.routes.register_inputs_use_action(env.request("/insumos/uso"), csrf_token="x", lines=bad, return_to=None,
                                          replace_output_id=str(new_use.id), db=env.db, user=env.user)
    assert env.session["flash"]["kind"] == "error"
    env.db.expire_all()
    assert env.db.get(StockOutput, new_use.id) is not None


def test_count_save_then_apply_creates_adjustments(hub):
    env, x = hub
    farm_id = env.scope["active_farm_id"]

    def post(*pairs):
        req = env.request("/insumos/estoque/contagem", form=list(pairs))
        return asyncio.run(env.routes.stock_count_action(req, db=env.db, user=env.user))

    post(("counted_%d" % x["kcl"].id, "6"), ("acao", "salvar"))
    draft = env.db.query(StockCountDraft).filter_by(farm_id=farm_id).one()
    assert json.loads(draft.counts) == {str(x["kcl"].id): "6"}
    html = _page(env, "/insumos/estoque?aba=contagens", aba="contagens")
    assert 'name="counted_%d" value="6"' % x["kcl"].id in html and "Contagem em andamento" in html
    assert "1 de " in html and "− 2 sc" in html
    post(("counted_%d" % x["kcl"].id, "6"), ("counted_%d" % x["luva"].id, "14"), ("acao", "aplicar"), ("data", "2026-11-21"))
    assert "1 ajuste foi registrado" in env.session["flash"]["message"]
    assert env.db.query(StockCountDraft).count() == 0
    kcl = next(i for i in ih.build_stock_items(env.routes._repository(env.db), farm_id, date.today()) if i["name"] == "Cloreto de potássio")
    assert kcl["available"] == 6
    html = _page(env, "/insumos/estoque?aba=contagens", aba="contagens")
    assert "1 ajuste" in html


def test_legacy_links_land_on_new_views(hub):
    env, x = hub
    use = _use(env, x)
    resp = env.routes.edit_stock_output_entry(use.id, env.request("/x"), db=env.db, user=env.user)
    assert resp.headers["location"] == f"/insumos/estoque?aba=movimentacoes&periodo=safra&mov=o-{use.id}"
    assert 'aria-current="page">Contagens' in _page(env, "/insumos/estoque?stock_tab=inventories")


def test_movements_paginate_15_by_default_and_allow_bigger_pages(hub):
    env, x = hub
    repo = env.routes._repository(env.db)
    ih.register_uses(repo, [{"input_id": x["luva"].id, "quantity": "0,5"} for _ in range(20)],
                     farm_id=env.scope["active_farm_id"], season_id=env.scope["active_season_id"], plots={})
    html = _page(env, "/insumos/estoque", aba="movimentacoes", periodo="safra", tipo="uso")
    assert html.count('class="io-mv"') == 15 and "Mostrando 1–15 de 20" in html and "Página 1 de 2" in html
    html = _page(env, "/insumos/estoque", aba="movimentacoes", periodo="safra", tipo="uso", pagina=2)
    assert html.count('class="io-mv"') == 5 and "Mostrando 16–20 de 20" in html
    html = _page(env, "/insumos/estoque", aba="movimentacoes", periodo="safra", tipo="uso", por=30)
    assert html.count('class="io-mv"') == 20 and '<option value="30" selected>' in html
    assert _page(env, "/insumos/estoque", aba="movimentacoes", periodo="safra", tipo="uso", por=999).count('class="io-mv"') == 15


def test_count_history_card_and_absurd_quantities(hub):
    env, x = hub

    def post(*pairs):
        req = env.request("/insumos/estoque/contagem", form=list(pairs))
        return asyncio.run(env.routes.stock_count_action(req, db=env.db, user=env.user))

    post(("counted_%d" % x["kcl"].id, "151.515.151.515,00"), ("acao", "aplicar"))
    assert "grande demais" in env.session["flash"]["message"]
    post(("counted_%d" % x["kcl"].id, "8,00"), ("acao", "aplicar"))
    html = _page(env, "/insumos/estoque", aba="contagens")
    assert "built-in method" not in html and " · 1 item</span>" in html and "tudo bateu" in html
    sheet = ih.count_sheet([{"id": 1, "name": "X", "unit": "L", "available": 5, "available_label": "5 L", "value": 50,
                             "group_label": "G", "category": "C"}], {"1": "151515151515"})
    assert sheet["rows"][0]["status"] == "todo"
