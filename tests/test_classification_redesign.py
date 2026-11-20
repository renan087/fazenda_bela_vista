import asyncio
import json
from datetime import date

from starlette.datastructures import FormData

from app.models.coffee_classification import CoffeeClassification
from app.models.coffee_sensory_profile import CoffeeSensoryProfile
from app.services.coffee_classification import (attribute_rows, attribute_usage, default_scale, radar_points,
                                                 save_attributes, score_band)
from tests.test_coffee_sales import env  # noqa: F401  (fixture)


def _post(env, handler, fields, *args):
    req = env.request("/producao/classificacao")

    async def form():
        return FormData([("csrf_token", "x"), *fields])

    req.form = form
    return asyncio.run(handler(*args, req, env.db, env.user))


def _scope_harvests(env, monkeypatch):
    monkeypatch.setattr(env.routes, "_classification_harvests", lambda repo, scope: list(env.lots))


def test_score_band_and_radar():
    assert score_band(84.5) == ("Especial · excelente", "top")
    assert score_band(80) == ("Especial", "ok")
    assert score_band(78)[1] == "std" and score_band(None)[1] == "none"
    assert radar_points([10, 10, 10], 10, cx=0, cy=0, radius=10).startswith("0.0,-10.0")
    assert radar_points([5, 5], 10) == ""


def test_create_physical_only_analysis_skips_cup_and_keeps_new_fields(env, monkeypatch):
    _scope_harvests(env, monkeypatch)
    lot = env.lots[0]
    response = _post(env, env.routes.create_coffee_classification, [
        ("harvest_id", str(lot.id)), ("analysis_date", "2026-11-10"), ("analysis_kind", "fisica"),
        ("beverage", "Duro"), ("coffee_type", "Tipo 6"), ("sieve", "16 acima"), ("moisture", "11,5"),
        ("defects", "86"), ("broca_percentage", "1,8"), ("sample_number", "2026-0457"),
        ("sensory_score", "90"), ("sensory_aroma", "9"), ("flavor", "Chocolate")])
    analysis = env.db.query(CoffeeClassification).one()
    assert response.headers["location"] == f"/producao/classificacao/{analysis.id}"
    assert analysis.analysis_kind == "fisica" and analysis.method == "Classificação física" and analysis.source == "informado"
    assert analysis.sensory_score is None and analysis.sensory_profile is None
    assert float(analysis.broca_percentage) == 1.8 and analysis.sample_number == "2026-0457"


def test_create_cup_analysis_with_flavors_then_detail_and_compare(env, monkeypatch):
    _scope_harvests(env, monkeypatch)
    a, b = env.lots
    for lot, score in ((a, "82"), (b, "84,5")):
        _post(env, env.routes.create_coffee_classification, [
            ("harvest_id", str(lot.id)), ("analysis_date", "2026-11-10"), ("analysis_kind", "ambos"),
            ("beverage", "Mole"), ("sensory_score", score), ("sensory_scale", "10"),
            ("sensory_aroma", "8"), ("sensory_sabor", "7,5"), ("sensory_acidez", "7"),
            ("flavor", "Chocolate"), ("flavor", "Caramelo")])
    first = env.db.query(CoffeeClassification).filter_by(harvest_id=a.id).one()
    assert first.flavors == ["Chocolate", "Caramelo"] and first.sensory_profile.attributes["sabor"] == 7.5
    html = env.routes.coffee_classification_detail_page(first.id, env.request(f"/producao/classificacao/{first.id}"),
                                                        db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Prova de xícara" in html and "Chocolate" in html and "Especial" in html and "<polygon" in html
    page = env.routes.coffee_classification_page(env.request("/producao/classificacao"), db=env.db, user=env.user,
                                                 csrf_token="x").body.decode()
    assert "Qualidade por lote" in page and "Melhor lote" in page and "L-011" in page and "84,5" in page
    especiais = env.routes.coffee_classification_page(env.request("/producao/classificacao?filtro=comerciais"), db=env.db,
                                                      user=env.user, csrf_token="x").body.decode()
    assert "Nenhum lote neste filtro" in especiais
    cmp = env.routes.coffee_classification_compare_page(env.request(f"/producao/classificacao/comparar?lotes={a.id},{b.id}"),
                                                        db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Comparar lotes" in cmp and 'class="is-best"' in cmp and "Aroma (0–10)" in cmp


def test_error_rerenders_form_with_values(env, monkeypatch):
    _scope_harvests(env, monkeypatch)
    lot = env.lots[0]
    response = _post(env, env.routes.create_coffee_classification, [
        ("harvest_id", str(lot.id)), ("analysis_date", "2030-01-01"), ("analysis_kind", "ambos"),
        ("evaluator", "Lab. Café do Sul"), ("beverage", "Mole")])
    html = response.body.decode()
    assert response.status_code == 422 and "não pode ser futura" in html
    assert 'value="Lab. Café do Sul"' in html and 'value="Mole" checked' in html
    assert f'name="harvest_id" value="{lot.id}" data-code="L-016" checked' in html


def test_legacy_query_params_redirect(env):
    r = env.routes
    assert r.coffee_classification_page(env.request("/producao/classificacao?harvest_id=5&novo=1&origem=producao"), db=env.db,
                                        user=env.user, csrf_token="x").headers["location"] == "/producao/classificacao/nova?lote=5&origem=producao"
    assert r.coffee_classification_page(env.request("/producao/classificacao?edit_id=7"), db=env.db, user=env.user,
                                        csrf_token="x").headers["location"] == "/producao/classificacao/7/editar"


def test_attributes_order_hint_scale_and_safe_delete(env, monkeypatch):
    _scope_harvests(env, monkeypatch)
    db, org = env.db, env.user.organization_id
    rows = attribute_rows(db, org)
    items = [{"key": r["key"], "label": r["label"], "hint": "", "active": r["key"] in {"sabor", "aroma", "acidez"}} for r in rows]
    items.insert(0, items.pop(1))  # sabor primeiro
    items[0]["hint"] = "gosto na boca"
    items.append({"key": "", "label": "Limpeza extra", "hint": "", "active": True})
    save_attributes(db, org, {"items": items, "scale": 5, "deleted": []}, can_delete=True)
    db.commit()
    rows = attribute_rows(db, org)
    assert rows[0]["key"] == "sabor" and rows[0]["hint"] == "gosto na boca" and default_scale(db, org) == 5
    assert [r["label"] for r in rows if r["active"]][-1] == "Limpeza extra"

    analysis = CoffeeClassification(harvest_id=env.lots[0].id, analysis_date=date(2026, 11, 1), source="informado")
    analysis.sensory_profile = CoffeeSensoryProfile(scale_max=10, attributes_json=json.dumps({"aroma": {"label": "Aroma", "value": 8}}))
    db.add(analysis)
    db.commit()
    assert attribute_usage(db, org).get("aroma") == 1
    keep = [{"key": r["key"], "label": r["label"], "hint": r["hint"], "active": r["active"]} for r in attribute_rows(db, org)]
    try:
        save_attributes(db, org, {"items": [i for i in keep if i["key"] != "aroma"], "deleted": ["aroma"]}, can_delete=True)
        raise AssertionError("deveria recusar excluir atributo usado")
    except ValueError as exc:
        assert "já foi usado" in str(exc)
    db.rollback()
    custom = next(r for r in attribute_rows(db, org) if r["label"] == "Limpeza extra")
    save_attributes(db, org, {"items": [i for i in keep if i["key"] != custom["key"]], "deleted": [custom["key"]]}, can_delete=True)
    db.commit()
    assert all(r["label"] != "Limpeza extra" for r in attribute_rows(db, org))

    req = env.request("/x")

    async def form():
        return FormData([("csrf_token", "x"), ("payload", json.dumps({"items": [{"key": "sabor", "label": "Sabor", "active": False}]}))])

    req.form = form
    response = asyncio.run(env.routes.save_coffee_sensory_attributes(req, db, env.user))
    assert response.status_code == 422 and "pelo menos um atributo ativo" in response.body.decode()
