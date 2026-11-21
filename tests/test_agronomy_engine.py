"""Motor agronômico v3: os casos reproduzem os exemplos resolvidos das próprias referências."""
import json
from datetime import date
from types import SimpleNamespace

import pytest

from app.core.permissions_catalog import PAGE_AGENDA, PAGE_INPUTS, PAGE_IRRIGATION, PAGE_OPERATIONS
from app.models import (
    AgronomicProfile, FertilizationRecord, FertilizationSchedule, InputCatalog, Plot, PurchasedInput, SoilAnalysis,
)
from app.models.coffee_variety import CoffeeVariety
from app.models.fertilization_item import FertilizationItem
from app.models.fertilization_schedule_item import FertilizationScheduleItem
from app.services import agronomy, openai_service as service
from app.services.agronomy import calc, products
from app.services.agronomy.context import gather
from app.services.agronomy.engine import build_plan
from app.services.agronomy.soil import read_soil, texture_for
from tests.test_stock_assistant import _route_env

TODAY = date(2026, 8, 10)
PAGES = {PAGE_AGENDA, PAGE_INPUTS, PAGE_IRRIGATION, PAGE_OPERATIONS}
UNITS = {"organic_matter": "%", "phosphorus": "mg/dm³", "potassium": "mg/dm³", "calcium": "cmolc/dm³",
         "magnesium": "cmolc/dm³", "aluminum": "cmolc/dm³", "h_al": "cmolc/dm³", "ctc": "cmolc/dm³",
         "base_saturation": "%", "clay": "%", "p_rem": "mg/L", "boron": "mg/dm³", "zinc": "mg/dm³",
         "copper": "mg/dm³", "manganese": "mg/dm³"}


def _metadata(depth=(0, 20), extra=None, methods=None):
    return json.dumps({"units": UNITS, "methods": {"phosphorus": "Mehlich-1", "boron": "Água quente",
                                                   "micronutrients": "Mehlich-1", **(methods or {})},
                       "extra_values": extra or {}, "collected": "2026-07-01",
                       "depth_from_cm": depth[0], "depth_to_cm": depth[1]})


@pytest.fixture
def farm(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    farm_id = scope["active_farm_id"]
    variety = CoffeeVariety(name="Catuaí", species="Arabica", organization_id=user.organization_id)
    db.add(variety)
    db.flush()
    plot = Plot(name="Talhão 1", farm_id=farm_id, area_hectares=2, plant_count=7000, planting_date=date(2015, 11, 1),
                estimated_yield_sacks=130, variety_id=variety.id, soil_texture="medio")
    lime = InputCatalog(name="Calcário", normalized_name="calcario", category="Corretivos de Solo", default_unit="kg")
    urea = InputCatalog(name="Ureia", normalized_name="ureia", category="Fertilizantes", default_unit="kg")
    db.add_all([plot, lime, urea, AgronomicProfile(farm_id=farm_id, culture="Café", region="Sul de Minas")])
    db.flush()
    # Exemplo resolvido do Manual do Café (p. 38): camada 0–20 cm.
    analysis = SoilAnalysis(
        farm_id=farm_id, plot_id=plot.id, analysis_date=date(2026, 7, 10), laboratory="Lab",
        ph=5.42, organic_matter=1.43, phosphorus=1.5, potassium=38, calcium=0.62, magnesium=0.16, aluminum=0.48,
        h_al=3.70, ctc=4.58, base_saturation=19.21,
        laboratory_metadata=_metadata(extra={"clay": 26, "p_rem": 29.5, "boron": 0.06, "zinc": 0.30,
                                             "copper": 0.10, "manganese": 5.30}),
    )
    # Mesmo exemplo (p. 40): camada 20–40 cm.
    subsoil = SoilAnalysis(
        farm_id=farm_id, plot_id=plot.id, analysis_date=date(2026, 7, 10), laboratory="Lab", ph=5.3, phosphorus=4,
        potassium=27, calcium=0.45, magnesium=0.12, aluminum=0.55, h_al=3.60, ctc=4.24, base_saturation=15.1,
        laboratory_metadata=_metadata(depth=(20, 40), extra={"clay": 30, "p_rem": 15}),
    )
    db.add_all([analysis, subsoil])
    for catalog, name, notes in ((lime, "Calcário dolomítico", "PRNT 87%"), (urea, "Ureia granulada", "")):
        db.add(PurchasedInput(farm_id=farm_id, input_id=catalog.id, name=name, quantity_purchased=1, package_size=1000,
                              package_unit="kg", unit_price=1, total_quantity=1000, available_quantity=1000,
                              total_cost=1, notes=notes))
    db.commit()
    yield SimpleNamespace(routes=routes, db=db, user=user, plot=plot, analysis=analysis, subsoil=subsoil, farm_id=farm_id)
    db.close()


def _plan(farm, today=TODAY, pages=PAGES):
    return build_plan(gather(farm.db, farm.analysis, today, pages))


def test_reading_derives_soil_chemistry_like_the_manual(farm):
    reading = read_soil(farm.analysis)
    assert reading.values["sb"] == pytest.approx(0.88, abs=0.01)
    assert reading.values["t_effective"] == pytest.approx(1.36, abs=0.01)
    assert reading.values["m"] == pytest.approx(35.29, abs=0.2)
    assert reading.values["organic_matter"] == pytest.approx(1.43)


def test_liming_matches_manual_worked_example(farm):
    plan = _plan(farm)
    lime = next(item for item in plan["corretivos"] if item["tipo"] == "calagem")
    steps = " ".join(lime["calculo"]["passos"])
    assert lime["nc_t_ha"] == pytest.approx(1.88, abs=0.01)          # menor entre 1,88 (V%) e 2,39 (Al)
    assert "2,39" in steps and "1,24" in steps                       # Y por P-rem interpolado = 1,24
    # Com gessagem recomendada, mantém-se a camada de 0–20 cm (PF = 20) e corrige pelo PRNT 87 % do estoque.
    assert lime["dose_t_ha"] == pytest.approx(1.88 * 100 / 87, abs=0.02)
    assert lime["tipo_calcario"].startswith("Calcário dolomítico")
    assert lime["calculo"]["referencia"]["id"] == "manual_cafe_mg"


def test_gypsum_by_prem_matches_manual(farm):
    gyp = next(item for item in _plan(farm)["corretivos"] if item["tipo"] == "gessagem")
    assert gyp["necessario"] is True
    assert gyp["dose_t_ha"] == pytest.approx(0.85, abs=0.01)         # Manual p. 42: 850 kg/ha
    assert "m no subsolo" in gyp["calculo"]["passos"][0]


def test_npk_follows_production_tables(farm):
    needs = {n["nutriente"]: n for n in _plan(farm)["necessidades"]}
    assert needs["N"]["dose_kg_ha"] == 450        # 65 sc/ha, sem análise foliar
    assert needs["K2O"]["dose_kg_ha"] == 450      # K 38 mg/dm³ = baixo
    assert needs["P2O5"]["dose_kg_ha"] == 80      # P 1,5 com P-rem 29,5 = muito baixo
    assert needs["N"]["calculo"]["referencia"]["criterio"].startswith("Tabela 15")
    # O S do gesso (850 kg × 13 % ≈ 110 kg/ha) já cobre N/8 (56 kg/ha): nenhuma dose extra de S.
    assert "S" not in needs
    farm.subsoil.laboratory_metadata = None
    farm.db.commit()
    needs = {n["nutriente"]: n for n in _plan(farm)["necessidades"]}
    assert needs["S"]["dose_kg_ha"] == pytest.approx(450 / 8, abs=0.1)  # sem análise de S: 1/8 do N


def test_products_cover_needs_without_exceeding_and_convert_per_plant(farm):
    plan = _plan(farm)
    supplied = {}
    for product in plan["produtos"]:
        for nutrient, pct in product["garantia"].items():
            if product["kg_ha"]:
                supplied[nutrient] = supplied.get(nutrient, 0) + product["kg_ha"] * pct / 100
    for need in plan["necessidades"]:
        assert supplied[need["label"]] == pytest.approx(need["saldo_kg_ha"], rel=0.02)
    urea = next(p for p in plan["produtos"] if p["produto"] == "Ureia granulada")
    assert urea["origem"] == "estoque" and urea["estoque"]["situacao"].startswith("Faltam")
    assert urea["g_planta"] == pytest.approx(urea["kg_ha"] * 1000 / 3500, rel=0.01)
    assert urea["total_kg"] == pytest.approx(urea["kg_ha"] * 2, rel=0.01)
    assert not any("mais" in alert for alert in plan["alertas"])


def test_schedule_splits_rainy_season_and_respects_liming_gap(farm):
    plan = _plan(farm)
    kinds = [event["chave"] for event in plan["cronograma"]]
    assert kinds[:2] == ["calagem", "gessagem"]
    fertilizations = [e for e in plan["cronograma"] if e["chave"] == "adubacao"]
    assert len(fertilizations) == 4                       # N > 250 kg/ha → 4 parcelas
    # Calcário em 17/ago: a 1ª parcela (modelo 15/out) é adiada para cumprir 60 dias de intervalo.
    assert fertilizations[0]["data"] == "2026-10-16"
    assert not [c for c in plan["compatibilidade"] if "dias da calagem" in c["texto"]]
    first_n = sum(i["kg_ha"] for i in fertilizations[0]["itens"] if i["fonte"] == "estoque")
    last_n = sum(i["kg_ha"] for i in fertilizations[-1]["itens"] if i["fonte"] == "estoque")
    assert first_n == pytest.approx(last_n, rel=0.01)
    foliar = [e for e in plan["cronograma"] if e["chave"] == "foliar"]
    assert len(foliar) == 3 and {i["produto"] for i in foliar[0]["itens"]} >= {"Sulfato de zinco", "Sulfato de cobre"}
    boron = next(i for i in fertilizations[0]["itens"] if i["produto"] == "Ácido bórico")
    assert boron["kg_ha"] == pytest.approx(3 / 0.17, rel=0.01)  # B baixo (água quente) = 3 kg/ha


def test_history_and_agenda_are_discounted(farm):
    db = farm.db
    record = FertilizationRecord(plot_id=farm.plot.id, product="NPK", dose="-", cost=0, application_date=date(2026, 7, 20))
    record.items.append(FertilizationItem(name="NPK 20-05-20", unit="kg", quantity_per_hectare=500, total_quantity=1000))
    db.add(record)
    db.add(FertilizationSchedule(plot_id=farm.plot.id, scheduled_date=date(2026, 11, 1), status="scheduled",
                                 items=[FertilizationScheduleItem(name="Cloreto de potássio", quantity=200, unit="kg")]))
    db.commit()
    plan = _plan(farm)
    needs = {n["nutriente"]: n for n in plan["necessidades"]}
    assert needs["N"]["aplicado_kg_ha"] == pytest.approx(100)      # 1000 kg × 20 % ÷ 2 ha
    assert needs["K2O"]["agendado_kg_ha"] == pytest.approx(60)     # 200 kg × 60 % ÷ 2 ha
    assert needs["K2O"]["saldo_kg_ha"] == pytest.approx(450 - 100 - 60)
    assert any(e["status"] == "agendado" for e in plan["cronograma"])


def test_missing_data_blocks_only_dependent_doses(farm):
    farm.plot.estimated_yield_sacks = None
    farm.plot.soil_texture = None
    farm.analysis.laboratory_metadata = _metadata(methods={"micronutrients": "Não informado"}, extra={"zinc": 0.3})
    farm.db.commit()
    plan = _plan(farm, pages=set())
    pending = {p["dado"] for p in plan["pendencias"]}
    assert "Produtividade esperada (sc/ha)" in pending
    assert "Extrator de Zn no laudo" in pending
    assert not plan["necessidades"] and not plan["micronutrientes"]
    lime = next(item for item in plan["corretivos"] if item["tipo"] == "calagem")
    assert lime["necessario"] and lime["dose_prnt100_t_ha"] > 0     # calagem segue calculável
    assert lime["dose_t_ha"] is None and "PRNT do calcário" in pending  # sem estoque, sem PRNT inventado


def test_unit_validation_rejects_implausible_values(farm):
    farm.analysis.potassium = 0.09
    farm.analysis.laboratory_metadata = _metadata()
    farm.db.commit()
    plan = _plan(farm)
    assert any("cmolc/dm³" in alert for alert in plan["alertas"])
    assert "K2O" not in {n["nutriente"] for n in plan["necessidades"]}
    assert to_canonical_ok()


def to_canonical_ok():
    from app.services.agronomy.soil import to_canonical
    return (to_canonical("potassium", 0.1, "cmolc/dm³") == pytest.approx(39.1)
            and to_canonical("calcium", 25, "mmolc/dm³") == 2.5 and to_canonical("phosphorus", 3, "mg/kg") is None)


def test_conilon_uses_incaper_tables(farm):
    farm.db.get(CoffeeVariety, farm.plot.variety_id).species = "Conilon"
    farm.plot.estimated_yield_sacks = 160  # 80 sc/ha
    farm.db.commit()
    plan = _plan(farm)
    needs = {n["nutriente"]: n for n in plan["necessidades"]}
    assert needs["N"]["dose_kg_ha"] == 440 and needs["K2O"]["dose_kg_ha"] == 410
    assert needs["N"]["calculo"]["referencia"]["id"] == "incaper_conilon"
    lime = next(item for item in plan["corretivos"] if item["tipo"] == "calagem")
    assert lime["dose_prnt100_t_ha"] == pytest.approx(4.58 * (70 - 19.21) / 100 * 0.5, abs=0.01)  # V2 70 %, p 0,5
    gyp = next(item for item in plan["corretivos"] if item["tipo"] == "gessagem")
    assert gyp["dose_t_ha"] == pytest.approx(0.3 * 4.24 * (60 - 15.1) / 100, abs=0.01)
    assert [e["finalidade"] for e in plan["cronograma"] if e["chave"] == "adubacao"]


def test_formation_converts_per_plant_doses(farm):
    farm.db.get(CoffeeVariety, farm.plot.variety_id).species = "Conilon"
    farm.plot.planting_date = date(2025, 3, 1)  # 17 meses → 1º ano de formação
    farm.db.commit()
    needs = {n["nutriente"]: n for n in _plan(farm)["necessidades"]}
    assert needs["N"]["dose_kg_ha"] == pytest.approx(15 * 3 * 3500 / 1000)
    assert needs["K2O"]["dose_kg_ha"] == pytest.approx(30 * 3 * 3500 / 1000)


def test_fertigation_schedule_and_compatibility(farm):
    farm.plot.irrigation_type = "gotejo"
    farm.db.commit()
    plan = _plan(farm)
    fertilizations = [e for e in plan["cronograma"] if e["chave"] == "adubacao"]
    assert len(fertilizations) == 8 and "Fertirrigação" in fertilizations[0]["metodo"]
    events = [{"quando": "x", "data_obj": TODAY, "itens": [{"fonte": "nitrato_calcio"}, {"fonte": "map"}]}]
    from app.services.agronomy.schedule import compatibility
    assert compatibility(events, {})[0]["nivel"] == "critico"


def test_guarantee_parsing_and_formula_fit():
    assert products.parse_guarantee("Adubo NPK 20-05-20")[0] == {"N": 20, "P2O5": 5, "K2O": 20}
    assert products.parse_guarantee("Fertilizante", "Garantia: 18% de P2O5")[0] == {"P2O5": 18}
    assert products.parse_guarantee("Ureia agrícola")[1].startswith("garantia típica")
    assert products.parse_prnt("Calcário PRNT 90%") == 90
    lot = products.StockLot("estoque:1", "NPK 20-05-20", 5000, "kg", {"N": 20, "P2O5": 5, "K2O": 20}, "rótulo")
    plan, _ = products.fit_npk({"N": 300, "P2O5": 40, "K2O": 300}, [lot], fertigation=False)
    supplied = {}
    for allocation in plan:
        for nutrient, value in allocation.supplies().items():
            supplied[nutrient] = supplied.get(nutrient, 0) + value
    assert plan[0].source.name == "NPK 20-05-20" and plan[0].kg_ha == pytest.approx(800)  # limitado pelo P
    assert all(supplied[n] == pytest.approx(v) for n, v in {"N": 300, "P2O5": 40, "K2O": 300}.items())


def test_safety_limit_blocks_absurd_doses():
    needs = [{"nutriente": "N", "dose_kg_ha": 900}]
    assert calc.check_safety(needs) and needs[0]["bloqueada"]


def test_ai_only_writes_text_and_cannot_change_doses(farm, monkeypatch):
    calls = []

    def parse(**kwargs):
        calls.append(kwargs)
        from app.services.agronomy.narrative import Narrative
        draft = Narrative.model_validate({
            "resumo": "O solo está ácido e pobre em potássio. Aplique 999 kg/ha de ureia.",
            "justificativas": [{"acao": "calagem", "texto": "Corrige a acidez."},
                               {"acao": "inventada", "texto": "Não deve aparecer."}],
            "orientacoes": ["Aplique com solo úmido.", "Use 5 t/ha de calcário."],
        })
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(parsed=draft))])

    monkeypatch.setattr("openai.OpenAI", lambda **kw: SimpleNamespace(
        beta=SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(parse=parse)))))
    monkeypatch.setattr(service, "get_settings", lambda: SimpleNamespace(
        openai_api_key="k", openai_recommendation_model="modelo-teste"))
    monkeypatch.setattr(service, "today_in_app_timezone", lambda: TODAY)
    result = service.gerar_recomendacao_adubacao(farm.db, farm.analysis, PAGES)
    plan = service.read_soil_recommendation(result["recommendation"])
    assert result["status"] == "generated" and "modelo-teste" in result["model"]
    assert plan["resumo"] == "O solo está ácido e pobre em potássio."
    assert plan["orientacoes"] == ["Aplique com solo úmido."]
    assert next(c for c in plan["corretivos"] if c["tipo"] == "calagem")["explicacao"] == "Corrige a acidez."
    payload = json.loads(calls[0]["messages"][1]["content"])
    assert "kg_ha" not in json.dumps(payload) and payload["acoes"]
    assert service.soil_recommendation_summary(result["recommendation"], PAGES).startswith("O solo")
    assert not service.soil_recommendation_summary(result["recommendation"], set())


def test_generation_works_without_ai_key(farm, monkeypatch):
    monkeypatch.setattr(service, "get_settings", lambda: SimpleNamespace(openai_api_key="", openai_recommendation_model="m"))
    monkeypatch.setattr(service, "today_in_app_timezone", lambda: TODAY)
    result = service.gerar_recomendacao_adubacao(farm.db, farm.analysis, PAGES)
    plan = agronomy.read(result["recommendation"])
    assert result["status"] == "generated" and plan["ia"] == {"usada": False, "modelo": None}
    assert "Café arábica em produção" in plan["resumo"]


def test_texture_fallback_uses_plot_class(farm):
    reading = read_soil(farm.analysis)
    reading.values.pop("p_rem"), reading.values.pop("clay")
    texture = texture_for(reading, "argiloso")
    assert texture.clay_range() == (35, 60)


def test_template_renders_v3_plan_and_legacy(farm, monkeypatch):
    monkeypatch.setattr(service, "get_settings", lambda: SimpleNamespace(openai_api_key="", openai_recommendation_model="m"))
    monkeypatch.setattr(service, "today_in_app_timezone", lambda: TODAY)
    result = service.gerar_recomendacao_adubacao(farm.db, farm.analysis, PAGES)
    template = farm.routes.templates.env.get_template("partials/soil_ai_recommendation.html")
    item = SimpleNamespace(id=7, ai_recommendation=result["recommendation"], ai_error=None, ai_status="generated")
    html = template.render(item=item, soil_ai_allowed_pages=PAGES, dialog_id="rec-7")
    for text in ("O que fazer primeiro", "Calagem: 2,15 t/ha de calcário dolomítico", "Laudo interpretado",
                 "Como calculamos", "Produtos e quantidades", "Adubação — parcela 1 de 4", "Ácido bórico",
                 "Memória de cálculo", "Manual do Café", "Em breve", 'id="rec-7-panel-crono"', "Resumo gerado pelo sistema"):
        assert text in html, text
    assert "Seu acesso atual" in template.render(item=item, soil_ai_allowed_pages=set())
    legacy = SimpleNamespace(id=1, ai_recommendation=json.dumps({"versao": 2, "diagnostico": ["Antigo"], "modulos_utilizados": []}),
                             ai_error=None, ai_status="generated")
    assert "modelo antigo" in template.render(item=legacy, soil_ai_allowed_pages=PAGES)
    text = SimpleNamespace(id=2, ai_recommendation="Texto livre", ai_error=None, ai_status=None)
    assert "Texto livre" in template.render(item=text, soil_ai_allowed_pages=PAGES)
