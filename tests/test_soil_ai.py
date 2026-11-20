import json
from datetime import date
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from app.core.permissions_catalog import PAGE_AGENDA, PAGE_INPUTS, PAGE_IRRIGATION, PAGE_OPERATIONS
from app.models import (
    AgronomicProfile, Farm, FertilizationRecord, FertilizationSchedule, InputCatalog,
    InputRecommendation, IrrigationRecord, Plot, PurchasedInput, SoilAnalysis,
)
from app.models.fertilization_item import FertilizationItem
from app.models.fertilization_schedule_item import FertilizationScheduleItem
from app.services import openai_service as service
from app.services.soil_ai_context import build_soil_ai_context
from tests.test_stock_assistant import _route_env

TODAY = date(2026, 11, 20)
PAGES = {PAGE_AGENDA, PAGE_INPUTS, PAGE_IRRIGATION, PAGE_OPERATIONS}
LAB = "Profundidade 0–20 cm. Fósforo em mg/dm³, extrator Mehlich-1."
GUARANTEE = "Garantia: 18% de P2O5."


@pytest.fixture
def soil_context(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    other_farm = Farm(name="Outra Fazenda", location="MG", total_area=10, organization_id=user.organization_id)
    db.add(other_farm)
    db.flush()
    farm_id = scope["active_farm_id"]
    main = Plot(name="Setor 1", farm_id=farm_id, area_hectares=2, plant_count=500)
    other_plot = Plot(name="Setor 2", farm_id=farm_id, area_hectares=3, plant_count=500)
    foreign = Plot(name="Outro setor", farm_id=other_farm.id, area_hectares=2, plant_count=500)
    fertilizer = InputCatalog(name="Superfosfato simples", normalized_name="superfosfato simples",
                              category="Fertilizantes", default_unit="kg")
    db.add_all([main, other_plot, foreign, fertilizer, AgronomicProfile(
        farm_id=farm_id, culture="Café", region="Sul de Minas", crop_stage="Produção", target_productivity_sacks_per_ha=35,
    )])
    db.flush()
    analysis = SoilAnalysis(
        plot_id=main.id, farm_id=farm_id, analysis_date=date(2026, 7, 20), laboratory="Laboratório",
        ph=5.3, phosphorus=8, observations=LAB, npk_recommendation="FIXA", liming_need_t_ha=42,
    )
    db.add(analysis)
    for plot, name, when in [
        (main, "APLICAÇÃO DEPOIS DA COLETA", date(2026, 10, 1)),
        (main, "ANTIGA", date(2024, 10, 1)),
        (other_plot, "VIZINHA", date(2026, 9, 1)),
        (foreign, "EXTERNA", date(2026, 9, 1)),
    ]:
        applied = FertilizationRecord(plot_id=plot.id, product=name, dose="50 kg", application_date=when, cost=0)
        if plot is main and when.year == 2026:
            applied.items.append(FertilizationItem(name=name, unit="kg", quantity_per_hectare=50, total_quantity=50))
        db.add(applied)
    db.add(FertilizationSchedule(
        plot_id=main.id, scheduled_date=date(2026, 12, 1), status="scheduled",
        items=[FertilizationScheduleItem(name="AGENDADO", quantity=45, unit="kg")],
    ))
    db.add(IrrigationRecord(plot_id=main.id, irrigation_date=date(2026, 10, 1),
                            volume_liters=7200, duration_minutes=35, origin="manual"))
    for farm, plot, name in [
        (farm_id, main.id, "DO SETOR"), (farm_id, None, "DA FAZENDA"),
        (farm_id, other_plot.id, "OUTRO SETOR"), (other_farm.id, None, "OUTRA FAZENDA"),
    ]:
        db.add(InputRecommendation(farm_id=farm, plot_id=plot, application_name=name))
    for farm, name, count, expiry in [
        (farm_id, fertilizer.name, 100, date(2027, 3, 1)),
        (farm_id, "SEM SALDO", 0, None),
        (farm_id, "VENCIDO", 500, date(2026, 11, 1)),
        (other_farm.id, "ESTOQUE EXTERNO", 500, None),
    ]:
        db.add(PurchasedInput(
            farm_id=farm, input_id=fertilizer.id, name=name, quantity_purchased=1, package_size=500,
            package_unit="kg", unit_price=1, total_quantity=500, available_quantity=count, total_cost=1,
            expiration_date=expiry, notes=GUARANTEE,
        ))
    db.commit()
    yield routes, db, user, analysis, other_farm
    db.close()


def test_context_limits_data_to_farm_plot_period_and_authorized_modules(soil_context):
    _, db, _, analysis, _ = soil_context
    context = build_soil_ai_context(db, analysis, TODAY, PAGES)
    assert len(context["aplicacoes"]) == 1
    assert context["aplicacoes"][0]["produtos"] == [
        {"produto": "APLICAÇÃO DEPOIS DA COLETA", "quantidade_total": 50, "unidade": "kg"},
    ]
    assert context["agendamentos_pendentes"][0]["produtos"][0]["quantidade_total"] == 45
    assert len(context["estoque_atual"]) == 1 and context["estoque_atual"][0]["saldo"] == 100
    assert len(context["irrigacoes_manuais"]) == 1
    assert {row["nome"] for row in context["recomendacoes_cadastradas_sem_data"]} == {"DO SETOR", "DA FAZENDA"}
    assert "FIXA" not in str(context)
    assert context["solo"]["resultados"]["cálcio"] is None

    restricted = build_soil_ai_context(db, analysis, TODAY, set())
    assert not restricted["modulos_utilizados"]
    assert not restricted["estoque_atual"] and not restricted["aplicacoes"]
    assert not restricted["agendamentos_pendentes"] and not restricted["recomendacoes_cadastradas_sem_data"]


def _recommendation(stock=None, **dose_changes):
    return service.SoilRecommendation.model_validate({
        "diagnostico": ["Fósforo medido: 8 mg/dm³.", "Aplicar 90 kg/ha sem validação."],
        "comparacao_manejo": ["Revisar as aplicações depois da amostragem."],
        "alertas": [],
        "orientacoes": ["Comparar com nova amostragem."],
        "plano": [{
            "tipo": "Adubação", "objetivo": "Planejar o aporte de fósforo",
            "produto": stock["produto"] if stock else "Fonte fosfatada adequada",
            "estoque_ref": stock["referencia"] if stock else None,
            "nutrientes": ["P2O5"], "epoca": "Início das chuvas", "frequencia": "Em 2 parcelas",
            "forma_aplicacao": "Em cobertura", "referencia_id": "embrapa_mg",
            "criterio_referencia": "Capítulo de fertilidade e adubação", "premissas": [], "confirmar": [],
            "fundamentacao": "Revisar fósforo do laudo para a fase do setor.",
            "dose": {"valor": 40, "unidade": "kg/ha", "base": "P2O5", "periodo": "meta_ciclo",
                     "indicadores": ["fósforo"], "trecho_da_garantia": None, **dose_changes},
        }],
    })


def test_generation_and_rendering_use_configured_model_and_preserve_legacy(soil_context, monkeypatch):
    routes, db, _, analysis, _ = soil_context
    calls = []

    def parse(**kwargs):
        calls.append(kwargs)
        context = build_soil_ai_context(db, analysis, TODAY, PAGES)
        draft = _recommendation(context["estoque_atual"][0])
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(parsed=draft))])

    monkeypatch.setattr(service, "get_settings", lambda: SimpleNamespace(
        openai_api_key="fake-key", openai_recommendation_model="configured-model",
    ))
    monkeypatch.setattr(service, "today_in_app_timezone", lambda: TODAY)
    completions = SimpleNamespace(parse=parse)
    monkeypatch.setattr("openai.OpenAI", lambda **kwargs: SimpleNamespace(
        beta=SimpleNamespace(chat=SimpleNamespace(completions=completions)),
    ))
    result = service.gerar_recomendacao_adubacao(db, analysis, PAGES)
    assert result["status"] == "generated" and result["model"] == "configured-model"
    assert calls[0]["model"] == "configured-model" and "FIXA" not in calls[0]["messages"][1]["content"]
    structured = service.read_soil_recommendation(result["recommendation"])
    assert "referencias_tecnicas" in calls[0]["messages"][1]["content"]
    assert structured["plano"][0]["dose"] == {
        "valor": 40.0, "unidade": "kg/ha", "base": "P₂O₅", "periodo": "Meta do ciclo",
    }
    assert structured["estoque"][0]["saldo"] == 100
    assert structured["diagnostico"] == ["Fósforo medido: 8 mg/dm³."]

    template = routes.templates.env.get_template("partials/soil_ai_recommendation.html")
    item = SimpleNamespace(ai_recommendation=result["recommendation"], ai_error=None)
    html = template.render(item=item, soil_ai_allowed_pages=PAGES)
    assert "Revisão técnica antes da aplicação" in html and "40,00 kg/ha" in html
    assert "Diagnóstico nutricional" in html and "FIXA" not in html
    assert "Em 2 parcelas" in html and "Em cobertura" in html
    assert "de P₂O₅" in html and "Aproveitamento do estoque" in html
    structured["versao"] = 1
    assert service.read_soil_recommendation(json.dumps(structured))
    assert "40,00 kg/ha" not in template.render(item=item, soil_ai_allowed_pages=set())
    assert not service.soil_recommendation_summary(result["recommendation"], set())
    assert "Histórico anterior" in template.render(
        item=SimpleNamespace(ai_recommendation="Histórico anterior", ai_error=None), soil_ai_allowed_pages=PAGES,
    )


def _draft_context(db, analysis):
    context = build_soil_ai_context(db, analysis, TODAY, PAGES)
    context["referencias_tecnicas"] = service._technical_references(context)
    return context


def test_rejects_unfounded_doses_and_checks_balance(soil_context):
    _, db, _, analysis, _ = soil_context
    context = _draft_context(db, analysis)
    stock = context["estoque_atual"][0]
    for changes in ({"valor": -1}, {"indicadores": ["cálcio"]}, {"valor": 3.0001}, {"valor": 1000},
                    {"base": "produto", "trecho_da_garantia": "20%"}, {"unidade": "L/ha"}):
        result = service._prepare_recommendation(_recommendation(stock, **changes), context, TODAY)
        assert result["plano"][0]["dose"] is None
        assert result["plano"][0]["confirmar"]

    with pytest.raises(ValidationError):
        _recommendation(stock, unidade="kg/planta")
    with pytest.raises(ValidationError):
        _recommendation(stock, valor=float("nan"))

    result = service._prepare_recommendation(
        _recommendation(stock, valor=60, base="produto", trecho_da_garantia=GUARANTEE), context, TODAY,
    )
    assert result["plano"][0]["dose"]["valor"] == 60
    assert "abaixo da quantidade" in result["estoque"][0]["situacao"]
    excessive = _recommendation(stock, valor=3000, base="produto", trecho_da_garantia=GUARANTEE)
    assert service._prepare_recommendation(excessive, context, TODAY)["plano"][0]["dose"] is None

    context["agendamentos_pendentes"][0]["produtos"][0]["produto"] = stock["produto"]
    result = service._prepare_recommendation(_recommendation(stock, periodo="saldo_ciclo"), context, TODAY)
    assert result["plano"][0]["dose"] is None
    assert any("aplicação pendente" in item for item in result["plano"][0]["confirmar"])
    result = service._prepare_recommendation(_recommendation(stock), context, TODAY)
    assert result["plano"][0]["dose"]["periodo"] == "Meta do ciclo"
    assert any("aplicação pendente" in item for item in result["plano"][0]["confirmar"])


def test_secondary_gap_and_empty_stock_keep_supported_nutrient_dose(soil_context):
    _, db, _, analysis, _ = soil_context
    context = _draft_context(db, analysis)
    context["estoque_atual"] = []
    context["escopo"]["area_ha"] = None
    context["solo"]["observacoes"] = None
    context["modulos_consultados"].remove(PAGE_AGENDA)
    phosphorus = _recommendation()
    nitrogen = _recommendation(base="N", indicadores=[])
    nitrogen.plano[0].nutrientes = ["N"]
    nitrogen.plano[0].produto = "Ureia"
    nitrogen.plano[0].fundamentacao = "Considera a meta informada para o cafeeiro em produção."
    phosphorus.plano.extend(nitrogen.plano)

    result = service._prepare_recommendation(phosphorus, context, TODAY)
    assert result["plano"][0]["dose"] is None
    assert "unidades do laudo" in result["plano"][0]["confirmar"][0]
    assert result["plano"][1]["dose"]["base"] == "N"
    assert "apurar o saldo" in result["plano"][1]["confirmar"][0]
    assert result["estoque"] == []

    context["perfil"]["target_productivity_sacks_per_ha"] = None
    context["escopo"]["estimativa_setor_sacas_ha"] = 35
    result = service._prepare_recommendation(nitrogen, context, TODAY)
    assert result["plano"][0]["dose"]
    assert "estimativa de produção" in result["plano"][0]["premissas"][0]

    nitrogen.plano[0].dose.periodo = "saldo_ciclo"
    result = service._prepare_recommendation(nitrogen, context, TODAY)
    assert result["plano"][0]["dose"] is None


def test_regeneration_rejects_analysis_outside_active_farm(soil_context, monkeypatch):
    routes, db, user, analysis, other_farm = soil_context
    request = SimpleNamespace(session={})
    monkeypatch.setattr(routes, "_launch_scope_or_redirect", lambda *args, **kwargs: ({"active_farm_id": other_farm.id}, None))
    monkeypatch.setattr(routes, "_apply_soil_ai_recommendation", lambda *args: pytest.fail("Geração fora da fazenda ativa"))
    response = routes.regenerate_soil_recommendation_action(analysis.id, request, "x", db, user)
    assert response.headers["Location"] == "/analise-solo"


def test_incomplete_profile_can_generate_a_plan(soil_context, monkeypatch):
    routes, db, user, analysis, _ = soil_context
    completion, _ = routes._agronomic_profile_completion(analysis.farm.agronomic_profile)
    assert completion < 70
    monkeypatch.setattr(routes, "_soil_ai_allowed_pages", lambda *_: PAGES)
    monkeypatch.setattr(routes, "gerar_recomendacao_adubacao", lambda *_: {
        "status": "generated", "recommendation": "Plano com os dados disponíveis", "model": "configured-model",
    })
    updated = routes._apply_soil_ai_recommendation(routes._repository(db), analysis, user)
    assert updated.ai_status == "generated" and updated.ai_recommendation == "Plano com os dados disponíveis"
