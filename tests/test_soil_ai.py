import json
from datetime import date
from types import SimpleNamespace

import pytest

from app.core.permissions_catalog import PAGE_AGENDA, PAGE_INPUTS, PAGE_IRRIGATION, PAGE_OPERATIONS
from app.models import (
    AgronomicProfile, Farm, FertilizationRecord, FertilizationSchedule, InputCatalog,
    InputRecommendation, IrrigationRecord, Plot, PurchasedInput, SoilAnalysis,
)
from app.models.fertilization_item import FertilizationItem
from app.models.fertilization_schedule_item import FertilizationScheduleItem
from app.services import openai_service as service
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


def test_ai_timeout_has_no_retry_and_falls_back_to_deterministic_plan(soil_context, monkeypatch, caplog):
    _, db, _, analysis, _ = soil_context
    calls, clients = [], []

    def parse(**kwargs):
        calls.append(kwargs)
        raise TimeoutError("simulated external timeout")

    def client(**kwargs):
        clients.append(kwargs)
        return SimpleNamespace(beta=SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(parse=parse))))

    monkeypatch.setattr(service, "get_settings", lambda: SimpleNamespace(
        openai_api_key="fake-key", openai_recommendation_model="configured-model",
    ))
    monkeypatch.setattr(service, "today_in_app_timezone", lambda: TODAY)
    monkeypatch.setattr("openai.OpenAI", client)
    with caplog.at_level("INFO", logger="uvicorn.error"):
        result = service.gerar_recomendacao_adubacao(db, analysis, PAGES)
    plan = service.read_soil_recommendation(result["recommendation"])
    assert result["status"] == "generated" and plan["ia"]["usada"] is False and plan["resumo"]
    assert len(calls) == 1 and calls[0]["timeout"] == 45 and clients[0]["max_retries"] == 0
    assert "FIXA" not in result["recommendation"]
    assert "soil_ai stage=openai duration_ms=" in caplog.text
    assert "soil_ai stage=context duration_ms=" in caplog.text


def test_context_limits_history_and_stock_to_farm_plot_and_modules(soil_context):
    from app.services.agronomy.context import gather
    _, db, _, analysis, _ = soil_context
    inputs = gather(db, analysis, TODAY, PAGES)
    assert [a.products[0]["produto"] for a in inputs.applications] == ["APLICAÇÃO DEPOIS DA COLETA"]
    assert [s.products[0]["produto"] for s in inputs.scheduled] == ["AGENDADO"]
    assert [lot.name for lot in inputs.stock] == ["Superfosfato simples"]
    restricted = gather(db, analysis, TODAY, set())
    assert not restricted.applications and not restricted.scheduled and not restricted.stock
    assert not restricted.history_available


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


def test_failed_manual_generation_keeps_outdated_warning(soil_context, monkeypatch):
    routes, db, user, analysis, _ = soil_context
    analysis.ai_recommendation = "Recomendação anterior"
    analysis.ai_status = "outdated"
    db.commit()
    monkeypatch.setattr(routes, "_soil_ai_allowed_pages", lambda *args: set())
    monkeypatch.setattr(routes, "gerar_recomendacao_adubacao", lambda *args: {
        "status": "error", "error": "Falha simulada",
    })
    updated = routes._apply_soil_ai_recommendation(routes._repository(db), analysis, user)
    assert updated.ai_status == "outdated" and updated.ai_recommendation == "Recomendação anterior"
    assert updated.ai_error == "Falha simulada"


