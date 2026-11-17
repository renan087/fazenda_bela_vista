from datetime import date
from types import SimpleNamespace as N

import pytest

from app.core.permissions_catalog import (BACKUPS_MANAGE, USERS_MANAGE, PAGE_AGENDA, PAGE_AGRONOMIC, PAGE_ASSETS, PAGE_IRRIGATION,
                                          PAGE_OPERATIONS, PAGE_PESTS, PAGE_SOIL)
from app.models import Farm, Organization, Plot
from app.models.agricultural_operation import AgriculturalOperation
from app.models.crop_season import CropSeason
from app.models.irrigation import IrrigationRecord
from app.models.pesticide import PestIncident
from app.models.soil_analysis import SoilAnalysis
from app.models.agronomic_profile import AgronomicProfile
from app.models.equipment_asset import EquipmentAsset
from app.models.equipment_maintenance import EquipmentMaintenance
from app.models.equipment_maintenance_plan import EquipmentMaintenancePlan
from app.models.fertilization_schedule import FertilizationSchedule
from app.services.assistant_queries import AssistantQueries
from tests.test_stock_assistant import _route_env


def _environment(monkeypatch, allowed=lambda page: True):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    farm, season = scope["active_farm"], scope["active_season"]
    plot = Plot(name="Setor 1", farm_id=farm.id, area_hectares=1, plant_count=100)
    other_org = Organization(name="Outra organização", slug="outra-organizacao", is_active=True)
    db.add(other_org)
    db.flush()
    other_farm = Farm(name="Outra", location="BA", total_area=5, organization_id=other_org.id)
    db.add_all([plot, other_farm])
    db.flush()
    other_plot = Plot(name="Setor 2", farm_id=other_farm.id, area_hectares=1, plant_count=100)
    other_season = CropSeason(name="Outra safra", farm_id=farm.id, culture="Café", start_date=date(2026, 1, 1),
                              end_date=date(2026, 12, 31), cultivated_area=1, area_unit="ha", status="ativa")
    db.add_all([other_plot, other_season])
    db.flush()
    queries = AssistantQueries(repo=routes._repository(db), farm=farm, season=season, plots=[plot],
                               today=date(2026, 11, 15), allowed=allowed)
    return db, farm, season, plot, other_farm, other_plot, other_season, queries


def test_new_tools_follow_permissions(monkeypatch):
    _, _, _, _, _, _, _, queries = _environment(monkeypatch, lambda page: page == PAGE_IRRIGATION)
    names = {schema["function"]["name"] for schema, _ in queries.tools()}
    assert "consultar_irrigacoes" in names
    assert not names.intersection({"consultar_pragas", "consultar_analises_solo", "consultar_patrimonio", "consultar_agenda"})
    expected = {PAGE_IRRIGATION, PAGE_PESTS, PAGE_SOIL, PAGE_AGRONOMIC, PAGE_OPERATIONS, PAGE_AGENDA, PAGE_ASSETS}
    queries.allowed = lambda page: page in expected
    names = {schema["function"]["name"] for schema, _ in queries.tools()}
    assert {"consultar_irrigacoes", "consultar_pragas", "consultar_analises_solo", "consultar_perfil_agronomico",
            "consultar_operacoes_agricolas", "consultar_agenda", "consultar_patrimonio", "consultar_manutencoes"} <= names


def test_plot_history_excludes_other_farms_and_outside_season(monkeypatch):
    db, farm, season, plot, other_farm, other_plot, other_season, q = _environment(monkeypatch)
    db.add_all([
        IrrigationRecord(plot_id=plot.id, irrigation_date=date(2026, 8, 1), volume_liters=100, duration_minutes=30),
        IrrigationRecord(plot_id=other_plot.id, irrigation_date=date(2026, 8, 1), volume_liters=999, duration_minutes=30),
        IrrigationRecord(plot_id=plot.id, irrigation_date=date(2025, 8, 1), volume_liters=888, duration_minutes=30),
        PestIncident(plot_id=plot.id, occurrence_date=date(2026, 8, 1), category="Praga", name="Bicho-mineiro", severity=2),
        PestIncident(plot_id=other_plot.id, occurrence_date=date(2026, 8, 1), category="Praga", name="Broca", severity=2),
        AgriculturalOperation(plot_id=plot.id, season_id=season.id, operation_date=date(2026, 8, 1), operation_type="Roçagem", responsible_name="Ana"),
        AgriculturalOperation(plot_id=plot.id, season_id=other_season.id, operation_date=date(2026, 8, 1), operation_type="Outra", responsible_name="Ana"),
        SoilAnalysis(farm_id=farm.id, plot_id=plot.id, analysis_date=date(2026, 8, 1), laboratory="Lab", ph=6),
        SoilAnalysis(farm_id=other_farm.id, plot_id=plot.id, analysis_date=date(2026, 8, 2), laboratory="Outro", ph=5),
    ])
    db.commit()
    assert [r["volume_litros"] for r in q.consultar_irrigacoes()["registros"]] == [100]
    assert [r["nome"] for r in q.consultar_pragas(termo="Bicho")["registros"]] == ["Bicho-mineiro"]
    assert [r["operacao"] for r in q.consultar_operacoes_agricolas()["registros"]] == ["Roçagem"]
    soil = q.consultar_analises_solo()["registros"]
    assert len(soil) == 1 and soil[0]["laboratorio"] == "Lab" and soil[0]["fosforo"] is None
    with pytest.raises(ValueError):
        q.consultar_irrigacoes(inicio="2025-01-01")


def test_agenda_profile_and_equipment_are_scoped(monkeypatch):
    db, farm, season, plot, other_farm, other_plot, other_season, q = _environment(monkeypatch)
    asset = EquipmentAsset(farm_id=farm.id, name="Trator", category="Máquina", status="ativo")
    foreign = EquipmentAsset(farm_id=other_farm.id, name="Outro trator", category="Máquina", status="ativo")
    db.add_all([asset, foreign, AgronomicProfile(farm_id=farm.id, culture="Café", region="Bahia")])
    db.flush()
    db.add_all([
        FertilizationSchedule(plot_id=plot.id, season_id=season.id, scheduled_date=date(2026, 11, 20), status="scheduled"),
        FertilizationSchedule(plot_id=plot.id, season_id=other_season.id, scheduled_date=date(2026, 11, 20), status="scheduled"),
        FertilizationSchedule(plot_id=other_plot.id, scheduled_date=date(2026, 11, 20), status="scheduled"),
        EquipmentMaintenance(asset_id=asset.id, maintenance_date=date(2026, 10, 1), description="Troca", status="concluida"),
        EquipmentMaintenance(asset_id=foreign.id, maintenance_date=date(2026, 10, 1), description="Outro", status="concluida"),
        EquipmentMaintenancePlan(asset_id=asset.id, description="Revisão", next_due_date=date(2026, 12, 1), is_active=True),
    ])
    db.commit()
    assert len(q.consultar_agenda()["registros"]) == 1
    assert q.consultar_perfil_agronomico()["regiao"] == "Bahia"
    assert [r["nome"] for r in q.consultar_patrimonio()["registros"]] == ["Trator"]
    assert [r["descricao"] for r in q.consultar_manutencoes()["registros"]] == ["Troca"]
    assert [r["descricao"] for r in q.consultar_manutencoes_previstas()["registros"]] == ["Revisão"]


def test_irrigation_reports_truncated_history(monkeypatch):
    db, _, _, plot, _, _, _, q = _environment(monkeypatch)
    db.add_all([IrrigationRecord(plot_id=plot.id, irrigation_date=date(2026, 8, 1), volume_liters=i, duration_minutes=1)
                for i in range(20)])
    db.commit()
    result = q.consultar_irrigacoes()
    assert len(result["registros"]) == 15 and result["mais_resultados"] is True


def test_administrative_tools_require_explicit_access_and_scope(monkeypatch):
    db, farm, _, _, _, _, _, q = _environment(monkeypatch, lambda page: page in {BACKUPS_MANAGE, USERS_MANAGE})
    names = lambda: {schema["function"]["name"] for schema, _ in q.tools()}
    assert "consultar_usuarios_organizacao" in names()
    assert "consultar_backups_sistema" not in names()
    assert "consultar_organizacao_atual" not in names()
    q.allow_system_backup = True
    q.allow_org_admin = True
    assert {"consultar_backups_sistema", "consultar_organizacao_atual"} <= names()
    assert q.consultar_organizacao_atual()["nome"] == "Org"
    assert [user["nome"] for user in q.consultar_usuarios_organizacao()["registros"]] == ["Ana"]
    q.allowed = lambda page: False
    assert "consultar_backups_sistema" not in names()
    assert "consultar_usuarios_organizacao" not in names()


def test_whatsapp_uses_same_scoped_irrigation_query(monkeypatch):
    import json
    from app.models import User
    from app.services import whatsapp_bot
    from app.web import routes
    from tests.test_whatsapp import FakeClient, _payload, _receive, _settings

    _settings(monkeypatch)
    db, farm, season, plot, _, other_plot, _, _ = _environment(monkeypatch)
    db.add_all([IrrigationRecord(plot_id=plot.id, irrigation_date=date(2026, 8, 1), volume_liters=100, duration_minutes=30),
                IrrigationRecord(plot_id=other_plot.id, irrigation_date=date(2026, 8, 1), volume_liters=999, duration_minutes=30)])
    db.commit()
    user = db.query(User).filter_by(organization_id=farm.organization_id).first()
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda db, user: {PAGE_IRRIGATION})

    def run(history, assistant):
        result = assistant.call_tool("consultar_irrigacoes", json.dumps({"inicio": None, "fim": None, "setor": None}))
        assert [row["volume_litros"] for row in result["registros"]] == [100]
        return {"resposta": "A irrigação registrada foi de 100 litros.", "draft_id": None}

    monkeypatch.setattr(routes, "run_farm_assistant", run)
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, farm, season)
    _receive(db, client, _payload("5535999", "expanded-link", f"SISFARM {code}"))
    assert _receive(db, client, _payload("5535999", "expanded-query", "Quanta água usei na irrigação?")) == ["done"]
    assert "100 litros" in client.sent[-1][2]


def test_financial_queries_exclude_other_seasons_and_plots(monkeypatch):
    _, farm, season, plot, _, other_plot, other_season, q = _environment(monkeypatch)
    part = N(due_date=date(2026, 11, 10), amount=50, installment_number=1, status="pendente", paid_at=None)
    def transaction(**changes):
        data = {"farm_id": farm.id, "season_id": season.id, "plot_id": plot.id, "is_reversed": False,
                "launch_date": date(2026, 11, 1), "amount": 50, "category": "Diesel",
                "product_service": "Combustível", "operation_type": "despesa", "installments": [part],
                "payment_method": "Pix", "counterparty_name": None}
        return N(**(data | changes))
    rows = [transaction(), transaction(season_id=other_season.id), transaction(plot_id=other_plot.id),
            transaction(is_reversed=True)]
    monkeypatch.setattr(q.repo, "list_finance_transactions", lambda **kwargs: rows)
    assert q.consultar_financeiro()["despesas"] == 50
    assert len(q.consultar_contas()["registros"]) == 1
