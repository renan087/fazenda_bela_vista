import json
from datetime import date, timedelta
from io import BytesIO
from types import SimpleNamespace

from fastapi import BackgroundTasks
from starlette.datastructures import Headers, UploadFile

from app.models import (
    AgriculturalOperation,
    EquipmentAsset,
    FertilizationSchedule,
    InputRecommendation,
    IrrigationRecord,
    PestIncident,
    RainfallRecord,
    SoilAnalysis,
    StockOutput,
)
from app.services import openai_service as agronomy_service
from app.services.climate_water import irrigation_mm
from app.services.operations_pests_views import active_withdrawals_by_plot
from tests.test_coffee_sales import env  # noqa: F401
from tests.test_fertilization_redesign import _post, _seed_stock_and_drip_plot


def test_recipes_unified_under_fertilization_and_wizard_save_as_recipe(env):
    plot_a, plot_b, ureia, compra = _seed_stock_and_drip_plot(env)
    r = env.routes

    # Cria receita com dose_mode e application_method
    resp = _post(
        env,
        r.create_input_recommendation_action,
        [
            ("application_name", "Cobertura Fracionada N"),
            ("application_method", "fertirrigacao"),
            ("dose_mode", "per_ha"),
            ("notes", "Aplicar com solo úmido"),
            ("input_id", str(ureia.id)),
            ("quantity", "15"),
            ("unit", "kg"),
        ],
        path="/fertilizacao/receitas",
    )
    assert resp.status_code == 303
    assert resp.headers["location"].startswith("/fertilizacao/receitas")

    rec = env.db.query(InputRecommendation).filter(InputRecommendation.application_name == "Cobertura Fracionada N").first()
    assert rec is not None
    assert rec.dose_mode == "per_ha"
    assert rec.application_method == "fertirrigacao"

    # Página de receitas exibe KPIs, abas e o cartão da receita
    html = r.input_recommendations_page(
        env.request("/fertilizacao/receitas"), db=env.db, user=env.user, csrf_token="x"
    ).body.decode()
    assert "Cobertura Fracionada N" in html
    assert "Agendamentos" in html and "Receitas" in html
    assert f"/fertilizacao/agendamentos/novo?recipe_id={rec.id}" in html

    # Wizard com recipe_id pré-selecionado
    wiz = r.new_fertilization_schedule_wizard_page(
        env.request(f"/fertilizacao/agendamentos/novo?recipe_id={rec.id}"),
        recipe_id=rec.id,
        db=env.db,
        user=env.user,
        csrf_token="x",
    ).body.decode()
    assert '"recipeName": "Cobertura Fracionada N"' in wiz
    assert '"doseMode": "per_ha"' in wiz

    # Wizard com saveAsRecipe cria nova receita reutilizável ao agendar
    payload = {
        "plotIds": [plot_a.id],
        "method": "adubacao_solida",
        "dates": ["2026-11-22"],
        "doseMode": "per_ha",
        "items": [{"inputId": ureia.id, "value": 20}],
        "saveAsRecipe": True,
        "saveRecipeName": "Receita Gerada no Wizard",
    }
    resp2 = _post(
        env,
        r.create_fertilization_schedule_action,
        [("wizard_payload_json", json.dumps(payload))],
        path="/fertilizacao/agendamentos",
    )
    assert resp2.status_code == 303
    saved = env.db.query(InputRecommendation).filter(InputRecommendation.application_name == "Receita Gerada no Wizard").first()
    assert saved is not None
    assert saved.dose_mode == "per_ha"
    assert saved.application_method == "adubacao_solida"
    assert len(saved.items) == 1 and float(saved.items[0].quantity) == 20.0


def test_soil_analysis_schedule_bridge_creates_fertilization_schedules(env, monkeypatch):
    plot_a, _, ureia, _ = _seed_stock_and_drip_plot(env)
    r = env.routes
    farm_id = env.scope["active_farm_id"]

    analysis = SoilAnalysis(
        farm_id=farm_id,
        plot_id=plot_a.id,
        analysis_date=date(2026, 7, 10),
        laboratory="Lab Café",
        ph=5.1,
        organic_matter=2.8,
        phosphorus=8.0,
        potassium=70.0,
        calcium=1.8,
        magnesium=0.6,
        aluminum=0.4,
        h_al=4.2,
        ctc=7.0,
        base_saturation=40.0,
    )
    env.db.add(analysis)
    env.db.commit()

    monkeypatch.setattr(agronomy_service, "get_settings", lambda: SimpleNamespace(openai_api_key="", openai_recommendation_model="m"))
    monkeypatch.setattr(agronomy_service, "today_in_app_timezone", lambda: date(2026, 7, 15))
    res = agronomy_service.gerar_recomendacao_adubacao(env.db, analysis, {"analise_solo", "fertilizacao", "insumos"})
    analysis.ai_recommendation = res["recommendation"]
    analysis.ai_status = "generated"
    env.db.add(analysis)
    env.db.commit()

    # GET preview JSON
    preview_resp = r.preview_soil_schedule_api(
        analysis.id, env.request(f"/analise-solo/{analysis.id}/agendar"), db=env.db, user=env.user
    )
    assert preview_resp.status_code == 200
    preview = json.loads(preview_resp.body.decode())
    assert preview["plot_id"] == plot_a.id
    assert len(preview["events"]) >= 1

    # POST cria agendamentos reais (auto-cadastrando insumo se não mapeado)
    post_events = [
        {
            "selected": True,
            "operation": ev["operation"],
            "scheduled_date": ev["scheduled_date"],
            "method": ev["method"],
            "items": [
                {
                    "input_id": it["input_id"] or "auto",
                    "recommended_name": it["recommended_name"],
                    "quantity": it["quantity"],
                    "unit": it["unit"],
                }
                for it in ev["items"]
                if it["quantity"] > 0
            ],
        }
        for ev in preview["events"][:2]
    ]
    monkeypatch.setattr(r, "_require_permission", lambda *a, **k: None)

    create_resp = _post(
        env,
        r.confirm_soil_schedule_action,
        [("entries_json", json.dumps(post_events))],
        analysis.id,
        path=f"/analise-solo/{analysis.id}/agendar",
    )
    assert create_resp.status_code == 303
    assert create_resp.headers["location"].startswith("/fertilizacao/agendamentos")

    schedules = env.db.query(FertilizationSchedule).filter(FertilizationSchedule.plot_id == plot_a.id).all()
    assert len(schedules) >= 1
    assert any("solo" in (s.notes or "").lower() for s in schedules)


def test_agronomic_profile_relocated_to_farm_and_mobile_redirect(env):
    _seed_stock_and_drip_plot(env)
    r = env.routes
    farm_id = env.scope["active_farm_id"]

    # /perfil-agronomico redireciona para /fazendas/{id}?aba=agronomico
    redir = r.agronomic_profiles_page(env.request("/perfil-agronomico"), db=env.db, user=env.user, csrf_token="x")
    assert redir.status_code == 303
    assert redir.headers["location"] == f"/fazendas/{farm_id}?aba=agronomico"

    # /mobile redireciona para /lavoura
    mob = r.mobile_page(env.request("/mobile"), db=env.db, user=env.user)
    assert mob.status_code == 303
    assert mob.headers["location"] == "/lavoura"

    # Aba agronômico em /fazendas/{id} renderiza dados herdados e formulário complementar
    farm_html = r.farm_detail_page(
        farm_id,
        env.request(f"/fazendas/{farm_id}?aba=agronomico"),
        background_tasks=BackgroundTasks(),
        aba="agronomico",
        db=env.db,
        user=env.user,
        csrf_token="x",
    ).body.decode()
    assert "Dados agronômicos" in farm_html
    assert "Manejo do solo" in farm_html
    assert f'action="/fazendas/{farm_id}/agronomia/ambiente"' in farm_html
    assert "Vem de Setores e Safras" in farm_html


def test_climate_water_module_and_lavoura_overview_and_rainfall_without_season(env):
    plot_a, _, _, _ = _seed_stock_and_drip_plot(env)
    r = env.routes

    # Cria irrigação e chuva por talhão
    env.db.add(
        IrrigationRecord(
            plot_id=plot_a.id,
            irrigation_date=date.today(),
            volume_liters=50000,
            duration_minutes=120,
            origin="manual",
        )
    )
    env.db.commit()
    assert irrigation_mm(50000, 5.0) == 1.0  # 50.000 L / (5 ha * 10.000 m²) = 1,0 mm

    # Registra chuva sem exigir safra ativa e vinculada a talhão
    env.scope["active_season"] = None
    env.scope["active_season_id"] = None
    rain_resp = r.create_rainfall_action(
        env.request("/pluviometria"),
        csrf_token="x",
        rainfall_date=date.today().isoformat(),
        millimeters=18.5,
        plot_id=str(plot_a.id),
        source="Pluviômetro Sede",
        notes="Chuva mansa",
        db=env.db,
        user=env.user,
    )
    assert rain_resp.status_code == 303
    rain = env.db.query(RainfallRecord).order_by(RainfallRecord.id.desc()).first()
    assert rain is not None and rain.plot_id == plot_a.id and float(rain.millimeters) == 18.5

    # Páginas /clima-agua, /irrigacao, /pluviometria e /lavoura renderizam corretamente
    cw_html = r.climate_water_overview_page(env.request("/clima-agua"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Clima e Água" in cw_html and "Balanço hídrico por talhão (últimos 15 dias)" in cw_html

    irr_html = r.irrigation_page(env.request("/irrigacao"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Lâmina média aplicada" in irr_html and "Balanço e Previsão" in irr_html

    pluv_html = r.rainfall_page(env.request("/pluviometria"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Chuva e Alertas" in pluv_html and "Configurar alertas" in pluv_html

    lav_html = r.lavoura_overview_page(env.request("/lavoura"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Lavoura" in lav_html and "lv-summary" in lav_html and "Registrar no campo" in lav_html


def test_operations_multitplot_stock_fifo_hourmeter_pest_withdrawal_and_revert(env):
    plot_a, plot_b, ureia, compra = _seed_stock_and_drip_plot(env)
    r = env.routes
    farm_id = env.scope["active_farm_id"]

    # Cria máquina com horímetro e foco de praga ativo
    tractor = EquipmentAsset(
        farm_id=farm_id,
        name="Trator Valtra A750",
        category="trator",
        measurement_unit="horas",
        measurement_value=100.0,
    )
    pest = PestIncident(
        plot_id=plot_a.id,
        occurrence_date=date.today() - timedelta(days=1),
        category="Doença",
        name="Ferrugem",
        severity=4,
        status="ativa",
        incidence_pct=18.0,
    )
    env.db.add_all([tractor, pest])
    env.db.commit()

    # Lança pulverização multi-talhão com baixa FIFO de 30 kg, 4h de trator, carência 14d e vínculo à praga
    resp = _post(
        env,
        r.create_agricultural_operation_action,
        [
            ("plot_id", str(plot_a.id)),
            ("plot_id", str(plot_b.id)),
            ("operation_type", "pulverizacao"),
            ("operation_date", date.today().isoformat()),
            ("status", "realizada"),
            ("equipment_asset_id", str(tractor.id)),
            ("equipment_hours", "4.0"),
            ("pest_incident_id", str(pest.id)),
            ("target_pest", "Ferrugem"),
            ("spray_volume_liters", "400"),
            ("withdrawal_days", "14"),
            ("op_input_id", str(ureia.id)),
            ("op_input_qty", "30"),
        ],
        path="/operacoes",
    )
    assert resp.status_code == 303

    ops = env.db.query(AgriculturalOperation).order_by(AgriculturalOperation.id.asc()).all()
    assert len(ops) == 2
    # Rateio proporcional por área (5 ha vs 10 ha => 10 kg + 20 kg = 30 kg)
    env.db.refresh(compra)
    assert float(compra.available_quantity) == 220.0
    assert round(sum(float(o.inputs_cost or 0) for o in ops), 2) == round(30.0 * (875.0 / 250.0), 2)

    # Horímetro atualizado (rateado 4h total entre os 2 talhões => 100 + 4 = 104h)
    env.db.refresh(tractor)
    assert float(tractor.measurement_value) == 104.0

    # Ocorrência de praga passou para "em_tratamento"
    env.db.refresh(pest)
    assert pest.status == "em_tratamento"

    # Carência ativa aparece no formulário de colheita!
    withdrawals = active_withdrawals_by_plot(ops, date.today())
    assert plot_a.id in withdrawals and withdrawals[plot_a.id]["days_left"] == 14
    hv_html = r.harvest_form_page(
        env.request("/producao/colheitas/nova"), db=env.db, user=env.user, csrf_token="x"
    ).body.decode()
    assert "Atenção à carência de defensivos" in hv_html

    # Excluir ambas as operações estorna 100% do saldo FIFO do lote!
    for op in ops:
        del_resp = r.delete_agricultural_operation_action(
            op.id, env.request(f"/operacoes/{op.id}/excluir"), csrf_token="x", db=env.db, user=env.user
        )
        assert del_resp.status_code == 303
    env.db.refresh(compra)
    assert float(compra.available_quantity) == 250.0
    assert env.db.query(StockOutput).count() == 0


def test_pests_photo_reinspection_and_map_sanidade(env):
    plot_a, _, _, _ = _seed_stock_and_drip_plot(env)
    plot_a.boundary_geojson = json.dumps({
        "type": "Polygon",
        "coordinates": [[[-46.0, -21.0], [-46.0, -21.01], [-46.01, -21.01], [-46.0, -21.0]]],
    })
    env.db.add(plot_a)
    env.db.commit()
    r = env.routes

    fake_png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
    upload = UploadFile(
        file=BytesIO(fake_png),
        filename="folha.png",
        headers=Headers({"content-type": "image/png"}),
    )

    resp = _post(
        env,
        r.create_pest_action,
        [
            ("plot_id", str(plot_a.id)),
            ("occurrence_date", date.today().isoformat()),
            ("category", "Praga"),
            ("name", "Broca-do-café"),
            ("severity", "5"),
            ("incidence_pct", "12.5"),
            ("status", "ativa"),
            ("photo", upload),
        ],
        path="/pragas",
    )
    assert resp.status_code == 303
    inc = env.db.query(PestIncident).filter(PestIncident.name == "Broca-do-café").first()
    assert inc is not None
    assert inc.photo_filename == "folha.png"
    assert float(inc.incidence_pct) == 12.5

    # Endpoint de foto retorna imagem
    photo_resp = r.pest_incident_photo(inc.id, env.request(f"/pragas/{inc.id}/foto"), db=env.db, user=env.user)
    assert photo_resp.status_code == 200
    assert photo_resp.media_type == "image/png"
    assert photo_resp.body == fake_png

    # Mapa exibe camada Por sanidade com o foco crítico
    map_html = r.map_page(env.request("/mapa"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Por sanidade" in map_html
    assert "1 foco(s) · Nv. 5" in map_html

    # Reinspeção com contagem e decisão de encerrar marca foco como controlado
    reinsp = _post(
        env,
        r.reinspect_pest_action,
        [
            ("sampled_count", "100"),
            ("affected_count", "0"),
            ("decision", "encerrar"),
            ("inspection_date", date.today().isoformat()),
            ("note", "Controle eficiente após pulverização"),
        ],
        inc.id,
        path=f"/pragas/{inc.id}/reinspecao",
    )
    assert reinsp.status_code == 303
    env.db.refresh(inc)
    assert inc.status == "controlada"
    assert inc.resolved_date == date.today()


def test_phytosanitary_unified_tabs_and_sidebar_without_operacoes(env):
    _seed_stock_and_drip_plot(env)
    r = env.routes

    pests_html = r.pests_page(env.request("/pragas"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Pragas e pulverizações" in pests_html
    assert "/pragas/pulverizacoes" in pests_html
    assert "/pragas/tratos" in pests_html

    sprays_html = r.agricultural_operations_page(
        env.request("/pragas/pulverizacoes"), db=env.db, user=env.user, csrf_token="x"
    ).body.decode()
    assert "Nova pulverização" in sprays_html
    assert "Pulverização fitossanitária" in sprays_html
    assert "Calagem / Gessagem" not in sprays_html

    tratos_html = r.agricultural_operations_page(
        env.request("/pragas/tratos"), db=env.db, user=env.user, csrf_token="x"
    ).body.decode()
    assert "Novo trato cultural" in tratos_html
    assert "Poda" in tratos_html
    assert "Roçagem" in tratos_html
    assert "Calagem / Gessagem" not in tratos_html

    sidebar_src = open("app/templates/partials/sidebar.html", encoding="utf-8").read()
    assert "Operações e pulverizações" not in sidebar_src
    assert "Pragas e pulverizações" in sidebar_src

