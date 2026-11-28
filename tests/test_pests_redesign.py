from datetime import date, timedelta

from app.models import AgriculturalOperation, PestIncident, PestInspection, PestRound, StockOutput
from app.services.pest_monitoring import catalog_entry, classify, compute_incidence, decision_for, forecast_strip
from tests.test_coffee_sales import env  # noqa: F401
from tests.test_fertilization_redesign import _post, _seed_stock_and_drip_plot


def _flash_text(env):
    return str(env.request("/pragas").session.get("flash") or "")


def test_regras_de_incidencia_nivel_e_sugestao():
    assert compute_incidence(100, 34) == 34.0
    assert compute_incidence(100, 120) is None
    assert compute_incidence(0, 1) is None
    assert classify(34, 30) == ("ativa", 4)
    assert classify(70, 30)[1] == 5
    assert classify(2, 3)[0] == "monitoramento"
    assert classify(None, 10, level=3)[0] == "ativa"
    assert classify(None, 10, level=1)[0] == "monitoramento"
    assert decision_for(1, 10) == "encerrar"
    assert decision_for(8, 10) == "manter"
    assert decision_for(12, 10) == "novo"
    assert catalog_entry("Cercosporiose (olho-pardo)")["key"] == "cercosporiose"
    assert catalog_entry("Phoma / Ascochyta")["key"] == "phoma"
    strip = forecast_strip(
        [
            {"date": date.today().isoformat(), "precip_mm": 12, "prob": 80},
            {"date": (date.today() + timedelta(days=1)).isoformat(), "precip_mm": 0, "prob": 10},
        ],
        date.today(),
    )
    assert [d["kind"] for d in strip] == ["rain", "sun"]
    assert strip[1]["ideal"] and strip[1]["value"] == "Ideal"


def test_registrar_com_contagem_bloqueia_duplicado_e_planeja_controle(env):
    plot_a, plot_b, ureia, compra = _seed_stock_and_drip_plot(env)
    r = env.routes
    resp = _post(
        env,
        r.create_pest_action,
        [
            ("pest_key", "bicho_mineiro"),
            ("plot_id", str(plot_a.id)),
            ("plot_id", str(plot_b.id)),
            ("occurrence_date", date.today().isoformat()),
            ("sampled_count", "100"),
            ("affected_count", "34"),
            ("location", "Bordadura"),
            ("plan_next", "1"),
        ],
        path="/pragas",
    )
    assert resp.status_code == 303
    incs = env.db.query(PestIncident).filter(PestIncident.name == "Bicho-mineiro").all()
    assert len(incs) == 2
    inc = next(i for i in incs if i.plot_id == plot_a.id)
    assert inc.status == "ativa"
    assert float(inc.incidence_pct) == 34.0
    assert float(inc.control_threshold_pct) == 30.0
    assert inc.location_in_plot == "Bordadura"
    assert [i.kind for i in inc.inspections] == ["registro"]
    assert resp.headers["location"].startswith("/pragas?controle=")

    # Mesmo foco no mesmo talhão não duplica: manda para a reinspeção
    dup = _post(
        env,
        r.create_pest_action,
        [("pest_key", "bicho_mineiro"), ("plot_id", str(plot_a.id)), ("sampled_count", "100"), ("affected_count", "10")],
        path="/pragas",
    )
    assert dup.headers["location"] == f"/pragas?reinspecao={inc.id}"
    assert env.db.query(PestIncident).filter(PestIncident.name == "Bicho-mineiro").count() == 2

    # Estoque insuficiente bloqueia o planejamento
    short = _post(
        env,
        r.plan_pest_control_action,
        [("input_id", str(ureia.id)), ("dose_per_ha", "1.000,00"), ("withdrawal_days", "21"), ("application_date", date.today().isoformat())],
        inc.id,
        path=f"/pragas/{inc.id}/controle",
    )
    assert short.headers["location"] == f"/pragas?controle={inc.id}"
    assert env.db.query(AgriculturalOperation).count() == 0

    # Planejamento cria pulverização planejada ligada ao foco, sem baixar estoque ainda
    app_day = date.today() + timedelta(days=2)
    ok = _post(
        env,
        r.plan_pest_control_action,
        [("input_id", str(ureia.id)), ("dose_per_ha", "2,00"), ("withdrawal_days", "21"), ("application_date", app_day.isoformat())],
        inc.id,
        path=f"/pragas/{inc.id}/controle",
    )
    assert ok.headers["location"] == f"/pragas?foco={inc.id}"
    op = env.db.query(AgriculturalOperation).one()
    assert op.status == "planejada" and op.pest_incident_id == inc.id
    assert float(op.planned_input_qty) == 10.0  # 2 kg/ha × 5 ha
    assert op.withdrawal_days == 21
    env.db.refresh(inc)
    assert inc.status == "em_tratamento"
    assert inc.reinspection_date == app_day + timedelta(days=7)
    assert env.db.query(StockOutput).count() == 0

    # Marcar como realizada em Pulverizações dá baixa no produto planejado
    done = r.conclude_agricultural_operation_action(op.id, env.request(f"/operacoes/{op.id}/concluir"), csrf_token="x", db=env.db, user=env.user)
    assert done.status_code == 303
    outs = env.db.query(StockOutput).all()
    assert sum(float(o.quantity) for o in outs if o.reference_type == "agricultural_operation") == 10.0
    env.db.refresh(compra)
    assert float(compra.available_quantity) == 240.0

    # Página mostra o foco com o histórico da pulverização
    html = r.pests_page(env.request("/pragas"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Em controle" in html and "Pulveriza\\u00e7\\u00e3o realizada" in html
    assert "Bicho-mineiro" in html


def test_reinspecao_decide_e_outra_praga_digitada(env):
    plot_a, _plot_b, _ureia, _compra = _seed_stock_and_drip_plot(env)
    r = env.routes
    _post(
        env,
        r.create_pest_action,
        [
            ("pest_key", "outra"),
            ("other_name", "Mosca-das-raízes"),
            ("other_category", "Praga"),
            ("other_threshold", "20"),
            ("plot_id", str(plot_a.id)),
            ("sampled_count", "50"),
            ("affected_count", "5"),
        ],
        path="/pragas",
    )
    inc = env.db.query(PestIncident).filter(PestIncident.name == "Mosca-das-raízes").one()
    assert inc.status == "monitoramento"
    assert float(inc.control_threshold_pct) == 20.0
    assert inc.reinspection_date is not None

    # Piorou: novo controle
    resp = _post(env, r.reinspect_pest_action, [("sampled_count", "50"), ("affected_count", "15")], inc.id, path=f"/pragas/{inc.id}/reinspecao")
    assert resp.headers["location"] == f"/pragas?controle={inc.id}"
    env.db.refresh(inc)
    assert inc.status == "ativa" and float(inc.incidence_pct) == 30.0
    assert [i.kind for i in inc.inspections] == ["registro", "reinspecao"]

    # Corrigir registro ajusta a contagem
    fix = _post(env, r.update_pest_action, [("sampled_count", "50"), ("affected_count", "4"), ("location", "Reboleira")], inc.id, path=f"/pragas/{inc.id}/editar")
    assert fix.status_code == 303
    env.db.refresh(inc)
    assert float(inc.incidence_pct) == 8.0 and inc.status == "monitoramento"

    # A praga digitada fica disponível na lista da fazenda
    html = r.pests_page(env.request("/pragas"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "custom:Mosca-das-raízes" in html

    # Excluir remove o foco e suas avaliações
    gone = r.delete_pest_action(inc.id, env.request(f"/pragas/{inc.id}/excluir"), csrf_token="x", db=env.db, user=env.user)
    assert gone.status_code == 303
    assert env.db.query(PestIncident).count() == 0
    assert env.db.query(PestInspection).count() == 0


def test_ronda_cria_focos_novos_e_reinspeciona_ativos(env):
    plot_a, plot_b, _ureia, _compra = _seed_stock_and_drip_plot(env)
    r = env.routes
    _post(
        env,
        r.create_pest_action,
        [("pest_key", "ferrugem"), ("plot_id", str(plot_a.id)), ("sampled_count", "100"), ("affected_count", "2")],
        path="/pragas",
    )
    created = _post(
        env,
        r.create_pest_round_action,
        [
            ("plot_id", str(plot_a.id)),
            ("plot_id", str(plot_b.id)),
            ("pest_key", "ferrugem"),
            ("pest_key", "bicho_mineiro"),
            ("responsible_name", "João"),
            ("mode", "agora"),
        ],
        path="/pragas/rondas",
    )
    rnd = env.db.query(PestRound).one()
    assert created.headers["location"] == f"/pragas/rondas/{rnd.id}"
    page = r.pest_round_page(rnd.id, env.request(f"/pragas/rondas/{rnd.id}"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Ronda de monitoramento" in page and "Ferrugem-alaranjada" in page

    sent = _post(
        env,
        r.submit_pest_round_action,
        [
            (f"level_{plot_a.id}_ferrugem", "3"),
            (f"level_{plot_a.id}_bicho_mineiro", "0"),
            (f"level_{plot_b.id}_ferrugem", "0"),
            (f"level_{plot_b.id}_bicho_mineiro", "2"),
        ],
        rnd.id,
        path=f"/pragas/rondas/{rnd.id}",
    )
    assert sent.status_code == 303
    env.db.refresh(rnd)
    assert rnd.status == "concluida"
    assert (rnd.new_count, rnd.reinspect_count, rnd.worse_count) == (1, 1, 1)
    ferrugem = env.db.query(PestIncident).filter(PestIncident.name == "Ferrugem-alaranjada").one()
    assert ferrugem.status == "ativa"
    assert ferrugem.inspections[-1].kind == "ronda" and ferrugem.inspections[-1].level == 3
    bicho = env.db.query(PestIncident).filter(PestIncident.name == "Bicho-mineiro").one()
    assert bicho.plot_id == plot_b.id and bicho.status == "monitoramento"

    rondas_html = r.pests_page(env.request("/pragas"), aba="rondas", db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "1 foco novo" in rondas_html
