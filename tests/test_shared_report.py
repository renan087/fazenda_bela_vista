import math
import json
import re
from types import SimpleNamespace

import pytest

from app.core.permissions_catalog import PAGE_FINANCE, PAGE_INPUTS
from app.services.shared_report import build_financial_report, build_report
from tests.test_stock_assistant import _route_env


def test_purchase_report_uses_only_server_allowed_fields_and_bounded_rows():
    result = {"periodo": "01/01/2026 a 31/12/2026", "quantidade": 30,
              "compras": [{"data": "15/11/2026", "item": "Gasolina", "quantidade_total": 20,
                           "unidade": "L", "valor_total": 114, "fornecedor": "Posto",
                           "html": "<script>alert(1)</script>", "token": "segredo"}] * 20}
    report = build_report("consultar_compras", result, farm_name="Bela Vista", season_name="Café")
    assert report["permission"] == PAGE_INPUTS and len(report["rows"]) == 15
    assert report["truncated"] and "script" not in str(report) and "segredo" not in str(report)


def test_invalid_report_and_nonfinite_values_are_not_accepted_as_numbers():
    with pytest.raises(ValueError):
        build_report("arbitrary_tool", {"registros": []}, farm_name="F", season_name="S")
    report = build_report("consultar_saldos_estoque", {"itens": [{"item": "X", "saldo": math.inf, "unidade": "L"}]},
                          farm_name="F", season_name="S")
    assert report["rows"][0][1] is None
    with pytest.raises(ValueError):
        build_financial_report({"receitas": math.nan, "despesas": 1, "saldo": 1}, farm_name="F", season_name="S")


def test_financial_chart_has_bounded_proportions():
    report = build_financial_report({"receitas": 100, "despesas": 25, "saldo": 75}, farm_name="F", season_name="S")
    assert report["permission"] == PAGE_FINANCE
    assert [bar["width"] for bar in report["bars"]] == [100, 25]


def test_report_intent_rejects_negative_requests():
    from app.web.routes import _assistant_wants_shared_report
    assert _assistant_wants_shared_report("Consulte minhas compras e gere um link para compartilhar")
    assert not _assistant_wants_shared_report("Não quero que compartilhe esses dados")
    assert _assistant_wants_shared_report("Crie uma página HTML com a consulta")
    assert not _assistant_wants_shared_report("Não gere um link para compartilhar")
    assert routes_followup("Compartilhe essa consulta")
    assert not routes_followup("Compartilhe a última consulta de gasolina")


def routes_followup(message):
    from app.web.routes import _assistant_share_followup
    return _assistant_share_followup(message)


def test_real_query_creates_safe_shareable_page_and_revalidates_permission(monkeypatch):
    from datetime import date, timedelta
    from starlette.requests import Request
    from app.models.purchased_input import PurchasedInput
    from app.models.shared_report import SharedReport
    from app.routers import assistant_shared_reports as controller
    from app.services.shared_report import authorize_shared_report, find_shared_report

    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    purchase = db.query(PurchasedInput).first()
    purchase.purchase_date = date(2026, 11, 15)
    purchase.input_catalog.name = "Gasolina <script>alert(1)</script>"
    db.commit()
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda db, user: {PAGE_INPUTS})
    monkeypatch.setattr(routes, "_entitlements_for", lambda db, user: SimpleNamespace(module_allowed=lambda page: True))

    def query(history, assistant):
        assistant.call_tool("consultar_compras", json.dumps({"inicio": None, "fim": None, "item": "gasolina"}))
        return {"resposta": "Consultei os dados.", "draft_id": None}

    monkeypatch.setattr(routes, "run_farm_assistant", query)
    thread, reply = routes._assistant_turn(db, user, routes._repository(db), scope, None,
                                           "Consulte minhas compras de gasolina e gere um link para compartilhar")
    token = reply["_shared_url"].split("#")[-1]
    assert len(token) == 43 and token not in thread.messages_json
    report = db.query(SharedReport).one()
    assert report.expires_at - report.created_at == timedelta(hours=48)
    assert report.id == reply["shared_report_id"] and token not in report.data_json and token != report.token_hash
    assert find_shared_report(db, token).id == report.id and authorize_shared_report(db, report)
    request = Request({"type": "http", "method": "GET", "path": "/acesso-compartilhado/relatorio",
                       "headers": [], "query_string": b"", "session": {}})
    confirmation = controller.confirmation(request)
    csrf = re.search(r'name="csrf_token" value="([^"]+)"', confirmation.body.decode()).group(1)
    opened = controller.show_report(request, token, csrf, db)
    html = opened.body.decode()
    assert opened.status_code == 200 and "Gasolina &lt;script&gt;" in html and "<script>alert(1)</script>" not in html
    assert opened.headers["cache-control"] == "no-store" and "user_email" not in request.session
    confirmation = controller.confirmation(request)
    csrf = re.search(r'name="csrf_token" value="([^"]+)"', confirmation.body.decode()).group(1)
    assert controller.show_report(request, token, csrf, db).status_code == 200
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda db, user: set())
    assert not authorize_shared_report(db, report)


def test_followup_report_requeries_current_data(monkeypatch):
    from datetime import date
    from app.models.input_catalog import InputCatalog
    from app.models.purchased_input import PurchasedInput
    from app.models.shared_report import SharedReport

    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    purchase = db.query(PurchasedInput).first()
    purchase.purchase_date = date(2026, 11, 15)
    db.commit()
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda db, user: {PAGE_INPUTS})
    monkeypatch.setattr(routes, "_entitlements_for", lambda db, user: SimpleNamespace(module_allowed=lambda page: True))
    def query(history, assistant):
        assistant.call_tool("consultar_compras", json.dumps({"inicio": None, "fim": None, "item": "gasolina"}))
        return {"resposta": "Consulta feita.", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", query)
    thread, first = routes._assistant_turn(db, user, routes._repository(db), scope, None, "Minhas compras de gasolina")
    assert first["share_query"]["tool"] == "consultar_compras"
    purchase.total_cost = 999
    diesel = InputCatalog(name="Diesel", normalized_name="diesel", item_type="combustivel", default_unit="L")
    db.add(diesel)
    db.flush()
    db.add(PurchasedInput(farm_id=scope["active_farm_id"], input_id=diesel.id, name="Diesel", purchase_date=date(2026, 11, 15),
                          quantity_purchased=1, package_size=5, package_unit="L", unit_price=10, total_quantity=5,
                          available_quantity=5, total_cost=10))
    db.commit()
    def wrong_broad_query(history, assistant):
        assistant.call_tool("consultar_compras", json.dumps({"inicio": None, "fim": None, "item": None}))
        return {"resposta": "Vou preparar.", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", wrong_broad_query)
    _, result = routes._assistant_turn(db, user, routes._repository(db), scope, thread, "Compartilhe essa consulta")
    assert result.get("_shared_url")
    rows = json.loads(db.query(SharedReport).one().data_json)["rows"]
    assert len(rows) == 1 and 999.0 in rows[0]


def test_whatsapp_shares_page_without_storing_token_and_revokes_with_binding(monkeypatch):
    from datetime import date, timedelta
    from app.models.purchased_input import PurchasedInput
    from app.models.shared_report import SharedReport
    from app.models.whatsapp import WhatsAppLink
    from app.services import whatsapp_bot
    from app.services.shared_report import authorize_shared_report, find_shared_report
    from tests.test_whatsapp import FakeClient, _payload, _receive, _settings

    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    purchase = db.query(PurchasedInput).first()
    purchase.purchase_date = date(2026, 11, 15)
    db.commit()
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda db, user: {PAGE_INPUTS})
    monkeypatch.setattr(routes, "_entitlements_for", lambda db, user: SimpleNamespace(module_allowed=lambda page: True))
    def query(history, assistant):
        assistant.call_tool("consultar_compras", json.dumps({"inicio": None, "fim": None, "item": "gasolina"}))
        return {"resposta": "Feito.", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", query)
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "shared-link", f"SISFARM {code}"))
    assert _receive(db, client, _payload("5535999", "shared-query", "Consulte compras e gere link para compartilhar")) == ["done"]
    url = client.sent[-1][2].splitlines()[-1]
    assert "/acesso-compartilhado/relatorio#" in url
    report = db.query(SharedReport).one()
    thread = db.get(routes.AssistantThread, db.query(WhatsAppLink).one().thread_id)
    assert url not in thread.messages_json and authorize_shared_report(db, report)
    link = db.query(WhatsAppLink).one()
    link.verified_at += timedelta(seconds=1)
    assert not authorize_shared_report(db, report)
    link.verified_at -= timedelta(seconds=1)
    response = routes.revoke_assistant_shared_report(SimpleNamespace(session={}), thread.id, report.id, "csrf", db, user)
    assert response.status_code == 303 and find_shared_report(db, url.split("#")[-1]) is None


def test_share_url_is_encrypted_in_transient_session():
    from app.services.shared_report import seal_shared_url, unseal_shared_url
    url = "https://app.sisfarm.com.br/acesso-compartilhado/relatorio#test-secret"
    sealed = seal_shared_url(url)
    assert "test-secret" not in sealed and unseal_shared_url(sealed) == url
    assert unseal_shared_url("inválido") is None


def test_history_chart_is_derived_from_numbers_and_capped():
    result = {"registros": [{"data": "2026-11-15", "setor": "Setor 1", "volume_litros": 50, "duracao_minutos": 10},
                             {"data": "2026-11-14", "setor": "Setor 2", "volume_litros": 25, "duracao_minutos": 5}]}
    report = build_report("consultar_irrigacoes", result, farm_name="F", season_name="S")
    assert [bar["width"] for bar in report["chart"]["bars"]] == [100, 50]


def test_web_chat_and_mascot_receive_temporary_link_without_history_secret(monkeypatch):
    from datetime import date
    from app.models.purchased_input import PurchasedInput
    from app.services.shared_report import unseal_shared_url

    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    db.query(PurchasedInput).first().purchase_date = date(2026, 11, 15)
    db.commit()
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda db, user: {PAGE_INPUTS})
    monkeypatch.setattr(routes, "_entitlements_for", lambda db, user: SimpleNamespace(module_allowed=lambda page: True))
    def query(history, assistant):
        assistant.call_tool("consultar_compras", json.dumps({"inicio": None, "fim": None, "item": "gasolina"}))
        return {"resposta": "Consultei os dados.", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", query)
    request = SimpleNamespace(session={})
    response = routes.prepare_stock_assistant_action(request, message="Compras de gasolina em página para compartilhar",
                                                     csrf_token="csrf", thread_id=None, photo=None, db=db, user=user)
    thread_id = response.headers["location"].split("c=")[-1]
    sealed = request.session["assistant_shared_once"]["sealed_url"]
    url = unseal_shared_url(sealed)
    assert url and url not in sealed and url not in db.get(routes.AssistantThread, thread_id).messages_json
    scope["context_selection_required"] = False
    request.query_params, request.url = {}, SimpleNamespace(path="/mobile/assistente")
    monkeypatch.setattr(routes, "current_maintenance_state", lambda *args: None)
    monkeypatch.setattr(routes, "_build_global_notifications", lambda *args: {"important": [], "operational": [], "total": 0, "important_count": 0})
    rendered = {}
    def capture(template, context):
        rendered.update(context)
        return SimpleNamespace(headers={})
    monkeypatch.setattr(routes.templates, "TemplateResponse", capture)
    shown = routes.stock_assistant_page(request, c=thread_id, q=None, db=db, user=user, csrf_token="csrf")
    assert rendered["shared_once_url"] == url and shown.headers["Cache-Control"] == "no-store"
    assert "assistant_shared_once" not in request.session
    mascot = routes.pezinho_send_message(request, message="Compras de gasolina em página para compartilhar",
                                        csrf_token="csrf", thread_id=None, photo=None, db=db, user=user)
    payload = json.loads(mascot.body)
    assert "/acesso-compartilhado/relatorio#" in payload["shared_url"]
    assert payload["shared_url"] not in db.get(routes.AssistantThread, payload["thread_id"]).messages_json
