from datetime import date, datetime, timedelta, timezone
from app.services.chat_format import to_html as chat_to_html
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import ChoiceLoader, DictLoader, Environment, FileSystemLoader
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base
from app.models import InputCatalog, PurchasedInput, StockOutput
from app.models.stock_assistant_draft import StockAssistantDraft
from app.repositories.farm import FarmRepository
from app.services.stock_assistant import FarmAssistant, claim_draft, confirm_consumption, parse_consumption


@pytest.mark.parametrize("message", [
    "Hoje usei dois sacos de Vita Nit S de 20 kg no Setor 3.",
    "Hoje usei dois sacos de 20 kg de Vita Nit S no Setor 3.",
    "Hoje usei 40 kg de Vita Nit S no Setor 3.",
])
def test_parses_total_quantity(message):
    fields = parse_consumption(message, date(2026, 11, 15))
    assert fields == {"quantity": Decimal("40"), "unit": "kg", "product": "vita nit s",
                      "plot": "setor 3", "movement_date": date(2026, 11, 15)}


@pytest.mark.parametrize("message", [
    "Amanhã vou aplicar 40 kg de Vita Nit S no Setor 3.",
    "Hoje usei 40 kg/ha de Vita Nit S no Setor 3.",
    "Hoje usei Vita Nit S no Setor 3.",
    "31/02/2026 usei 40 kg de Vita Nit S no Setor 3.",
])
def test_refuses_missing_or_unsafe_values(message):
    with pytest.raises(ValueError):
        parse_consumption(message, date(2026, 11, 15))


@pytest.fixture
def scenario():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        repo = FarmRepository(db)
        product = InputCatalog(name="Vita Nit S", normalized_name="vita nit s")
        db.add(product)
        db.flush()
        lot_fields = dict(input_id=product.id, name=product.name, quantity_purchased=1,
                          package_size=50, package_unit="kg", unit_price=50,
                          total_quantity=50, available_quantity=50, total_cost=50)
        lot = PurchasedInput(farm_id=1, **lot_fields)
        shared_lot = PurchasedInput(farm_id=None, **lot_fields)
        draft = StockAssistantDraft(id="draft", user_id=1, farm_id=1, season_id=1, plot_id=1,
                                    input_id=product.id, movement_date=date(2026, 11, 15),
                                    quantity=40, unit="kg", expires_at=datetime.now(timezone.utc) + timedelta(minutes=30))
        db.add_all([lot, shared_lot, draft])
        db.commit()
        yield repo, draft, lot, shared_lot


def test_confirm_consumes_once(scenario):
    repo, draft, lot, shared_lot = scenario
    draft.notes = "Abastecimento do tratorito"
    assert repo.db.query(StockOutput).count() == 0 and lot.available_quantity == 50
    output = confirm_consumption(repo, draft)
    repo.db.commit()
    assert output.quantity == 40 and output.origin == "Assistente SiSFarm" and output.notes == "Abastecimento do tratorito"
    assert confirm_consumption(repo, draft) is None
    assert lot.available_quantity == 10 and shared_lot.available_quantity == 50
    assert repo.db.query(StockOutput).filter_by(purchased_input_id=None).count() == 1


def test_insufficient_farm_stock_rolls_back_claim(scenario):
    repo, draft, lot, _ = scenario
    draft.quantity = 60
    repo.db.commit()
    with pytest.raises(ValueError, match="Estoque insuficiente"):
        confirm_consumption(repo, draft)
    repo.db.rollback()
    assert draft.status == "pending" and lot.available_quantity == 50
    assert repo.db.query(StockOutput).count() == 0


def test_discard_prevents_confirmation(scenario):
    repo, draft, lot, _ = scenario
    assert claim_draft(repo.db, draft, "discarded")
    repo.db.commit()
    assert confirm_consumption(repo, draft) is None and lot.available_quantity == 50


def test_expired_draft_cannot_be_confirmed(scenario):
    repo, draft, lot, _ = scenario
    draft.expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    repo.db.commit()
    assert confirm_consumption(repo, draft) is None and lot.available_quantity == 50


def test_purchase_is_explained_without_preparing_a_consumption():
    with pytest.raises(ValueError, match="descreve uma compra"):
        parse_consumption("Comprei 20 litros de gasolina, R$5,70 o litro", date(2026, 11, 15))


def test_prepare_form_includes_csrf_token():
    templates_dir = Path(__file__).resolve().parents[1] / "app" / "templates"
    env = Environment(loader=ChoiceLoader([
        DictLoader({"base.html": "{% block content %}{% endblock %}"}),
        FileSystemLoader(templates_dir),
    ]), autoescape=True)
    env.filters["decimal_br"] = str
    env.globals["url_for"] = lambda *a, **k: "/static/x"
    env.filters["chat_html"] = chat_to_html
    html = env.get_template("stock_assistant.html").render(
        assistant_scope={"active_farm": {"name": "Fazenda"}, "active_season": {"name": "Safra"}},
        csrf_token="test-token",
    )
    form = html.split('action="/mobile/assistente"', 1)[1].split("</form>", 1)[0]
    assert 'name="csrf_token" value="test-token"' in form


def test_whatsapp_conversation_template_is_read_only():
    templates_dir = Path(__file__).resolve().parents[1] / "app" / "templates"
    env = Environment(loader=ChoiceLoader([
        DictLoader({"base.html": "{% block content %}{% endblock %}"}), FileSystemLoader(templates_dir),
    ]), autoescape=True)
    env.globals["url_for"] = lambda *a, **k: "/static/x"
    env.filters["chat_html"] = chat_to_html
    html = env.get_template("stock_assistant.html").render(
        assistant_scope={"active_farm": {"name": "Fazenda"}, "active_season": {"name": "Safra"}},
        thread=SimpleNamespace(id="t", title="WhatsApp · teste"), thread_channel="whatsapp", csrf_token="x",
        messages=[{"role": "assistant", "content": "Confira", "time": "", "draft": {
            "id": "draft", "kind": "compra", "state": "pending", "item": "Café", "unit": "kg"}}],
    )
    assert "Conversa do WhatsApp em modo consulta." in html
    assert 'action="/mobile/assistente/draft/confirmar"' not in html
    assert 'data-as-form' not in html


def test_voice_controls_initialize_without_composer():
    script = (Path(__file__).resolve().parents[1] / "app/static/js/assistant.js").read_text()
    assert script.index("$$('[data-as-voice]').forEach(initVoice)") < script.index("if (!form || !box) return")


def _tool_call(name, args, call_id="c1"):
    import json as _json
    return SimpleNamespace(id=call_id, function=SimpleNamespace(name=name, arguments=_json.dumps(args)))


def _fake_client(steps, calls):
    def create(**kwargs):
        calls.append(kwargs)
        step = steps.pop(0)
        message = SimpleNamespace(content=step if isinstance(step, str) else None,
                                  tool_calls=None if isinstance(step, str) else step)
        return SimpleNamespace(choices=[SimpleNamespace(message=message)])
    return SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))


def _assistant(prepared):
    season = SimpleNamespace(name="Café 2026", start_date=date(2026, 1, 1), end_date=date(2026, 12, 31))
    items = [SimpleNamespace(id=1, name="Gasolina comum", default_unit="L", item_type="combustivel", is_active=True),
             SimpleNamespace(id=2, name="Ureia", default_unit="kg", item_type="insumo_agricola", is_active=True)]
    plots = [SimpleNamespace(id=7, name="Setor 1", area_hectares=5), SimpleNamespace(id=8, name="Setor 10", area_hectares=3)]
    return FarmAssistant(farm_name="Bela Vista", season=season, items=items, balances={1: 50, 2: 10}, plots=plots,
                         today=date(2026, 11, 15),
                         prepare_draft=lambda **kw: prepared.append(kw) or "d" * 32)


def test_agent_searches_item_then_prepares_draft():
    prepared, calls = [], []
    steps = [
        [_tool_call("buscar_itens", {"termo": "gasolina"})],
        [_tool_call("preparar_consumo", {"item_id": 1, "quantidade": 5, "unidade": "litros", "data": "2026-11-15", "setor": None,
                                         "observacao": "Abastecimento  do tratorito"}, "c2")],
    ]
    result = _assistant(prepared).run([{"role": "user", "content": "usei 5 litros de gasolina hoje"}],
                                      client=_fake_client(steps, calls), model="m")
    assert result["draft_id"] == "d" * 32 and prepared[0]["unit"] == "L" and prepared[0]["plot"] is None
    assert prepared[0]["notes"] == "Abastecimento do tratorito"
    tool_replies = [m["content"] for m in calls[1]["messages"] if m.get("role") == "tool"]
    assert "Gasolina comum" in tool_replies[0] and '"saldo_na_fazenda": 50' in tool_replies[0]
    assert "15/11/2026" in calls[0]["messages"][0]["content"] and calls[0]["tools"]


def test_agent_tool_errors_go_back_to_the_model():
    prepared, calls = [], []
    steps = [
        [_tool_call("preparar_consumo", {"item_id": 2, "quantidade": 40, "unidade": "kg", "data": "2026-11-15", "setor": None})],
        "Em qual setor você usou a ureia?",
    ]
    result = _assistant(prepared).run([{"role": "user", "content": "usei 40 kg de ureia"}], client=_fake_client(steps, calls), model="m")
    assert result == {"resposta": "Em qual setor você usou a ureia?", "draft_id": None} and not prepared
    assert any("erro" in m["content"] for m in calls[1]["messages"] if m.get("role") == "tool")


def test_pezinho_conversation_and_confirmation_stay_in_same_thread(monkeypatch):
    draft = {"item_id": 0, "quantidade": 5, "unidade": "L", "data": "2026-11-15", "setor": None}
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, ["Foi hoje?", draft])
    req = SimpleNamespace(session={})
    response = routes.pezinho_send_message(req, message="Usei gasolina", csrf_token="x", thread_id=None, db=db, user=user)
    first = json.loads(response.body)
    assert first["messages"][-1]["content"] == "Foi hoje?"
    thread_id = first["thread_id"]
    response = routes.pezinho_send_message(req, message="Sim, 5 litros", csrf_token="x", thread_id=thread_id, db=db, user=user)
    second = json.loads(response.body)
    assert second["thread_id"] == thread_id and second["messages"][-1]["draft"]["state"] == "pending"
    assert db.query(StockOutput).filter_by(purchased_input_id=None).count() == 0
    draft_id = second["messages"][-1]["draft"]["id"]
    response = routes.pezinho_resolve_draft(req, draft_id, "confirmar", csrf_token="x", thread_id=thread_id, db=db, user=user)
    assert json.loads(response.body)["messages"][-1]["content"].startswith("Pronto!")
    assert db.query(StockOutput).filter_by(purchased_input_id=None).count() == 1


def test_pezinho_rejects_whatsapp_and_foreign_thread(monkeypatch):
    from fastapi import HTTPException
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, ["Oi"])
    send("Oi")
    thread = db.query(routes.AssistantThread).one()
    thread.channel = "whatsapp"; db.commit()
    req = SimpleNamespace(session={})
    with pytest.raises(HTTPException) as exc:
        routes.pezinho_get_chat(req, thread_id=thread.id, db=db, user=user)
    assert exc.value.status_code == 404
    with pytest.raises(HTTPException):
        routes.pezinho_send_message(req, message="Outro texto", csrf_token="x", thread_id=thread.id, db=db, user=user)
    other = SimpleNamespace(id=user.id + 1, organization_id=user.organization_id)
    with pytest.raises(HTTPException):
        routes.pezinho_get_chat(req, thread_id=thread.id, db=db, user=other)


def test_pezinho_purchase_remains_draft_until_confirmation(monkeypatch):
    purchase = {"_tool": "preparar_compra", "item_id": 0, "nome_novo": None, "tipo": None, "categoria": None,
                "unidade": "L", "quantidade_embalagens": 1, "tamanho_embalagem": 10,
                "preco_por_embalagem": 50, "valor_total": None, "data": "2026-11-15", "fornecedor": None,
                "observacao": None, "forma_pagamento": None, "condicao": None, "parcelas": None,
                "periodicidade": None, "primeira_parcela": None, "conta_id": None, "cartao_id": None,
                "cartao_desconhecido": False}
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, [purchase])
    request = SimpleNamespace(session={})
    response = routes.pezinho_send_message(request, message="Comprei gasolina", csrf_token="x", thread_id=None, db=db, user=user)
    payload = json.loads(response.body)
    draft = payload["messages"][-1]["draft"]
    assert draft["state"] == "pending" and draft["total"] == "R$ 50,00"
    assert db.query(PurchasedInput).count() == 1
    routes.pezinho_resolve_draft(request, draft["id"], "confirmar", csrf_token="x", thread_id=payload["thread_id"], db=db, user=user)
    assert db.query(PurchasedInput).count() == 2


def test_agent_sends_pdf_as_file_not_image():
    calls = []
    pdf = "data:application/pdf;base64,JVBERi0xLjQ="
    _assistant([]).run([{"role": "user", "content": "Leia a nota"}],
                       client=_fake_client(["Pode confirmar os dados?"], calls), model="m", image=pdf)
    content = calls[0]["messages"][-1]["content"]
    assert content[1] == {"type": "file", "file": {"filename": "documento.pdf", "file_data": pdf}}


def test_invoice_items_are_presented_without_creating_a_draft():
    prepared, calls = [], []
    assistant = _assistant(prepared)
    assistant.prepare_purchase = lambda **kwargs: ("never", [])
    assistant.handlers["apresentar_itens_nota"] = assistant.apresentar_itens_nota
    invoice = {"itens": [{"nome": "Glifosato", "quantidade": "2", "unidade": "L", "valor": "total R$ 80"},
                         {"nome": "Ureia", "quantidade": None, "unidade": "kg", "valor": None}],
               "aviso": "Quantidade da ureia ilegível"}
    result = assistant.run([{"role": "user", "content": "Leia a nota"}], client=_fake_client(
        [[_tool_call("apresentar_itens_nota", invoice)]], calls), model="m", image="data:application/pdf;base64,JVBERg==")
    assert result["draft_id"] is None and not prepared
    assert "1. Glifosato" in result["resposta"] and "2. Ureia" in result["resposta"]
    assert "não identificado" in result["resposta"] and assistant.invoice == invoice


def test_invoice_requires_attachment_and_disallows_draft_during_reading():
    assistant = _assistant([])
    assistant.handlers["apresentar_itens_nota"] = assistant.apresentar_itens_nota
    assert "anexada" in assistant.call_tool("apresentar_itens_nota", json.dumps({"itens": [{"nome": "X"}]}))["erro"]
    assistant.reading_attachment = True
    assert "aguarde" in assistant.call_tool("preparar_compra", '{}')["erro"]


def test_invoice_list_is_remembered_for_followup(monkeypatch):
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, [])
    calls = []
    invoice = {"itens": [{"nome": "Glifosato", "quantidade": "2", "unidade": "L", "valor": "R$ 80"},
                         {"nome": "Ureia", "quantidade": None, "unidade": "kg", "valor": None}], "aviso": None}
    def run(history, assistant, **kwargs):
        calls.append(history)
        if len(calls) == 1:
            assistant.invoice = invoice
            return {"resposta": "1. Glifosato\n2. Ureia", "draft_id": None}
        return {"resposta": "Vamos conferir a ureia.", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", run)
    send("leia a nota")
    assert history()[-1]["invoice"] == invoice
    send("quero o item 2")
    assert any("Glifosato" in entry["content"] and "Ureia" in entry["content"]
               for entry in calls[1] if entry["role"] == "system")


def test_assistant_template_accepts_pdf():
    template = (Path(__file__).resolve().parents[1] / "app/templates/stock_assistant.html").read_text()
    assert 'accept="image/jpeg,image/png,image/webp,application/pdf"' in template


def test_assistant_knows_its_name():
    assert "Seu nome é Pezinho" in _assistant([]).instructions()


def test_card_question_does_not_replace_unrelated_answer(monkeypatch):
    from app.models import FinanceAccount, FinanceCreditCard
    answer = "Seus cartões são Nubank e Ourocard. Qual deles você quer consultar?"
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, [answer])
    account = FinanceAccount(farm_id=scope["active_farm"].id, account_name="Conta", initial_balance=0,
                             initial_balance_date=date(2026, 1, 1), bank_code="756", bank_name="Banco",
                             created_at=datetime.now(timezone.utc))
    db.add(account); db.flush()
    for name in ("Nubank", "Ourocard"):
        db.add(FinanceCreditCard(farm_id=scope["active_farm"].id, payment_account_id=account.id, card_name=name,
                                 issuer=name, brand="Visa", closing_day=5, due_day=12, is_active=True,
                                 created_at=datetime.now(timezone.utc)))
    db.commit()
    send("Quais cartões tenho cadastrados?")
    assert history()[-1]["content"] == answer
    assert "card_options" not in history()[-1]


def test_drag_and_drop_is_available_only_for_editable_chat():
    templates_dir = Path(__file__).resolve().parents[1] / "app" / "templates"
    env = Environment(loader=ChoiceLoader([
        DictLoader({"base.html": "{% block content %}{% endblock %}"}), FileSystemLoader(templates_dir),
    ]), autoescape=True)
    env.globals["url_for"] = lambda *a, **k: "/static/x"
    common = {"assistant_scope": {"active_farm": {"name": "Fazenda"}, "active_season": {"name": "Safra"}},
              "csrf_token": "x"}
    template = env.get_template("stock_assistant.html")
    assert "data-as-drop-overlay" in template.render(**common, thread_channel="web")
    assert "data-as-drop-overlay" not in template.render(**common, thread_channel="whatsapp")


def test_openai_sdk_serializes_pdf_file_part(monkeypatch):
    import json
    import httpx
    from openai import OpenAI
    requests = []
    def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, request=request, json={"id": "chat-test", "object": "chat.completion", "created": 0,
                                        "model": "test", "choices": [{"index": 0, "finish_reason": "stop",
                                        "message": {"role": "assistant", "content": "Confira a nota."}}]})
    client = OpenAI(api_key="test", http_client=httpx.Client(transport=httpx.MockTransport(handler)))
    monkeypatch.setattr(client._client, "send", lambda request, **kwargs: handler(request))
    _assistant([]).run([{"role": "user", "content": "Leia"}], client=client, model="test",
                       image="data:application/pdf;base64,JVBERi0xLjQ=")
    assert requests[0]["messages"][-1]["content"][1]["type"] == "file"


def test_agent_validations():
    assistant = _assistant([])
    assert assistant.call_tool("preparar_consumo", '{"item_id": 2, "quantidade": 40, "unidade": "kg", "data": "2026-11-15", "setor": "Setor 1"}')["erro"].startswith("Saldo insuficiente")
    assert "futura" in assistant.call_tool("preparar_consumo", '{"item_id": 1, "quantidade": 1, "unidade": "L", "data": "2026-11-16", "setor": null}')["erro"]
    assert "ambíguo" in assistant.call_tool("preparar_consumo", '{"item_id": 2, "quantidade": 1, "unidade": "kg", "data": "2026-11-15", "setor": "Setor"}')["erro"]
    assert "unidade cadastrada" in assistant.call_tool("preparar_consumo", '{"item_id": 2, "quantidade": 1, "unidade": "L", "data": "2026-11-15", "setor": "Setor 1"}')["erro"]
    assert assistant.call_tool("buscar_itens", '{"termo": "abacaxi"}')["itens"] == []
    assert assistant.call_tool("nada", "{}")["erro"]


def _openai_error(cls, status, code):
    import httpx
    request = httpx.Request("POST", "https://api.openai.com/v1/chat/completions")
    body = {"message": "x", "type": "x", "code": code}
    return cls("x", response=httpx.Response(status, request=request, json={"error": body}), body=body)


def test_openai_errors_get_specific_messages():
    import openai
    from app.services.stock_assistant import explain_openai_error
    assert "sem créditos" in explain_openai_error(_openai_error(openai.RateLimitError, 429, "insufficient_quota"), "gpt-4o-mini")
    assert "Aguarde" in explain_openai_error(_openai_error(openai.RateLimitError, 429, "rate_limit_exceeded"), "gpt-4o-mini")
    assert "inválida" in explain_openai_error(_openai_error(openai.AuthenticationError, 401, "invalid_api_key"), "m")
    assert "acesso ao modelo gpt-x" in explain_openai_error(_openai_error(openai.NotFoundError, 404, "model_not_found"), "gpt-x")
    assert "Tente novamente" in explain_openai_error(RuntimeError("boom"), "m")


def _route_env(monkeypatch, replies):
    from app.models import Farm, Organization, User
    from app.models.crop_season import CropSeason
    from app.web import routes

    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    db = Session(engine)
    org = Organization(name="Org", slug="org", is_active=True)
    db.add(org); db.flush()
    user = User(name="Ana", email="ana@x.com", hashed_password="x", organization_id=org.id, is_active=True)
    farm = Farm(name="Bela Vista", location="MG", total_area=10, organization_id=org.id)
    db.add_all([user, farm]); db.flush()
    season = CropSeason(name="Café 2026", farm_id=farm.id, culture="Café", start_date=date(2026, 1, 1),
                        end_date=date(2026, 12, 31), cultivated_area=0, area_unit="ha", status="ativa")
    fuel = InputCatalog(name="Gasolina", normalized_name="gasolina", item_type="combustivel", default_unit="L")
    db.add_all([season, fuel]); db.flush()
    db.add(PurchasedInput(farm_id=farm.id, input_id=fuel.id, name="Gasolina", quantity_purchased=1, package_size=50,
                          package_unit="L", unit_price=300, total_quantity=50, available_quantity=50, total_cost=300))
    db.commit()
    scope = {"active_farm": farm, "active_season": season, "active_farm_id": farm.id, "active_season_id": season.id}
    sent = []

    def fake_run(history, assistant):
        sent.append((history, [item.name for item in assistant.items.values()]))
        reply = replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        if isinstance(reply, dict):
            reply = dict(reply)
            tool = reply.pop("_tool", "preparar_consumo")
            expect_error = reply.pop("_expect_error", False)
            if reply.get("item_id") == 0:
                reply["item_id"] = next(iter(assistant.items))
            result = assistant.call_tool(tool, __import__("json").dumps(reply))
            if expect_error:
                return {"resposta": result["erro"], "draft_id": None}
            assert "erro" not in result, result
            return {"resposta": "Preparei o lançamento.", "draft_id": assistant.draft_id}
        return {"resposta": reply, "draft_id": None}

    monkeypatch.setattr(routes, "validate_csrf", lambda *a: None)
    monkeypatch.setattr(routes, "_can_use_stock_assistant", lambda *a: True)
    monkeypatch.setattr(routes, "_global_scope_context", lambda *a, **k: scope)
    monkeypatch.setattr(routes, "_season_launch_plots", lambda plots, scope: plots)
    monkeypatch.setattr(routes, "_plot_matches_scope", lambda plot, scope: True)
    monkeypatch.setattr(routes, "run_farm_assistant", fake_run)
    monkeypatch.setattr(routes, "today_in_app_timezone", lambda: date(2026, 11, 15))
    request = SimpleNamespace(session={})
    state = {"thread": None}

    def send(text):
        response = routes.prepare_stock_assistant_action(request, message=text, csrf_token="x", thread_id=state["thread"],
                                                         photo=None, db=db, user=user)
        state["thread"] = response.headers["location"].split("c=")[-1]
        return response

    def history():
        return routes._thread_messages(routes._assistant_thread(db, user, scope, state["thread"]))
    return routes, db, user, scope, send, sent, history


def test_route_keeps_conversation_on_error_and_drafts_fuel_without_plot(monkeypatch):
    replies = [
        ValueError("A conta da OpenAI usada pelo SiSFarm está sem créditos."),
        "Foi hoje?",
        {"item_id": 0, "quantidade": 5, "unidade": "litros", "data": "2026-11-15", "setor": None},
    ]
    routes, db, user, scope, send, sent, thread_history = _route_env(monkeypatch, replies)
    send("Utilizei 5 litros de gasolina")
    history = thread_history()
    assert [m["role"] for m in history] == ["user", "assistant"] and history[1]["error"]
    send("Utilizei 5 litros de gasolina")
    # o aviso de erro nao vai para a IA; o item de combustivel com estoque na fazenda vai
    assert sent[1][0] == [{"role": "user", "content": "Utilizei 5 litros de gasolina"},
                          {"role": "user", "content": "Utilizei 5 litros de gasolina"}]
    assert sent[1][1] == ["Gasolina"]
    send("sim, hoje")
    draft = db.query(StockAssistantDraft).one()
    assert draft.plot_id is None and draft.unit == "L" and draft.quantity == 5
    history = thread_history()
    assert len(history) == 6
    card = history[-1]["draft"]
    assert card["id"] == draft.id and card["state"] == "pending" and card["plot"] == "Uso geral da fazenda"
    assert card["balance"] == "50" or card["balance"].startswith("50")


def test_audio_timeline_access_delete_and_retention(monkeypatch):
    from app.models.assistant_audio import AssistantAudio
    from app.services.assistant_audio import delete_expired_audio
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, ["Entendi"])
    now = datetime.now(timezone.utc)
    audio = AssistantAudio(id="a" * 32, user_id=user.id, organization_id=user.organization_id,
                           farm_id=scope["active_farm_id"], season_id=scope["active_season_id"],
                           mime="audio/ogg", data=b"voz", expires_at=now + timedelta(days=30))
    db.add(audio); db.commit()
    req = SimpleNamespace(session={})
    routes.prepare_stock_assistant_action(req, message="Minha transcrição", csrf_token="x", thread_id=None,
                                           photo=None, audio_id=audio.id, db=db, user=user)
    assert audio.thread_id
    thread = db.get(routes.AssistantThread, audio.thread_id)
    assert routes._thread_messages(thread)[0]["audio_id"] == audio.id
    assert routes.assistant_audio_file(audio.id, req, db=db, user=user).body == b"voz"
    other = SimpleNamespace(id=user.id + 1, organization_id=user.organization_id)
    assert routes._assistant_audio_for_user(db, other, scope, audio.id) is None
    routes.delete_assistant_audio(audio.id, req, csrf_token="x", db=db, user=user)
    assert db.get(AssistantAudio, audio.id) is None
    assert routes._thread_messages(thread)[0]["content"] == "Minha transcrição"
    expired = AssistantAudio(id="b" * 32, user_id=user.id, organization_id=user.organization_id,
                             farm_id=scope["active_farm_id"], season_id=scope["active_season_id"],
                             mime="audio/ogg", data=b"voz", expires_at=now - timedelta(seconds=1))
    db.add(expired); db.commit()
    assert delete_expired_audio(db) == 1


def test_voice_submission_transcribes_and_records_audio(monkeypatch):
    from io import BytesIO
    from app.models.assistant_audio import AssistantAudio
    from app.services import assistant_audio
    from starlette.datastructures import Headers, UploadFile
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, ["Entendi"])
    monkeypatch.setattr(assistant_audio, "transcribe_audio", lambda data, mime: "Tem gasolina no estoque?")
    upload = UploadFile(file=BytesIO(b"voice"), filename="voz.webm", headers=Headers({"content-type": "audio/webm"}))
    request = SimpleNamespace(session={})
    response = routes.prepare_stock_assistant_action(request, message="[mensagem de áudio]", csrf_token="x",
                                                      thread_id=None, photo=None, audio_id=None, voice=upload, db=db, user=user)
    thread = db.get(routes.AssistantThread, response.headers["location"].split("c=")[-1])
    entry = routes._thread_messages(thread)[0]
    assert entry["content"] == "Tem gasolina no estoque?"
    assert db.get(AssistantAudio, entry["audio_id"]).data == b"voice"


def test_attachment_persists_in_history_and_is_private(monkeypatch):
    from io import BytesIO
    from starlette.datastructures import Headers, UploadFile
    from app.models.assistant_attachment import AssistantAttachment
    from fastapi import HTTPException
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, [])
    monkeypatch.setattr(routes, "run_farm_assistant", lambda history, assistant, **kw: {"resposta": "Recebi.", "draft_id": None})
    upload = UploadFile(file=BytesIO(b"%PDF-1.4 test"), filename="nota.pdf", headers=Headers({"content-type": "application/pdf"}))
    req = SimpleNamespace(session={})
    response = routes.prepare_stock_assistant_action(req, message="Analise", csrf_token="x", thread_id=None,
                                                     photo=upload, audio_id=None, voice=None, db=db, user=user)
    thread = db.get(routes.AssistantThread, response.headers["location"].split("c=")[-1])
    messages = routes._thread_messages(thread)
    attachment_id = messages[0]["attachment_id"]
    attachment = db.get(AssistantAttachment, attachment_id)
    assert attachment.filename == "nota.pdf" and attachment.thread_id == thread.id
    assert routes.assistant_attachment_file(attachment_id, req, db=db, user=user).body == b"%PDF-1.4 test"
    other = SimpleNamespace(id=user.id + 1, organization_id=user.organization_id)
    with pytest.raises(HTTPException) as exc:
        routes.assistant_attachment_file(attachment_id, req, db=db, user=other)
    assert exc.value.status_code == 404
    routes._save_thread(db, thread, messages + [{"role": "user", "content": str(i)} for i in range(50)])
    assert routes._thread_messages(thread)[0]["attachment_id"] == attachment_id
    routes.delete_assistant_attachment(attachment_id, req, csrf_token="x", db=db, user=user)
    assert db.get(AssistantAttachment, attachment_id) is None


def test_assistant_rejects_attachment_with_wrong_signature():
    from app.web.routes import _assistant_attachment_data
    with pytest.raises(ValueError, match="não corresponde"):
        _assistant_attachment_data(b"not an image", "image/png")


def test_new_draft_replaces_pending_one_and_confirm_updates_thread(monkeypatch):
    draft_reply = {"item_id": 0, "quantidade": 5, "unidade": "L", "data": "2026-11-15", "setor": None}
    routes, db, user, scope, send, sent, thread_history = _route_env(monkeypatch, [draft_reply, {"_tool": "ajustar_rascunho", "alteracoes": {"quantidade": 4}}])
    send("5 litros de gasolina hoje")
    send("na verdade foram 4")
    first, second = db.query(StockAssistantDraft).order_by(StockAssistantDraft.quantity.desc()).all()
    assert first.status == "discarded" and second.status == "pending"
    history = thread_history()
    assert history[1]["draft"]["state"] == "replaced" and history[3]["draft"]["state"] == "pending"
    assert history[3]["draft"]["quantity"] in ("4", "4,00")
    location = routes.resolve_stock_assistant_action(
        SimpleNamespace(session={}), second.id, "confirmar", csrf_token="x",
        thread_id=db.query(routes.AssistantThread).one().id, db=db, user=user,
    ).headers["location"]
    assert "c=" in location
    history = thread_history()
    assert history[3]["draft"]["state"] == "confirmed" and history[-1]["content"].startswith("Pronto!")
    summary = db.query(StockOutput).filter_by(purchased_input_id=None).one()
    assert float(summary.quantity) == 4


def test_adjusting_consumption_preserves_other_fields(monkeypatch):
    draft_reply = {"item_id": 0, "quantidade": 5, "unidade": "L", "data": "2026-11-15", "setor": None,
                   "observacao": "Trator"}
    adjust = {"_tool": "ajustar_rascunho", "alteracoes": {"quantidade": 4}}
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, [draft_reply, adjust])
    send("usei 5 litros de gasolina no trator hoje")
    first = db.query(StockAssistantDraft).one()
    send("quero ajustar a quantidade para 4")
    second = db.query(StockAssistantDraft).filter_by(status="pending").one()
    assert first.status == "discarded" and second.quantity == 4
    assert second.input_id == first.input_id and second.movement_date == first.movement_date
    assert second.unit == first.unit and second.notes == "Trator"
    assert "Rascunho de consumo pendente" in sent[1][0][0]["content"]


def test_explicit_adjustment_rejects_full_recreation_and_unsupported_field():
    assistant = _assistant([])
    assistant.pending_draft = {"kind": "consumo", "args": {"item_id": 1, "quantidade": 5, "unidade": "L",
                                                               "data": "2026-11-15", "setor": None}}
    assistant.require_patch = True
    assistant.handlers["ajustar_rascunho"] = assistant.ajustar_rascunho
    assert "use ajustar_rascunho" in assistant.call_tool("preparar_consumo", '{}')["erro"]
    assert "não pode ser ajustado" in assistant.call_tool("ajustar_rascunho", '{"alteracoes":{"item_id":2}}')["erro"]


def test_adjusting_purchase_preserves_unmodified_fields(monkeypatch):
    purchase = {"_tool": "preparar_compra", "item_id": 0, "nome_novo": None, "tipo": None, "categoria": None,
                "unidade": "L", "quantidade_embalagens": 2, "tamanho_embalagem": 20,
                "preco_por_embalagem": 100, "valor_total": None, "data": "2026-11-15",
                "fornecedor": "Revenda", "observacao": "Nota 7", "forma_pagamento": None,
                "condicao": "a_vista", "parcelas": None, "periodicidade": None, "primeira_parcela": None,
                "conta_id": None, "cartao_id": None, "cartao_desconhecido": False, "lote": "L7"}
    adjust = {"_tool": "ajustar_rascunho", "alteracoes": {"preco_por_embalagem": 120}}
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, [purchase, adjust])
    send("comprei gasolina")
    first = db.query(StockAssistantDraft).one()
    old = json.loads(first.payload_json)
    send("ajuste o preço para 120 por embalagem")
    second = db.query(StockAssistantDraft).filter_by(status="pending").one()
    updated = json.loads(second.payload_json)
    assert first.status == "discarded" and updated["unit_price"] == "120.0000"
    for field in ("name", "quantity_purchased", "package_size", "purchase_date", "supplier_name",
                  "notes", "payment_method", "payment_condition", "credit_card_id", "finance_account_id"):
        assert updated[field] == old[field]


def test_adjusting_supplier_does_not_change_card(monkeypatch):
    from app.models import FinanceAccount, FinanceCreditCard
    purchase = {"_tool": "preparar_compra", "item_id": 0, "nome_novo": None, "tipo": None, "categoria": None,
                "unidade": "L", "quantidade_embalagens": 1, "tamanho_embalagem": 20, "preco_por_embalagem": 100,
                "valor_total": None, "data": "2026-11-15", "fornecedor": "Loja", "observacao": None,
                "forma_pagamento": "Cartão de crédito", "condicao": "a_prazo", "parcelas": 3,
                "periodicidade": "mensal", "primeira_parcela": None, "conta_id": None, "cartao_id": None,
                "cartao_desconhecido": False}
    adjust = {"_tool": "ajustar_rascunho", "alteracoes": {"fornecedor": "Nubank"}}
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, [purchase, adjust])
    farm = scope["active_farm"]
    account = FinanceAccount(farm_id=farm.id, account_name="Conta", initial_balance=0,
                             initial_balance_date=date(2026, 1, 1), bank_code="756", bank_name="Banco",
                             created_at=datetime.now(timezone.utc))
    db.add(account); db.flush()
    cards = [FinanceCreditCard(farm_id=farm.id, payment_account_id=account.id, card_name=name, issuer=name,
                               brand="Visa", closing_day=5, due_day=12, is_active=True,
                               created_at=datetime.now(timezone.utc)) for name in ("Nubank", "Ourocard")]
    db.add_all(cards); db.commit()
    purchase["cartao_id"] = cards[1].id
    send("comprei gasolina no Ourocard em 3 parcelas")
    old = json.loads(db.query(StockAssistantDraft).one().payload_json)
    send("ajuste o fornecedor para Nubank")
    new = json.loads(db.query(StockAssistantDraft).filter_by(status="pending").one().payload_json)
    assert new["supplier_name"] == "Nubank"
    for field in ("credit_card_id", "finance_account_id", "payment_method", "payment_condition",
                  "installment_count", "installment_frequency", "first_installment_date"):
        assert new[field] == old[field]


def test_purchase_of_new_and_existing_items(monkeypatch):
    from app.models import PurchasedInput as Lot
    new_item = {"_tool": "preparar_compra", "item_id": None, "nome_novo": "Ureia 46%", "tipo": "insumo_agricola",
                "categoria": "fertilizantes", "unidade": "kg", "quantidade_embalagens": 10, "tamanho_embalagem": 50,
                "preco_por_embalagem": None, "valor_total": 1800, "data": "2026-11-14", "fornecedor": "Cooperativa",
                "observacao": "Nota 123"}
    fuel = {"_tool": "preparar_compra", "item_id": 0, "nome_novo": None, "tipo": None, "categoria": None, "unidade": "litros",
            "quantidade_embalagens": 1, "tamanho_embalagem": 200, "preco_por_embalagem": 1200, "valor_total": None,
            "data": "2026-11-15", "fornecedor": None, "observacao": None}
    routes, db, user, scope, send, sent, thread_history = _route_env(monkeypatch, [new_item, fuel])
    thread_id = lambda: db.query(routes.AssistantThread).one().id

    send("comprei 10 sacos de ureia 46% de 50 kg, paguei 1800")
    draft = db.query(StockAssistantDraft).one()
    assert draft.kind == "compra" and draft.input_id is None and float(draft.quantity) == 500
    card = thread_history()[-1]["draft"]
    assert card["is_new"] and card["total"] == "R$ 1.800,00" and card["packs_label"] == "10 × 50 kg"
    routes.resolve_stock_assistant_action(SimpleNamespace(session={}), draft.id, "confirmar", csrf_token="x",
                                          thread_id=thread_id(), db=db, user=user)
    lot = db.query(Lot).filter(Lot.name == "Ureia 46%").one()
    assert float(lot.total_quantity) == 500 and float(lot.total_cost) == 1800 and lot.input_catalog.category == "Fertilizantes"
    assert "Nota 123" in lot.notes and thread_history()[-1]["content"].startswith("Prontinho!")
    assert lot.supplier_name == "Cooperativa"

    send("comprei 200 litros de gasolina por 1200 hoje")
    fuel_draft = db.query(StockAssistantDraft).filter_by(status="pending").one()
    assert fuel_draft.unit == "L" and thread_history()[-1]["draft"]["after"] == "250,00"
    req = SimpleNamespace(session={})
    routes.resolve_stock_assistant_action(req, fuel_draft.id, "confirmar", csrf_token="x",
                                          thread_id=thread_id(), db=db, user=user)
    assert not req.session.get("flash"), req.session
    assert db.query(Lot).filter(Lot.name == "Gasolina").count() == 2


def test_purchase_validations():
    from app.services.stock_assistant import FarmAssistant
    import json
    item = SimpleNamespace(id=1, name="Diesel S10", default_unit="L", item_type="combustivel", category="Combustíveis", is_active=True)
    assistant = FarmAssistant(farm_name="F", season=None, items=[item], balances={1: 0}, plots=[], today=date(2026, 11, 15),
                              prepare_draft=lambda **kw: None, prepare_purchase=lambda **kw: ("x" * 32, []),
                              purchase_rules={"combustivel": {"Combustíveis": ["L", "ml"]}})
    base = {"item_id": None, "nome_novo": "Gasolina aditivada", "tipo": "combustivel", "categoria": "Combustíveis",
            "unidade": "L", "quantidade_embalagens": 1, "tamanho_embalagem": 50, "preco_por_embalagem": None,
            "valor_total": None, "data": "2026-11-15", "fornecedor": None, "observacao": None}
    call = lambda **kw: assistant.call_tool("preparar_compra", json.dumps({**base, **kw}))
    assert "preço" in call()["erro"]
    assert "Unidade inválida" in call(unidade="kg", valor_total=10)["erro"]
    assert "Categoria inválida" in call(categoria="Fungicida", valor_total=10)["erro"]
    assert "já está cadastrado" in call(nome_novo="diesel s10", valor_total=10)["erro"]
    assert "futura" in call(data="2026-11-16", valor_total=10)["erro"]
    assert call(valor_total=300)["ok"]
    # consumo de item sem saldo pede a compra antes
    assert "não tem saldo" in assistant.call_tool("preparar_consumo", json.dumps(
        {"item_id": 1, "quantidade": 1, "unidade": "L", "data": "2026-11-15", "setor": None, "observacao": None}))["erro"]


def test_purchase_payment_and_lot_degrade_gracefully(monkeypatch):
    from app.models import FinanceAccount, FinanceCreditCard, FinanceTransaction, PurchasedInput as Lot
    base = {"_tool": "preparar_compra", "item_id": 0, "nome_novo": None, "tipo": None, "categoria": None, "unidade": "L",
            "quantidade_embalagens": 1, "tamanho_embalagem": 100, "preco_por_embalagem": 600, "valor_total": None,
            "data": "2026-11-15", "fornecedor": None, "observacao": None, "forma_pagamento": None, "condicao": None,
            "parcelas": None, "periodicidade": None, "primeira_parcela": None, "conta_id": None, "cartao_id": None,
            "lote": None, "fabricacao": None, "validade": None, "estoque_minimo": None}
    replies = [
        # cartao sem nenhum cartao cadastrado e datas de lote invertidas -> a vista, sem datas
        {**base, "forma_pagamento": "cartão de crédito", "parcelas": 10, "lote": "L-77",
         "fabricacao": "2026-12-01", "validade": "2026-01-01"},
        # parcelado no cartao (agora existe) com lote e estoque minimo
        {**base, "forma_pagamento": "Cartão de crédito", "parcelas": 3, "lote": "A1", "validade": "2027-05-01", "estoque_minimo": 20},
    ]
    routes, db, user, scope, send, sent, thread_history = _route_env(monkeypatch, replies)
    # o item do cenario e combustivel (sem lote); aqui o foco e a degradacao segura dos dados de lote
    monkeypatch.setattr(routes, "purchase_traceability_enabled", lambda *a: True)
    send("comprei 100 litros de gasolina por 600 no cartão em 10x")
    card = thread_history()[-1]["draft"]
    assert card["payment"] == "À vista" and card["finance"] == "Sem lançamento no financeiro" and card["lot"] == "Lote L-77"
    payload = __import__("json").loads(db.query(StockAssistantDraft).one().payload_json)
    assert payload["payment_condition"] == "a_vista" and payload["manufacture_date"] is None

    farm = scope["active_farm"]
    account = FinanceAccount(farm_id=farm.id, account_name="Sicoob", initial_balance=0, initial_balance_date=date(2026, 1, 1), is_default=True,
                             bank_code="756", bank_name="Sicoob", display_order=0, created_at=datetime.now(timezone.utc))
    db.add(account); db.flush()
    db.add(FinanceCreditCard(farm_id=farm.id, payment_account_id=account.id, card_name="Nubank", issuer="Nubank",
                             brand="Visa", closing_day=5, due_day=12, is_default=True, is_active=True,
                             display_order=0, created_at=datetime.now(timezone.utc)))
    db.commit()
    send("comprei de novo, no cartão em 3x")
    assert sent[1][0][0]["role"] == "system" and "Rascunho de compra pendente" in sent[1][0][0]["content"]
    card = thread_history()[-1]["draft"]
    assert card["payment"].startswith("Parcelado em 3x · Cartão de crédito Nubank") and "cartão Nubank" in card["finance"]
    assert card["lot"] == "Lote A1 · Val. 01/05/2027" and card["minimum"] == "20,00 L"
    draft = db.query(StockAssistantDraft).filter_by(status="pending").one()
    req = SimpleNamespace(session={})
    routes.resolve_stock_assistant_action(req, draft.id, "confirmar", csrf_token="x",
                                          thread_id=db.query(routes.AssistantThread).one().id, db=db, user=user)
    assert not req.session.get("flash"), req.session
    lot = db.query(Lot).filter(Lot.batch_number == "A1").one()
    assert lot.payment_condition == "a_prazo" and lot.installment_count == 3 and lot.credit_card_id is not None
    assert str(lot.expiration_date) == "2027-05-01" and float(lot.low_stock_threshold) == 20
    assert db.query(FinanceTransaction).count() == 1


def test_assistant_never_picks_a_card_when_there_are_several(monkeypatch):
    from app.models import FinanceAccount, FinanceCreditCard
    base = {"_tool": "preparar_compra", "item_id": 0, "nome_novo": None, "tipo": None, "categoria": None, "unidade": "L",
            "quantidade_embalagens": 1, "tamanho_embalagem": 50, "preco_por_embalagem": 300, "valor_total": None,
            "data": "2026-11-15", "fornecedor": None, "observacao": None, "forma_pagamento": "Cartão de crédito",
            "condicao": None, "parcelas": None, "periodicidade": None, "primeira_parcela": None, "conta_id": None,
            "cartao_id": None, "cartao_desconhecido": False, "lote": None, "fabricacao": None, "validade": None,
            "estoque_minimo": None}
    replies = [{**base, "_expect_error": True}, {**base, "cartao_id": -1}, {**base, "cartao_desconhecido": True, "_expect_error": True}]
    routes, db, user, scope, send, sent, thread_history = _route_env(monkeypatch, replies)
    farm = scope["active_farm"]
    account = FinanceAccount(farm_id=farm.id, account_name="Sicoob", initial_balance=0, initial_balance_date=date(2026, 1, 1),
                             is_default=True, bank_code="756", bank_name="Sicoob", display_order=0, created_at=datetime.now(timezone.utc))
    db.add(account); db.flush()
    cards = [FinanceCreditCard(farm_id=farm.id, payment_account_id=account.id, card_name=name, issuer=name, brand="Visa",
                               closing_day=5, due_day=12, is_default=name == "Nubank", is_active=True, display_order=0,
                               created_at=datetime.now(timezone.utc)) for name in ("Nubank", "Sicredi")]
    db.add_all(cards); db.commit()

    send("comprei 50 litros de gasolina no cartão")
    assert "1. Nubank\n2. Sicredi" in thread_history()[-1]["content"] and not db.query(StockAssistantDraft).count()
    replies[0]["cartao_id"] = cards[0].id  # IA errou: usuario escolheu Sicredi, nao Nubank
    send("foi no Sicredi")
    assert "Sicredi" in thread_history()[-1]["draft"]["payment"]
    send("na verdade não lembro o cartão")
    assert "ajustar_rascunho" in thread_history()[-1]["content"]
    assert "Sicredi" in thread_history()[-3]["draft"]["payment"]


@pytest.mark.parametrize("answer", ["2", "ourocard"])
def test_selects_card_by_number_or_unique_partial_name(monkeypatch, answer):
    from app.models import FinanceAccount, FinanceCreditCard
    base = {"_tool": "preparar_compra", "item_id": 0, "nome_novo": None, "tipo": None, "categoria": None,
            "unidade": "L", "quantidade_embalagens": 1, "tamanho_embalagem": 50, "preco_por_embalagem": 300,
            "valor_total": None, "data": "2026-11-15", "fornecedor": None, "observacao": None,
            "forma_pagamento": "Cartão de crédito", "condicao": None, "parcelas": None, "periodicidade": None,
            "primeira_parcela": None, "conta_id": None, "cartao_id": None, "cartao_desconhecido": False}
    replies = [{**base, "_expect_error": True}, {**base, "cartao_id": 1}]
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, replies)
    account = FinanceAccount(farm_id=scope["active_farm"].id, account_name="Conta", initial_balance=0,
                             initial_balance_date=date(2026, 1, 1), bank_code="756", bank_name="Banco",
                             created_at=datetime.now(timezone.utc))
    db.add(account); db.flush()
    for name in ("Nubank Ultravioleta", "Ourocard Infinite"):
        db.add(FinanceCreditCard(farm_id=scope["active_farm"].id, payment_account_id=account.id, card_name=name,
                                 issuer=name, brand="Visa", closing_day=5, due_day=12, is_active=True,
                                 created_at=datetime.now(timezone.utc)))
    db.commit()
    send("comprei gasolina no cartão")
    assert "2. Ourocard Infinite" in history()[-1]["content"]
    send(answer)
    assert "Ourocard Infinite" in history()[-1]["draft"]["payment"]


def test_purchase_rejects_category_as_product_and_records_manufacturer():
    from app.services.stock_assistant import FarmAssistant
    captured = {}
    assistant = FarmAssistant(
        farm_name="Fazenda", season=None, items=[], balances={}, plots=[], today=date(2026, 11, 15),
        prepare_draft=lambda **kwargs: None,
        prepare_purchase=lambda **kwargs: (captured.update(kwargs) or "draft", []),
        purchase_rules={"insumo_agricola": {"Herbicida": ["L"]}},
    )
    base = dict(item_id=None, nome_novo="Herbicida", tipo="insumo_agricola", categoria="Herbicida",
                unidade="L", quantidade_embalagens=5, tamanho_embalagem=20, preco_por_embalagem=300,
                data="2026-11-15", fornecedor="Revenda A", fabricante="Fabricante B")
    with pytest.raises(ValueError, match="é a categoria"):
        assistant.preparar_compra(**base)
    assistant.preparar_compra(**{**base, "nome_novo": "Glifosato"})
    assert captured["name"] == "Glifosato"
    assert captured["category"] == "Herbicida"
    assert captured["supplier"] == "Revenda A"
    assert captured["notes"] == "Fabricante: Fabricante B"


def test_assistant_knows_registered_items_and_matches_partial_names():
    prepared, calls = [], []
    assistant = _assistant(prepared)
    assistant.items[3] = SimpleNamespace(id=3, name="Diesel S10", default_unit="L", item_type="combustivel", is_active=True)
    assert [item.name for item in assistant.matching_items("comprei 100 l de diesel")] == ["Diesel S10"]
    assert assistant.matching_items("bom dia") == []
    result = assistant.run([{"role": "user", "content": "Comprei 100 l de diesel"}],
                           client=_fake_client(["Beleza, lanço no Diesel S10? Quanto pagou?"], calls), model="gpt-4o-mini")
    messages = calls[0]["messages"]
    assert "Diesel S10 [item_id 3]" in messages[0]["content"] and "NÃO pergunte se é item novo" in messages[0]["content"]
    assert messages[-2]["role"] == "system" and "Diesel S10 [item_id 3]" in messages[-2]["content"]
    assert messages[-1] == {"role": "user", "content": "Comprei 100 l de diesel"}
    assert calls[0]["temperature"] == 0.2 and 3 in assistant.seen and result["resposta"].startswith("Beleza")
    _assistant([]).run([{"role": "user", "content": "oi"}], client=_fake_client(["Oi!"], calls), model="gpt-5-mini")
    assert "temperature" not in calls[-1] and calls[-1]["reasoning_effort"] == "low"


def test_route_remembers_items_between_turns(monkeypatch):
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, [
        {"_tool": "buscar_itens", "termo": "gasolina"},
        "Foi hoje?",
    ])
    send("comprei gasolina")
    assert history()[-1]["items"][0]["nome"] == "Gasolina"
    send("paguei 300 reais")
    notes = [m["content"] for m in sent[1][0] if m["role"] == "system"]
    assert any("Gasolina [item_id" in note for note in notes)
    assert sent[1][0][-1] == {"role": "user", "content": "paguei 300 reais"}


def test_purchase_traceability_depends_on_type_and_category():
    from app.web.routes import PURCHASE_INPUT_CATEGORY_OPTIONS_BY_TYPE, purchase_traceability_enabled

    assert not any(purchase_traceability_enabled("combustivel", c) for c in PURCHASE_INPUT_CATEGORY_OPTIONS_BY_TYPE["combustivel"])
    assert not purchase_traceability_enabled("insumo_agricola", "Outros Custos de Insumos")
    for category in ("Fungicida", "Herbicida", "Inseticida", "Acaricida", "Adjuvante", "Fertilizantes",
                     "Corretivos de Solo", "Outros Defensivos", "Outros Itens"):
        assert purchase_traceability_enabled("insumo_agricola", category)
    agent = _assistant([])
    agent.prepare_purchase = lambda **kwargs: None
    assert "NÃO pergunte lote" in agent.instructions()


def test_assistant_fuel_purchase_drops_lot_data(monkeypatch):
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, [
        {"_tool": "preparar_compra", "item_id": 0, "quantidade_embalagens": 1, "tamanho_embalagem": 100,
         "preco_por_embalagem": 600, "valor_total": None, "data": "2026-11-15", "unidade": "L",
         "lote": "LT-9", "validade": "2027-01-01", "fabricacao": "2026-10-01"},
    ])
    send("comprei 100 l de gasolina por 600")
    draft = db.query(StockAssistantDraft).one()
    payload = __import__("json").loads(draft.payload_json)
    assert payload["batch_number"] is None and payload["expiration_date"] is None and payload["manufacture_date"] is None
    assert history()[-1]["draft"]["lot"] == ""


def test_missing_fields_are_listed_at_once_and_kept_as_partial():
    agent = _assistant([])
    result = agent.call_tool("preparar_consumo", __import__("json").dumps({"item_id": 2, "quantidade": None, "unidade": "kg",
                                                                          "data": None, "setor": None, "observacao": None}))
    assert "a quantidade total usada" in result["erro"] and "a data" in result["erro"] and "o setor" in result["erro"]
    assert agent.partial["acao"] == "consumo" and agent.partial["dados"]["item"] == "Ureia"
    agent.prepare_purchase = lambda **kwargs: ("p" * 32, [])
    agent.handlers["preparar_compra"] = agent.preparar_compra
    result = agent.call_tool("preparar_compra", __import__("json").dumps({"item_id": 1, "quantidade_embalagens": 1,
                                                                         "tamanho_embalagem": 100, "unidade": "L"}))
    assert "o preço" in result["erro"] and "a data da compra" in result["erro"] and "embalagens" not in result["erro"]
    assert agent.partial["acao"] == "compra" and agent.partial["dados"]["tamanho_embalagem"] == 100
    from app.services.stock_assistant import partial_note
    assert "Gasolina comum" in partial_note(agent.partial) and "o preço" in partial_note(agent.partial)


def test_route_carries_partial_launch_to_next_turn(monkeypatch):
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, [
        {"_tool": "preparar_compra", "_expect_error": True, "item_id": 0, "quantidade_embalagens": 1,
         "tamanho_embalagem": 100, "unidade": "L"},
        "Foi hoje?",
    ])
    send("comprei 100 l de gasolina")
    assert history()[-1]["partial"]["pendente"].startswith("Faltam dados")
    send("paguei 600")
    notes = [m["content"] for m in sent[1][0] if m["role"] == "system"]
    assert any("Lançamento de compra em andamento" in note and "tamanho_embalagem=100" in note for note in notes)
    assert sent[1][0][-1] == {"role": "user", "content": "paguei 600"}


def test_pezinho_accepts_attachment_without_text(monkeypatch):
    import io
    from fastapi import HTTPException, UploadFile
    from starlette.datastructures import Headers
    routes, db, user, scope, send, sent, history = _route_env(monkeypatch, ["Li a nota."])
    seen = []
    original = routes.run_farm_assistant
    def run_with_file(ai_history, assistant, image=None):
        seen.append(image)
        return original(ai_history, assistant)
    monkeypatch.setattr(routes, "run_farm_assistant", run_with_file)
    req = SimpleNamespace(session={})
    pdf = UploadFile(io.BytesIO(b"%PDF-1.4 nota"), filename="nota.pdf", headers=Headers({"content-type": "application/pdf"}))
    payload = json.loads(routes.pezinho_send_message(req, message="", csrf_token="x", thread_id=None, photo=pdf,
                                                    db=db, user=user).body)
    first = payload["messages"][0]
    assert first["attachment_name"] == "nota.pdf" and first["content"] == "Veja o documento que enviei."
    assert payload["messages"][-1]["content"] == "Li a nota."
    assert seen and seen[0].startswith("data:application/pdf;base64,")
    fake = UploadFile(io.BytesIO(b"nao e pdf"), filename="x.pdf", headers=Headers({"content-type": "application/pdf"}))
    with pytest.raises(HTTPException) as exc:
        routes.pezinho_send_message(req, message="Leia", csrf_token="x", thread_id=None, photo=fake, db=db, user=user)
    assert exc.value.status_code == 400
