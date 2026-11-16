from datetime import date, datetime, timedelta, timezone
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
    html = env.get_template("stock_assistant.html").render(
        assistant_scope={"active_farm": {"name": "Fazenda"}, "active_season": {"name": "Safra"}},
        csrf_token="test-token",
    )
    form = html.split('action="/mobile/assistente"', 1)[1].split("</form>", 1)[0]
    assert 'name="csrf_token" value="test-token"' in form


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
            if reply.get("item_id") == 0:
                reply["item_id"] = next(iter(assistant.items))
            result = assistant.call_tool(tool, __import__("json").dumps(reply))
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


def test_new_draft_replaces_pending_one_and_confirm_updates_thread(monkeypatch):
    draft_reply = {"item_id": 0, "quantidade": 5, "unidade": "L", "data": "2026-11-15", "setor": None}
    routes, db, user, scope, send, sent, thread_history = _route_env(monkeypatch, [draft_reply, {**draft_reply, "quantidade": 4}])
    send("5 litros de gasolina hoje")
    send("na verdade foram 4")
    first, second = db.query(StockAssistantDraft).order_by(StockAssistantDraft.quantity.desc()).all()
    assert first.status == "discarded" and second.status == "pending"
    history = thread_history()
    assert history[1]["draft"]["state"] == "replaced" and history[3]["draft"]["state"] == "pending"
    assert history[3]["tools"] == ["Preparou rascunho de consumo"]
    location = routes.resolve_stock_assistant_action(
        SimpleNamespace(session={}), second.id, "confirmar", csrf_token="x",
        thread_id=db.query(routes.AssistantThread).one().id, db=db, user=user,
    ).headers["location"]
    assert "c=" in location
    history = thread_history()
    assert history[3]["draft"]["state"] == "confirmed" and history[-1]["content"].startswith("Pronto!")
    summary = db.query(StockOutput).filter_by(purchased_input_id=None).one()
    assert float(summary.quantity) == 4


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
