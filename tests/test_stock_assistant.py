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
    assert repo.db.query(StockOutput).count() == 0 and lot.available_quantity == 50
    output = confirm_consumption(repo, draft)
    repo.db.commit()
    assert output.quantity == 40 and output.origin == "Assistente SiSFarm"
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
        [_tool_call("preparar_consumo", {"item_id": 1, "quantidade": 5, "unidade": "litros", "data": "2026-11-15", "setor": None}, "c2")],
    ]
    result = _assistant(prepared).run([{"role": "user", "content": "usei 5 litros de gasolina hoje"}],
                                      client=_fake_client(steps, calls), model="m")
    assert result["draft_id"] == "d" * 32 and prepared[0]["unit"] == "L" and prepared[0]["plot"] is None
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
            reply = {**reply, "item_id": reply["item_id"] or next(iter(assistant.items))}
            assistant.call_tool("preparar_consumo", __import__("json").dumps(reply))
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
    send = lambda text: routes.prepare_stock_assistant_action(request, message=text, csrf_token="x", db=db, user=user)
    return routes, db, user, scope, send, sent


def test_route_keeps_conversation_on_error_and_drafts_fuel_without_plot(monkeypatch):
    replies = [
        ValueError("A conta da OpenAI usada pelo SiSFarm está sem créditos."),
        "Foi hoje?",
        {"item_id": 0, "quantidade": 5, "unidade": "litros", "data": "2026-11-15", "setor": None},
    ]
    routes, db, user, scope, send, sent = _route_env(monkeypatch, replies)
    send("Utilizei 5 litros de gasolina")
    history = routes._assistant_history(db, user, scope)
    assert [m["role"] for m in history] == ["user", "assistant"] and history[1]["error"]
    send("Utilizei 5 litros de gasolina")
    # o aviso de erro nao vai para a IA; o item de combustivel com estoque na fazenda vai
    assert sent[1][0] == [{"role": "user", "content": "Utilizei 5 litros de gasolina"},
                          {"role": "user", "content": "Utilizei 5 litros de gasolina"}]
    assert sent[1][1] == ["Gasolina"]
    response = send("sim, hoje")
    assert "draft_id=" in response.headers["location"]
    draft = db.query(StockAssistantDraft).one()
    assert draft.plot_id is None and draft.unit == "L" and draft.quantity == 5
    assert len(routes._assistant_history(db, user, scope)) == 6
