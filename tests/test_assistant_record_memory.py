from types import SimpleNamespace

import pytest

from app.core.permissions_catalog import PAGE_INPUTS
from app.models.purchased_input import PurchasedInput
from app.models.whatsapp import WhatsAppRecordAccess
from app.services.whatsapp_record_access import create_token
from tests.test_stock_assistant import _route_env


def _setup(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda db, user: {PAGE_INPUTS})
    monkeypatch.setattr(routes, "_entitlements_for", lambda db, user: SimpleNamespace(module_allowed=lambda page: True))
    record = db.query(PurchasedInput).first()
    ref = {"kind": "compra", "id": record.id, "label": "Gasolina", "date": "15/11/2026"}
    return routes, db, user, scope, ref


def test_followup_recovers_recent_reference_and_rechecks_permission(monkeypatch):
    routes, db, user, scope, ref = _setup(monkeypatch)
    history = [{"role": "assistant", "record_references": [ref], "content": "Encontrei a compra."}]
    get = lambda message: routes._assistant_record_followup(db, user, scope["active_farm"], scope["active_season"], history, message)
    assert get("Gere novamente o link, pois expirou.") == [ref]
    assert get("Me envie de novo.") == [ref]
    assert get("Me envie de novo, por favor.") == [ref]
    assert get("Esse lançamento") == [ref]
    assert get("Corrija esse lançamento") is None
    assert get("Gere novamente o link do diesel") is None
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda db, user: set())
    assert get("Gere novamente o link") == []


def test_followup_preserves_ambiguity_without_guessing(monkeypatch):
    routes, db, user, scope, ref = _setup(monkeypatch)
    first = db.get(PurchasedInput, ref["id"])
    second = PurchasedInput(farm_id=first.farm_id, input_id=first.input_id, name="Gasolina",
                            quantity_purchased=1, package_size=10, package_unit="L", unit_price=10,
                            total_quantity=10, available_quantity=10, total_cost=10)
    db.add(second)
    db.commit()
    other = {**ref, "id": second.id, "date": "14/11/2026"}
    history = [{"role": "assistant", "record_references": [ref, other]}]
    result = routes._assistant_record_followup(db, user, scope["active_farm"], scope["active_season"], history, "Gere o link novamente")
    assert [entry["id"] for entry in result] == [ref["id"], other["id"]]


def test_followup_recovers_legacy_whatsapp_link_by_owner_and_context(monkeypatch):
    from datetime import datetime, timezone
    routes, db, user, scope, ref = _setup(monkeypatch)
    link = SimpleNamespace(id=1, user_id=user.id, organization_id=user.organization_id,
                           farm_id=scope["active_farm_id"], season_id=scope["active_season_id"],
                           wa_id="5511999999999", verified_at=datetime.now(timezone.utc))
    create_token(db, link=link, kind="compra", record_id=ref["id"])
    db.flush()
    access = db.query(WhatsAppRecordAccess).one()
    history = [{"role": "assistant", "content": "Link de consulta enviado.", "record_access_id": access.id}]
    result = routes._assistant_record_followup(db, user, scope["active_farm"], scope["active_season"], history, "Me envie de novo")
    assert result and result[0]["id"] == ref["id"]


def test_web_followup_keeps_reference_in_new_message_without_calling_model(monkeypatch):
    from datetime import datetime, timezone
    from uuid import uuid4
    routes, db, user, scope, ref = _setup(monkeypatch)
    now = datetime.now(timezone.utc)
    thread = routes.AssistantThread(id=uuid4().hex, user_id=user.id, organization_id=user.organization_id,
                                    farm_id=scope["active_farm_id"], season_id=scope["active_season_id"],
                                    channel="web", title="Compra", created_at=now, updated_at=now)
    routes._save_thread(db, thread, [{"role": "assistant", "content": "Encontrei a compra.", "record_references": [ref]}])
    monkeypatch.setattr(routes, "run_farm_assistant", lambda *args, **kwargs: pytest.fail("Não deve chamar o modelo"))
    _, reply = routes._assistant_turn(db, user, routes._repository(db), scope, thread, "Gere novamente o link")
    assert reply["record_references"] == [ref]
    assert routes._thread_messages(thread)[-1]["record_references"] == [ref]


def test_whatsapp_followup_issues_new_link_for_same_record(monkeypatch):
    from datetime import date
    import json
    from app.models.whatsapp import WhatsAppLink
    from app.services import whatsapp_bot
    from tests.test_whatsapp import FakeClient, _payload, _receive, _settings

    _settings(monkeypatch)
    routes, db, user, scope, ref = _setup(monkeypatch)
    purchase = db.get(PurchasedInput, ref["id"])
    purchase.purchase_date = date(2026, 11, 15)
    db.commit()
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "memory-link", f"SISFARM {code}"))

    def query(history, assistant):
        assistant.call_tool("consultar_compras", json.dumps({"inicio": None, "fim": None, "item": "gasolina"}))
        return {"resposta": "Encontrei a compra.", "draft_id": None}

    monkeypatch.setattr(routes, "run_farm_assistant", query)
    _receive(db, client, _payload("5535999", "memory-query", "Qual a última compra de gasolina?"))
    first_url = client.sent[-1][2].split("registro#")[-1]
    assert len(first_url) == 43
    monkeypatch.setattr(routes, "run_farm_assistant", lambda *args, **kwargs: pytest.fail("Não deve chamar o modelo"))
    _receive(db, client, _payload("5535999", "memory-again", "Gere novamente o link, pois expirou"))
    second_url = client.sent[-1][2].split("registro#")[-1]
    assert len(second_url) == 43 and first_url != second_url
    link = db.query(WhatsAppLink).one()
    thread = db.get(routes.AssistantThread, link.thread_id)
    assert routes._thread_messages(thread)[-1]["record_references"][0]["id"] == ref["id"]
