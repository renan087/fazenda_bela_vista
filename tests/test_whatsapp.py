import hashlib
import hmac
import json
import time
from types import SimpleNamespace

from app.models.stock_assistant_draft import StockAssistantDraft
from app.models.stock_output import StockOutput
from app.models.whatsapp import WhatsAppInboundMessage, WhatsAppLink
from app.routers import whatsapp_webhook
from app.services import whatsapp, whatsapp_bot
from tests.test_stock_assistant import _route_env

SETTINGS = SimpleNamespace(whatsapp_verify_token="verifica", whatsapp_access_token="tok", whatsapp_phone_number_id="123",
                           whatsapp_app_secret="segredo", whatsapp_graph_version="v23.0", whatsapp_display_number=None)


def _settings(monkeypatch):
    monkeypatch.setattr(whatsapp, "get_settings", lambda: SETTINGS)
    monkeypatch.setattr(whatsapp_webhook, "get_settings", lambda: SETTINGS)


class FakeClient:
    def __init__(self):
        self.sent = []

    def send_text(self, to, body):
        self.sent.append(("text", to, body))

    def send_buttons(self, to, body, buttons):
        self.sent.append(("buttons", to, body, buttons))

    def mark_read(self, wamid):
        pass

    def download_media(self, media_id):
        raise whatsapp.WhatsAppError("Não consegui baixar o arquivo enviado.")


def _payload(wa_id, wamid, text=None, button=None, ts=None, phone_id="123"):
    message = {"from": wa_id, "id": wamid, "timestamp": str(ts or int(time.time()))}
    if button:
        message.update(type="interactive", interactive={"type": "button_reply", "button_reply": {"id": button, "title": "Confirmar"}})
    else:
        message.update(type="text", text={"body": text})
    return {"object": "whatsapp_business_account", "entry": [{"changes": [{"field": "messages", "value": {
        "metadata": {"phone_number_id": phone_id}, "messages": [message], "statuses": []}}]}]}


def _sign(body: bytes) -> str:
    return "sha256=" + hmac.new(b"segredo", body, hashlib.sha256).hexdigest()


def test_verification_signature_and_parsing(monkeypatch):
    _settings(monkeypatch)
    assert whatsapp.verify_subscription("subscribe", "verifica", "42") == "42"
    assert whatsapp.verify_subscription("subscribe", "errado", "42") is None
    body = b'{"a":1}'
    assert whatsapp.valid_signature(body, _sign(body))
    assert not whatsapp.valid_signature(body + b" ", _sign(body))
    assert not whatsapp.valid_signature(body, None)
    assert [m.wamid for m in whatsapp.extract_messages(_payload("5535999", "w1", "oi"))] == ["w1"]
    assert whatsapp.extract_messages(_payload("5535999", "w1", "oi", phone_id="999")) == []


def _request(method, query=b"", body=b"", headers=None):
    from starlette.requests import Request

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    return Request({"type": "http", "method": method, "path": "/api/webhooks/whatsapp", "query_string": query,
                    "headers": raw_headers}, receive)


def test_webhook_endpoints(monkeypatch):
    import asyncio
    from urllib.parse import urlencode

    from fastapi import BackgroundTasks, HTTPException

    _settings(monkeypatch)
    stored = []
    monkeypatch.setattr(whatsapp_webhook, "_store", lambda messages: stored.extend(messages) or [1])
    ok = whatsapp_webhook.whatsapp_verify(_request("GET", urlencode(
        {"hub.mode": "subscribe", "hub.verify_token": "verifica", "hub.challenge": "777"}).encode()))
    assert ok.body == b"777"
    try:
        whatsapp_webhook.whatsapp_verify(_request("GET", b"hub.mode=subscribe&hub.verify_token=x&hub.challenge=1"))
        raise AssertionError("deveria recusar")
    except HTTPException as exc:
        assert exc.status_code == 403
    body = json.dumps(_payload("5535999", "w1", "oi")).encode()
    tasks = BackgroundTasks()
    try:
        asyncio.run(whatsapp_webhook.whatsapp_receive(_request("POST", body=body, headers={"X-Hub-Signature-256": "sha256=00"}), tasks))
        raise AssertionError("deveria recusar")
    except HTTPException as exc:
        assert exc.status_code == 401
    assert stored == [] and not tasks.tasks
    result = asyncio.run(whatsapp_webhook.whatsapp_receive(
        _request("POST", body=body, headers={"X-Hub-Signature-256": _sign(body)}), tasks))
    assert result == {"status": "ok"} and stored[0].wamid == "w1"
    assert tasks.tasks[0].func is whatsapp_webhook.process_queue and tasks.tasks[0].args == ([1],)


def _receive(db, client, payload):
    ids = whatsapp_bot.store_inbound(db, whatsapp.extract_messages(payload))
    return [whatsapp_bot.process_inbound(db, row_id, client) for row_id in ids]


def test_link_chat_confirm_and_isolation(monkeypatch):
    _settings(monkeypatch)
    draft_reply = {"item_id": 0, "quantidade": 5, "unidade": "L", "data": "2026-11-15", "setor": None}
    routes, db, user, scope, *_ = _route_env(monkeypatch, [draft_reply])
    client = FakeClient()

    # numero desconhecido: so instrucoes, uma vez por dia, sem dados
    assert _receive(db, client, _payload("5511000", "u1", "quanto tenho de gasolina?")) == ["unknown"]
    assert _receive(db, client, _payload("5511000", "u2", "oi")) == ["unknown"]
    assert len(client.sent) == 1 and client.sent[0][2] == whatsapp_bot.UNKNOWN_TEXT

    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    link = db.query(WhatsAppLink).one()
    assert link.code_hash != code and link.wa_id is None
    assert _receive(db, client, _payload("5535999", "m1", f"sisfarm {code}")) == ["linked"]
    assert db.query(WhatsAppLink).one().wa_id == "5535999"
    # codigo de uso unico
    assert _receive(db, client, _payload("5511000", "u3", f"SISFARM {code}")) == ["link_failed"]

    assert _receive(db, client, _payload("5535999", "m2", "usei 5 litros de gasolina hoje")) == ["done"]
    kind, to, body, buttons = client.sent[-1]
    draft = db.query(StockAssistantDraft).one()
    assert kind == "buttons" and to == "5535999" and "Gasolina" in body and buttons[0][0] == f"sf:confirmar:{draft.id}"
    # reentrega da Meta (mesmo wamid) nao processa de novo
    assert _receive(db, client, _payload("5535999", "m2", "usei 5 litros de gasolina hoje")) == []

    # outro numero nao consegue confirmar o rascunho
    _receive(db, client, _payload("5511000", "u4", button=f"sf:confirmar:{draft.id}"))
    assert db.query(StockOutput).filter_by(purchased_input_id=None).count() == 0
    assert _receive(db, client, _payload("5535999", "m3", button=f"sf:confirmar:{draft.id}")) == ["done"]
    assert db.query(StockOutput).filter_by(purchased_input_id=None).count() == 1 and "Registrei a saída" in client.sent[-1][2]
    thread = db.get(routes.AssistantThread, db.query(WhatsAppLink).one().thread_id)
    assert thread.title.startswith("WhatsApp")


def test_denied_expired_and_failure(monkeypatch):
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [RuntimeError("boom")])
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "m1", f"SISFARM {code}"))
    # fora da janela de 24 h: nao responde
    assert _receive(db, client, _payload("5535999", "old", "oi", ts=int(time.time()) - 90000)) == ["expired"]
    sent = len(client.sent)
    assert _receive(db, client, _payload("5535999", "m2", "oi")) == ["failed"]
    row = db.query(WhatsAppInboundMessage).filter_by(wamid="m2").one()
    assert row.error.startswith("RuntimeError") and len(client.sent) == sent + 1
    monkeypatch.setattr(routes, "_can_use_stock_assistant", lambda *a: False)
    assert _receive(db, client, _payload("5535999", "m3", "oi")) == ["denied"]
    assert "permissão" in client.sent[-1][2]
