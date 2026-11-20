import hashlib
import hmac
import json
import time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
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


def test_record_choices_issue_only_selected_link_without_storing_token(monkeypatch):
    from datetime import datetime, timezone
    from uuid import uuid4
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "link-choice", f"SISFARM {code}"))
    issued = []

    def fake_turn(db, user, repo, scope, thread, message, **kwargs):
        now = datetime.now(timezone.utc)
        thread = routes.AssistantThread(id=uuid4().hex, user_id=user.id, organization_id=user.organization_id,
                                        farm_id=scope["active_farm_id"], season_id=scope["active_season_id"],
                                        channel="whatsapp", title="WhatsApp · Consulta", created_at=now, updated_at=now)
        item = {"role": "assistant", "content": "Encontrei os registros.", "record_references": [
            {"kind": "compra", "id": 11, "label": "Diesel", "date": "15/11/2026"},
            {"kind": "compra", "id": 12, "label": "Gasolina", "date": "14/11/2026"},
        ]}
        routes._save_thread(db, thread, [item])
        return thread, item

    monkeypatch.setattr(routes, "_assistant_turn", fake_turn)
    monkeypatch.setattr(whatsapp_bot, "_record_link", lambda db, link, ref: (issued.append(ref["id"]) or "https://app.sisfarm.com.br/acesso-whatsapp/registro#secret", 123))
    _receive(db, client, _payload("5535999", "query-choice", "mostre compras"))
    assert "1. Compra · Diesel" in client.sent[-1][2] and "2. Compra · Gasolina" in client.sent[-1][2]
    assert issued == []
    _receive(db, client, _payload("5535999", "choose-record", "2"))
    assert issued == [12] and "#secret" in client.sent[-1][2]
    thread = db.get(routes.AssistantThread, db.query(WhatsAppLink).one().thread_id)
    assert "#secret" not in thread.messages_json


def test_record_link_revalidates_permissions_and_whatsapp_link(monkeypatch):
    from datetime import timedelta
    from app.core.permissions_catalog import PAGE_INPUTS
    from app.models.purchased_input import PurchasedInput
    from app.services import whatsapp_record_access
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "link-access", f"SISFARM {code}"))
    link = db.query(WhatsAppLink).one()
    record = db.query(PurchasedInput).first()
    access = SimpleNamespace(kind="compra", record_id=record.id, link_id=link.id, user_id=user.id,
                             organization_id=user.organization_id, farm_id=link.farm_id, season_id=link.season_id,
                             binding_hash=whatsapp_record_access.link_binding(link))
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda db, user: {PAGE_INPUTS})
    monkeypatch.setattr(routes, "_entitlements_for", lambda db, user: SimpleNamespace(module_allowed=lambda page: True))
    assert whatsapp_record_access.authorized_record(db, access).id == record.id
    url, access_id = whatsapp_bot._record_link(db, link, {"kind": "compra", "id": record.id})
    assert url.startswith("https://") and "/acesso-whatsapp/registro#" in url
    issued = whatsapp_record_access.find_token(db, url.rsplit("#", 1)[1])
    assert issued is not None and issued.id == access_id and whatsapp_record_access.authorized_record(db, issued).id == record.id
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda db, user: set())
    assert whatsapp_record_access.authorized_record(db, access) is None
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda db, user: {PAGE_INPUTS})
    original_verified = link.verified_at
    link.verified_at = original_verified + timedelta(seconds=1)
    assert whatsapp_record_access.authorized_record(db, access) is None
    link.verified_at = original_verified
    link.wa_id = None
    assert whatsapp_record_access.authorized_record(db, access) is None


def test_revoke_link_requires_owning_conversation(monkeypatch):
    from datetime import datetime, timezone
    from uuid import uuid4
    from app.models.whatsapp import WhatsAppRecordAccess
    from app.services.whatsapp_record_access import create_token, find_token
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "link-revoke", f"SISFARM {code}"))
    link = db.query(WhatsAppLink).one()
    token = create_token(db, link=link, kind="compra", record_id=1)
    db.flush()
    access = db.query(WhatsAppRecordAccess).one()
    now = datetime.now(timezone.utc)
    thread = routes.AssistantThread(id=uuid4().hex, user_id=user.id, organization_id=user.organization_id,
                                    farm_id=link.farm_id, season_id=link.season_id, channel="whatsapp",
                                    title="WhatsApp · Consulta", created_at=now, updated_at=now)
    routes._save_thread(db, thread, [{"role": "assistant", "content": "Link enviado", "record_access_id": access.id}])
    with pytest.raises(HTTPException) as error:
        routes.revoke_assistant_record_link(SimpleNamespace(session={}), uuid4().hex, access.id, "csrf", db, user)
    assert error.value.status_code == 404 and find_token(db, token) is not None
    response = routes.revoke_assistant_record_link(SimpleNamespace(session={}), thread.id, access.id, "csrf", db, user)
    assert response.status_code == 303 and find_token(db, token) is None


def test_link_chat_confirm_and_isolation(monkeypatch):
    _settings(monkeypatch)
    draft_reply = {"item_id": 0, "quantidade": 5, "unidade": "L", "data": "2026-11-15", "setor": None}
    routes, db, user, scope, *_ = _route_env(monkeypatch, [draft_reply])
    client = FakeClient()

    # numero desconhecido: so instrucoes, uma vez por dia, sem dados
    assert _receive(db, client, _payload("5511000", "u1", "quanto tenho de gasolina?")) == ["unknown"]
    assert _receive(db, client, _payload("5511000", "u2", "oi")) == ["unknown_silenced"]
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
    whatsapp_thread = db.get(routes.AssistantThread, db.query(WhatsAppLink).one().thread_id)
    assert whatsapp_thread.channel == "whatsapp"
    with pytest.raises(HTTPException) as exc:
        routes.resolve_stock_assistant_action(SimpleNamespace(session={}), draft.id, "confirmar", csrf_token="x",
                                              thread_id=whatsapp_thread.id, db=db, user=user)
    assert exc.value.status_code == 403
    with pytest.raises(HTTPException) as exc:
        routes.prepare_stock_assistant_action(SimpleNamespace(session={}), message="oi", csrf_token="x",
                                               thread_id=whatsapp_thread.id, photo=None, db=db, user=user)
    assert exc.value.status_code == 403
    with pytest.raises(HTTPException) as exc:
        routes.delete_stock_assistant_thread(SimpleNamespace(session={}), whatsapp_thread.id, csrf_token="x", db=db, user=user)
    assert exc.value.status_code == 403
    # O identificador de uma conversa interna não pode ser usado para contornar o bloqueio.
    with pytest.raises(HTTPException) as exc:
        routes.resolve_stock_assistant_action(SimpleNamespace(session={}), draft.id, "confirmar", csrf_token="x",
                                              thread_id=None, db=db, user=user)
    assert exc.value.status_code == 404
    # reentrega da Meta (mesmo wamid) nao processa de novo
    assert _receive(db, client, _payload("5535999", "m2", "usei 5 litros de gasolina hoje")) == []

    # outro numero nao consegue confirmar o rascunho
    _receive(db, client, _payload("5511000", "u4", button=f"sf:confirmar:{draft.id}"))
    assert db.query(StockOutput).filter_by(purchased_input_id=None).count() == 0
    assert _receive(db, client, _payload("5535999", "m3", button=f"sf:confirmar:{draft.id}")) == ["done"]
    assert db.query(StockOutput).filter_by(purchased_input_id=None).count() == 1 and "Registrei a saída" in client.sent[-1][2]
    thread = db.get(routes.AssistantThread, db.query(WhatsAppLink).one().thread_id)
    assert thread.title.startswith("WhatsApp")


def test_legacy_whatsapp_thread_is_read_only():
    from app.web.routes import _assistant_thread_channel
    assert _assistant_thread_channel(SimpleNamespace(channel=None, title="WhatsApp · Olá")) == "whatsapp"
    assert _assistant_thread_channel(SimpleNamespace(channel="web", title="WhatsApp · assunto")) == "web"


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


def test_brazil_ninth_digit_retry(monkeypatch):
    _settings(monkeypatch)
    assert whatsapp.br_ninth_digit("553588887777") == "5535988887777"
    assert whatsapp.br_ninth_digit("5535988887777") is None and whatsapp.br_ninth_digit("553532221111") is None
    calls = []

    class Http:
        def post(self, url, json, headers):
            calls.append(json["to"])
            if json["to"] == "553588887777":
                return SimpleNamespace(status_code=400, json=lambda: {"error": {"code": 131030, "message": "not allowed"}})
            return SimpleNamespace(status_code=200, json=lambda: {"messages": [{"id": "x"}]})

    whatsapp.WhatsAppClient(http=Http()).send_text("553588887777", "oi")
    assert calls == ["553588887777", "5535988887777"]


def test_reply_context_is_included_for_text_and_buttons(monkeypatch):
    _settings(monkeypatch)
    payloads = []
    class Http:
        def post(self, url, json, headers):
            payloads.append(json)
            return SimpleNamespace(status_code=200, json=lambda: {"messages": [{"id": "sent"}]})
    client = whatsapp.WhatsAppClient(http=Http())
    whatsapp_bot.send_assistant_reply(client, "5535999", {"content": "Olá"}, reply_to="wamid.audio")
    whatsapp_bot.send_assistant_reply(client, "5535999", {"content": "Confira", "draft": {
        "id": "draft", "kind": "compra", "item": "Insumo", "quantity": "1", "unit": "L",
        "date_label": "16/11/2026", "balance": "0", "after": "1"}}, reply_to="wamid.audio")
    assert [payload["context"] for payload in payloads] == [{"message_id": "wamid.audio"}] * 2
    assert payloads[1]["type"] == "interactive"


def test_audio_gets_one_contextual_reply_without_transcription_message(monkeypatch):
    from app.services import assistant_audio
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, ["Há gasolina em estoque."])
    class AudioClient(FakeClient):
        def download_media(self, media_id):
            return b"audio", "audio/ogg"
        def send_text(self, to, body, *, reply_to=None):
            self.sent.append(("text", to, body, reply_to))
    monkeypatch.setattr(assistant_audio, "transcribe_audio", lambda data, mime: "Tem gasolina?")
    client = AudioClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "link", f"SISFARM {code}"))
    client.sent.clear()
    payload = _payload("5535999", "audio-id", "")
    msg = payload["entry"][0]["changes"][0]["value"]["messages"][0]
    msg.update(type="audio", audio={"id": "media-id"})
    assert _receive(db, client, payload) == ["done"]
    assert client.sent == [("text", "5535999", "Há gasolina em estoque.", "audio-id")]
    thread = db.get(routes.AssistantThread, db.query(WhatsAppLink).one().thread_id)
    assert routes._thread_messages(thread)[0]["content"] == "Tem gasolina?"


def test_whatsapp_document_is_saved_read_only(monkeypatch):
    from app.models.assistant_attachment import AssistantAttachment
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    monkeypatch.setattr(routes, "run_farm_assistant", lambda history, assistant, **kw: {"resposta": "Recebi.", "draft_id": None})
    class DocumentClient(FakeClient):
        def download_media(self, media_id):
            return b"%PDF-1.4 test", "application/pdf"
    client = DocumentClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "link-document", f"SISFARM {code}"))
    payload = _payload("5535999", "document-id", "")
    msg = payload["entry"][0]["changes"][0]["value"]["messages"][0]
    msg.update(type="document", document={"id": "media-id", "filename": "nota.pdf"})
    assert _receive(db, client, payload) == ["done"]
    thread = db.get(routes.AssistantThread, db.query(WhatsAppLink).one().thread_id)
    attachment_id = routes._thread_messages(thread)[0]["attachment_id"]
    assert db.get(AssistantAttachment, attachment_id).filename == "nota.pdf"
    req = SimpleNamespace(session={})
    assert routes.assistant_attachment_file(attachment_id, req, db=db, user=user).body == b"%PDF-1.4 test"
    with pytest.raises(HTTPException) as exc:
        routes.delete_assistant_attachment(attachment_id, req, csrf_token="x", db=db, user=user)
    assert exc.value.status_code == 403


def test_summary_shows_delivery_error_reason(monkeypatch):
    _settings(monkeypatch)
    payload = {"object": "whatsapp_business_account", "entry": [{"changes": [{"field": "messages", "value": {
        "metadata": {"phone_number_id": "123"}, "statuses": [{"status": "failed", "recipient_id": "5535988887667",
        "errors": [{"code": 131031, "title": "Business Account locked", "error_data": {"details": "Conta bloqueada"}}]}]}}]}]}
    summary = whatsapp.summarize(payload)
    assert "failed:131031 (Business Account locked - Conta bloqueada) para ***7667" in summary
    assert "5535988887667" not in summary


def test_brazilian_number_with_and_without_ninth_digit(monkeypatch):
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, ["Oi! Em que posso ajudar?", "Claro."])
    client = FakeClient()
    assert whatsapp.wa_id_variants("557781177667") == ["557781177667", "5577981177667"]
    assert whatsapp.wa_id_variants("5577981177667") == ["5577981177667", "557781177667"]
    assert whatsapp.wa_id_variants("5511000") == ["5511000"]

    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    assert _receive(db, client, _payload("557781177667", "m1", f"SISFARM {code}")) == ["linked"]
    # a Meta passa a entregar o mesmo celular com o 9: continua reconhecido e a resposta vai no formato novo
    assert _receive(db, client, _payload("5577981177667", "m2", "oi")) == ["done"]
    assert client.sent[-1][:2] == ("text", "5577981177667")
    assert db.query(WhatsAppLink).one().wa_id == "5577981177667"
    assert _receive(db, client, _payload("557781177667", "m3", "e agora?")) == ["done"]

    # outro usuario que vincula o mesmo celular (em qualquer formato) tira o numero do vinculo anterior
    other = routes.User(name="Bia", email="bia@x.com", hashed_password="x", organization_id=user.organization_id, is_active=True)
    db.add(other); db.commit()
    code = whatsapp_bot.start_link(db, other, scope["active_farm"], scope["active_season"])
    assert _receive(db, client, _payload("557781177667", "m4", f"SISFARM {code}")) == ["linked"]
    links = {link.user_id: link.wa_id for link in db.query(WhatsAppLink)}
    assert links == {user.id: None, other.id: "557781177667"}


def test_unknown_instruction_is_resent_after_an_hour(monkeypatch):
    from datetime import datetime, timedelta, timezone

    _settings(monkeypatch)
    routes, db, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    assert _receive(db, client, _payload("557781177667", "u1", "oi")) == ["unknown"]
    assert _receive(db, client, _payload("5577981177667", "u2", "oi")) == ["unknown_silenced"]
    first = db.query(WhatsAppInboundMessage).filter_by(wamid="u1").one()
    first.received_at = datetime.now(timezone.utc) - timedelta(hours=2)
    db.commit()
    assert _receive(db, client, _payload("5577981177667", "u3", "oi")) == ["unknown"]
    assert [item[2] for item in client.sent] == [whatsapp_bot.UNKNOWN_TEXT] * 2


def test_link_code_is_recognized_in_common_variations(monkeypatch):
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    candidate = whatsapp_bot._link_code_candidate
    assert candidate("SISFARM ABCD2345") == ("ABCD2345", True)
    assert candidate("sisfarm: abcd-2345") == ("ABCD2345", True)
    assert candidate("*SISFARM* ABCD 2345") == ("ABCD2345", True)
    assert candidate("Sis Farm A B C D 2 3 4 5") == ("ABCD2345", True)
    assert candidate("ABCD 2345") == ("ABCD2345", False)
    assert candidate("obrigado") == (None, False)

    # uma palavra de 8 letras nao vira tentativa de vinculo se nao existir codigo pendente igual
    assert _receive(db, client, _payload("557781177667", "u1", "PERGUNTA")) == ["unknown"]
    # citou SISFARM mas o codigo veio quebrado: responde mesmo que a instrucao tenha sido enviada ha pouco
    assert _receive(db, client, _payload("557781177667", "u2", "sisfarm 1234")) == ["link_failed"]
    assert "Não reconheci o código" in client.sent[-1][2]

    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    spaced = f"{code[:4]} {code[4:]}".lower()
    assert _receive(db, client, _payload("557781177667", "u3", spaced)) == ["linked"]
    assert db.query(WhatsAppLink).one().wa_id == "557781177667"


def test_link_page_qr_code_points_to_whatsapp_with_code():
    from app.web import routes

    svg = str(routes._whatsapp_qr_svg("https://wa.me/5535999990000?text=SISFARM%20ABCD2345"))
    assert svg.startswith("<svg") and "QR code" in svg and 'class="h-full w-full"' in svg


def test_link_status_endpoint_reports_new_link(monkeypatch):
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    status = lambda: json.loads(routes.whatsapp_link_status(db=db, user=user).body)
    assert status() == {"linked": False, "pending_code": False, "verified_at": None, "phone_end": None, "farm": None, "season": None}
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    assert status()["pending_code"] is True and status()["linked"] is False
    _receive(db, FakeClient(), _payload("557781177667", "m1", f"SISFARM {code}"))
    data = status()
    assert data["linked"] and not data["pending_code"] and data["phone_end"] == "7667"
    assert data["farm"] == "Bela Vista" and data["season"] == "Café 2026" and data["verified_at"]
    assert "557781177667" not in json.dumps(data)


def test_explicit_closure_preserves_history_and_ignores_stale_buttons(monkeypatch):
    from datetime import datetime, timezone
    from uuid import uuid4
    from app.models.whatsapp import WhatsAppClosingRequest

    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "link-close", f"SISFARM {code}"))
    now = datetime.now(timezone.utc)
    thread = routes.AssistantThread(id=uuid4().hex, user_id=user.id, organization_id=user.organization_id,
                                    farm_id=scope["active_farm_id"], season_id=scope["active_season_id"],
                                    channel="whatsapp", title="WhatsApp", created_at=now, updated_at=now)
    routes._save_thread(db, thread, [{"role": "assistant", "content": "Olá"}])
    link = db.query(WhatsAppLink).one()
    link.thread_id = thread.id
    db.commit()
    assert _receive(db, client, _payload("5535999", "close-1", "encerrar atendimento")) == ["close_pending"]
    request = db.query(WhatsAppClosingRequest).one()
    assert link.thread_id == thread.id and request.status == "pending"
    close_button = client.sent[-1][3][1][0]
    assert _receive(db, client, _payload("5535999", "close-2", button=close_button)) == ["closed"]
    assert link.thread_id is None and request.status == "closed" and db.get(routes.AssistantThread, thread.id)
    assert _receive(db, client, _payload("5535999", "close-3", button=close_button)) == ["close_stale"]
    db.close()


def test_closure_can_be_cancelled(monkeypatch):
    from app.models.whatsapp import WhatsAppClosingRequest
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "link-continue", f"SISFARM {code}"))
    _receive(db, client, _payload("5535999", "close-continue-1", "finalizar"))
    button = client.sent[-1][3][0][0]
    assert _receive(db, client, _payload("5535999", "close-continue-2", button=button)) == ["continued"]
    assert db.query(WhatsAppClosingRequest).one().status == "continued"
    db.close()


def test_failed_closure_prompt_does_not_leave_pending_confirmation(monkeypatch):
    from app.models.whatsapp import WhatsAppClosingRequest
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "link-close-fail", f"SISFARM {code}"))
    def fail(*args, **kwargs):
        raise whatsapp.WhatsAppError("indisponível")
    monkeypatch.setattr(client, "send_buttons", fail)
    assert _receive(db, client, _payload("5535999", "close-fail", "encerrar")) == ["failed"]
    assert db.query(WhatsAppClosingRequest).count() == 0
    db.close()


def test_idle_closure_and_survey(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from uuid import uuid4
    from app.models.whatsapp import WhatsAppClosingRequest, WhatsAppSurvey
    from app.services.whatsapp_lifecycle import process_lifecycle
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "link-idle", f"SISFARM {code}"))
    now = datetime.now(timezone.utc)
    thread = routes.AssistantThread(id=uuid4().hex, user_id=user.id, organization_id=user.organization_id,
                                    farm_id=scope["active_farm_id"], season_id=scope["active_season_id"],
                                    channel="whatsapp", title="WhatsApp", created_at=now, updated_at=now)
    routes._save_thread(db, thread, [{"role": "assistant", "content": "Olá"}])
    link = db.query(WhatsAppLink).one()
    link.thread_id, link.last_inbound_at = thread.id, now - timedelta(minutes=31)
    db.commit()
    process_lifecycle(db, client, now)
    request = db.query(WhatsAppClosingRequest).one()
    assert request.status == "pending" and link.thread_id == thread.id
    process_lifecycle(db, client, now + timedelta(minutes=11))
    survey = db.query(WhatsAppSurvey).one()
    assert request.status == "closed" and link.thread_id is None and survey.status == "pending"
    assert db.get(routes.AssistantThread, thread.id) is not None
    process_lifecycle(db, client, now + timedelta(minutes=42))
    assert db.get(WhatsAppSurvey, survey.id).status == "unrated"
    db.close()


def test_survey_button_is_separate_from_new_conversation(monkeypatch):
    from datetime import datetime, timezone
    from app.models.whatsapp import WhatsAppClosingRequest, WhatsAppSurvey
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "link-survey", f"SISFARM {code}"))
    link = db.query(WhatsAppLink).one()
    now = datetime.now(timezone.utc)
    request = WhatsAppClosingRequest(id="a" * 32, link_id=link.id, status="closed", created_at=now, resolved_at=now)
    survey = WhatsAppSurvey(id="b" * 32, closing_id=request.id, link_id=link.id, status="pending", created_at=now)
    db.add(request)
    db.flush()
    db.add(survey)
    db.commit()
    assert _receive(db, client, _payload("5535999", "survey-good", button=f"survey:good:{survey.id}")) == ["survey_answered"]
    assert survey.rating == "good" and survey.status == "answered" and link.thread_id is None
    assert _receive(db, client, _payload("5535999", "survey-again", button=f"survey:bad:{survey.id}")) == ["survey_stale"]
    db.close()


def test_idle_worker_respects_queued_message_and_expired_window(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from uuid import uuid4
    from app.services.whatsapp_lifecycle import process_lifecycle
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "link-idle-queued", f"SISFARM {code}"))
    now = datetime.now(timezone.utc)
    thread = routes.AssistantThread(id=uuid4().hex, user_id=user.id, organization_id=user.organization_id,
                                    farm_id=scope["active_farm_id"], season_id=scope["active_season_id"],
                                    channel="whatsapp", title="WhatsApp", created_at=now, updated_at=now)
    routes._save_thread(db, thread, [{"role": "assistant", "content": "Olá"}])
    link = db.query(WhatsAppLink).one()
    link.thread_id, link.last_inbound_at = thread.id, now - timedelta(minutes=31)
    db.commit()
    ids = whatsapp_bot.store_inbound(db, whatsapp.extract_messages(_payload("5535999", "queued-idle", "nova dúvida")))
    sent = len(client.sent)
    process_lifecycle(db, client, now)
    assert len(client.sent) == sent and link.thread_id == thread.id
    db.get(WhatsAppInboundMessage, ids[0]).status = "done"
    link.last_inbound_at = now - timedelta(hours=25)
    db.commit()
    process_lifecycle(db, client, now)
    assert len(client.sent) == sent and link.thread_id == thread.id
    db.close()


def test_explicit_close_sends_one_survey(monkeypatch):
    from app.models.whatsapp import WhatsAppSurvey
    from app.services.whatsapp_lifecycle import process_lifecycle
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "link-explicit-survey", f"SISFARM {code}"))
    _receive(db, client, _payload("5535999", "explicit-request", "encerrar"))
    button = client.sent[-1][3][1][0]
    _receive(db, client, _payload("5535999", "explicit-confirm", button=button))
    process_lifecycle(db, client)
    count = len(client.sent)
    process_lifecycle(db, client)
    assert db.query(WhatsAppSurvey).one().status == "pending" and len(client.sent) == count
    db.close()


def test_ai_requests_closure_with_natural_language(monkeypatch):
    from app.models.whatsapp import WhatsAppClosingRequest
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope['active_farm'], scope['active_season'])
    _receive(db, client, _payload('5535999', 'link-natural-close', f'SISFARM {code}'))

    def ai_requests_close(history, assistant, **kwargs):
        assert 'solicitar_encerramento_conversa' in assistant.handlers
        assistant.call_tool('solicitar_encerramento_conversa', '{}')
        return {'resposta': 'Aguardando confirmação de encerramento.', 'draft_id': None}

    monkeypatch.setattr(routes, 'run_farm_assistant', ai_requests_close)
    assert _receive(db, client, _payload('5535999', 'natural-close', 'pode parar nosso papo por aqui')) == ['close_pending']
    link = db.query(WhatsAppLink).one()
    request = db.query(WhatsAppClosingRequest).one()
    assert request.status == 'pending' and request.thread_id == link.thread_id
    assert client.sent[-1][0] == 'buttons'
    close_button = client.sent[-1][3][1][0]
    assert _receive(db, client, _payload('5535999', 'natural-close-confirm', button=close_button)) == ['closed']
    assert link.thread_id is None
    db.close()


def test_exact_user_example_requests_buttons(monkeypatch):
    _settings(monkeypatch)
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope['active_farm'], scope['active_season'])
    _receive(db, client, _payload('5535999', 'link-user-example', f'SISFARM {code}'))
    assert _receive(db, client, _payload('5535999', 'user-example', 'finaliza essa conversa')) == ['close_pending']
    assert client.sent[-1][0] == 'buttons'
    db.close()
