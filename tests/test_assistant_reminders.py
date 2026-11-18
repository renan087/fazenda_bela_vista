import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from app.models.assistant_reminder import AssistantReminder
from app.services.assistant_reminders import ReminderTools
from tests.test_stock_assistant import _route_env


def test_reminder_requires_confirmation_and_appears_only_for_owner(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    due = datetime.now(timezone.utc) + timedelta(hours=1)
    local = ReminderTools.local_time(due).strftime("%Y-%m-%dT%H:%M")

    def prepare(history, assistant):
        result = assistant.call_tool("preparar_lembrete", json.dumps({"descricao": "Conferir diesel", "data_hora": local}))
        assert result["aguardando_confirmacao"]
        return {"resposta": "Pronto!", "draft_id": None}

    monkeypatch.setattr(routes, "run_farm_assistant", prepare)
    thread, reply = routes._assistant_turn(db, user, routes._repository(db), scope, None, "Me lembre de conferir diesel")
    assert "confirma" in reply["content"] and db.query(AssistantReminder).count() == 0
    thread, reply = routes._assistant_turn(db, user, routes._repository(db), scope, thread, "sim")
    reminder = db.query(AssistantReminder).one()
    assert reminder.status == "active"
    reminder.due_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    db.commit()
    repo = routes._repository(db)
    request = SimpleNamespace()
    mine = routes._build_global_notifications(request, repo, scope, frozenset(), user)
    assert any(item["message"] == "Conferir diesel" for item in mine["operational"])
    other = SimpleNamespace(id=user.id + 1, organization_id=user.organization_id)
    assert not routes._build_global_notifications(request, repo, scope, frozenset(), other)["operational"]
    reminder.status = "paused"
    db.commit()
    assert not routes._build_global_notifications(request, repo, scope, frozenset(), user)["operational"]


def test_duplicate_confirmation_and_scope_are_enforced(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    tools = ReminderTools(db, user, scope["active_farm"], scope["active_season"])
    future = tools.local_time(datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M")
    tools.propose_create("Verificar adubo", future)
    proposal = tools.proposal
    tools.confirm(proposal)
    db.commit()
    tools.confirm(proposal)
    db.commit()
    assert db.query(AssistantReminder).count() == 1
    other = ReminderTools(db, SimpleNamespace(id=user.id + 1, organization_id=user.organization_id), scope["active_farm"], scope["active_season"])
    assert other.list_reminders()["lembretes"] == []


def test_cancellation_and_pause_require_confirmation(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    tools = ReminderTools(db, user, scope["active_farm"], scope["active_season"])
    future = tools.local_time(datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M")
    def create(history, assistant):
        assistant.call_tool("preparar_lembrete", json.dumps({"descricao": "Verificar adubo", "data_hora": future}))
        return {"resposta": "", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", create)
    thread, _ = routes._assistant_turn(db, user, routes._repository(db), scope, None, "Crie um lembrete")
    thread, _ = routes._assistant_turn(db, user, routes._repository(db), scope, thread, "não")
    assert db.query(AssistantReminder).count() == 0
    tools.propose_create("Verificar adubo", future)
    tools.confirm(tools.proposal)
    db.commit()
    reminder = db.query(AssistantReminder).one()
    def pause(history, assistant):
        assistant.call_tool("preparar_alteracao_lembrete", json.dumps({"lembrete_id": reminder.id, "acao": "pausar"}))
        return {"resposta": "", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", pause)
    thread, _ = routes._assistant_turn(db, user, routes._repository(db), scope, thread, "Pause meu lembrete")
    assert reminder.status == "active"
    routes._assistant_turn(db, user, routes._repository(db), scope, thread, "sim")
    assert reminder.status == "paused"


def test_manual_management_uses_same_store_and_enforces_context(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    request = SimpleNamespace(session={})
    tools = ReminderTools(db, user, scope["active_farm"], scope["active_season"])
    local = tools.local_time(datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M")
    context = f"farm:{scope['active_farm_id']}:season:{scope['active_season_id']}"
    assert routes.manage_personal_reminders(request, "csrf", context, "create", "Verificar diesel", local, None, db, user)["ok"]
    reminder = db.query(AssistantReminder).one()
    assert tools.list_reminders()["lembretes"][0]["descricao"] == "Verificar diesel"
    with pytest.raises(HTTPException) as error:
        routes.manage_personal_reminders(request, "csrf", "farm:999:season:1", "pausar", "", "", reminder.id, db, user)
    assert error.value.status_code == 409 and reminder.status == "active"
    routes.manage_personal_reminders(request, "csrf", context, "pausar", "", "", reminder.id, db, user)
    assert reminder.status == "paused"
    routes.manage_personal_reminders(request, "csrf", context, "retomar", "", "", reminder.id, db, user)
    assert reminder.status == "active"
    routes.manage_personal_reminders(request, "csrf", context, "excluir", "", "", reminder.id, db, user)
    assert tools.list_reminders()["lembretes"] == []


def test_manual_reminder_rejects_past_date_and_foreign_reminder(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    request = SimpleNamespace(session={})
    tools = ReminderTools(db, user, scope["active_farm"], scope["active_season"])
    context = f"farm:{scope['active_farm_id']}:season:{scope['active_season_id']}"
    with pytest.raises(HTTPException):
        routes.manage_personal_reminders(request, "csrf", context, "create", "Teste", "2000-01-01T12:00", None, db, user)
    future = tools.local_time(datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M")
    tools.propose_create("Pessoal", future)
    tools.confirm(tools.proposal)
    db.commit()
    other = SimpleNamespace(id=user.id + 1, organization_id=user.organization_id)
    with pytest.raises(HTTPException):
        routes.manage_personal_reminders(request, "csrf", context, "excluir", "", "", db.query(AssistantReminder).one().id, db, other)
    assert db.query(AssistantReminder).one().status == "active"


def test_pezinho_alerts_only_current_users_due_or_nearby_active_reminders(monkeypatch):
    from uuid import uuid4
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    monkeypatch.setattr(routes, "_pezinho_scope", lambda db, user, request: (routes._repository(db), scope))
    now = datetime.now(timezone.utc)
    for description, delta, status in [("Vencido", -10, "active"), ("Próximo", 30, "active"),
                                       ("Depois", 120, "active"), ("Pausado", -1, "paused")]:
        db.add(AssistantReminder(proposal_id=uuid4().hex, user_id=user.id, organization_id=user.organization_id,
                                 farm_id=scope["active_farm_id"], season_id=scope["active_season_id"],
                                 description=description, due_at=now + timedelta(minutes=delta), status=status, created_at=now))
    db.commit()
    context = f"{user.id}:{user.organization_id}:{scope['active_farm_id']}:{scope['active_season_id']}"
    response = routes.pezinho_reminder_alerts(SimpleNamespace(), context, db, user)
    items = json.loads(response.body)["reminders"]
    assert {item["description"]: item["state"] for item in items} == {"Vencido": "vencido", "Próximo": "proximo"}
    assert response.headers["cache-control"] == "no-store"
    with pytest.raises(HTTPException) as error:
        routes.pezinho_reminder_alerts(SimpleNamespace(), "outro-contexto", db, user)
    assert error.value.status_code == 409
    other = SimpleNamespace(id=user.id + 1, organization_id=user.organization_id)
    other_context = f"{other.id}:{other.organization_id}:{scope['active_farm_id']}:{scope['active_season_id']}"
    assert json.loads(routes.pezinho_reminder_alerts(SimpleNamespace(), other_context, db, other).body)["reminders"] == []


def test_agent_stops_immediately_after_preparing_reminder(monkeypatch):
    from tests.test_stock_assistant import _assistant, _fake_client, _tool_call
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    tools = ReminderTools(db, user, scope["active_farm"], scope["active_season"])
    assistant = _assistant([])
    assistant.handlers.update({schema["function"]["name"]: handler for schema, handler in tools.tools()})
    future = tools.local_time(datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M")
    calls = []
    client = _fake_client([[_tool_call("preparar_lembrete", {"descricao": "Verificar diesel", "data_hora": future})]], calls)
    result = assistant.run([{"role": "user", "content": "Crie um lembrete"}], client=client, model="test")
    assert len(calls) == 1 and result["resposta"] == "Aguardando confirmação do lembrete."
    assert tools.proposal and db.query(AssistantReminder).count() == 0


def test_edit_reminder_requires_confirmation_and_rejects_stale_changes(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    tools = ReminderTools(db, user, scope["active_farm"], scope["active_season"])
    future = tools.local_time(datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M")
    tools.propose_create("Original", future); tools.confirm(tools.proposal); db.commit()
    row = db.query(AssistantReminder).one()
    def edit(history, assistant):
        result = assistant.call_tool("preparar_edicao_lembrete", json.dumps({"lembrete_id": row.id, "descricao": "Atualizado", "data_hora": None}))
        assert result["aguardando_confirmacao"]
        return {"resposta": "", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", edit)
    thread, message = routes._assistant_turn(db, user, routes._repository(db), scope, None, "Altere a descrição")
    assert "Atualizado" in message["content"] and row.description == "Original"
    routes._assistant_turn(db, user, routes._repository(db), scope, thread, "sim")
    assert row.description == "Atualizado"
    tools.propose_edit(row.id, "Mudança antiga", None)
    stale = tools.proposal
    row.description = "Outra alteração"; db.commit()
    with pytest.raises(ValueError, match="foi alterado"):
        tools.confirm(stale)


def test_whatsapp_reminder_confirmation_buttons_reject_replay(monkeypatch):
    from app.models.whatsapp import WhatsAppLink
    from app.services import whatsapp_bot
    from tests.test_whatsapp import FakeClient, _payload, _receive, _settings
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    tools = ReminderTools(db, user, scope["active_farm"], scope["active_season"])
    future = tools.local_time(datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M")
    def create(history, assistant):
        assistant.call_tool("preparar_lembrete", json.dumps({"descricao": "Conferir diesel", "data_hora": future}))
        return {"resposta": "", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", create)
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "link-rem", f"SISFARM {code}"))
    _receive(db, client, _payload("5535999", "make-rem", "Crie um lembrete"))
    assert client.sent[-1][0] == "buttons" and db.query(AssistantReminder).count() == 0
    assert "botões abaixo" in client.sent[-1][2] and "Responda sim" not in client.sent[-1][2]
    button = client.sent[-1][3][0][0]
    _receive(db, client, _payload("5535999", "confirm-rem", button=button))
    assert db.query(AssistantReminder).count() == 1 and "criado" in client.sent[-1][2]
    _receive(db, client, _payload("5535999", "repeat-rem", button=button))
    assert db.query(AssistantReminder).count() == 1 and "não está mais disponível" in client.sent[-1][2]
    _receive(db, client, _payload("5535999", "make-rem-again", "Crie outro lembrete"))
    cancel = client.sent[-1][3][1][0]
    _receive(db, client, _payload("5535999", "cancel-rem", button=cancel))
    assert db.query(AssistantReminder).count() == 1 and "não fiz" in client.sent[-1][2]


def test_whatsapp_retries_when_model_invents_confirmation(monkeypatch):
    from app.services import whatsapp_bot
    from tests.test_whatsapp import FakeClient, _payload, _receive, _settings
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    tools = ReminderTools(db, user, scope["active_farm"], scope["active_season"])
    future = tools.local_time(datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M")
    calls = []
    def model(history, assistant):
        calls.append(history)
        if len(calls) == 1:
            return {"resposta": "Quer criar este lembrete? Responda sim para confirmar.", "draft_id": None}
        assistant.call_tool("preparar_lembrete", json.dumps({"descricao": "Plantio de café", "data_hora": future}))
        return {"resposta": "", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", model)
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "fake-link", f"SISFARM {code}"))
    _receive(db, client, _payload("5535999", "fake-question", "Crie um lembrete para plantio de café"))
    assert len(calls) == 2 and client.sent[-1][0] == "buttons"
    assert db.query(AssistantReminder).count() == 0
    _receive(db, client, _payload("5535999", "real-confirm", button=client.sent[-1][3][0][0]))
    reminder = db.query(AssistantReminder).one()
    reminder.due_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    db.commit()
    notifications = routes._build_global_notifications(SimpleNamespace(), routes._repository(db), scope, frozenset(), user)
    assert any(item["message"] == "Plantio de café" for item in notifications["operational"])


def test_no_false_reminder_success_when_model_repeats_hallucination(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    calls = []
    def model(history, assistant):
        calls.append(1)
        return {"resposta": "Lembrete criado com sucesso!", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", model)
    _, item = routes._assistant_turn(db, user, routes._repository(db), scope, None, "Crie um lembrete para amanhã")
    assert len(calls) == 2 and item["error"] and "Nenhuma alteração foi salva" in item["content"]
    assert db.query(AssistantReminder).count() == 0


def test_old_fake_confirmation_is_replaced_by_real_proposal(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    tools = ReminderTools(db, user, scope["active_farm"], scope["active_season"])
    future = tools.local_time(datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M")
    now = datetime.now(timezone.utc)
    thread = routes.AssistantThread(id="a" * 32, user_id=user.id, organization_id=user.organization_id,
                                    farm_id=scope["active_farm_id"], season_id=scope["active_season_id"],
                                    channel="whatsapp", title="WhatsApp", created_at=now, updated_at=now)
    routes._save_thread(db, thread, [{"role": "user", "content": "Crie um lembrete de plantio"},
                                    {"role": "assistant", "content": "Quer criar o lembrete? Responda sim."}])
    calls = []
    def model(history, assistant):
        calls.append(1)
        if len(calls) == 1:
            return {"resposta": "Lembrete criado!", "draft_id": None}
        assistant.call_tool("preparar_lembrete", json.dumps({"descricao": "Plantio", "data_hora": future}))
        return {"resposta": "", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", model)
    _, reply = routes._assistant_turn(db, user, routes._repository(db), scope, thread, "sim")
    assert len(calls) == 2 and reply.get("reminder_proposal") and "botões" in reply["content"]
    assert db.query(AssistantReminder).count() == 0


def test_missing_reminder_time_is_still_asked_normally():
    from app.web.routes import _assistant_unverified_reminder_claim
    history = [{"role": "user", "content": "Crie um lembrete"}]
    assert not _assistant_unverified_reminder_claim("Qual data e horário você prefere?", history)
    assert not _assistant_unverified_reminder_claim("Não foi possível criar o lembrete.", history)
