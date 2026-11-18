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
