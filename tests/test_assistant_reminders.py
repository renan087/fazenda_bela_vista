import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

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
