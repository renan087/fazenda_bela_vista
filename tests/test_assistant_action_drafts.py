from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.models.assistant_action_draft import AssistantActionDraft
from app.services.assistant_action_drafts import claim_action, prepare_action
from tests.test_stock_assistant import _route_env
from app.core.permissions_catalog import PAGE_RAINFALL, WRITE_RAINFALL
from app.models.rainfall import RainfallRecord


def test_claim_is_scoped_idempotent_and_transactional(monkeypatch):
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    farm, season = scope["active_farm"], scope["active_season"]
    draft = prepare_action(db, user=user, farm=farm, season=season, thread_id="a" * 32,
                           kind="rainfall", payload={"millimeters": 5})
    db.commit()
    args = dict(draft_id=draft.id, user=user, farm=farm, season=season, thread_id="a" * 32, status="confirmed")
    other = SimpleNamespace(id=user.id + 1, organization_id=user.organization_id)
    assert not claim_action(db, **{**args, "user": other})
    assert not claim_action(db, **{**args, "thread_id": "b" * 32})
    assert claim_action(db, **args)
    assert not claim_action(db, **args)
    db.rollback()
    assert claim_action(db, **args)
    db.commit()
    assert not claim_action(db, **args)


def test_expired_and_invalid_actions_are_rejected(monkeypatch):
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    farm, season = scope["active_farm"], scope["active_season"]
    with pytest.raises(ValueError):
        prepare_action(db, user=user, farm=farm, season=season, thread_id="a" * 32, kind="unknown", payload={})
    draft = prepare_action(db, user=user, farm=farm, season=season, thread_id="a" * 32, kind="pest", payload={})
    draft.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    db.commit()
    assert not claim_action(db, draft_id=draft.id, user=user, farm=farm, season=season,
                            thread_id="a" * 32, status="confirmed")


def test_rainfall_requires_confirmation_and_rechecks_permission(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda *a: {PAGE_RAINFALL, WRITE_RAINFALL})
    monkeypatch.setattr(routes, "_entitlements_for", lambda *a: SimpleNamespace(read_only=False, module_allowed=lambda p: True))
    def prepare(history, assistant):
        result = assistant.call_tool("preparar_chuva", '{"data":"2026-11-14","milimetros":12.5}')
        assert result["aguardando_confirmacao"]
        return {"resposta": "", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", prepare)
    thread, reply = routes._assistant_turn(db, user, routes._repository(db), scope, None, "Registre 12,5 mm de chuva ontem")
    assert reply.get("module_proposal") and db.query(RainfallRecord).count() == 0
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda *a: {PAGE_RAINFALL})
    thread, denied = routes._assistant_turn(db, user, routes._repository(db), scope, thread, "sim")
    assert denied["error"] and db.query(RainfallRecord).count() == 0


def test_rainfall_confirmed_once(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda *a: {PAGE_RAINFALL, WRITE_RAINFALL})
    monkeypatch.setattr(routes, "_entitlements_for", lambda *a: SimpleNamespace(read_only=False, module_allowed=lambda p: True))
    def prepare(history, assistant):
        assistant.call_tool("preparar_chuva", '{"data":"2026-11-14","milimetros":12.5}')
        return {"resposta": "", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", prepare)
    thread, reply = routes._assistant_turn(db, user, routes._repository(db), scope, None, "Registre chuva")
    thread, reply = routes._assistant_turn(db, user, routes._repository(db), scope, thread, "sim")
    assert "registrada" in reply["content"] and db.query(RainfallRecord).count() == 1
    assert float(db.query(RainfallRecord).one().millimeters) == 12.5


def test_rainfall_whatsapp_buttons_and_replay(monkeypatch):
    from app.services import whatsapp_bot
    from tests.test_whatsapp import FakeClient, _payload, _receive, _settings
    _settings(monkeypatch)
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda *a: {PAGE_RAINFALL, WRITE_RAINFALL})
    monkeypatch.setattr(routes, "_entitlements_for", lambda *a: SimpleNamespace(read_only=False, module_allowed=lambda p: True))
    def prepare(history, assistant):
        assistant.call_tool("preparar_chuva", '{"data":"2026-11-14","milimetros":12.5}')
        return {"resposta": "", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", prepare)
    client = FakeClient()
    code = whatsapp_bot.start_link(db, user, scope["active_farm"], scope["active_season"])
    _receive(db, client, _payload("5535999", "rain-link", f"SISFARM {code}"))
    _receive(db, client, _payload("5535999", "rain-prepare", "Registre chuva"))
    assert client.sent[-1][0] == "buttons" and db.query(RainfallRecord).count() == 0
    button = client.sent[-1][3][0][0]
    _receive(db, client, _payload("5535999", "rain-confirm", button=button))
    assert db.query(RainfallRecord).count() == 1
    _receive(db, client, _payload("5535999", "rain-replay", button=button))
    assert db.query(RainfallRecord).count() == 1


def test_rainfall_cancel_and_invalid_amount(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    monkeypatch.setattr(routes, "permission_codes_for_user", lambda *a: {PAGE_RAINFALL, WRITE_RAINFALL})
    monkeypatch.setattr(routes, "_entitlements_for", lambda *a: SimpleNamespace(read_only=False, module_allowed=lambda p: True))
    from app.services.assistant_action_drafts import validate_rainfall
    with pytest.raises(ValueError):
        validate_rainfall({"date": "2026-11-14", "millimeters": -1}, scope["active_season"], datetime(2026, 11, 15).date())
    def prepare(history, assistant):
        assistant.call_tool("preparar_chuva", '{"data":"2026-11-14","milimetros":12.5}')
        return {"resposta": "", "draft_id": None}
    monkeypatch.setattr(routes, "run_farm_assistant", prepare)
    thread, _ = routes._assistant_turn(db, user, routes._repository(db), scope, None, "Registre chuva")
    routes._assistant_turn(db, user, routes._repository(db), scope, thread, "não")
    assert db.query(RainfallRecord).count() == 0
