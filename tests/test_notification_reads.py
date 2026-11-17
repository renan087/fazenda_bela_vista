import json
from types import SimpleNamespace as N

import pytest
from fastapi import HTTPException

from app.core.permissions_catalog import PAGE_INPUTS
from app.models import NotificationRead, User
from app.web import routes
from tests.test_stock_assistant import _route_env


def test_alert_keys_are_distinct_and_resolved_stock_disappears():
    farm = N(id=1)
    first = N(id=10, name="Ureia", item_type="insumo_agricola", low_stock_threshold=10, default_unit="kg")
    second = N(id=11, name="Boro", item_type="insumo_agricola", low_stock_threshold=10, default_unit="kg")
    entries = [N(id=20, farm_id=1, input_id=10, input_catalog=first, available_quantity=1, expiration_date=None),
               N(id=21, farm_id=1, input_id=11, input_catalog=second, available_quantity=2, expiration_date=None)]
    repo = N(list_purchased_inputs=lambda **kwargs: entries)
    scope = {"active_farm": farm, "active_season": None}
    result = routes._build_global_notifications(N(), repo, scope, frozenset({PAGE_INPUTS}))
    assert len(result["important"]) == 2
    assert len({item["key"] for item in result["important"]}) == 2
    entries[0].available_quantity = 20
    resolved = routes._build_global_notifications(N(), repo, scope, frozenset({PAGE_INPUTS}))
    assert len(resolved["important"]) == 1 and "Boro" in resolved["important"][0]["message"]


def test_read_status_is_scoped_to_user_and_ignores_unknown_keys(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    key, other = "a" * 64, "b" * 64
    farm_scope = f"farm:{scope['active_farm_id']}"
    context_scope = f"{farm_scope}:season:{scope['active_season_id']}"
    monkeypatch.setattr(routes, "_build_global_notifications", lambda *args: {
        "important": [{"key": key, "scope": farm_scope}], "operational": []})
    request = N(session={})
    response = routes.mark_notifications_read(request, "csrf", context_scope, json.dumps([key, other]), db, user)
    assert json.loads(response.body)["read"] == [key]
    routes.mark_notifications_read(request, "csrf", context_scope, json.dumps([key]), db, user)
    assert db.query(NotificationRead).count() == 1
    second = User(name="Outro", email="outro@example.com", hashed_password="x", organization_id=user.organization_id, is_active=True)
    db.add(second)
    db.commit()
    assert db.query(NotificationRead).filter_by(user_id=second.id).count() == 0
    with pytest.raises(HTTPException) as error:
        routes.mark_notifications_read(request, "csrf", "farm:999:season:1", json.dumps([key]), db, user)
    assert error.value.status_code == 409


def test_read_state_survives_device_change_and_resolved_alert_is_forgotten(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    scope["context_selection_required"] = False
    farm_scope = f"farm:{scope['active_farm_id']}"
    key = "c" * 64
    active = {"value": True}
    monkeypatch.setattr(routes, "_build_global_notifications", lambda *args: {
        "important": [{"key": key, "scope": farm_scope}] if active["value"] else [],
        "operational": [], "total": int(active["value"]), "important_count": int(active["value"])})
    monkeypatch.setattr(routes, "current_maintenance_state", lambda *args: None)
    request = N(session={}, query_params={}, url=N(path="/dashboard"))
    context_scope = f"{farm_scope}:season:{scope['active_season_id']}"
    routes.mark_notifications_read(request, "csrf", context_scope, json.dumps([key]), db, user)
    repo = routes._repository(db)
    context = routes._base_context(request, user, "csrf", "dashboard", _repo=repo, pezinho=None)
    assert context["global_notifications"]["important"][0]["is_read"]
    assert context["global_notifications"]["unread_total"] == 0
    active["value"] = False
    routes._base_context(request, user, "csrf", "dashboard", _repo=repo, pezinho=None)
    db.expire_all()
    assert db.query(NotificationRead).filter_by(user_id=user.id).count() == 0
    active["value"] = True
    context = routes._base_context(request, user, "csrf", "dashboard", _repo=repo, pezinho=None)
    assert not context["global_notifications"]["important"][0]["is_read"]
