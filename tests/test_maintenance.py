import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from starlette.responses import PlainTextResponse

import app.models  # noqa: F401
from app.db.base import Base
from app.middleware import maintenance as mw
from app.services import maintenance as svc

NOW = datetime(2026, 11, 15, 22, 0, tzinfo=timezone.utc)


def _db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return Session(engine)


def test_save_and_compute_with_auto_end_and_announcement():
    db = _db()
    assert svc.compute_state(db, NOW) == svc.OFF
    with pytest.raises(svc.MaintenanceError):
        svc.save_state(db, mode='leitura', message='', announce_at=None, ends_at=None, user_email='eq@x', now=NOW)
    with pytest.raises(svc.MaintenanceError):
        svc.save_state(db, mode='fora_do_ar', message='', announce_at=None, ends_at=NOW + timedelta(hours=25), user_email='eq@x', now=NOW)
    svc.save_state(db, mode='leitura', message='Deploy', announce_at=None, ends_at=NOW + timedelta(minutes=15), user_email='eq@x', now=NOW)
    state = svc.compute_state(db, NOW)
    assert state.active and state.mode == 'leitura' and state.retry_after_seconds(NOW) == 900
    assert not svc.compute_state(db, NOW + timedelta(minutes=16)).active  # volta sozinho
    svc.save_state(db, mode='off', message='', announce_at=NOW + timedelta(hours=2), ends_at=NOW + timedelta(hours=3), user_email='eq@x', now=NOW)
    announced = svc.compute_state(db, NOW)
    assert not announced.active and announced.announced(NOW) and svc.compute_state(db, NOW + timedelta(hours=2, minutes=1)) == svc.OFF


def test_env_variable_wins(monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(svc, 'get_settings', lambda: SimpleNamespace(maintenance_mode='fora_do_ar'))
    assert svc.compute_state(_db(), NOW).mode == 'fora_do_ar'


def _call(monkeypatch, state, path, method='GET', email=None):
    monkeypatch.setattr(mw, 'current_state', lambda factory: state)
    monkeypatch.setattr(mw, 'is_super_admin_email', lambda e: e == 'eq@sisfarm')
    request = Request({'type': 'http', 'method': method, 'path': path, 'headers': [], 'query_string': b'', 'session': {'user_email': email} if email else {}})
    async def call_next(_):
        return PlainTextResponse('ok')
    return asyncio.run(mw.MaintenanceMiddleware(app=None).dispatch(request, call_next))


def test_middleware_blocks_by_mode_and_lets_team_and_essentials_through(monkeypatch):
    down = svc.MaintenanceState(mode='fora_do_ar', ends_at=NOW + timedelta(minutes=10))
    blocked = _call(monkeypatch, down, '/dashboard', email='cliente@x')
    assert blocked.status_code == 503 and 'Retry-After' in blocked.headers and b'Voltamos em instantes' in blocked.body
    assert _call(monkeypatch, down, '/api/farms').status_code == 503
    for path in ('/health', '/api/webhooks/asaas', '/', '/login', '/static/x.css'):
        assert _call(monkeypatch, down, path, method='POST' if 'webhooks' in path else 'GET').status_code == 200, path
    assert _call(monkeypatch, down, '/dashboard', email='eq@sisfarm').status_code == 200

    read_only = svc.MaintenanceState(mode='leitura')
    assert _call(monkeypatch, read_only, '/fazendas', email='cliente@x').status_code == 200
    post = _call(monkeypatch, read_only, '/fazendas', method='POST', email='cliente@x')
    assert post.status_code == 503 and b'modo consulta' in post.body
    assert _call(monkeypatch, read_only, '/logout', method='POST', email='cliente@x').status_code == 200
    assert _call(monkeypatch, svc.OFF, '/fazendas', method='POST', email='cliente@x').status_code == 200


def test_login_throttle_blocks_after_failures_and_resets_on_success():
    from app.services import login_throttle as lt
    for _ in range(lt.MAX_PER_EMAIL):
        assert lt.blocked_for('a@x.com', '1.1.1.1', now=1000) == 0
        lt.register_failure('A@x.com', '1.1.1.1', now=1000)
    assert lt.blocked_for('a@x.com', '2.2.2.2', now=1001) > 0  # bloqueia o e-mail mesmo vindo de outra rede
    assert lt.blocked_for('outro@x.com', '1.1.1.1', now=1001) == 0
    assert lt.blocked_for('a@x.com', '1.1.1.1', now=1000 + lt.WINDOW_SECONDS + 1) == 0  # expira sozinho
    lt.register_success('a@x.com')
    assert lt.blocked_for('a@x.com', '1.1.1.1', now=1001) == 0
    for i in range(lt.MAX_PER_IP):
        lt.register_failure(f'u{i}@x.com', '9.9.9.9', now=1000)
    assert lt.blocked_for('novo@x.com', '9.9.9.9', now=1001) > 0  # muitas contas a partir do mesmo IP
