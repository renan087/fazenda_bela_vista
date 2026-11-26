import json

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from starlette.middleware.sessions import SessionMiddleware

from app.core.config import get_settings
from app.core.csrf import ensure_csrf_token
from app.core.security import get_password_hash
from app.db.base import Base
from app.db.session import get_db
from app.models import AuditLog, Organization, User
from app.routers import auth
from app.services import audit_log_service, login_throttle
from app.web import routes

pytestmark = pytest.mark.suite_security

ADMIN_EMAIL = "admin@unlock.example.test"
SUPER_ADMIN_EMAIL = "support@unlock.example.test"
MEMBER_EMAIL = "member@unlock.example.test"
EXTERNAL_EMAIL = "external@unlock.example.test"
BLOCKED_IP = "192.0.2.10"
OTHER_IP = "192.0.2.20"
PASSWORD = "senha-somente-para-teste"


class LocalTestClient(TestClient):
    # The ASGI transport stays in process while conftest blocks outbound HTTP.
    send = httpx.Client.send


@pytest.fixture
def unlock_app(monkeypatch):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine)
    settings = get_settings()
    monkeypatch.setattr(settings, "super_admin_email", SUPER_ADMIN_EMAIL)
    monkeypatch.setattr(settings, "asaas_environment", "sandbox")
    monkeypatch.setattr(audit_log_service, "SessionLocal", sessions)
    monkeypatch.setattr(auth, "send_access_code_email", lambda *_: None)

    with sessions() as db:
        db.add_all([
            Organization(id=1, name="Cooperativa", slug="cooperativa"),
            Organization(id=2, name="Fazenda", slug="fazenda"),
        ])
        db.add_all([
            User(id=1, organization_id=1, email=ADMIN_EMAIL, name="Admin", hashed_password="x", is_admin=True),
            User(id=2, organization_id=1, email=MEMBER_EMAIL, name="Membro", hashed_password=get_password_hash(PASSWORD)),
            User(id=3, organization_id=2, email=EXTERNAL_EMAIL, name="Externo", hashed_password="x"),
            User(id=4, organization_id=1, email=SUPER_ADMIN_EMAIL, name="Suporte", hashed_password="x"),
        ])
        db.commit()

    def db_dependency():
        with sessions() as db:
            yield db

    app = FastAPI()
    app.add_middleware(SessionMiddleware, secret_key="test-unlock")
    app.dependency_overrides[get_db] = db_dependency
    app.include_router(auth.router)
    app.include_router(routes.router)
    app.mount("/static", StaticFiles(directory="app/static"), name="static")

    @app.post("/test-session")
    def set_test_session(request: Request, email: str | None = None):
        request.session.clear()
        if email:
            request.session["user_email"] = email
        return {"csrf_token": ensure_csrf_token(request)}

    with LocalTestClient(app, follow_redirects=False) as client:
        yield client, sessions
    engine.dispose()


def _block_email(email):
    for _ in range(login_throttle.MAX_PER_EMAIL):
        login_throttle.register_failure(email, BLOCKED_IP)


def test_org_admin_unlocks_only_the_target_email_and_login_still_requires_2fa(unlock_app):
    client, sessions = unlock_app
    _block_email(MEMBER_EMAIL)
    _block_email(EXTERNAL_EMAIL)
    for _ in range(login_throttle.MAX_PER_IP - 2 * login_throttle.MAX_PER_EMAIL):
        login_throttle.register_failure("other@unlock.example.test", BLOCKED_IP)
    csrf = client.post("/test-session").json()
    credentials = {"email": MEMBER_EMAIL, "password": PASSWORD}
    blocked_login = client.post("/login", data={**credentials, **csrf})
    assert blocked_login.status_code == 400
    assert "Muitas tentativas de acesso" in blocked_login.text

    action = "/usuarios/2/desbloquear-login"
    csrf = client.post("/test-session", params={"email": ADMIN_EMAIL}).json()
    page = client.get("/usuarios")
    assert page.status_code == 200
    assert "Bloqueado por tentativas" in page.text
    assert "Desbloquear login" in page.text
    assert f'action="{action}"' in page.text
    assert EXTERNAL_EMAIL not in page.text

    response = client.post(action, data=csrf)
    assert response.status_code == 303
    assert response.headers["location"] == "/usuarios"
    assert f'action="{action}"' not in client.get("/usuarios").text
    assert client.post(action, data=csrf).status_code == 303
    assert login_throttle.blocked_for(EXTERNAL_EMAIL, OTHER_IP) > 0
    assert login_throttle.blocked_for(MEMBER_EMAIL, BLOCKED_IP) > 0
    assert login_throttle.blocked_for(MEMBER_EMAIL, OTHER_IP) == 0

    csrf = client.post("/test-session").json()
    response = client.post("/login", data={**credentials, **csrf})
    assert response.status_code == 303
    assert response.headers["location"] == "/login/verificacao"
    with sessions() as db:
        event = db.query(AuditLog).filter_by(event_type="auth.login.unlock").one()
        assert event.actor_email == ADMIN_EMAIL
        assert json.loads(event.metadata_json)["target_user_id"] == 2


def test_unlock_rejects_regular_users_other_organizations_and_invalid_csrf(unlock_app):
    client, sessions = unlock_app
    _block_email(MEMBER_EMAIL)
    _block_email(EXTERNAL_EMAIL)
    local_action = "/usuarios/2/desbloquear-login"
    csrf = client.post("/test-session", params={"email": MEMBER_EMAIL}).json()
    response = client.post(local_action, data=csrf)
    assert response.headers["location"] == "/dashboard"

    csrf = client.post("/test-session", params={"email": ADMIN_EMAIL}).json()
    response = client.post("/usuarios/3/desbloquear-login", data=csrf)
    assert response.headers["location"] == "/usuarios"
    response = client.post(local_action, data={"csrf_token": "invalid"})
    assert response.status_code == 403
    response = client.post("/organizacoes/usuarios/3/desbloquear-login", data=csrf)
    assert response.headers["location"] == "/dashboard"
    assert login_throttle.blocked_for(MEMBER_EMAIL, OTHER_IP) > 0
    assert login_throttle.blocked_for(EXTERNAL_EMAIL, OTHER_IP) > 0
    with sessions() as db:
        assert db.query(AuditLog).filter_by(event_type="auth.login.unlock").count() == 0


def test_super_admin_can_unlock_a_user_in_another_organization(unlock_app):
    client, sessions = unlock_app
    _block_email(EXTERNAL_EMAIL)
    csrf = client.post("/test-session", params={"email": SUPER_ADMIN_EMAIL}).json()
    action = "/organizacoes/usuarios/3/desbloquear-login"
    page = client.get("/organizacoes")
    assert page.status_code == 200
    assert "Usuários bloqueados por tentativas" in page.text
    assert f'action="{action}"' in page.text
    assert client.post(action, data={"csrf_token": "invalid"}).status_code == 403
    assert login_throttle.blocked_for(EXTERNAL_EMAIL, OTHER_IP) > 0

    response = client.post(action, data=csrf)
    assert response.status_code == 303
    assert response.headers["location"] == "/organizacoes"
    assert f'action="{action}"' not in client.get("/organizacoes").text
    assert login_throttle.blocked_for(EXTERNAL_EMAIL, OTHER_IP) == 0
    with sessions() as db:
        event = db.query(AuditLog).filter_by(event_type="auth.login.unlock").one()
        assert event.actor_email == SUPER_ADMIN_EMAIL
        assert json.loads(event.metadata_json)["target_user_id"] == 3
