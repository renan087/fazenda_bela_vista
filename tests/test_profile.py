"""Testes da página Meu Perfil remodelada (abas, drill-down de segurança, navegadores, notificações e preferências)."""

from __future__ import annotations

import re
from datetime import timedelta

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
from app.core.security import get_password_hash, hash_browser_fingerprint, hash_persistent_token
from app.core.timezone import utc_now
from app.db.base import Base
from app.db.session import get_db
from app.models import Organization, TrustedBrowserToken, User
from app.services.user_preferences import get_user_preferences, parse_user_agent_label
from app.web import routes

EMAIL = "produtor@fazenda.example.test"


class LocalTestClient(TestClient):
    send = httpx.Client.send


@pytest.fixture
def env():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine)
    with sessions() as db:
        db.add(Organization(id=1, name="Fazenda Serra do Vale", slug="fazenda-serra-do-vale"))
        db.add(
            User(
                id=1,
                organization_id=1,
                email=EMAIL,
                name="Produtor Teste",
                display_name="Produtor",
                hashed_password=get_password_hash("SenhaForte123"),
                is_active=True,
                is_admin=True,
                is_two_factor_enabled=True,
            )
        )
        db.commit()
    yield sessions
    engine.dispose()


@pytest.fixture
def client(env):
    def db_dependency():
        with env() as db:
            yield db

    app = FastAPI()
    app.add_middleware(SessionMiddleware, secret_key="test-profile-secret")
    app.dependency_overrides[get_db] = db_dependency
    app.include_router(routes.router)
    app.mount("/static", StaticFiles(directory="app/static"), name="static")

    @app.post("/test-login")
    def set_test_session(request: Request):
        request.session["user_email"] = EMAIL
        token = ensure_csrf_token(request)
        return {"csrf_token": token}

    with LocalTestClient(app) as test_client:
        test_client.post("/test-login")
        yield test_client


def _csrf_from_html(html: str) -> str:
    m = re.search(r'data-csrf="([^"]+)"', html)
    if m:
        return m.group(1)
    m = re.search(r'name="csrf_token"\s+value="([^"]+)"', html)
    assert m, "CSRF token não encontrado no HTML"
    return m.group(1)


def test_profile_tabs_and_removed_fields(client):
    resp = client.get("/meu-perfil")
    assert resp.status_code == 200
    html = resp.text
    assert "Dados pessoais" in html
    assert "Como prefere ser chamado" in html
    assert 'name="gender"' not in html
    assert 'name="birth_date"' not in html
    assert 'name="notes"' not in html
    assert "Editar foto" in html

    # Aba Segurança (visão principal drill-down + Autenticador Google Em breve)
    sec = client.get("/meu-perfil?aba=seguranca")
    assert sec.status_code == 200
    assert "Conta protegida" in sec.text
    assert "Verificação em duas etapas" in sec.text
    assert "Autenticador Google" in sec.text
    assert "Em breve" in sec.text
    assert "Navegadores confiáveis" in sec.text

    # Sub-telas drill-down
    assert "← Voltar para Segurança" in client.get("/meu-perfil?aba=seguranca&secao=senha").text
    assert "← Voltar para Segurança" in client.get("/meu-perfil?aba=seguranca&secao=2fa").text
    assert "← Voltar para Segurança" in client.get("/meu-perfil?aba=seguranca&secao=navegadores").text

    # Abas Notificações e Preferências
    notif = client.get("/meu-perfil?aba=notificacoes")
    assert notif.status_code == 200
    assert "Horário silencioso" in notif.text
    assert "Estoque abaixo do mínimo" in notif.text

    pref = client.get("/meu-perfil?aba=preferencias")
    assert pref.status_code == 200
    assert "Densidade das telas" in pref.text
    assert "Assistente no WhatsApp" in pref.text


def test_profile_update_and_email_change(client, env):
    csrf = _csrf_from_html(client.get("/meu-perfil").text)

    resp = client.post(
        "/meu-perfil",
        data={
            "csrf_token": csrf,
            "name": "Pandora Correia",
            "display_name": "Pandora",
            "job_title": "Gerente agrícola",
            "phone": "(77) 98117-7667",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    with env() as db:
        user = db.get(User, 1)
        assert user.name == "Pandora Correia"
        assert user.display_name == "Pandora"
        assert user.job_title == "Gerente agrícola"
        assert user.phone == "(77) 98117-7667"

    resp_email = client.post(
        "/meu-perfil/email",
        data={"csrf_token": csrf, "email": "pandora@fazenda.example.test"},
        follow_redirects=False,
    )
    assert resp_email.status_code == 303
    with env() as db:
        user = db.get(User, 1)
        assert user.email == "pandora@fazenda.example.test"


def test_profile_trusted_browsers_revoke_single_and_others(client, env):
    now = utc_now()
    with env() as db:
        t1 = TrustedBrowserToken(
            user_id=1,
            token_hash=hash_persistent_token("tok-current"),
            user_agent_hash=hash_browser_fingerprint("Mozilla/5.0 Macintosh Chrome/120.0"),
            device_label="Chrome · macOS",
            expires_at=now + timedelta(days=30),
            last_used_at=now,
        )
        t2 = TrustedBrowserToken(
            user_id=1,
            token_hash=hash_persistent_token("tok-iphone"),
            user_agent_hash=hash_browser_fingerprint("iPhone Safari"),
            device_label="Safari · iPhone",
            expires_at=now + timedelta(days=30),
            last_used_at=now - timedelta(hours=3),
        )
        t3 = TrustedBrowserToken(
            user_id=1,
            token_hash=hash_persistent_token("tok-edge"),
            user_agent_hash=hash_browser_fingerprint("Windows Edge"),
            device_label="Edge · Windows",
            expires_at=now + timedelta(days=30),
            last_used_at=now - timedelta(days=2),
        )
        db.add_all([t1, t2, t3])
        db.commit()
        t1_id, t2_id, t3_id = t1.id, t2.id, t3.id

    page = client.get("/meu-perfil?aba=seguranca&secao=navegadores")
    assert page.status_code == 200
    assert "Chrome · macOS" in page.text
    assert "Safari · iPhone" in page.text
    assert "Edge · Windows" in page.text
    csrf = _csrf_from_html(page.text)

    r_single = client.post(
        f"/meu-perfil/seguranca/navegadores/{t2_id}/remover",
        data={"csrf_token": csrf},
        follow_redirects=False,
    )
    assert r_single.status_code == 303
    with env() as db:
        assert db.get(TrustedBrowserToken, t2_id).revoked_at is not None

    client.cookies.set(get_settings().trusted_browser_cookie_name, "tok-current")
    r_others = client.post(
        "/meu-perfil/seguranca/navegadores/revogar-outros",
        data={"csrf_token": csrf},
        follow_redirects=False,
    )
    assert r_others.status_code == 303
    with env() as db:
        assert db.get(TrustedBrowserToken, t1_id).revoked_at is None
        assert db.get(TrustedBrowserToken, t3_id).revoked_at is not None


def test_profile_notifications_preferences_and_google_toggle(client, env):
    csrf = _csrf_from_html(client.get("/meu-perfil?aba=notificacoes").text)

    r_notif = client.post(
        "/meu-perfil/notificacoes",
        json={
            "csrf_token": csrf,
            "notifications": {"stock": {"email": False, "whatsapp": True}},
            "quiet_hours": {"enabled": False, "start": "22:00", "end": "06:00"},
        },
    )
    assert r_notif.status_code == 200
    assert r_notif.json()["ok"] is True

    r_pref = client.post(
        "/meu-perfil/preferencias",
        json={
            "csrf_token": csrf,
            "start_page": "/producao",
            "context_mode": "last",
            "density": "compact",
        },
    )
    assert r_pref.status_code == 200
    assert r_pref.json()["preferences"]["density"] == "compact"

    r_google = client.post(
        "/meu-perfil/seguranca/google",
        data={"csrf_token": csrf, "enabled": "0"},
        follow_redirects=False,
    )
    assert r_google.status_code == 303

    with env() as db:
        user = db.get(User, 1)
        prefs = get_user_preferences(user)
        assert prefs["notifications"]["stock"]["email"] is False
        assert prefs["notifications"]["stock"]["whatsapp"] is True
        assert prefs["quiet_hours"] == {"enabled": False, "start": "22:00", "end": "06:00"}
        assert prefs["start_page"] == "/producao"
        assert prefs["density"] == "compact"
        assert prefs["google_login_enabled"] is False

    assert parse_user_agent_label("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) AppleWebKit Safari/604.1") == "Safari · iPhone"
