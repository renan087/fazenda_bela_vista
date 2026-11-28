import base64
from datetime import timedelta
from urllib.parse import parse_qs, urlsplit

import httpx
import pyotp
import pytest
from cryptography.fernet import Fernet
from fastapi import Depends, FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.middleware.sessions import SessionMiddleware

from app.core.config import get_settings
from app.core.csrf import ensure_csrf_token
from app.core.deps import get_current_user_api, get_current_user_web
from app.core.security import create_access_token, get_password_hash, verify_password
from app.core.timezone import utc_now
from app.db.base import Base
from app.db.session import get_db
from app.models import AuditLog, AuthenticatorChallenge, AuthenticatorRecoveryCode, TrustedBrowserToken, User
from app.routers import auth, authenticator
from app.services import audit_log_service, trusted_browser
from app.services import authenticator as mfa
from app.services.password_reset import issue_password_reset_token

EMAIL = "marina@example.test"
PASSWORD = "Senha-Totp-Teste-42"
PROFILE = "/meu-perfil/seguranca/autenticador"
VERIFY = "/login/verificacao"
JSON = {"Accept": "application/json"}


class LocalTestClient(TestClient):
    send = httpx.Client.send


@pytest.fixture
def env(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'authenticator.sqlite3'}")
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine, autoflush=False)
    now = [utc_now()]
    settings = get_settings()
    monkeypatch.setattr(settings, "totp_encryption_key", Fernet.generate_key().decode("ascii"))
    monkeypatch.setattr(settings, "two_factor_max_attempts", 5)
    monkeypatch.setattr(mfa, "utc_now", lambda: now[0])
    monkeypatch.setattr(auth, "utc_now", lambda: now[0])
    monkeypatch.setattr(audit_log_service, "SessionLocal", sessions)
    sent = []
    monkeypatch.setattr(authenticator, "send_access_code_email", lambda email, code: sent.append((email, code)))
    monkeypatch.setattr(auth, "send_access_code_email", lambda email, code: sent.append((email, code)))
    with sessions() as db:
        db.add(User(id=1, email=EMAIL, name="Marina", is_two_factor_enabled=False, hashed_password=get_password_hash(PASSWORD)))
        db.commit()

    app = FastAPI()
    app.add_middleware(SessionMiddleware, secret_key="test-session-signing-key")
    app.include_router(auth.router)
    app.include_router(auth.api_router)
    app.include_router(authenticator.router)
    app.mount("/static", StaticFiles(directory="app/static"), name="static")

    def db_dependency():
        with sessions() as db:
            yield db

    app.dependency_overrides[get_db] = db_dependency

    @app.get("/test/csrf")
    def csrf(request: Request):
        return {"csrf_token": ensure_csrf_token(request)}

    @app.get("/test/web")
    def protected_web(user: User = Depends(get_current_user_web)):
        return {"account": user.id}

    @app.get("/test/api")
    def protected_api(user: User = Depends(get_current_user_api)):
        return {"account": user.id}

    @app.get("/test/trust")
    def trusted_token(request: Request, user: User = Depends(get_current_user_web), db=Depends(get_db)):
        return {"token": trusted_browser.issue_trusted_browser_token(db, user, request)}

    try:
        yield app, sessions, now, sent
    finally:
        engine.dispose()


def csrf(client) -> str:
    return client.get("/test/csrf").json()["csrf_token"]


def post(client, path, **fields):
    return client.post(path, data={"csrf_token": csrf(client), **fields}, headers=JSON, follow_redirects=False)


def password_login(client, password=PASSWORD):
    return post(client, "/login", email=EMAIL, password=password)


def current_code(secret, now):
    return pyotp.TOTP(secret).at(now[0])


def enroll(client, now):
    setup = post(client, PROFILE + "/vincular/iniciar", current_password=PASSWORD)
    assert setup.status_code == 200, setup.text
    secret = setup.json()["manual_key"]
    result = post(client, PROFILE + "/vincular/confirmar", code=current_code(secret, now))
    assert result.status_code == 200, result.text
    return secret, result.json()["recovery_codes"]


def test_activation_encryption_reauthentication_and_regeneration(env):
    app, sessions, now, _ = env
    with LocalTestClient(app) as client, LocalTestClient(app) as older_session:
        assert password_login(client).headers["location"] == "/dashboard"
        assert password_login(older_session).headers["location"] == "/dashboard"
        trusted_cookie = older_session.get("/test/trust").json()["token"]
        old_api_token = create_access_token(EMAIL)
        assert client.post(PROFILE + "/vincular/iniciar", data={"current_password": PASSWORD}).status_code == 422
        assert post(client, PROFILE + "/vincular/iniciar", current_password="incorrect").json()["detail"]["code"] == "invalid_credentials"

        start = post(client, PROFILE + "/vincular/iniciar", current_password=PASSWORD)
        body = start.json()
        assert start.headers["cache-control"] == "no-store"
        secret = body["manual_key"]
        assert base64.b64decode(body["qr_code_data_url"].split(",")[1]).startswith(b"\x89PNG")
        assert (body["digits"], body["period"], body["algorithm"]) == (6, 30, "SHA1")
        with sessions() as db:
            pending = db.get(AuthenticatorChallenge, (1, mfa.ENROLLMENT_CHALLENGE))
            assert pending.pending_secret_encrypted != secret
            assert Fernet(get_settings().totp_encryption_key.encode()).decrypt(pending.pending_secret_encrypted.encode()).decode() == secret
            assert db.get(User, 1).totp_secret_encrypted is None

        wrong = post(client, PROFILE + "/vincular/confirmar", code="abc123")
        assert wrong.json()["detail"]["code"] == "invalid_code"
        assert older_session.get(PROFILE).json()["enabled"] is False

        initial_code = current_code(secret, now)
        response = post(client, PROFILE + "/vincular/confirmar", code=initial_code)
        assert response.status_code == 200, response.text
        initial_codes = response.json()["recovery_codes"]
        assert len(set(initial_codes)) == 10
        assert response.json()["preferred_method"] == "totp"
        assert client.get(PROFILE).json()["recovery_codes_remaining"] == 10
        assert "recovery_codes" not in client.get(PROFILE).json()
        assert secret not in client.get(PROFILE).text
        assert older_session.get("/test/web", follow_redirects=False).status_code == 303

        with sessions() as db:
            hashes = list(db.scalars(select(AuthenticatorRecoveryCode.code_hash)))
            assert len(hashes) == 10 and all(len(value) == 64 for value in hashes)
            assert not set(hashes) & set(initial_codes)
            assert db.scalar(select(TrustedBrowserToken)).revoked_at is not None

        with LocalTestClient(app) as login_client:
            login_client.cookies.set(get_settings().trusted_browser_cookie_name, trusted_cookie)
            assert password_login(login_client).headers["location"] == VERIFY
            assert login_client.get("/test/web", follow_redirects=False).status_code == 303
            assert login_client.post("/api/v1/auth/token", data={"username": EMAIL, "password": PASSWORD}).status_code == 403
            assert login_client.get("/test/api", headers={"Authorization": f"Bearer {old_api_token}"}).status_code == 403
            assert post(login_client, VERIFY, code=initial_code).json()["detail"]["code"] == "reused_code"

        credentials = {"current_password": PASSWORD, "action": "regenerate_recovery_codes", "code": initial_code}
        assert post(client, PROFILE + "/gerenciar", **credentials).json()["detail"]["code"] == "reused_code"
        now[0] += timedelta(seconds=30)
        credentials["code"] = current_code(secret, now)
        rotated = post(client, PROFILE + "/gerenciar", **credentials)
        assert rotated.status_code == 200, rotated.text
        assert len(rotated.json()["recovery_codes"]) == 10
        assert not set(rotated.json()["recovery_codes"]) & set(initial_codes)
        credentials.update(action="disable", method="recovery", code=initial_codes[0])
        assert post(client, PROFILE + "/gerenciar", **credentials).status_code == 400
        credentials["code"] = rotated.json()["recovery_codes"][0]
        disabled = post(client, PROFILE + "/gerenciar", **credentials)
        assert disabled.status_code == 200, disabled.text
        assert disabled.json()["enabled"] is False
        assert disabled.json()["recovery_codes_remaining"] == 0
        assert client.get("/test/api", headers={"Authorization": f"Bearer {old_api_token}"}).status_code == 401

    with sessions() as db:
        rows = db.scalars(select(AuditLog).where(AuditLog.event_type.like("auth.totp.%"))).all()
        events = {(row.event_type, row.outcome) for row in rows}
        assert ("auth.totp.enabled", "success") in events
        assert ("auth.totp.recovery_regenerated", "success") in events
        assert ("auth.totp.disabled", "success") in events
        assert ("auth.totp.login.verified", "failure") in events
        serialized = " ".join(row.metadata_json or "" for row in rows)
        assert all(value not in serialized for value in (PASSWORD, secret, initial_code, *initial_codes))


def test_email_enrollment_and_email_first_login_never_releases_session_early(env):
    app, sessions, now, sent = env
    with LocalTestClient(app) as client:
        assert password_login(client).headers["location"] == "/dashboard"
        with sessions() as db:
            db.get(User, 1).is_two_factor_enabled = True
            db.commit()

        start = post(client, PROFILE + "/vincular/iniciar", current_password=PASSWORD)
        assert start.json()["stage"] == "email"
        assert "manual_key" not in start.json()
        enrollment_email_code = sent[-1][1]
        assert post(client, PROFILE + "/vincular/confirmar", code=enrollment_email_code).json()["detail"]["code"] == "wrong_stage"
        payload = post(client, PROFILE + "/vincular/email", code=enrollment_email_code).json()
        secret = payload["manual_key"]
        assert post(client, PROFILE + "/vincular/email", code=enrollment_email_code).status_code == 409
        assert post(client, PROFILE + "/vincular/confirmar", code=current_code(secret, now)).status_code == 200

        now[0] += timedelta(seconds=30)
        preference = post(client, PROFILE + "/gerenciar", current_password=PASSWORD, action="preference",
                          preferred_method="email_then_totp", code=current_code(secret, now))
        assert preference.status_code == 200, preference.text
        assert preference.json()["preferred_method"] == "email_then_totp"
        assert client.get("/logout", follow_redirects=False).status_code == 303

        assert password_login(client).headers["location"] == VERIFY
        state = client.get(VERIFY + "/estado").json()
        assert state["methods"] == ["email"] and state["authenticator_required"] is True
        login_email_code = sent[-1][1]
        assert post(client, VERIFY, method="totp", code="123456").status_code == 409
        cooldown = post(client, VERIFY + "/reenviar")
        assert cooldown.status_code == 429
        assert int(cooldown.headers["retry-after"]) == 60
        email_ok = post(client, VERIFY, method="email", code=login_email_code)
        assert email_ok.json()["stage"] == "totp"
        assert client.get("/test/web", follow_redirects=False).status_code == 303
        assert post(client, VERIFY + "/reenviar").status_code == 409
        assert post(client, VERIFY, method="email", code=login_email_code).status_code == 409
        now[0] += timedelta(seconds=30)
        result = post(client, VERIFY, method="totp", code=current_code(secret, now), trust_browser="on")
        assert result.json() == {"stage": "complete", "redirect_url": "/dashboard"}
        assert get_settings().trusted_browser_cookie_name not in client.cookies
        assert client.get("/test/web").json()["account"] == 1


def test_recovery_use_is_shared_between_sessions_and_attempts_are_persistent(env):
    app, _, now, _ = env
    with LocalTestClient(app) as client, LocalTestClient(app) as second:
        password_login(client)
        _, codes = enroll(client, now)
        client.get("/logout", follow_redirects=False)
        assert password_login(client).headers["location"] == VERIFY
        assert post(client, VERIFY, method="recovery", code=codes[0].lower()).json()["stage"] == "complete"
        assert client.get(PROFILE).json()["recovery_codes_remaining"] == 9

        assert password_login(second).headers["location"] == VERIFY
        for _ in range(4):
            assert post(second, VERIFY, method="recovery", code=codes[0]).status_code == 400
        assert password_login(second).headers["location"] == VERIFY
        blocked = post(second, VERIFY, method="recovery", code=codes[0])
        assert blocked.status_code == 429
        assert blocked.json()["detail"]["code"] == "rate_limited"
        assert int(blocked.headers["retry-after"]) == 300
        assert post(second, VERIFY, method="recovery", code=codes[1]).status_code == 429
        with LocalTestClient(app) as third:
            assert password_login(third).status_code == 429

        now[0] += timedelta(minutes=5)
        assert post(second, VERIFY, method="recovery", code=codes[1]).json()["stage"] == "complete"
        assert second.get(PROFILE).json()["recovery_codes_remaining"] == 8


def test_password_reset_and_google_login_still_require_authenticator(env, monkeypatch):
    app, sessions, now, sent = env
    with LocalTestClient(app) as owner, LocalTestClient(app) as client:
        password_login(owner)
        secret, _ = enroll(owner, now)
        assert password_login(client).headers["location"] == VERIFY
        with sessions() as db:
            token = issue_password_reset_token(db, db.get(User, 1))

        new_password = "Nova-Senha-TOTP-47"
        with LocalTestClient(app) as reset_client:
            response = post(reset_client, "/login/redefinir-senha", token=token, new_password=new_password, confirm_password=new_password)
            assert response.headers["location"] == "/login?notice=password-reset-success"
            assert reset_client.get("/test/web", follow_redirects=False).status_code == 303
        with sessions() as db:
            user = db.get(User, 1)
            assert verify_password(new_password, user.hashed_password)
            assert user.totp_secret_encrypted
        assert owner.get("/test/web", follow_redirects=False).status_code == 303
        assert client.get(VERIFY + "/estado").status_code == 409
        assert password_login(client, new_password).headers["location"] == VERIFY
        assert client.get(VERIFY + "/estado").json()["stage"] == "totp"
        client.get("/logout", follow_redirects=False)

        monkeypatch.setattr(auth, "_google_login_enabled", lambda: True)
        monkeypatch.setattr(auth, "_exchange_google_code", lambda code: {"id_token": "simulado"})
        monkeypatch.setattr(auth, "_validate_google_id_token", lambda *args, **kwargs: {"email": EMAIL})
        authorize = client.get("/auth/google", follow_redirects=False)
        state = parse_qs(urlsplit(authorize.headers["location"]).query)["state"][0]
        callback = client.get("/auth/google/callback", params={"code": "simulado", "state": state}, follow_redirects=False)
        assert callback.headers["location"] == VERIFY
        assert not sent
        assert client.get("/test/web", follow_redirects=False).status_code == 303
        now[0] += timedelta(seconds=30)
        assert post(client, VERIFY, method="totp", code=current_code(secret, now)).json()["stage"] == "complete"


def test_enrollment_and_login_expire_and_missing_key_preserves_legacy_access(env, monkeypatch):
    app, _, now, _ = env
    with LocalTestClient(app) as client:
        assert password_login(client).headers["location"] == "/dashboard"
        with monkeypatch.context() as missing_key:
            missing_key.setattr(get_settings(), "totp_encryption_key", None)
            assert client.get(PROFILE).json()["enrollment_available"] is False
            assert post(client, PROFILE + "/vincular/iniciar", current_password=PASSWORD).status_code == 503

        setup = post(client, PROFILE + "/vincular/iniciar", current_password=PASSWORD).json()
        ttl = timedelta(minutes=get_settings().two_factor_code_minutes)
        now[0] += ttl
        failed = post(client, PROFILE + "/vincular/confirmar", code=current_code(setup["manual_key"], now))
        assert failed.json()["detail"]["code"] == "challenge_expired"
        secret, codes = enroll(client, now)
        client.get("/logout", follow_redirects=False)
        assert password_login(client).headers["location"] == VERIFY
        now[0] += ttl
        assert post(client, VERIFY, method="recovery", code=codes[0]).status_code == 409
        assert client.get("/test/web", follow_redirects=False).status_code == 303

        assert password_login(client).headers["location"] == VERIFY
        monkeypatch.setattr(get_settings(), "totp_encryption_key", Fernet.generate_key().decode("ascii"))
        denied = post(client, VERIFY, method="totp", code=current_code(secret, now))
        assert denied.status_code == 503
        assert post(client, VERIFY, method="recovery", code=codes[0]).json()["stage"] == "complete"


def test_legacy_email_login_and_trusted_browser_remain_available(env, monkeypatch):
    app, sessions, _, sent = env
    monkeypatch.setattr(get_settings(), "totp_encryption_key", None)
    with sessions() as db:
        db.get(User, 1).is_two_factor_enabled = True
        db.commit()
    with LocalTestClient(app) as client:
        assert password_login(client).headers["location"] == VERIFY
        state = client.get(VERIFY + "/estado").json()
        assert state["authenticator_required"] is False
        assert state["methods"] == ["email"]
        assert client.get("/test/web", follow_redirects=False).status_code == 303
        confirmed = post(client, VERIFY, method="email", code=sent[-1][1], trust_browser="on")
        assert confirmed.json() == {"stage": "complete", "redirect_url": "/dashboard"}
        assert get_settings().trusted_browser_cookie_name in client.cookies
        client.get("/logout", follow_redirects=False)
        assert password_login(client).headers["location"] == "/dashboard"


def test_suspend_and_resume_totp_keeps_secret_and_recovery_codes(env):
    app, sessions, now, _ = env
    with LocalTestClient(app) as client:
        assert password_login(client).headers["location"] == "/dashboard"
        secret, codes = enroll(client, now)

        # Suspender exige senha e código TOTP válidos
        now[0] += timedelta(seconds=30)
        bad_pw = post(
            client,
            PROFILE + "/gerenciar",
            action="suspend",
            current_password="errada",
            code=current_code(secret, now),
            method="totp",
        )
        assert bad_pw.status_code == 400
        bad_code = post(
            client,
            PROFILE + "/gerenciar",
            action="suspend",
            current_password=PASSWORD,
            code="000000",
            method="totp",
        )
        assert bad_code.status_code == 400

        # Suspensão bem-sucedida mantém segredo e 10 códigos de recuperação
        suspended = post(
            client,
            PROFILE + "/gerenciar",
            action="suspend",
            current_password=PASSWORD,
            code=current_code(secret, now),
            method="totp",
        )
        assert suspended.status_code == 200
        body = suspended.json()
        assert body["enabled"] is True
        assert body["suspended"] is True
        assert body["recovery_codes_remaining"] == 10

        # Login enquanto suspenso não exige TOTP (entra direto pois 2FA por e-mail está desligado na fixture)
        client.get("/logout", follow_redirects=False)
        assert password_login(client).headers["location"] == "/dashboard"

        # Reativar exige senha + próximo código TOTP e volta a exigir no login
        now[0] += timedelta(seconds=30)
        resumed = post(
            client,
            PROFILE + "/gerenciar",
            action="resume",
            current_password=PASSWORD,
            code=current_code(secret, now),
            method="totp",
        )
        assert resumed.status_code == 200
        assert resumed.json()["enabled"] is True
        assert resumed.json()["suspended"] is False
        assert resumed.json()["recovery_codes_remaining"] == 10

        client.get("/logout", follow_redirects=False)
        assert password_login(client).headers["location"] == VERIFY
        state = client.get(VERIFY + "/estado").json()
        assert state["authenticator_required"] is True
        assert state["stage"] == "totp"

        # Mesmo segredo e códigos originais continuam válidos
        now[0] += timedelta(seconds=30)
        done = post(client, VERIFY, method="totp", code=current_code(secret, now))
        assert done.json() == {"stage": "complete", "redirect_url": "/dashboard"}
        with sessions() as db:
            user = db.get(User, 1)
            assert user.totp_secret_encrypted is not None
            assert user.totp_suspended is False
            assert len(codes) == 10
