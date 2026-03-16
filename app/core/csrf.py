import secrets

from fastapi import HTTPException, Request, status


CSRF_SESSION_KEY = "csrf_token"


def ensure_csrf_token(request: Request) -> str:
    token = request.session.get(CSRF_SESSION_KEY)
    if not token:
        token = secrets.token_urlsafe(32)
        request.session[CSRF_SESSION_KEY] = token
    return token


def validate_csrf(request: Request, token: str | None) -> None:
    session_token = request.session.get(CSRF_SESSION_KEY)
    if not session_token or not token or session_token != token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="CSRF token invalido")
