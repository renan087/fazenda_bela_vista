from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session

from app.core.admin_access import is_super_admin_email
from app.core.csrf import ensure_csrf_token
from app.core.session import authenticator_session_matches, authenticator_session_version, clear_expired_session, touch_session_activity
from app.core.user_context import sync_user_context_from_preferences
from app.core.security import decode_token
from app.db.session import get_db
from app.models.user import User
from app.services.farm_plan_guard import bind_farm_plan_actor

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")


def get_current_user_api(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
) -> User:
    payload = decode_token(token)
    email = payload.get("sub") if payload else None
    if not email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalido",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = db.query(User).filter(User.email == email, User.is_active.is_(True)).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario nao encontrado ou inativo",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if user.is_two_factor_enabled or getattr(user, "totp_secret_encrypted", None) or is_super_admin_email(user.email):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Use o acesso web com verificação em duas etapas.")
    if payload.get("mfa_version", 0) != authenticator_session_version(user):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token revogado", headers={"WWW-Authenticate": "Bearer"})
    bind_farm_plan_actor(db, user)
    return user


def get_current_user_web(
    request: Request,
    db: Session = Depends(get_db),
) -> User:
    if clear_expired_session(request):
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, headers={"Location": "/login"})

    email = request.session.get("user_email")
    if not email:
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, headers={"Location": "/login"})

    user = db.query(User).filter(User.email == email, User.is_active.is_(True)).first()
    if not user or not authenticator_session_matches(request, user):
        request.session.clear()
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, headers={"Location": "/login"})
    sync_user_context_from_preferences(request, db, user)
    touch_session_activity(request)
    bind_farm_plan_actor(db, user)
    return user


def get_csrf_token(request: Request) -> str:
    return ensure_csrf_token(request)
