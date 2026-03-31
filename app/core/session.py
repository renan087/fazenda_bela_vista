import time

from fastapi import Request

from app.core.config import get_settings

SESSION_LAST_ACTIVITY_KEY = "last_activity_at"
SESSION_AUTH_KEYS = ("user_email", "pending_2fa_user_id")

settings = get_settings()


def has_managed_session(request: Request) -> bool:
    return any(request.session.get(key) for key in SESSION_AUTH_KEYS)


def clear_expired_session(request: Request) -> bool:
    if not has_managed_session(request):
        return False

    now = int(time.time())
    last_activity_at = request.session.get(SESSION_LAST_ACTIVITY_KEY)
    if last_activity_at is None:
        request.session[SESSION_LAST_ACTIVITY_KEY] = now
        return False

    try:
        last_activity_at = int(last_activity_at)
    except (TypeError, ValueError):
        request.session.clear()
        return True

    if last_activity_at > now:
        request.session[SESSION_LAST_ACTIVITY_KEY] = now
        return False

    if now - last_activity_at < settings.session_idle_timeout_seconds:
        return False

    request.session.clear()
    return True


def touch_session_activity(request: Request) -> None:
    if has_managed_session(request):
        request.session[SESSION_LAST_ACTIVITY_KEY] = int(time.time())
