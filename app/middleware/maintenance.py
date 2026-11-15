"""Bloqueio do modo manutencao. A equipe SiSFarm (super admin) sempre passa, para testar antes de liberar."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.core.admin_access import is_super_admin_email
from app.core.timezone import app_now
from app.db.session import SessionLocal
from app.services.maintenance import READ_ONLY_WRITE_ALLOWED, current_state, is_open_path

SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
_templates = Jinja2Templates(directory="app/templates")


def _session_email(request: Request) -> str | None:
    try:
        return request.session.get("user_email") if "session" in request.scope else None
    except (AssertionError, RuntimeError):
        return None


class MaintenanceMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path or "/"
        if path.startswith(("/static/", "/health")):
            return await call_next(request)
        state = current_state(SessionLocal)
        if not state.active or is_super_admin_email(_session_email(request)):
            return await call_next(request)
        if state.mode == "leitura":
            if request.method in SAFE_METHODS or path.startswith(READ_ONLY_WRITE_ALLOWED):
                return await call_next(request)
        elif is_open_path(path):
            return await call_next(request)
        now = datetime.now(timezone.utc)
        headers = {"Retry-After": str(state.retry_after_seconds(now)), "Cache-Control": "no-store"}
        if path.startswith("/api/"):
            return JSONResponse({"detail": "Sistema em manutenção. Tente novamente em instantes.", "maintenance": state.mode},
                                status_code=503, headers=headers)
        ends_local = state.ends_at.astimezone(app_now().tzinfo) if state.ends_at else None
        return _templates.TemplateResponse(request, "maintenance.html", {"state": state, "ends_local": ends_local,
                                                                         "logged_in": bool(_session_email(request))},
                                           status_code=503, headers=headers)
