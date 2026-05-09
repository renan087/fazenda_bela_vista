import asyncio
import logging
import time
import traceback
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Request
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware

from app.core.admin_access import is_super_admin_email
from app.core.config import get_settings
from app.db.init_db import create_tables
from app.db.init_db import seed_admin
from app.db.init_db import seed_demo_data
from app.db.session import SessionLocal
from app.models import User
from app.routers.asaas_webhook import router as asaas_webhook_router
from app.routers.api import router as api_router
from app.routers.auth import api_router as auth_api_router
from app.routers.auth import router as auth_router
from app.services.audit_log_service import append_audit_event
from app.services.backup_service import run_backup_automation_loop
from app.services.runtime_memory_monitor import get_current_rss_mb, run_runtime_memory_monitor
from app.web.routes import router as web_router

logger = logging.getLogger(__name__)
req_mem_logger = logging.getLogger("uvicorn.error")
settings = get_settings()


def _session_value(request: Request, key: str):
    try:
        if "session" not in request.scope:
            return None
        return request.session.get(key)
    except (AssertionError, RuntimeError, KeyError):
        return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_tables()
    with SessionLocal() as db:
        seed_admin(db)
        seed_demo_data(db)
    background_tasks: list[asyncio.Task] = [asyncio.create_task(run_backup_automation_loop())]
    if settings.memory_monitor_enabled:
        logger.info(
            "MEM_MONITOR enable requested interval_seconds=%s",
            settings.memory_monitor_interval_seconds,
        )
        background_tasks.append(
            asyncio.create_task(
                run_runtime_memory_monitor(settings.memory_monitor_interval_seconds)
            )
        )
    else:
        logger.info("MEM_MONITOR disabled by configuration")
    try:
        yield
    finally:
        for task in background_tasks:
            task.cancel()
        for task in background_tasks:
            with suppress(asyncio.CancelledError):
                await task


app = FastAPI(
    title=settings.app_name,
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


def _docs_access_allowed(request: Request) -> bool:
    email = _session_value(request, "user_email")
    if not email:
        return False
    with SessionLocal() as db:
        user = db.query(User).filter(User.email == email, User.is_active.is_(True)).first()
        if not user or not is_super_admin_email(user.email):
            return False
        return bool(user.organization and user.organization.slug == "sisfarm")


def _docs_redirect(request: Request) -> RedirectResponse:
    target = "/dashboard" if _session_value(request, "user_email") else "/login"
    return RedirectResponse(url=target, status_code=303)


def _custom_openapi_schema() -> dict:
    if app.openapi_schema:
        return app.openapi_schema
    app.openapi_schema = get_openapi(
        title=settings.app_name,
        version="1.0.0",
        routes=app.routes,
    )
    return app.openapi_schema


@app.middleware("http")
async def request_memory_diagnostics(request: Request, call_next):
    path = request.url.path or ""
    should_trace = not (path == "/health" or path.startswith("/static/"))
    rss_before = get_current_rss_mb() if should_trace else None
    started = time.perf_counter()
    status_code = 500
    response = None
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        if should_trace:
            duration_ms = (time.perf_counter() - started) * 1000.0
            rss_after = get_current_rss_mb()
            if rss_before is not None and rss_after is not None:
                delta_mb = rss_after - rss_before
                if abs(delta_mb) >= 0.5 or rss_after >= 280.0 or rss_before >= 280.0:
                    req_mem_logger.info(
                        (
                            "REQ_MEM method=%s path=%s status=%s duration_ms=%.1f "
                            "rss_before_mb=%.2f rss_after_mb=%.2f delta_mb=%.2f"
                        ),
                        request.method,
                        path,
                        status_code,
                        duration_ms,
                        rss_before,
                        rss_after,
                        delta_mb,
                    )


class AuditAuthenticatedHttpMiddleware(BaseHTTPMiddleware):
    """Deve ficar *depois* de SessionMiddleware no stack para existir request.session."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path or ""
        if path.startswith("/static/") or path == "/health" or path.startswith("/favicon"):
            return await call_next(request)
        started = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration_ms = (time.perf_counter() - started) * 1000.0
            try:
                try:
                    email = request.session.get("user_email")
                except AssertionError:
                    email = None
                # Nunca use "return" aqui: em finally isso substitui o return do try e devolve None ao Starlette.
                if email:
                    with SessionLocal() as db:
                        actor = db.query(User).filter(User.email == email, User.is_active.is_(True)).first()
                        if actor:
                            append_audit_event(
                                event_type="http.request",
                                outcome="success" if status_code < 400 else "failure",
                                request=request,
                                actor_user_id=actor.id,
                                actor_email=actor.email,
                                organization_id=actor.organization_id,
                                status_code=status_code,
                                duration_ms=round(duration_ms, 2),
                            )
            except Exception:
                logger.exception("Falha no middleware de auditoria HTTP")


@app.get("/health")
def health():
    """Resposta mínima para health check (evite usar /login no Render)."""
    return PlainTextResponse("ok", status_code=200)


@app.get("/docs", include_in_schema=False)
def api_docs(request: Request):
    if not _docs_access_allowed(request):
        return _docs_redirect(request)
    return get_swagger_ui_html(openapi_url="/openapi.json", title=f"{settings.app_name} - API Docs")


@app.get("/redoc", include_in_schema=False)
def api_redoc(request: Request):
    if not _docs_access_allowed(request):
        return _docs_redirect(request)
    return get_redoc_html(openapi_url="/openapi.json", title=f"{settings.app_name} - ReDoc")


@app.get("/openapi.json", include_in_schema=False)
def openapi_json(request: Request):
    if not _docs_access_allowed(request):
        return JSONResponse({"detail": "Acesso negado."}, status_code=403)
    return JSONResponse(_custom_openapi_schema())


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    if isinstance(exc, StarletteHTTPException):
        raise exc
    tb = traceback.format_exc()
    logger.error(
        "ERRO 500 — %s %s\n%s",
        request.method,
        request.url.path,
        tb,
    )
    return HTMLResponse(content="Internal Server Error", status_code=500)


app.add_middleware(
    SessionMiddleware,
    secret_key=settings.secret_key,
    session_cookie=settings.session_cookie_name,
    max_age=settings.session_idle_timeout_seconds,
    same_site="lax",
    https_only=settings.is_production,
)
app.add_middleware(AuditAuthenticatedHttpMiddleware)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(auth_router)
app.include_router(asaas_webhook_router)
app.include_router(web_router)
app.include_router(auth_api_router)
app.include_router(api_router)
