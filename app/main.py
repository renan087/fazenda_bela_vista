import asyncio
import logging
import traceback
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.sessions import SessionMiddleware

from app.core.config import get_settings
from app.db.init_db import create_tables
from app.db.init_db import seed_admin
from app.db.init_db import seed_demo_data
from app.db.session import SessionLocal
from app.routers.asaas_webhook import router as asaas_webhook_router
from app.routers.api import router as api_router
from app.routers.auth import api_router as auth_api_router
from app.routers.auth import router as auth_router
from app.services.backup_service import run_backup_automation_loop
from app.web.routes import router as web_router

logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_tables()
    with SessionLocal() as db:
        seed_admin(db)
        seed_demo_data(db)
    backup_task = asyncio.create_task(run_backup_automation_loop())
    try:
        yield
    finally:
        backup_task.cancel()
        with suppress(asyncio.CancelledError):
            await backup_task


app = FastAPI(title=settings.app_name, lifespan=lifespan)


@app.get("/health")
def health():
    """Resposta mínima para health check (evite usar /login no Render)."""
    return PlainTextResponse("ok", status_code=200)


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
app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(auth_router)
app.include_router(asaas_webhook_router)
app.include_router(web_router)
app.include_router(auth_api_router)
app.include_router(api_router)
