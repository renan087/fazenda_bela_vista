from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from app.core.config import get_settings
from app.db.init_db import create_tables
from app.db.init_db import seed_admin
from app.db.session import SessionLocal
from app.routers.api import router as api_router
from app.routers.auth import api_router as auth_api_router
from app.routers.auth import router as auth_router
from app.routers.web import router as web_router

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_tables()
    with SessionLocal() as db:
        seed_admin(db)
    yield


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=settings.secret_key)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(auth_router)
app.include_router(web_router)
app.include_router(auth_api_router)
app.include_router(api_router)
