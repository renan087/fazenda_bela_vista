"""Tour de primeiros passos ponta a ponta pelo app real (base.html de verdade, url_for do Starlette)."""
import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from starlette.middleware.sessions import SessionMiddleware

from app.core.csrf import ensure_csrf_token
from app.db.base import Base
from app.db.session import get_db
from app.models import Organization, User
from app.web import routes

EMAIL = "ana@tour.example.test"


class LocalTestClient(TestClient):
    send = httpx.Client.send


@pytest.fixture
def client():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine)
    with sessions() as db:
        db.add(Organization(id=1, name="Sítio Sol", slug="sitio-sol"))
        db.add(User(id=1, organization_id=1, email=EMAIL, name="Ana Souza", hashed_password="x", is_admin=True))
        db.commit()

    def db_dependency():
        with sessions() as db:
            yield db

    app = FastAPI()
    app.add_middleware(SessionMiddleware, secret_key="test-tour")
    app.dependency_overrides[get_db] = db_dependency
    app.include_router(routes.router)
    app.mount("/static", StaticFiles(directory="app/static"), name="static")

    @app.post("/test-session")
    def set_test_session(request: Request):
        request.session.clear()
        request.session["user_email"] = EMAIL
        return {"csrf_token": ensure_csrf_token(request)}

    with LocalTestClient(app, follow_redirects=False) as c:
        yield c
    engine.dispose()


def test_start_tour_and_open_pages_without_error(client):
    csrf = client.post("/test-session").json()
    assert client.get("/primeiros-passos").status_code == 200
    start = client.post("/primeiros-passos/tour", data={**csrf, "action": "start"})
    assert start.status_code == 303 and start.headers["location"] == "/primeiros-passos/continuar"
    nxt = client.get("/primeiros-passos/continuar")
    assert nxt.headers["location"] == "/fazendas"
    for url in ("/fazendas", "/fazendas/nova"):
        page = client.get(url)
        assert page.status_code == 200, url
        assert 'id="sf-tour-data"' in page.text and '"head": "/static/img/pezinho/cabeca-pronto.svg"' in page.text.replace("http://testserver", "")
