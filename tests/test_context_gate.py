"""Fazenda e safra ativas sem janela travando a tela: escolha automática, escolha na página e tour sem contexto."""
from datetime import date, datetime, timedelta, timezone

import json
import re

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
from app.core.user_context import auto_pick_context
from app.db.base import Base
from app.db.session import get_db
from app.models import CropSeason, Farm, Organization, User
from app.web import routes

EMAIL = "ana@contexto.example.test"
TODAY = date(2026, 11, 27)


def tour_data(html):
    match = re.search(r'<script type="application/json" id="sf-tour-data">(.*?)</script>', html, re.S)
    return json.loads(match.group(1)) if match else None


class LocalTestClient(TestClient):
    send = httpx.Client.send


class Env:
    def __init__(self):
        self.engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine)
        with self.sessions() as db:
            db.add(Organization(id=1, name="Sítio Sol", slug="sitio-sol"))
            db.add(User(id=1, organization_id=1, email=EMAIL, name="Ana Souza", hashed_password="x", is_admin=True))
            db.commit()

    def farm(self, name, seasons=()):
        with self.sessions() as db:
            farm = Farm(organization_id=1, name=name, location="Patrocínio, MG", total_area=120.5)
            db.add(farm)
            db.flush()
            for season_name, start, end, status in seasons:
                db.add(CropSeason(farm_id=farm.id, name=season_name, start_date=start, end_date=end, culture="Cafe", cultivated_area=100, status=status))
            db.commit()
            return farm.id

    def org(self, **values):
        with self.sessions() as db:
            org = db.get(Organization, 1)
            for key, value in values.items():
                setattr(org, key, value)
            db.commit()


RUNNING = ("Safra 2026/2027", date(2026, 7, 1), date(2027, 6, 30), "ativa")
OLD = ("Safra 2025/2026", date(2025, 7, 1), date(2026, 6, 30), "encerrada")


@pytest.fixture
def env():
    e = Env()
    yield e
    e.engine.dispose()


@pytest.fixture
def client(env):
    def db_dependency():
        with env.sessions() as db:
            yield db

    app = FastAPI()
    app.add_middleware(SessionMiddleware, secret_key="test-ctx")
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


def test_auto_pick_only_when_there_is_one_answer(env):
    with env.sessions() as db:
        assert auto_pick_context(db, 1, None, None, TODAY) == (None, None, "")
    only = env.farm("Fazenda Água Limpa", [RUNNING, OLD])
    with env.sessions() as db:
        farm_id, season_id, reason = auto_pick_context(db, 1, None, None, TODAY)
        running_id = db.query(CropSeason.id).filter(CropSeason.name == "Safra 2026/2027").scalar()
        old_id = db.query(CropSeason.id).filter(CropSeason.name == "Safra 2025/2026").scalar()
        assert (farm_id, season_id, reason) == (only, running_id, "single")
        # Safra lembrada já terminou e há uma em andamento: troca e avisa.
        assert auto_pick_context(db, 1, only, old_id, TODAY) == (only, running_id, "newer")
    env.farm("Sítio Boa Esperança", [RUNNING])
    with env.sessions() as db:
        assert auto_pick_context(db, 1, None, None, TODAY)[:2] == (None, None)


def test_single_farm_opens_directly_with_notice(env, client):
    env.farm("Fazenda Água Limpa", [RUNNING])
    client.post("/test-session")
    page = client.get("/dashboard")
    assert page.status_code == 200
    assert "Contexto obrigatorio" not in page.text and "data-cg-pick" not in page.text
    assert "a única cadastrada" in page.text


def test_many_farms_ask_inside_the_page(env, client):
    env.farm("Fazenda Água Limpa", [RUNNING])
    env.farm("Sítio Boa Esperança", [RUNNING])
    client.post("/test-session")
    page = client.get("/dashboard")
    assert page.status_code == 200
    assert "Para qual fazenda vamos hoje?" in page.text and page.text.count("data-cg-card ") == 2
    assert "data-cg-search>" not in page.text  # poucas fazendas: sem busca
    assert 'class="cg-ghost' in page.text and "Contexto obrigatorio" not in page.text
    assert "Escolha a fazenda" in page.text  # topo destacado, menu livre


def test_farm_without_season_suggests_and_creates_after_confirm(env, client):
    farm_id = env.farm("Fazenda Água Limpa")
    csrf = client.post("/test-session").json()
    page = client.get("/dashboard")
    assert "ainda não tem safra aberta" in page.text and "Confirmar e abrir" in page.text
    done = client.post("/contexto/safra-sugerida", data={**csrf, "farm_id": farm_id, "redirect_to": "/dashboard"})
    assert done.status_code == 303 and done.headers["location"] == "/dashboard"
    with env.sessions() as db:
        seasons = db.query(CropSeason).filter(CropSeason.farm_id == farm_id).all()
        assert len(seasons) == 1 and seasons[0].start_date.month == 7 and seasons[0].end_date.month == 6
        assert db.get(User, 1).active_season_id == seasons[0].id
    # Repetir não duplica.
    client.post("/contexto/safra-sugerida", data={**csrf, "farm_id": farm_id})
    with env.sessions() as db:
        assert db.query(CropSeason).filter(CropSeason.farm_id == farm_id).count() == 1


def test_suggested_season_rejects_farm_from_other_account(env, client):
    with env.sessions() as db:
        db.add(Organization(id=2, name="Outra", slug="outra"))
        db.add(Farm(id=99, organization_id=2, name="Alheia", location="X", total_area=10))
        db.commit()
    csrf = client.post("/test-session").json()
    client.post("/contexto/safra-sugerida", data={**csrf, "farm_id": 99})
    with env.sessions() as db:
        assert db.query(CropSeason).count() == 0


def test_no_farm_shows_next_step_instead_of_blocking(client):
    client.post("/test-session")
    page = client.get("/dashboard")
    assert "começa depois da fazenda" in page.text and "Começar primeiros passos" in page.text


def test_tour_never_asks_for_context_and_welcomes_back(env, client):
    env.org(onboarding_tour="active", created_at=datetime.now(timezone.utc),
            onboarding_last_step="Fazenda · CAR", onboarding_last_at=datetime.now(timezone.utc) - timedelta(days=1))
    client.post("/test-session")
    page = client.get("/dashboard")
    assert "data-cg-pick" not in page.text and "cg-empty" not in page.text and "cg-ghost" not in page.text
    welcome = tour_data(page.text)["welcome"]
    assert welcome["title"] == "Que bom que você voltou, Ana" and "parou no número do CAR" in welcome["sub"]
    assert welcome["meta"].startswith("Você saiu ontem às") and welcome["meta"].endswith("Parou em Fazenda › CAR") and welcome["next"] == "Retomar pelo número do CAR"
    # Só uma vez por acesso.
    assert tour_data(client.get("/fazendas").text)["welcome"] is None


def test_tour_welcome_follows_saved_progress_and_mark(env, client):
    env.farm("Fazenda Água Limpa")
    env.org(onboarding_tour="paused", created_at=datetime.now(timezone.utc))
    csrf = client.post("/test-session").json()
    welcome = tour_data(client.get("/dashboard").text)["welcome"]
    assert welcome["title"] == "Oi de novo, Ana" and welcome["sub"].startswith("Fazenda Água Limpa já está salva")
    assert welcome["act"] == "Abrir a safra"
    mark = client.post("/primeiros-passos/tour", data={**csrf, "action": "mark", "step": "Safra · Período<script>"}, headers={"x-sf-fetch": "1"})
    assert mark.json() == {"ok": True}
    with env.sessions() as db:
        org = db.get(Organization, 1)
        assert org.onboarding_last_step == "Safra · Períodoscript" and org.onboarding_last_at is not None
        assert org.onboarding_tour == "paused"


def test_many_farms_and_seasons_scale(env, client):
    import json as _json
    first = env.farm("Fazenda 00", [RUNNING, OLD] + [(f"Safra {y}/{y + 1}", date(y, 7, 1), date(y + 1, 6, 30), "encerrada") for y in range(2005, 2024)])
    for i in range(1, 12):
        env.farm(f"Fazenda {i:02d}", [RUNNING])
    with env.sessions() as db:
        farm = db.get(Farm, first)
        farm.boundary_geojson = _json.dumps({"type": "Polygon", "coordinates": [[[-46.9, -18.9], [-46.8, -18.9], [-46.8, -18.95], [-46.9, -18.9]]]})
        db.commit()
    client.post("/test-session")
    page = client.get("/dashboard").text
    assert "data-cg-search>" in page and "Mostrar todas as 12 fazendas" in page
    assert page.count("data-cg-extra hidden") == 4  # mostra 8 de início
    seasons = json.loads(re.search(r"data-id=\"%d\"[^>]*data-seasons='([^']*)'" % first, page).group(1))
    assert len(seasons) == 21 and seasons[0]["current"] and seasons[1]["name"] == "Safra 2025/2026"
    with env.sessions() as db:
        assert routes._boundary_path(db.get(Farm, first).boundary_geojson) in page  # contorno real da fazenda no cartão
