from datetime import date
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request

import app.models  # noqa: F401
from app.db.base import Base
from app.models import CropSeason, Farm, Plot, User
from app.services import self_signup as signup
from app.web import routes

TODAY = date(2026, 11, 15)


def _db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return Session(engine)


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setattr(routes, 'validate_csrf', lambda *a: None)
    monkeypatch.setattr(routes, '_flash', lambda *a: None)
    monkeypatch.setattr(routes, 'today_in_app_timezone', lambda: TODAY)
    monkeypatch.setattr(routes, '_base_context', lambda *args, **kwargs: kwargs)
    monkeypatch.setattr(routes, '_repository', lambda db: routes.FarmRepository(db))
    monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, ctx, status_code=200: SimpleNamespace(name=name, ctx=ctx, status_code=status_code))
    db = _db()
    user = signup.create_self_signup(db, email='ana@x.com', data=signup.validate_signup('Ana', 'Souza', '35999998888', 'Sítio Sol', '1'),
                                     password_hash='h', plan_id=None, today=TODAY)
    return db, user


def _request():
    return Request({'type': 'http', 'method': 'POST', 'path': '/primeiros-passos', 'headers': [], 'query_string': b'', 'session': {}})


def test_coffee_season_window():
    assert routes._coffee_season_window(date(2026, 11, 15)) == (date(2026, 7, 1), date(2027, 6, 30))
    assert routes._coffee_season_window(date(2027, 3, 1)) == (date(2026, 7, 1), date(2027, 6, 30))


def test_onboarding_creates_farm_season_and_activates_context(env):
    db, user = env
    page = routes.onboarding_page(_request(), db=db, user=user, csrf_token='t')
    assert page.name == 'onboarding.html' and page.ctx['form']['season_start'] == '2026-07-01' and page.ctx['first_name'] == 'Ana'
    bad = routes.onboarding_action(_request(), csrf_token='t', farm_name='Fazenda Sol', location='Três Pontas / MG', total_area='0',
                                   season_start='2026-07-01', season_end='2027-06-30', db=db, user=user)
    assert bad.status_code == 400 and 'área' in bad.ctx['error'] and not db.query(Farm).count()
    done = routes.onboarding_action(_request(), csrf_token='t', farm_name='Fazenda Sol', location='Três Pontas / MG', total_area='120,5',
                                    season_start='2026-07-01', season_end='2027-06-30', db=db, user=user)
    farm, season = db.query(Farm).one(), db.query(CropSeason).one()
    assert done.headers['location'] == '/dashboard' and float(farm.total_area) == 120.5 and season.status == 'ativa' and season.name == 'Safra 2026/2027'
    db.refresh(user)
    assert user.active_farm_id == farm.id and user.active_season_id == season.id
    assert routes.onboarding_page(_request(), db=db, user=user, csrf_token='t').headers['location'] == '/dashboard'


def test_existing_farm_only_needs_season(env):
    db, user = env
    db.add(Farm(name='Fazenda Antiga', location='MG', total_area=50, organization_id=user.organization_id))
    db.commit()
    routes.onboarding_action(_request(), csrf_token='t', farm_name='', location='', total_area='',
                             season_start='2026-07-01', season_end='2027-06-30', db=db, user=user)
    assert db.query(Farm).count() == 1 and db.query(CropSeason).one().farm_id == db.query(Farm).one().id


def test_checklist_progress_and_dismiss(env):
    db, user = env
    steps = routes._onboarding_steps(db, user)
    assert [s['done'] for s in steps] == [False, False, False, False]
    farm = Farm(name='F', location='MG', total_area=10, organization_id=user.organization_id)
    db.add(farm)
    db.flush()
    db.add(CropSeason(farm_id=farm.id, name='S', start_date=date(2026, 7, 1), end_date=date(2027, 6, 30), culture='Cafe', cultivated_area=10))
    db.add(Plot(name='T1', farm_id=farm.id, area_hectares=5, plant_count=100))
    db.add(User(name='Op', email='op@x.com', hashed_password='h', organization_id=user.organization_id))
    db.commit()
    assert routes._onboarding_steps(db, user) is None
    db.add(User(name='Z', email='z@x.com', hashed_password='h'))
    db.query(Plot).delete()
    db.commit()
    assert [s['done'] for s in routes._onboarding_steps(db, user)] == [True, True, False, True]
    routes.onboarding_dismiss_action(_request(), csrf_token='t', db=db, user=user)
    assert routes._onboarding_steps(db, user) is None


def test_plot_names_repeat_across_farms_but_not_in_the_same_farm(env):
    db, user = env
    a = Farm(name='A', location='MG', total_area=10, organization_id=user.organization_id)
    b = Farm(name='B', location='MG', total_area=10, organization_id=user.organization_id)
    db.add_all([a, b])
    db.flush()
    db.add_all([Plot(name='Talhão 1', farm_id=a.id, area_hectares=5, plant_count=1), Plot(name='Talhão 1', farm_id=b.id, area_hectares=5, plant_count=1)])
    db.commit()
    assert routes._plot_name_taken(db, a.id, ' talhão 1 ') and not routes._plot_name_taken(db, a.id, 'Talhão 2')


def _req_fetch():
    return Request({'type': 'http', 'method': 'POST', 'path': '/primeiros-passos/tour', 'headers': [(b'x-sf-fetch', b'1')], 'query_string': b'', 'session': {}})


def test_tour_state_actions_and_context(env):
    db, user = env
    assert routes._onboarding_tour_context(db, user, 't') is None  # nunca iniciado: só a tela de boas-vindas
    resp = routes.onboarding_tour_action(_request(), csrf_token='t', action='start', db=db, user=user)
    assert resp.headers['location'] == '/primeiros-passos/continuar' and user.organization.onboarding_tour == 'active'
    ctx = routes._onboarding_tour_context(db, user, 'tok')
    assert ctx['state'] == 'active' and ctx['farm'] is None and ctx['csrf'] == 'tok' and ctx['first_name'] == 'Ana'
    assert routes.onboarding_tour_continue(_request(), db=db, user=user).headers['location'] == '/fazendas'
    paused = routes.onboarding_tour_action(_req_fetch(), csrf_token='t', action='pause', db=db, user=user)
    assert paused.status_code == 200 and user.organization.onboarding_tour == 'paused'
    assert routes._onboarding_tour_context(db, user, 't')['state'] == 'paused'
    routes.onboarding_tour_action(_request(), csrf_token='t', action='end', db=db, user=user)
    assert routes._onboarding_tour_context(db, user, 't') is None


def test_tour_follows_real_records_and_skip(env, monkeypatch):
    db, user = env
    calls = []
    monkeypatch.setattr(routes, 'persist_user_context', lambda request, db, user, farm_id, season_id: calls.append((farm_id, season_id)))
    routes.onboarding_tour_action(_request(), csrf_token='t', action='start', db=db, user=user)
    farm = Farm(name='Fazenda Sol', location='Três Pontas, MG', total_area=120.5, organization_id=user.organization_id)
    db.add(farm)
    db.commit()
    assert routes.onboarding_tour_continue(_request(), db=db, user=user).headers['location'] == '/safras/nova'
    assert calls[-1] == (farm.id, None)  # a safra nasce na fazenda certa
    ctx = routes._onboarding_tour_context(db, user, 't')
    assert ctx['farm'] == {'id': farm.id, 'name': 'Fazenda Sol', 'area': '120,50', 'location': 'Três Pontas, MG'} and ctx['season'] is None
    season = CropSeason(farm_id=farm.id, name='Safra 2026/2027', start_date=date(2026, 7, 1), end_date=date(2027, 6, 30), culture='Cafe', cultivated_area=10)
    db.add(season)
    db.commit()
    assert routes.onboarding_tour_continue(_request(), db=db, user=user).headers['location'] == '/setores'
    routes.onboarding_tour_action(_request(), csrf_token='t', action='skip_plot', db=db, user=user)
    assert routes.onboarding_tour_continue(_request(), db=db, user=user).headers['location'] == '/dashboard'
    assert routes._onboarding_tour_context(db, user, 't')['plot_skipped'] is True
    back = routes.onboarding_tour_action(_request(), csrf_token='t', action='unskip_plot', db=db, user=user)
    assert back.headers['location'] == '/setores' and not user.organization.onboarding_plot_skipped
    db.add(Plot(name='Talhão da Represa', farm_id=farm.id, area_hectares=18.4, plant_count=100))
    db.commit()
    ctx = routes._onboarding_tour_context(db, user, 't')
    assert ctx['plot']['name'] == 'Talhão da Represa' and ctx['plots_area'] == '18,40'
    routes.onboarding_tour_action(_request(), csrf_token='t', action='finish', db=db, user=user)
    assert user.organization.onboarding_tour == 'done' and routes._onboarding_tour_context(db, user, 't') is None


def test_welcome_page_stays_while_tour_is_live(env):
    db, user = env
    farm = Farm(name='F', location='MG', total_area=10, organization_id=user.organization_id)
    db.add(farm)
    db.flush()
    db.add(CropSeason(farm_id=farm.id, name='S', start_date=date(2026, 7, 1), end_date=date(2027, 6, 30), culture='Cafe', cultivated_area=10))
    db.commit()
    assert routes.onboarding_page(_request(), db=db, user=user, csrf_token='t').headers['location'] == '/dashboard'
    user.organization.onboarding_tour = 'paused'
    db.commit()
    page = routes.onboarding_page(_request(), db=db, user=user, csrf_token='t')
    assert page.name == 'onboarding.html' and page.ctx['tour_progress']['farm'].id == farm.id and page.ctx['tour_state'] == 'paused'


def test_tour_hidden_for_old_or_dismissed_accounts(env):
    db, user = env
    user.organization.onboarding_tour = 'active'
    user.organization.onboarding_dismissed = True
    db.commit()
    assert routes._onboarding_tour_context(db, user, 't') is None
