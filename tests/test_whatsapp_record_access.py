from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.models.whatsapp import WhatsAppRecordAccess
from app.services.whatsapp_record_access import _record_in_scope, consume_token, create_token


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    WhatsAppRecordAccess.__table__.create(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@pytest.fixture
def link():
    return SimpleNamespace(id=1, user_id=2, organization_id=3, farm_id=4, season_id=5,
                           wa_id="5511999999999", verified_at=datetime(2026, 1, 1, tzinfo=timezone.utc))


def test_token_is_hashed_and_consumed_once(db, link):
    now = datetime(2026, 11, 17, tzinfo=timezone.utc)
    token = create_token(db, link=link, kind="compra", record_id=10, now=now)
    db.flush()
    row = db.query(WhatsAppRecordAccess).one()
    assert token not in row.token_hash
    result = consume_token(db, token, now=now)
    assert result.id == row.id and result.record_id == 10
    assert consume_token(db, token, now=now) is None


def test_token_expires_and_invalid_tokens_are_rejected(db, link):
    now = datetime(2026, 11, 17, tzinfo=timezone.utc)
    token = create_token(db, link=link, kind="consumo", record_id=1, now=now)
    db.flush()
    assert consume_token(db, token, now=now + timedelta(minutes=15)) is None
    assert consume_token(db, "inválido", now=now) is None
    with pytest.raises(ValueError):
        create_token(db, link=link, kind="desconhecido", record_id=1, now=now)


def test_record_scope_rejects_other_farm_season_and_plot():
    farm = SimpleNamespace(id=4)
    season = SimpleNamespace(id=5, start_date=date(2026, 1, 1), end_date=date(2026, 12, 31),
                             plots=[SimpleNamespace(id=7)])
    assert _record_in_scope(SimpleNamespace(farm_id=4), "compra", farm, season)
    assert not _record_in_scope(SimpleNamespace(farm_id=9), "compra", farm, season)
    record = SimpleNamespace(plot=SimpleNamespace(farm_id=4), plot_id=7, season_id=5,
                             application_date=date(2026, 5, 1))
    assert _record_in_scope(record, "aplicacao", farm, season)
    record.season_id = 8
    assert not _record_in_scope(record, "aplicacao", farm, season)
    record.season_id = 5
    record.plot_id = 9
    assert not _record_in_scope(record, "aplicacao", farm, season)
    record.plot_id = 7
    record.application_date = date(2027, 1, 1)
    assert not _record_in_scope(record, "aplicacao", farm, season)


def test_consumption_scope_allows_general_use_but_rejects_other_context():
    farm = SimpleNamespace(id=4)
    season = SimpleNamespace(id=5, start_date=date(2026, 1, 1), end_date=date(2026, 12, 31), plots=[])
    record = SimpleNamespace(farm_id=4, season_id=5, plot_id=None, movement_date=date(2026, 5, 1))
    assert _record_in_scope(record, "consumo", farm, season)
    record.farm_id = 9
    assert not _record_in_scope(record, "consumo", farm, season)


def test_confirmation_does_not_consume_token_and_post_is_single_use(db, link, monkeypatch):
    import re
    from starlette.requests import Request
    from app.routers import whatsapp_record_access as controller

    request = Request({"type": "http", "method": "GET", "path": "/acesso-whatsapp/registro",
                       "headers": [], "query_string": b"", "session": {}})
    monkeypatch.setattr(controller, "authorized_record", lambda db, access: SimpleNamespace(
        input_catalog=SimpleNamespace(name="Ureia"), purchase_date=date(2026, 11, 17),
        total_quantity=20, package_unit="kg", supplier_name=None))
    token = create_token(db, link=link, kind="compra", record_id=10)
    db.commit()
    confirmation = controller.record_confirmation(request)
    assert confirmation.status_code == 200
    assert db.query(WhatsAppRecordAccess).one().consumed_at is None
    assert controller.open_record(request, token, "wrong", db).status_code == 403
    confirmation = controller.record_confirmation(request)
    csrf = re.search(r'name="csrf_token" value="([^"]+)"', confirmation.body.decode()).group(1)
    opened = controller.open_record(request, token, csrf, db)
    assert opened.status_code == 200 and "Ureia" in opened.body.decode()
    assert opened.headers["cache-control"] == "no-store"
    confirmation = controller.record_confirmation(request)
    csrf = re.search(r'name="csrf_token" value="([^"]+)"', confirmation.body.decode()).group(1)
    assert controller.open_record(request, token, csrf, db).status_code == 410
