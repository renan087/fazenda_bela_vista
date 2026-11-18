import asyncio
from datetime import date
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.datastructures import FormData, UploadFile

from app.core.path_access import required_permission_for_web_path, required_permissions_for_web_path
from app.core.permissions_catalog import DATA_DELETE, PAGE_PRODUCTION, WRITE_PRODUCTION
from app.models.coffee_classification import CoffeeClassification
from app.models.harvest import HarvestRecord
from app.models.plot import Plot
from app.services.coffee_classification import validate_classification, validate_sensory, sensory_attributes_for_org
from app.models.coffee_sensory_profile import CoffeeSensoryProfile
from tests.test_stock_assistant import _route_env


def test_classification_header_covers_scrolling_content():
    root = Path(__file__).resolve().parents[1]
    template = (root / "app/templates/coffee_classification.html").read_text()
    css = (root / "app/static/css/styles.css").read_text()
    assert "coffee-classification-page" in template
    assert "body:has(.coffee-classification-page) .hd { background: #fff; }" in css
    assert "body .coffee-classification-page .stock-page-header { top: 69px; background: #f1f5f9; }" in css


def test_classification_validation_and_provenance():
    base = {"analysis_date": "2026-11-10", "source": "informado", "moisture": "11,5", "sensory_score": "85"}
    data = validate_classification(base, harvest_date=date(2026, 11, 1), today=date(2026, 11, 15), has_report=False)
    assert str(data["moisture"]) == "11.5" and str(data["sensory_score"]) == "85"
    for update, report in [({"source": "laudo"}, False), ({"moisture": "NaN"}, False),
                           ({"sensory_score": "101"}, False), ({"analysis_date": "2026-10-01"}, False)]:
        with pytest.raises(ValueError):
            validate_classification({**base, **update}, harvest_date=date(2026, 11, 1), today=date(2026, 11, 15), has_report=report)
    assert validate_classification({**base, "source": "laudo"}, harvest_date=None, today=date(2026, 11, 15), has_report=True)["source"] == "laudo"
    zero = validate_classification({"analysis_date": "2026-11-10", "source": "informado", "moisture": "0"},
                                   harvest_date=None, today=date(2026, 11, 15), has_report=False)
    assert zero["moisture"] == 0


def test_sensory_attributes_are_optional_bounded_and_finite():
    scale, values = validate_sensory({"sensory_scale": "9", "sensory_aroma": "7,5", "sensory_docura": "0"})
    assert scale == 9 and values == {"aroma": 7.5, "docura": 0.0}
    for fields in ({"sensory_scale": "9", "sensory_aroma": "10"}, {"sensory_scale": "10", "sensory_sabor": "NaN"},
                   {"sensory_scale": "100", "sensory_aroma": "2"}):
        with pytest.raises(ValueError):
            validate_sensory(fields)


def test_attributes_are_scoped_and_previous_analysis_keeps_names(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    monkeypatch.setattr(routes, "user_has_permission", lambda *args: True)
    request = SimpleNamespace(session={})
    assert any(key == "aroma" and active for key, _, active in sensory_attributes_for_org(db, user.organization_id))
    routes.manage_coffee_sensory_attribute(request, "x", "add", "", "Fragrância", db, user)
    definition = next(row for row in sensory_attributes_for_org(db, user.organization_id) if row[1] == "Fragrância")
    key = definition[0]
    profile = CoffeeSensoryProfile(scale_max=10, attributes_json=__import__("json").dumps({key: {"label": "Fragrância", "value": 8}}))
    routes.manage_coffee_sensory_attribute(request, "x", "deactivate", key, "", db, user)
    assert next(row for row in sensory_attributes_for_org(db, user.organization_id) if row[0] == key)[2] is False
    assert profile.attribute_pairs == [("Fragrância", 8)]
    routes.manage_coffee_sensory_attribute(request, "x", "delete", key, "", db, user)
    assert all(row[0] != key for row in sensory_attributes_for_org(db, user.organization_id))
    assert profile.attribute_pairs == [("Fragrância", 8)]
    assert all(row[0] != key for row in sensory_attributes_for_org(db, user.organization_id + 1))


def test_legacy_profile_and_inactive_attributes_are_not_reinterpreted():
    profile = CoffeeSensoryProfile(scale_max=10, attributes_json='{"aroma": 8, "sabor": 7}')
    assert profile.attribute_pairs == [("Aroma", 8), ("Sabor", 7)]
    _, result = validate_sensory({"sensory_aroma": "8", "sensory_custom": "9"}, [("aroma", "Aroma")])
    assert result == {"aroma": 8.0}


def test_attribute_delete_requires_permission_and_custom_key_is_private(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    request = SimpleNamespace(session={})
    routes.manage_coffee_sensory_attribute(request, "x", "add", "", "Notas cítricas", db, user)
    key = next(row[0] for row in sensory_attributes_for_org(db, user.organization_id) if row[1] == "Notas cítricas")
    monkeypatch.setattr(routes, "user_has_permission", lambda *args: False)
    with pytest.raises(HTTPException) as denied:
        routes.manage_coffee_sensory_attribute(request, "x", "delete", key, "", db, user)
    assert denied.value.status_code == 403
    other = SimpleNamespace(id=user.id + 1, organization_id=user.organization_id + 1, email="other@example.com")
    with pytest.raises(HTTPException) as missing:
        routes.manage_coffee_sensory_attribute(request, "x", "deactivate", key, "", db, other)
    assert missing.value.status_code == 404


def test_classification_is_linked_to_active_farm_and_report_is_private(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    plot = Plot(name="Setor 1", farm_id=scope["active_farm_id"], area_hectares=1, plant_count=1)
    db.add(plot); db.flush()
    harvest = HarvestRecord(plot_id=plot.id, season_id=scope["active_season_id"], harvest_date=date(2026, 11, 1), sacks_produced=1)
    db.add(harvest); db.commit()
    form = FormData({"csrf_token": "x", "harvest_id": str(harvest.id), "analysis_date": "2026-11-10", "source": "laudo",
                     "beverage": "Mole", "report": UploadFile(filename="laudo.pdf", file=BytesIO(b"%PDF-1.4\nteste"))})
    class Request:
        session = {}
        async def form(self):
            return form
    request = Request()
    asyncio.run(routes.create_coffee_classification(request, db, user))
    row = db.query(CoffeeClassification).one()
    assert row.harvest_id == harvest.id and row.source == "laudo" and row.beverage == "Mole"
    response = routes.coffee_classification_report(row.id, request, db, user)
    assert response.body.startswith(b"%PDF-") and response.headers["content-disposition"].startswith("attachment;")
    assert routes.coffee_classification_pdf(row.id, request, db, user).body.startswith(b"%PDF-")
    other_scope = dict(scope, active_farm=None)
    monkeypatch.setattr(routes, "_global_scope_context", lambda *args: other_scope)
    with pytest.raises(HTTPException) as denied:
        routes.coffee_classification_report(row.id, request, db, user)
    assert denied.value.status_code == 404
    assert required_permission_for_web_path("/producao/classificacao") == PAGE_PRODUCTION
    assert set(required_permissions_for_web_path("/producao/classificacao/1/excluir", "POST")) == {PAGE_PRODUCTION, WRITE_PRODUCTION, DATA_DELETE}


def test_saved_sensory_profile_and_pdf(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    plot = Plot(name="Setor 1", farm_id=scope["active_farm_id"], area_hectares=1, plant_count=1)
    db.add(plot); db.flush()
    harvest = HarvestRecord(plot_id=plot.id, season_id=scope["active_season_id"], harvest_date=date(2026, 11, 1), sacks_produced=1)
    db.add(harvest); db.commit()
    form = FormData({"csrf_token": "x", "harvest_id": str(harvest.id), "analysis_date": "2026-11-10", "source": "informado",
                     "sensory_scale": "10", "sensory_aroma": "8.25", "sensory_sabor": "7.5", "sensory_corpo": "8"})
    class Request:
        session = {}
        async def form(self):
            return form
    asyncio.run(routes.create_coffee_classification(Request(), db, user))
    row = db.query(CoffeeClassification).one()
    assert row.sensory_profile.attributes == {"aroma": 8.25, "sabor": 7.5, "corpo": 8.0}
    assert row.sensory_score is None
    response = routes.coffee_classification_pdf(row.id, Request(), db, user)
    assert response.body.startswith(b"%PDF-") and response.headers["cache-control"] == "private, no-store"
    monkeypatch.setattr(routes, "_global_scope_context", lambda *args: dict(scope, active_farm=None))
    with pytest.raises(HTTPException):
        routes.coffee_classification_pdf(row.id, Request(), db, user)


def test_custom_attribute_is_saved_with_its_label_and_printed_after_removal(monkeypatch):
    import json
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    monkeypatch.setattr(routes, "user_has_permission", lambda *args: True)
    request = SimpleNamespace(session={})
    routes.manage_coffee_sensory_attribute(request, "x", "add", "", "Fragrância", db, user)
    key = next(row[0] for row in sensory_attributes_for_org(db, user.organization_id) if row[1] == "Fragrância")
    plot = Plot(name="Setor 1", farm_id=scope["active_farm_id"], area_hectares=1, plant_count=1)
    db.add(plot); db.flush()
    harvest = HarvestRecord(plot_id=plot.id, season_id=scope["active_season_id"], harvest_date=date(2026, 11, 1), sacks_produced=1)
    db.add(harvest); db.commit()
    form = FormData({"csrf_token": "x", "harvest_id": str(harvest.id), "analysis_date": "2026-11-10", "source": "informado", "sensory_" + key: "8.5"})
    class Request:
        session = {}
        async def form(self):
            return form
    asyncio.run(routes.create_coffee_classification(Request(), db, user))
    item = db.query(CoffeeClassification).one()
    assert json.loads(item.sensory_profile.attributes_json)[key] == {"label": "Fragrância", "value": 8.5}
    routes.manage_coffee_sensory_attribute(request, "x", "delete", key, "", db, user)
    assert item.sensory_profile.attribute_pairs == [("Fragrância", 8.5)]
    assert routes.coffee_classification_pdf(item.id, Request(), db, user).body.startswith(b"%PDF-")
