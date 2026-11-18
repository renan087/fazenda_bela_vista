import asyncio
from datetime import date
from io import BytesIO
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.datastructures import FormData, UploadFile

from app.core.path_access import required_permission_for_web_path, required_permissions_for_web_path
from app.core.permissions_catalog import DATA_DELETE, PAGE_PRODUCTION, WRITE_PRODUCTION
from app.models.coffee_classification import CoffeeClassification
from app.models.harvest import HarvestRecord
from app.models.plot import Plot
from app.services.coffee_classification import validate_classification
from tests.test_stock_assistant import _route_env


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
    other_scope = dict(scope, active_farm=None)
    monkeypatch.setattr(routes, "_global_scope_context", lambda *args: other_scope)
    with pytest.raises(HTTPException) as denied:
        routes.coffee_classification_report(row.id, request, db, user)
    assert denied.value.status_code == 404
    assert required_permission_for_web_path("/producao/classificacao") == PAGE_PRODUCTION
    assert set(required_permissions_for_web_path("/producao/classificacao/1/excluir", "POST")) == {PAGE_PRODUCTION, WRITE_PRODUCTION, DATA_DELETE}
