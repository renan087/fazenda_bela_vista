import asyncio
import json
from datetime import date
from types import SimpleNamespace

import pytest
from starlette.datastructures import FormData

from app.models import Plot, SoilAnalysis
from app.services.soil_ai_context import build_soil_ai_context
from app.services.soil_measurements import canonical_value, validate_metadata
from tests.test_stock_assistant import _route_env


def test_conversions_preserve_units_and_refuse_density_assumptions():
    assert canonical_value("potassium", 0.3, "cmolc/dm³") == pytest.approx(117.3)
    assert canonical_value("calcium", 24, "mmolc/dm³") == pytest.approx(2.4)
    assert canonical_value("organic_matter", 2.5, "%") == pytest.approx(25)
    assert canonical_value("organic_matter", 25, "g/kg") is None
    assert canonical_value("phosphorus", 8, "mg/kg") is None
    with pytest.raises(ValueError):
        validate_metadata({"potassium": "g/ha"}, {}, None, None, None, None)
    with pytest.raises(ValueError):
        validate_metadata({}, {}, "2026-11-10", "2026-11-09", "0", "20")
    with pytest.raises(ValueError):
        validate_metadata({}, {}, None, None, "20", "0")


def test_create_edit_and_ai_keep_lab_metadata(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    plot = Plot(name="Setor 1", farm_id=scope["active_farm_id"], area_hectares=1, plant_count=1)
    db.add(plot)
    db.commit()
    monkeypatch.setattr(routes, "_resolve_farm_plot_in_scope", lambda *args: (plot, scope, None))
    monkeypatch.setattr(routes, "_apply_soil_ai_recommendation", lambda repo, analysis, user: analysis)

    class Request:
        def __init__(self, data):
            self.session = {}
            self.query_params = {}
            self.data = FormData(data)

        async def form(self):
            return self.data

    args = dict(csrf_token="x", plot_id=plot.id, analysis_date="2026-10-10", laboratory="Lab",
                ph="5.5", organic_matter="2.5", phosphorus="8", potassium="0.30", calcium="24",
                magnesium=None, aluminum=None, h_al=None, ctc=None, base_saturation=None,
                observations=None, analysis_pdf=None, db=db, user=user)
    request = Request({
        "unit_organic_matter": "%", "unit_phosphorus": "mg/dm³", "unit_potassium": "cmolc/dm³",
        "unit_calcium": "mmolc/dm³", "method_phosphorus": "Mehlich-1",
        "collection_date": "2026-10-01", "report_date": "2026-10-08",
        "depth_from": "0", "depth_to": "20", "boron": "0.34", "unit_boron": "mg/dm³", "method_boron": "Água quente",
    })
    asyncio.run(routes.create_soil_analysis_action(request, **args))
    row = db.query(SoilAnalysis).one()
    assert float(row.potassium) == 0.3 and float(row.calcium) == 24
    metadata = json.loads(row.laboratory_metadata)
    assert metadata["units"]["potassium"] == "cmolc/dm³"
    assert metadata["methods"]["phosphorus"] == "Mehlich-1"
    assert metadata["extra_values"]["boron"] == 0.34 and metadata["methods"]["boron"] == "Água quente"
    context = build_soil_ai_context(db, row, date(2026, 11, 15), set())
    assert context["solo"]["data"] == "2026-10-01"
    assert context["solo"]["resultados"]["potássio"] == pytest.approx(117.3)
    assert context["solo"]["resultados"]["cálcio"] == pytest.approx(2.4)
    assert context["solo"]["valores_originais"]["potássio"] == pytest.approx(0.3)
    chart = json.loads(routes._soil_history_chart([row]))
    assert chart["potassium"] == pytest.approx([117.3])

    # Older API clients that omit the new fields leave previously recorded metadata intact.
    asyncio.run(routes.update_soil_analysis_action(row.id, Request({}), **args))
    assert json.loads(row.laboratory_metadata) == metadata
    row.laboratory_metadata = None
    db.commit()
    asyncio.run(routes.update_soil_analysis_action(row.id, Request({}), **args))
    assert row.laboratory_metadata is None
    assert json.loads(routes._soil_history_chart([row]))["potassium"] == [None]
    db.close()
