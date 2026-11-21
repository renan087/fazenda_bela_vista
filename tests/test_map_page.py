import json
from types import SimpleNamespace as NS

from app.web.routes import _map_page_data


def _plot(pid, area, sacks, geometry=True, variety="Arara"):
    boundary = json.dumps({"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]}) if geometry else None
    return NS(id=pid, name=f"Setor {pid}", farm=NS(name="Bela Vista"), variety=NS(name=variety),
              area_hectares=area, estimated_yield_sacks=sacks, boundary_geojson=boundary)


def test_map_page_data_builds_plots_and_kpis():
    farm_geojson = json.dumps({"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"feature_type": "farm", "area": 100}, "geometry": None},
        {"type": "Feature", "properties": {"feature_type": "plot", "area": 5}, "geometry": None},
    ]})
    data = _map_page_data(farm_geojson, [_plot(1, 10, 400), _plot(2, 15, 600), _plot(3, 5, None, geometry=False)],
                          {"active_farm": NS(id=7, name="Bela Vista", location="MG")})

    kpis = data["map_kpis"]
    assert kpis["farm_area"] == 100
    assert kpis["plots_area"] == 30
    assert kpis["mapped"] == 2 and kpis["total"] == 3
    assert kpis["estimated"] == 1000
    assert [p["id"] for p in json.loads(data["map_plots_json"])] == [1, 2]
    assert [p["id"] for p in data["map_unmapped"]] == [3]
    assert len(json.loads(data["map_geojson"])["features"]) == 1
    assert data["map_kml_href"] == "/fazendas/7/geomapa.kml"
    assert data["map_plots"][0]["yield"] == 40


def test_map_page_data_tolerates_invalid_geometry():
    broken = _plot(1, 10, 0)
    broken.boundary_geojson = "{invalido"
    data = _map_page_data("", [broken], {})
    assert data["map_kpis"]["mapped"] == 0
    assert data["map_kml_href"] is None


def test_map_page_data_uses_variety_color_and_active_planting():
    v1 = NS(id=7, name="Arara", map_color="#C99A1E")
    v2 = NS(id=8, name="Topázio", map_color=None)
    plot = _plot(1, 10, 400)
    plot.variety_id = 8
    plot.active_planting = NS(variety_id=7)
    loose = _plot(2, 5, None)
    loose.variety = None
    loose.variety_id = None
    data = _map_page_data("", [plot, loose], {}, [v1, v2])
    first, second = data["map_plots"]
    assert first["variety"] == "Arara" and first["variety_id"] == 7 and first["variety_color"] == "#C99A1E"
    assert second["variety"] == "Sem variedade" and second["variety_id"] is None and second["variety_color"] == "#9AA59E"
