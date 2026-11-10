from app.services.farm_kml_export import farm_boundary_to_kml


def test_exports_saved_polygon_as_kml():
    boundary = '{"type":"Polygon","coordinates":[[[-43.1,-14.1],[-43.2,-14.1],[-43.2,-14.2],[-43.1,-14.1]]]}'

    result = farm_boundary_to_kml("Fazenda <Teste>", boundary).decode("utf-8")

    assert "<kml" in result
    assert "Fazenda &lt;Teste&gt;" in result
    assert "-43.10000000,-14.10000000" in result
    assert "<Polygon>" in result
