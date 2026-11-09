from datetime import date
from types import SimpleNamespace

from app.services import agronomic_profile_suggestions as suggestions


def _plot(**overrides):
    values = {
        "area_hectares": 10,
        "variety": SimpleNamespace(name="Catuaí 144", species="Arabica"),
        "irrigation_type": "drip",
        "spacing_row_meters": 3.5,
        "spacing_plant_meters": 0.7,
        "drip_spacing_meters": 0.5,
        "planting_date": date(2020, 1, 1),
        "centroid_lat": -14.0,
        "centroid_lng": -42.0,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_uses_registered_farm_and_plot_data_as_confirmed_suggestions(monkeypatch):
    monkeypatch.setattr(suggestions.settings, "google_maps_api_key", None)
    farm = SimpleNamespace(id=1, location="Vitória da Conquista, Bahia - Brasil", boundary_geojson=None)

    payload = suggestions.build_agronomic_profile_suggestions(farm, [_plot()])

    assert payload["suggestions"]["region"]["value"] == "Vitória da Conquista, Bahia - Brasil"
    assert payload["suggestions"]["region"]["source"] == "confirmed"
    assert payload["suggestions"]["culture"]["value"] == "Café arábica"
    assert payload["suggestions"]["cultivar"]["value"] == "Catuaí 144"
    assert payload["suggestions"]["production_system"]["value"] == "Irrigado por Gotejamento"
    assert payload["suggestions"]["irrigation_system"]["value"] == "Gotejamento"


def test_marks_elevation_and_terrain_as_estimates(monkeypatch):
    monkeypatch.setattr(suggestions.settings, "google_maps_api_key", "test-key")
    monkeypatch.setattr(
        suggestions,
        "_fetch_elevation_samples",
        lambda _coordinates: [(-14.0, -42.0, 700.0), (-14.001, -42.0, 720.0), (-14.001, -42.001, 710.0)],
    )
    farm = SimpleNamespace(
        id=1,
        location="Bahia - Brasil",
        boundary_geojson='{"type":"Polygon","coordinates":[[[-42,-14],[-42,-14.001],[-42.001,-14.001],[-42,-14]]]}',
    )

    payload = suggestions.build_agronomic_profile_suggestions(farm, [])

    assert payload["suggestions"]["altitude_meters"]["source"] == "estimate"
    assert payload["suggestions"]["terrain_slope"]["source"] == "estimate"
    assert payload["suggestions"]["sun_exposure"]["source"] == "estimate"
