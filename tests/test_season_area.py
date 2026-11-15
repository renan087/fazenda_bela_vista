from types import SimpleNamespace

from app.web import routes


def test_season_area_is_sum_of_plots_converted_to_unit():
    plots = [SimpleNamespace(area_hectares=12.5), SimpleNamespace(area_hectares=7.25)]
    assert routes._season_area_from_plots(plots, 'ha') == 19.75
    assert routes._season_area_from_plots(plots, 'm2') == 197500.0
    assert routes._season_area_from_plots(plots, 'alq') == 4.08
    assert routes._season_area_from_plots(plots, 'alq_sp') == 8.16


def test_cost_per_hectare_uses_area_converted_to_hectares():
    assert routes._season_area_hectares(SimpleNamespace(cultivated_area=4.84, area_unit='alq')) == 4.84 * 4.84
    assert routes._season_area_hectares(SimpleNamespace(cultivated_area=10000, area_unit='m2')) == 1.0
    assert routes._season_area_hectares(SimpleNamespace(cultivated_area=3, area_unit=None)) == 3.0
