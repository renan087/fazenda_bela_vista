import json
from types import SimpleNamespace

from app.services import weather_forecast as wf

DATES = [f'2026-11-{d}' for d in range(15, 22)]


def _payload(ecmwf=True, prob_ecmwf=True):
    daily = {'time': DATES}
    for model, base in (('ecmwf_ifs025', 1), ('gfs_seamless', 2), ('icon_seamless', 3)):
        if model == 'ecmwf_ifs025' and not ecmwf:
            continue
        daily[f'temperature_2m_max_{model}'] = [28.0 + base] * 7
        daily[f'temperature_2m_min_{model}'] = [18.0] * 7
        daily[f'precipitation_sum_{model}'] = [base * 10.0] * 6 + [None]
        daily[f'precipitation_probability_max_{model}'] = ([50 + base] * 7) if (model != 'ecmwf_ifs025' or prob_ecmwf) else [None] * 7
    return {'daily': daily}


def test_parse_uses_ecmwf_as_primary_and_keeps_all_models():
    data = wf.parse_forecast(_payload())
    assert len(data['days']) == 7 and data['days'][0]['tmax'] == 29.0 and data['days'][0]['precip'] == 10.0
    assert data['days'][0]['prob'] == 51 and data['days'][0]['weekday'] == 'Dom'
    assert [m['label'] for m in data['models']] == ['ECMWF', 'GFS', 'ICON']
    assert data['models'][1]['total'] == 120.0 and data['primary_available']


def test_missing_variable_or_model_falls_back_to_other_model():
    data = wf.parse_forecast(_payload(prob_ecmwf=False))
    assert data['days'][0]['prob'] == 52 and data['days'][0]['fallback'] == {'prob': 'GFS'}
    data = wf.parse_forecast(_payload(ecmwf=False))
    assert not data['primary_available'] and not data['models'][0]['available']
    assert data['days'][0]['precip'] == 20.0
    assert data['days'][6]['precip'] == 30.0 or data['days'][6]['precip'] is None


def test_cache_three_hours_and_stale_on_error():
    wf.clear_cache()
    calls = []

    def ok(lat, lng):
        calls.append(1)
        return wf.parse_forecast(_payload())

    def boom(lat, lng):
        raise RuntimeError('fora do ar')

    assert wf.get_farm_forecast(-19, -46.9, now=0, fetcher=ok)['status'] == 'ok'
    assert wf.get_farm_forecast(-19.001, -46.9, now=3 * 3600 - 1, fetcher=boom)['status'] == 'ok'
    assert len(calls) == 1
    stale = wf.get_farm_forecast(-19, -46.9, now=3 * 3600 + 1, fetcher=boom)
    assert stale['status'] == 'stale' and stale['fetched_at'] == 0
    # nao insiste na API logo apos uma falha
    assert wf.get_farm_forecast(-19, -46.9, now=3 * 3600 + 60, fetcher=ok)['status'] == 'stale' and len(calls) == 1
    assert wf.get_farm_forecast(-10, -40, now=0, fetcher=boom) == {'status': 'error'}
    wf.clear_cache()


def test_invalid_payload_is_error():
    wf.clear_cache()
    def bad(lat, lng):
        return wf.parse_forecast({'error': True, 'reason': 'x'})
    assert wf.get_farm_forecast(1, 1, now=0, fetcher=bad) == {'status': 'error'}
    wf.clear_cache()


def test_farm_coordinates_from_boundary_or_plots():
    feature = {'type': 'Feature', 'geometry': {'type': 'Polygon', 'coordinates': [[[-47, -19], [-46, -19], [-46, -18], [-47, -18]]]}}
    lat, lng, src = wf.farm_coordinates(SimpleNamespace(boundary_geojson=json.dumps(feature), plots=[]))
    assert (round(lat, 2), round(lng, 2), src) == (-18.5, -46.5, 'contorno da fazenda')
    plot = SimpleNamespace(centroid_lat=-20.0, centroid_lng=-45.0, boundary_geojson=None)
    assert wf.farm_coordinates(SimpleNamespace(boundary_geojson=None, plots=[plot]))[:2] == (-20.0, -45.0)
    assert wf.farm_coordinates(SimpleNamespace(boundary_geojson='lixo', plots=[])) is None
