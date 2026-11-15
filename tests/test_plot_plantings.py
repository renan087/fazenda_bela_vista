from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import Farm, PlotPlanting
from app.repositories.farm import FarmRepository
from app.services import forms


def _repo():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    db = Session(engine)
    farm = Farm(name='Fazenda', location='MG', total_area=50)
    db.add(farm)
    db.commit()
    return FarmRepository(db), farm


def _form(farm, **extra):
    data = dict(name='Setor 1', area_hectares=10, plant_count=30000, planting_date='2015-10-01', spacing_row_meters=3.5, spacing_plant_meters=0.7, farm_id=farm.id)
    data.update(extra)
    return data


def test_new_plot_starts_with_active_planting_and_edit_corrects_it():
    repo, farm = _repo()
    plot = forms.create_plot(repo, _form(farm))
    assert len(plot.plantings) == 1
    active = plot.active_planting
    assert active.culture == 'Café' and active.plant_count == 30000 and active.planting_date == date(2015, 10, 1)
    forms.update_plot(repo, plot, _form(farm, plant_count=28000))
    assert repo.db.query(PlotPlanting).count() == 1 and plot.active_planting.plant_count == 28000 and plot.past_plantings == []


def test_ended_plantings_stay_in_history_and_new_one_becomes_active():
    repo, farm = _repo()
    plot = forms.create_plot(repo, _form(farm))
    first = plot.active_planting
    first.ended_on = date(2019, 8, 1)
    first.end_reason = 'renovacao'
    repo.db.commit()
    forms.update_plot(repo, plot, _form(farm, planting_date='2020-01-15', plant_count=35000))
    repo.db.refresh(plot)
    assert plot.active_planting.plant_count == 35000
    assert [p.plant_count for p in plot.past_plantings] == [30000]


def test_renew_planting_moves_current_to_history_and_updates_plot():
    import pytest
    repo, farm = _repo()
    plot = forms.create_plot(repo, _form(farm))
    with pytest.raises(forms.PlantingRenewalError):
        forms.renew_planting(repo, plot, ended_on=date(2014, 1, 1), end_reason='renovacao', new={'plant_count': 100})
    with pytest.raises(forms.PlantingRenewalError):
        forms.renew_planting(repo, plot, ended_on=date(2019, 8, 1), end_reason='renovacao', new={'plant_count': 100, 'planting_date': date(2019, 7, 1)})
    new = forms.renew_planting(repo, plot, ended_on=date(2019, 8, 1), end_reason='renovacao',
                               new={'culture': 'Café', 'planting_date': date(2020, 1, 15), 'plant_count': 35000, 'spacing_row_meters': 3.5})
    assert plot.active_planting.id == new.id and plot.plant_count == 35000 and plot.planting_date == date(2020, 1, 15)
    old = plot.past_plantings[0]
    assert old.plant_count == 30000 and old.ended_on == date(2019, 8, 1) and old.end_reason == 'renovacao'


def test_season_keeps_planting_of_its_period_after_renewal():
    repo, farm = _repo()
    plot = forms.create_plot(repo, _form(farm))
    old_season = forms.create_crop_season(repo, {'farm_id': farm.id, 'plots': [plot], 'name': 'Safra 2018/19', 'start_date': '2018-07-01', 'end_date': '2019-06-30',
                                                 'culture': 'Café', 'cultivated_area': 10, 'area_unit': 'ha', 'status': 'encerrada'})
    first = plot.active_planting
    assert old_season.planting_by_plot[plot.id].id == first.id
    forms.renew_planting(repo, plot, ended_on=date(2019, 8, 1), end_reason='renovacao', new={'planting_date': date(2020, 1, 15), 'plant_count': 35000})
    new_season = forms.create_crop_season(repo, {'farm_id': farm.id, 'plots': [plot], 'name': 'Safra 2022/23', 'start_date': '2022-07-01', 'end_date': '2023-06-30',
                                                 'culture': 'Café', 'cultivated_area': 10, 'area_unit': 'ha', 'status': 'ativa'})
    repo.db.expire_all()
    assert old_season.planting_by_plot[plot.id].id == first.id
    assert new_season.planting_by_plot[plot.id].id == plot.active_planting.id != first.id


def test_forecast_ignores_harvests_from_previous_planting():
    from types import SimpleNamespace
    from app.services.dashboard import calculate_forecast
    plot = SimpleNamespace(id=1, name='S1', area_hectares=10, estimated_yield_sacks=None, active_planting=SimpleNamespace(planting_date=date(2020, 1, 15)))
    harvests = [SimpleNamespace(plot_id=1, plot=plot, harvest_date=date(2018, 6, 1), productivity_per_hectare=50, sacks_produced=500),
                SimpleNamespace(plot_id=1, plot=plot, harvest_date=date(2023, 6, 1), productivity_per_hectare=30, sacks_produced=300)]
    result = calculate_forecast(None, plots=[plot], harvests=harvests)
    assert result['total_projection'] == 300 and result['plots'][0]['productivity'] == 30
