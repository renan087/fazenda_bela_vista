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
