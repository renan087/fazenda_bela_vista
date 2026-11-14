from datetime import date
from types import SimpleNamespace

from app.services.harvest_scope import harvest_matches_season


def test_linked_harvest_does_not_match_an_overlapping_season():
    season = SimpleNamespace(id=2, start_date=date(2026, 1, 1), end_date=date(2026, 12, 31))
    harvest = SimpleNamespace(season_id=1, harvest_date=date(2026, 5, 1))
    assert not harvest_matches_season(harvest, season)
    harvest.season_id = 2
    assert harvest_matches_season(harvest, season)


def test_legacy_harvest_matches_by_date_without_assigning_a_season():
    season = SimpleNamespace(id=1, start_date=date(2026, 1, 1), end_date=date(2026, 12, 31))
    harvest = SimpleNamespace(season_id=None, harvest_date=date(2026, 5, 1))
    assert harvest_matches_season(harvest, season)
    assert harvest.season_id is None
    harvest.harvest_date = date(2027, 1, 1)
    assert not harvest_matches_season(harvest, season)
