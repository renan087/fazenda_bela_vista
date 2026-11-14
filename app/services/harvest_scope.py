"""Season matching for harvests, including records created before season links."""


def harvest_matches_season(harvest, season) -> bool:
    if season is None:
        return True
    if harvest.season_id is not None:
        return harvest.season_id == season.id
    return bool(harvest.harvest_date and season.start_date <= harvest.harvest_date <= season.end_date)
