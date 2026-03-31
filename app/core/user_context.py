from fastapi import Request
from sqlalchemy.orm import Session

from app.models import CropSeason, Farm, User


def _int_or_none(value) -> int | None:
    return int(value) if value not in (None, "") else None


def normalize_user_context(
    db: Session,
    farm_id,
    season_id,
) -> tuple[int | None, int | None]:
    normalized_farm_id = _int_or_none(farm_id)
    normalized_season_id = _int_or_none(season_id)

    farm = db.query(Farm).filter(Farm.id == normalized_farm_id).first() if normalized_farm_id else None
    season = db.query(CropSeason).filter(CropSeason.id == normalized_season_id).first() if normalized_season_id else None

    if normalized_farm_id and not farm:
        normalized_farm_id = None
    if normalized_season_id and not season:
        normalized_season_id = None
        season = None

    if season and not normalized_farm_id:
        normalized_farm_id = season.farm_id

    if season and normalized_farm_id and season.farm_id != normalized_farm_id:
        normalized_season_id = None

    return normalized_farm_id, normalized_season_id


def apply_context_to_session(request: Request, farm_id: int | None, season_id: int | None) -> None:
    if farm_id:
        request.session["active_farm_id"] = farm_id
    else:
        request.session.pop("active_farm_id", None)

    if season_id:
        request.session["active_season_id"] = season_id
    else:
        request.session.pop("active_season_id", None)


def persist_user_context(
    request: Request,
    db: Session,
    user: User,
    farm_id,
    season_id,
) -> tuple[int | None, int | None]:
    normalized_farm_id, normalized_season_id = normalize_user_context(db, farm_id, season_id)
    apply_context_to_session(request, normalized_farm_id, normalized_season_id)

    if user.active_farm_id != normalized_farm_id or user.active_season_id != normalized_season_id:
        user.active_farm_id = normalized_farm_id
        user.active_season_id = normalized_season_id
        db.add(user)
        db.commit()
        db.refresh(user)

    return normalized_farm_id, normalized_season_id


def sync_user_context_from_preferences(
    request: Request,
    db: Session,
    user: User,
) -> tuple[int | None, int | None]:
    session_farm_id = request.session.get("active_farm_id")
    session_season_id = request.session.get("active_season_id")
    if session_farm_id not in (None, "") or session_season_id not in (None, ""):
        normalized_farm_id, normalized_season_id = normalize_user_context(db, session_farm_id, session_season_id)
        apply_context_to_session(request, normalized_farm_id, normalized_season_id)
        return normalized_farm_id, normalized_season_id

    return persist_user_context(request, db, user, user.active_farm_id, user.active_season_id)
