from datetime import date

from app.core.timezone import today_in_app_timezone

from fastapi import Request
from sqlalchemy.orm import Session

from app.models import CropSeason, Farm, User


def _int_or_none(value) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_user_context(
    db: Session,
    farm_id,
    season_id,
    organization_id: int | None = None,
) -> tuple[int | None, int | None]:
    normalized_farm_id = _int_or_none(farm_id)
    normalized_season_id = _int_or_none(season_id)

    farm_query = db.query(Farm)
    if organization_id:
        farm_query = farm_query.filter(Farm.organization_id == organization_id)
    farm = farm_query.filter(Farm.id == normalized_farm_id).first() if normalized_farm_id else None

    season = db.query(CropSeason).filter(CropSeason.id == normalized_season_id).first() if normalized_season_id else None
    if season and organization_id:
        season_farm = db.query(Farm).filter(Farm.id == season.farm_id, Farm.organization_id == organization_id).first()
        if not season_farm:
            season = None

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
    normalized_farm_id, normalized_season_id = normalize_user_context(db, farm_id, season_id, user.organization_id)
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
        farm_id, season_id = normalize_user_context(db, session_farm_id, session_season_id, user.organization_id)
        apply_context_to_session(request, farm_id, season_id)
        restored = False
    else:
        farm_id, season_id = persist_user_context(request, db, user, user.active_farm_id, user.active_season_id)
        restored = bool(farm_id and season_id)

    # Só pergunta quando há dúvida de verdade: escolhe sozinho o que é óbvio.
    picked_farm, picked_season, reason = auto_pick_context(db, user.organization_id, farm_id, season_id)
    if (picked_farm, picked_season) != (farm_id, season_id):
        farm_id, season_id = persist_user_context(request, db, user, picked_farm, picked_season)
        if farm_id and season_id:
            _set_notice(request, db, reason, farm_id, season_id)
    elif restored and user.organization_id and db.query(Farm).filter(Farm.organization_id == user.organization_id).count() > 1:
        _set_notice(request, db, "remembered", farm_id, season_id)
    return farm_id, season_id


CONTEXT_NOTICE_KEY = "context_notice"


def _set_notice(request: Request, db: Session, kind: str, farm_id: int, season_id: int) -> None:
    farm = db.get(Farm, farm_id)
    season = db.get(CropSeason, season_id)
    if farm and season:
        request.session[CONTEXT_NOTICE_KEY] = {"kind": kind, "farm": farm.name, "season": season.name or "Safra"}


def season_in_progress(season: CropSeason, today: date | None = None) -> bool:
    today = today or today_in_app_timezone()
    return season.status != "encerrada" and season.start_date <= today <= season.end_date


def auto_pick_context(db: Session, organization_id: int | None, farm_id, season_id, today: date | None = None):
    """Completa o contexto quando só existe uma resposta possível.

    - uma única fazenda na conta: ela;
    - sem safra escolhida: a única safra da fazenda, ou a única em andamento;
    - safra escolhida já terminou e existe exatamente uma em andamento: troca para a nova.
    Devolve (fazenda, safra, motivo) — motivo: "single", "current" ou "newer".
    """
    if not organization_id:
        return farm_id, season_id, ""
    reason = ""
    if not farm_id:
        farms = db.query(Farm.id).filter(Farm.organization_id == organization_id).limit(2).all()
        if len(farms) != 1:
            return farm_id, season_id, ""
        farm_id, reason = farms[0][0], "single"
    seasons = db.query(CropSeason).filter(CropSeason.farm_id == farm_id).all()
    running = [s for s in seasons if season_in_progress(s, today)]
    if not season_id:
        if len(seasons) == 1:
            season_id = seasons[0].id
        elif len(running) == 1:
            season_id = running[0].id
        reason = reason or "current"
    else:
        current = next((s for s in seasons if s.id == season_id), None)
        if current and (current.status == "encerrada" or current.end_date < (today or today_in_app_timezone())) and len(running) == 1 and running[0].id != season_id:
            season_id, reason = running[0].id, "newer"
    return farm_id, season_id, reason
