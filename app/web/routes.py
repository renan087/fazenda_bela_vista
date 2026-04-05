from datetime import date, datetime, timedelta
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from urllib.parse import urlencode

import hashlib
import json
import logging
import unicodedata
import urllib.error
import urllib.request

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, Request, Response, UploadFile, status
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import Image, KeepInFrame, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from sqlalchemy.orm import Session
from starlette.datastructures import UploadFile as StarletteUploadFile

from app.core.config import get_settings
from app.core.admin_access import has_admin_access, is_super_admin_email
from app.core.csrf import validate_csrf
from app.core.deps import get_csrf_token, get_current_user_web
from app.core.security import get_password_hash, verify_password
from app.core.timezone import app_now, format_app_datetime, today_in_app_timezone
from app.core.user_context import persist_user_context, sync_user_context_from_preferences
from app.db.session import get_db
from app.models import (
    AgronomicProfile,
    CoffeeVariety,
    CropSeason,
    EquipmentAsset,
    EquipmentAssetAttachment,
    Farm,
    FertilizationItem,
    FertilizationSchedule,
    FertilizationRecord,
    HarvestRecord,
    InputRecommendation,
    IrrigationRecord,
    PestIncident,
    Plot,
    PurchasedInput,
    PurchasedInputAttachment,
    RainfallRecord,
    SoilAnalysis,
    User,
)
from app.repositories.farm import FarmRepository
from app.services.backup_service import delete_backup_run, execute_backup
from app.services.dashboard import build_dashboard_context
from app.services.farm_preview_image import (
    farm_preview_fs_path,
    generate_farm_preview_image,
    remove_farm_preview_image,
)
from app.services.forms import (
    calculate_geojson_area_hectares,
    calculate_irrigation_volume,
    calculate_soil_recommendations,
    create_agronomic_profile,
    create_crop_season,
    create_equipment_asset,
    create_farm,
    create_fertilization,
    create_fertilization_schedule,
    create_harvest,
    create_input_recommendation,
    create_irrigation,
    create_manual_stock_output,
    create_pest_incident,
    create_plot,
    create_purchased_input,
    create_rainfall,
    create_soil_analysis,
    create_user,
    create_variety,
    estimate_geojson_centroid,
    extract_geojson_file,
    normalize_geojson,
    update_agronomic_profile,
    update_crop_season,
    update_equipment_asset,
    update_farm,
    update_fertilization,
    update_fertilization_schedule,
    update_harvest,
    update_input_recommendation,
    update_irrigation,
    update_pest_incident,
    update_plot,
    update_purchased_input,
    update_rainfall,
    update_soil_analysis,
    update_user,
    update_variety,
    conclude_fertilization_schedule,
    delete_fertilization,
    delete_fertilization_schedule,
    delete_manual_stock_output,
    validate_schedule_stock,
    update_manual_stock_output,
)
from app.services.openai_service import gerar_recomendacao_adubacao
from app.services.password_change import (
    get_active_password_change_verification,
    issue_password_change_verification,
    revoke_password_change_verifications,
    verify_password_change_code,
)
from app.services.password_reset import revoke_user_password_reset_tokens
from app.services.email_service import send_access_code_email, send_password_change_code_email
from app.services.trusted_browser import revoke_user_trusted_browsers
from app.services.two_factor import get_active_login_code, issue_login_verification_code, revoke_active_login_codes, verify_login_code

settings = get_settings()
logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
templates.env.filters["datetime_sp"] = format_app_datetime

EQUIPMENT_ASSET_CATEGORY_OPTIONS = [
    "Benfeitoria",
    "Equipamento",
    "Máquina",
    "Pivô",
    "Silo",
    "Veículo",
]
PENDING_PASSWORD_CHANGE_SESSION_KEY = "pending_password_change_user_id"
PENDING_SUPER_ADMIN_2FA_DISABLE_SESSION_KEY = "pending_super_admin_2fa_disable"
HISTORY_PAGE_SIZE = 10
MENU_ITEM_VISIBILITY_RULES = {
    "users": has_admin_access,
    "backups": has_admin_access,
}


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=status.HTTP_303_SEE_OTHER)


def _is_modal_request(request: Request) -> bool:
    return str(request.query_params.get("modal") or "") == "1"


def _redirect_with_query(path: str, **params) -> RedirectResponse:
    filtered = {}
    for key, value in params.items():
        if value in (None, "", False):
            continue
        if isinstance(value, (list, tuple)):
            if value:
                filtered[key] = value
            continue
        filtered[key] = value
    return _redirect(f"{path}?{urlencode(filtered, doseq=True)}" if filtered else path)


def _redirect_for_request(request: Request, path: str, **params) -> RedirectResponse:
    if _is_modal_request(request):
        params["modal"] = "1"
    return _redirect_with_query(path, **params)


def _base_context(request: Request, user: User, csrf_token: str, page: str, **kwargs):
    modal_mode = _is_modal_request(request)
    flash = request.session.get("flash") if modal_mode else request.session.pop("flash", None)
    repo = kwargs.pop("_repo", None)
    context = {
        "request": request,
        "user": user,
        "csrf_token": csrf_token,
        "page": page,
        "flash": flash,
        "modal_mode": modal_mode,
        "menu_visibility": _build_menu_visibility(user),
    }
    if repo:
        scope_context = _global_scope_context(request, repo, user)
        context.update(scope_context)
        context["context_lock_exempt"] = page in {"farms", "seasons", "profile"}
        context["context_selection_blocking"] = scope_context["context_selection_required"] and not context["context_lock_exempt"]
    context.update(kwargs)
    return context


def _repository(db: Session) -> FarmRepository:
    return FarmRepository(db)


def _build_menu_visibility(user: User | None) -> dict[str, bool]:
    visibility: dict[str, bool] = {}
    for key, rule in MENU_ITEM_VISIBILITY_RULES.items():
        try:
            visibility[key] = bool(rule(user))
        except Exception:
            visibility[key] = False
    return visibility


def _render_profile_page(
    request: Request,
    user: User,
    csrf_token: str,
    repo: FarmRepository,
    pending_password_change=None,
    active_profile_tab: str = "profile",
):
    return templates.TemplateResponse(
        "profile.html",
        _base_context(
            request,
            user,
            csrf_token,
            "profile",
            title="Meu Perfil",
            _repo=repo,
            profile_gender_options=_profile_gender_options(),
            profile_gender_label=_profile_gender_label(user.gender),
            pending_password_change=pending_password_change,
            active_profile_tab=active_profile_tab,
        ),
    )


def _flash(request: Request, kind: str, message: str) -> None:
    request.session["flash"] = {"kind": kind, "message": message}


def _float_or_none(value: str | None):
    return float(value) if value not in (None, "") else None


def _int_or_none(value: str | None):
    return int(value) if value not in (None, "") else None


def _positive_int(value: str | None, default: int = 1) -> int:
    parsed = _int_or_none(value)
    if parsed is None or parsed < 1:
        return default
    return parsed


def _backup_details(value: str | None) -> dict:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def _sort_collection_desc(items, *getters):
    def sort_key(item):
        values = []
        for getter in getters:
            value = getter(item)
            values.append((value is not None, value))
        return tuple(values)

    return sorted(items, key=sort_key, reverse=True)


def _url_with_query(request: Request, **updates) -> str:
    params = [(key, value) for key, value in request.query_params.multi_items() if key not in updates]
    for key, value in updates.items():
        if value in (None, "", False):
            continue
        params.append((key, str(value)))
    query = urlencode(params, doseq=True)
    return f"{request.url.path}?{query}" if query else request.url.path


def _paginate_collection(
    request: Request,
    items,
    page_param: str,
    per_page: int = HISTORY_PAGE_SIZE,
):
    total_items = len(items)
    total_pages = max(1, (total_items + per_page - 1) // per_page)
    page = min(_positive_int(request.query_params.get(page_param), 1), total_pages)
    start = (page - 1) * per_page
    end = start + per_page
    return {
        "items": items[start:end],
        "page": page,
        "total_items": total_items,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "first_url": _url_with_query(request, **{page_param: None}),
        "prev_url": _url_with_query(request, **{page_param: page - 1 if page > 2 else None}),
        "next_url": _url_with_query(request, **{page_param: page + 1}),
        "last_url": _url_with_query(request, **{page_param: total_pages if total_pages > 1 else None}),
    }


def _mark_password_change_pending(request: Request, user_id: int) -> None:
    request.session[PENDING_PASSWORD_CHANGE_SESSION_KEY] = user_id


def _has_password_change_pending(request: Request, user_id: int) -> bool:
    return request.session.get(PENDING_PASSWORD_CHANGE_SESSION_KEY) == user_id


def _clear_password_change_pending(request: Request) -> None:
    request.session.pop(PENDING_PASSWORD_CHANGE_SESSION_KEY, None)


def _discard_password_change_pending(request: Request, db: Session, user_id: int) -> None:
    revoke_password_change_verifications(db, user_id)
    _clear_password_change_pending(request)


def _mark_super_admin_2fa_disable_pending(
    request: Request,
    *,
    target_user_id: int,
    actor_user_id: int,
    confirmed: bool = False,
) -> None:
    request.session[PENDING_SUPER_ADMIN_2FA_DISABLE_SESSION_KEY] = {
        "target_user_id": int(target_user_id),
        "actor_user_id": int(actor_user_id),
        "confirmed": bool(confirmed),
    }


def _get_super_admin_2fa_disable_pending(request: Request) -> dict | None:
    payload = request.session.get(PENDING_SUPER_ADMIN_2FA_DISABLE_SESSION_KEY)
    return payload if isinstance(payload, dict) else None


def _has_super_admin_2fa_disable_pending(request: Request, *, target_user_id: int, actor_user_id: int) -> bool:
    payload = _get_super_admin_2fa_disable_pending(request)
    if not payload:
        return False
    return (
        int(payload.get("target_user_id") or 0) == int(target_user_id)
        and int(payload.get("actor_user_id") or 0) == int(actor_user_id)
    )


def _clear_super_admin_2fa_disable_pending(request: Request) -> None:
    request.session.pop(PENDING_SUPER_ADMIN_2FA_DISABLE_SESSION_KEY, None)


def _super_admin_2fa_disable_pending_confirmed(request: Request, *, target_user_id: int, actor_user_id: int) -> bool:
    payload = _get_super_admin_2fa_disable_pending(request)
    if not payload:
        return False
    return (
        int(payload.get("target_user_id") or 0) == int(target_user_id)
        and int(payload.get("actor_user_id") or 0) == int(actor_user_id)
        and bool(payload.get("confirmed"))
    )


def _mark_super_admin_2fa_disable_confirmed(request: Request, *, target_user_id: int, actor_user_id: int) -> None:
    _mark_super_admin_2fa_disable_pending(
        request,
        target_user_id=target_user_id,
        actor_user_id=actor_user_id,
        confirmed=True,
    )


def _discard_super_admin_2fa_disable_pending(request: Request, db: Session, actor_user_id: int | None = None) -> None:
    if actor_user_id:
        revoke_active_login_codes(db, actor_user_id)
    _clear_super_admin_2fa_disable_pending(request)


def _resolve_fertilization_output_context(
    repo: FarmRepository,
    db: Session,
    output,
) -> tuple[FertilizationItem | None, FertilizationRecord | None]:
    if output.reference_type == "fertilization_item" and output.reference_id:
        fertilization_item = db.query(FertilizationItem).filter(FertilizationItem.id == output.reference_id).first()
        fertilization = (
            repo.get_fertilization(fertilization_item.fertilization_record_id)
            if fertilization_item and fertilization_item.fertilization_record_id
            else None
        )
        return fertilization_item, fertilization

    if output.reference_type == "fertilization_record" and output.reference_id:
        fertilization = repo.get_fertilization(output.reference_id)
        if not fertilization:
            return None, None

        candidates = [
            item for item in fertilization.items
            if output.input_id is not None and item.input_id == output.input_id
        ]
        if not candidates and len(fertilization.items) == 1:
            candidates = list(fertilization.items)
        if len(candidates) > 1 and output.quantity is not None:
            target_quantity = float(output.quantity or 0)
            exact_matches = [
                item for item in candidates
                if abs(float(item.total_quantity or 0) - target_quantity) < 0.01
            ]
            if exact_matches:
                candidates = exact_matches
        return (candidates[0] if candidates else None), fertilization

    return None, None


def _int_list(values: list[str]) -> list[int]:
    parsed: list[int] = []
    for value in values:
        if value in (None, ""):
            continue
        parsed.append(int(value))
    return parsed


def _date_or_none(value: str | None):
    return date.fromisoformat(value) if value not in (None, "") else None


def _clean_text(value: str | None) -> str | None:
    normalized = " ".join((value or "").strip().split())
    return normalized or None


def _profile_gender_options() -> list[tuple[str, str]]:
    return [
        ("feminino", "Feminino"),
        ("masculino", "Masculino"),
        ("nao_informar", "Prefiro nao informar"),
        ("outro", "Outro"),
    ]


def _profile_gender_label(value: str | None) -> str:
    for option_value, label in _profile_gender_options():
        if option_value == value:
            return label
    return "Nao informado"


def _parse_equipment_asset_manufacture_year(value: str | None) -> int | None:
    normalized = (value or "").strip()
    if not normalized:
        return None
    if len(normalized) != 4 or not normalized.isdigit():
        raise ValueError("Informe o ano de fabricacao no formato YYYY.")
    parsed = int(normalized)
    current_year = today_in_app_timezone().year
    if parsed < 1900 or parsed > current_year:
        raise ValueError(f"Informe um ano de fabricacao entre 1900 e {current_year}.")
    return parsed


async def _read_avatar_upload(avatar: UploadFile | None) -> tuple[dict | None, str | None]:
    if not avatar or not avatar.filename:
        return None, None

    if not (avatar.content_type or "").startswith("image/"):
        await avatar.close()
        return None, "Envie uma imagem valida para o avatar."

    avatar_bytes = await avatar.read()
    await avatar.close()
    if not avatar_bytes:
        return None, "A imagem selecionada esta vazia."

    return {
        "avatar_filename": Path(avatar.filename).name[:255],
        "avatar_content_type": (avatar.content_type or "image/jpeg")[:120],
        "avatar_data": avatar_bytes,
    }, None


def _active_farm_id(request: Request) -> int | None:
    return _int_or_none(request.session.get("active_farm_id"))


def _active_season_id(request: Request) -> int | None:
    return _int_or_none(request.session.get("active_season_id"))


def _global_scope_context(request: Request, repo: FarmRepository, user: User | None = None) -> dict:
    if user:
        sync_user_context_from_preferences(request, repo.db, user)

    farms = repo.list_farms()
    active_farm_id = _active_farm_id(request)
    active_season_id = _active_season_id(request)
    active_farm = repo.get_farm(active_farm_id) if active_farm_id else None
    active_season = repo.get_crop_season(active_season_id) if active_season_id else None

    if active_farm_id and not active_farm:
        request.session.pop("active_farm_id", None)
        active_farm_id = None
    if active_season_id and not active_season:
        request.session.pop("active_season_id", None)
        active_season_id = None

    if active_season and active_farm_id and active_season.farm_id != active_farm_id:
        request.session.pop("active_season_id", None)
        active_season = None
        active_season_id = None

    all_seasons = repo.list_crop_seasons()
    context_seasons = [
        season for season in all_seasons if active_farm_id and season.farm_id == active_farm_id
    ]
    context_season_options = [
        {"id": season.id, "farm_id": season.farm_id, "name": season.name}
        for season in all_seasons
    ]
    current_url = request.url.path
    if request.url.query:
        current_url = f"{current_url}?{request.url.query}"

    return {
        "context_farms": farms,
        "context_seasons": context_seasons,
        "context_season_options": context_season_options,
        "active_farm_id": active_farm_id,
        "active_season_id": active_season_id,
        "active_farm": active_farm,
        "active_season": active_season,
        "context_selection_required": not active_farm_id or not active_season_id,
        "context_missing_farm": not active_farm_id,
        "context_missing_season": not active_season_id,
        "current_url": current_url,
    }


def _scoped_plot_filters(request: Request, active_season: CropSeason | None) -> tuple[list[int] | None, list[int] | None]:
    farm_ids = [_active_farm_id(request)] if _active_farm_id(request) else None
    variety_ids = [active_season.variety_id] if active_season and active_season.variety_id else None
    return farm_ids, variety_ids


def _scoped_dates(active_season: CropSeason | None) -> tuple[date | None, date | None]:
    if not active_season:
        return None, None
    return active_season.start_date, active_season.end_date


def _within_scope(value: date | None, start_date: date | None, end_date: date | None) -> bool:
    if start_date and value and value < start_date:
        return False
    if end_date and value and value > end_date:
        return False
    return True


def _schedule_filter_date_bounds(
    request: Request,
    active_season,
    *,
    flash_invalid: bool = False,
) -> tuple[date | None, date | None, str, str]:
    """Restringe o intervalo da safra ativa com start_date/end_date opcionais na query."""
    scope_start, scope_end = _scoped_dates(active_season)
    raw_start = (request.query_params.get("start_date") or "").strip()
    raw_end = (request.query_params.get("end_date") or "").strip()
    user_start: date | None = None
    user_end: date | None = None
    if raw_start:
        try:
            user_start = date.fromisoformat(raw_start)
        except ValueError:
            raw_start = ""
    if raw_end:
        try:
            user_end = date.fromisoformat(raw_end)
        except ValueError:
            raw_end = ""
    if user_start and user_end and user_start > user_end:
        if flash_invalid:
            _flash(request, "error", "A data inicial do periodo nao pode ser posterior a data final.")
        raw_start = ""
        raw_end = ""
        user_start = None
        user_end = None
    eff_start = scope_start
    eff_end = scope_end
    if user_start:
        eff_start = user_start if eff_start is None else max(eff_start, user_start)
    if user_end:
        eff_end = user_end if eff_end is None else min(eff_end, user_end)
    return eff_start, eff_end, raw_start, raw_end


def _planning_filter_range_preset(raw_preset: str | None, raw_start: str, raw_end: str) -> str:
    valid_presets = {"next_10_days", "next_20_days", "next_month", "custom"}
    preset = (raw_preset or "").strip()
    if preset in valid_presets:
        return preset
    if raw_start or raw_end:
        return "custom"
    return "next_10_days"


def _fertilization_filter_range_preset(raw_preset: str | None, raw_start: str, raw_end: str) -> str:
    valid_presets = {"last_10_days", "last_20_days", "last_month", "custom"}
    preset = (raw_preset or "").strip()
    if preset in valid_presets:
        return preset
    if raw_start or raw_end:
        return "custom"
    return "last_10_days"


def _schedule_tab_filter_range_preset(
    raw_preset: str | None,
    raw_start: str,
    raw_end: str,
    *,
    schedule_tab: str,
) -> str:
    """Aba Ativos: presets futuros; aba Concluídos: presets passados."""
    preset = (raw_preset or "").strip()
    past_ok = {"last_10_days", "last_20_days", "last_month", "custom"}
    future_ok = {"next_10_days", "next_20_days", "next_month", "custom"}
    if schedule_tab == "completed":
        if preset in past_ok:
            return preset
        if raw_start or raw_end:
            return "custom"
        return "last_10_days"
    if preset in future_ok:
        return preset
    if raw_start or raw_end:
        return "custom"
    return "next_10_days"


def _normalize_search_value(value: object) -> str:
    return (
        unicodedata.normalize("NFD", str(value or ""))
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )


def _filter_fertilization_records(
    repo: FarmRepository,
    plot_ids: set[int],
    start_date: date | None,
    end_date: date | None,
    *,
    search: str | None = None,
) -> list[FertilizationRecord]:
    fertilizations = [
        item
        for item in repo.list_fertilizations()
        if item.plot_id in plot_ids and _within_scope(item.application_date, start_date, end_date)
    ]
    query = _normalize_search_value((search or "").strip())
    if query:
        fertilizations = [
            item
            for item in fertilizations
            if query in _normalize_search_value(
                f"{item.plot.name if item.plot else ''} "
                f"{item.application_date or ''} "
                f"{item.product or ''} "
                f"{item.notes or ''} "
                + " ".join(detail.name for detail in item.items)
            )
        ]
    fertilizations = _sort_collection_desc(
        fertilizations,
        lambda item: item.application_date,
        lambda item: item.id,
    )
    return fertilizations


def _launch_scope_or_redirect(
    request: Request,
    repo: FarmRepository,
    redirect_path: str,
    require_season: bool = True,
):
    scope = _global_scope_context(request, repo)
    if scope["active_farm_id"] and (scope["active_season_id"] or not require_season):
        return scope, None
    _flash(request, "error", "Selecione a fazenda e a safra ativas no topo antes de continuar.")
    return None, _redirect(redirect_path)


def _plot_matches_scope(plot: Plot | None, scope: dict) -> bool:
    if not plot:
        return False
    if scope.get("active_farm_id") and plot.farm_id != scope["active_farm_id"]:
        return False
    active_season = scope.get("active_season")
    if active_season and active_season.variety_id and plot.variety_id != active_season.variety_id:
        return False
    return True


def _resolve_plot_in_scope(
    request: Request,
    repo: FarmRepository,
    plot_id: int | None,
    redirect_path: str,
):
    scope, denied = _launch_scope_or_redirect(request, repo, redirect_path)
    if denied:
        return None, None, denied
    plot = repo.get_plot(plot_id) if plot_id else None
    if not plot:
        _flash(request, "error", "Setor nao encontrado para o contexto atual.")
        return None, scope, _redirect(redirect_path)
    if not _plot_matches_scope(plot, scope):
        _flash(request, "error", "O setor informado nao pertence ao contexto ativo de fazenda e safra.")
        return None, scope, _redirect(redirect_path)
    return plot, scope, None


def _resolve_optional_plot_in_scope(
    request: Request,
    repo: FarmRepository,
    plot_id: int | None,
    redirect_path: str,
):
    scope, denied = _launch_scope_or_redirect(request, repo, redirect_path)
    if denied:
        return None, scope, denied
    if not plot_id:
        return None, scope, None
    plot = repo.get_plot(plot_id)
    if not plot:
        _flash(request, "error", "Setor nao encontrado para o contexto atual.")
        return None, scope, _redirect(redirect_path)
    if not _plot_matches_scope(plot, scope):
        _flash(request, "error", "O setor informado nao pertence ao contexto ativo de fazenda e safra.")
        return None, scope, _redirect(redirect_path)
    return plot, scope, None


def _farm_matches_scope(farm_id: int | None, scope: dict) -> bool:
    return farm_id in (None, scope.get("active_farm_id"))


def _page_number(value: str | int | None, default: int = 1) -> int:
    try:
        return max(int(value or default), 1)
    except (TypeError, ValueError):
        return default


def _build_stock_context(
    repo: FarmRepository,
    farm_id: int | None = None,
    input_id: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    movement_type: str = "all",
    item_type: str | None = None,
):
    def _stock_priority(row: dict) -> tuple[int, float, str]:
        threshold = float(row.get("low_stock_threshold") or 0)
        available = float(row.get("available_quantity") or 0)
        if threshold > 0:
            if available <= threshold * 0.4:
                return (0, available, row["name"].lower())
            if available <= threshold:
                return (1, available, row["name"].lower())
        return (2, available, row["name"].lower())

    catalog_inputs = repo.list_input_catalog(item_type=item_type)
    if input_id:
        catalog_inputs = [item for item in catalog_inputs if item.id == input_id]

    purchase_entries = repo.list_purchased_inputs(item_type=item_type)
    if input_id:
        purchase_entries = [entry for entry in purchase_entries if entry.input_id == input_id]
    if farm_id:
        purchase_entries = [entry for entry in purchase_entries if entry.farm_id in (None, farm_id)]

    stock_outputs = repo.list_stock_outputs()
    if input_id:
        stock_outputs = [output for output in stock_outputs if output.input_id == input_id]
    if farm_id:
        stock_outputs = [output for output in stock_outputs if output.farm_id in (None, farm_id)]
    if item_type:
        stock_outputs = [output for output in stock_outputs if output.input_catalog and output.input_catalog.item_type == item_type]

    input_stock = {
        item.id: {
            "available": round(
                sum(float(entry.available_quantity or 0) for entry in purchase_entries if entry.input_id == item.id),
                2,
            ),
            "total": round(
                sum(float(entry.total_quantity or 0) for entry in purchase_entries if entry.input_id == item.id),
                2,
            ),
            "unit": item.default_unit,
        }
        for item in catalog_inputs
    }

    stock_catalog_rows = []
    extract_rows = []
    for item in catalog_inputs:
        related_entries = [entry for entry in purchase_entries if entry.input_id == item.id]
        related_outputs = [output for output in stock_outputs if output.input_id == item.id]
        total_quantity = sum(float(entry.total_quantity or 0) for entry in related_entries)
        total_value = sum(float(entry.total_cost or 0) for entry in related_entries)
        row = {
            "id": item.id,
            "name": item.name,
            "item_type": item.item_type,
            "unit": item.default_unit,
            "available_quantity": input_stock[item.id]["available"],
            "total_quantity": input_stock[item.id]["total"],
            "low_stock_threshold": float(item.low_stock_threshold or 0),
            "average_cost": round(total_value / total_quantity, 2) if total_quantity else 0,
            "entries_count": len(related_entries),
            "outputs_count": len(related_outputs),
            "last_movement_date": (
                max(
                    [entry.purchase_date for entry in related_entries if entry.purchase_date]
                    + [output.movement_date for output in related_outputs if output.movement_date]
                )
                if related_entries or related_outputs
                else None
            ),
        }
        stock_catalog_rows.append(row)

        events = []
        for entry in related_entries:
            unit_cost = round(float(entry.total_cost or 0) / max(float(entry.total_quantity or 0), 1), 4)
            events.append(
                {
                    "kind": "entrada",
                    "date": entry.purchase_date,
                    "quantity": float(entry.total_quantity or 0),
                    "unit": entry.package_unit,
                    "farm": entry.farm,
                    "plot": None,
                    "origin": "compra",
                    "reference": f"Lote #{entry.id}",
                    "value": float(entry.total_cost or 0),
                    "unit_cost": unit_cost,
                    "notes": entry.notes,
                    "sort_key": (entry.purchase_date or today_in_app_timezone(), 0, entry.id),
                }
            )
        for output in related_outputs:
            events.append(
                {
                    "kind": "saida",
                    "date": output.movement_date,
                    "quantity": float(output.quantity or 0),
                    "unit": output.unit,
                    "farm": output.farm,
                    "plot": output.plot,
                    "origin": output.origin,
                    "reference": f"{output.reference_type or 'movimento'}#{output.reference_id}" if output.reference_id else (output.reference_type or "movimento manual"),
                    "value": float(output.total_cost or 0),
                    "unit_cost": float(output.unit_cost or 0),
                    "notes": output.notes,
                    "sort_key": (output.movement_date or today_in_app_timezone(), 1, output.id),
                }
            )

        running_balance = 0.0
        for event in sorted(events, key=lambda item_: item_["sort_key"]):
            delta = event["quantity"] if event["kind"] == "entrada" else -event["quantity"]
            running_balance = round(running_balance + delta, 2)
            extract_rows.append(
                {
                    "input_id": item.id,
                    "input_name": item.name,
                    "kind": event["kind"],
                    "date": event["date"],
                    "quantity": event["quantity"],
                    "unit": event["unit"] or item.default_unit,
                    "farm": event["farm"],
                    "plot": event["plot"],
                    "origin": event["origin"],
                    "reference": event["reference"],
                    "unit_cost": event["unit_cost"],
                    "total_cost": event["value"],
                    "balance_after": running_balance,
                    "notes": event["notes"],
                    "sort_key": event["sort_key"],
                }
            )

    stock_catalog_rows.sort(key=_stock_priority)
    filtered_entries = purchase_entries
    filtered_outputs = stock_outputs
    filtered_extract_rows = extract_rows
    if start_date:
        filtered_entries = [entry for entry in filtered_entries if entry.purchase_date and entry.purchase_date >= start_date]
        filtered_outputs = [output for output in filtered_outputs if output.movement_date and output.movement_date >= start_date]
        filtered_extract_rows = [row for row in filtered_extract_rows if row["date"] and row["date"] >= start_date]
    if end_date:
        filtered_entries = [entry for entry in filtered_entries if entry.purchase_date and entry.purchase_date <= end_date]
        filtered_outputs = [output for output in filtered_outputs if output.movement_date and output.movement_date <= end_date]
        filtered_extract_rows = [row for row in filtered_extract_rows if row["date"] and row["date"] <= end_date]
    if movement_type in {"entrada", "saida"}:
        filtered_extract_rows = [row for row in filtered_extract_rows if row["kind"] == movement_type]
        if movement_type == "entrada":
            filtered_outputs = []
        else:
            filtered_entries = []

    filtered_extract_rows.sort(key=lambda row: row["sort_key"], reverse=True)
    return {
        "catalog_inputs": catalog_inputs,
        "purchase_entries": filtered_entries,
        "stock_outputs": filtered_outputs,
        "input_stock": input_stock,
        "stock_catalog_rows": stock_catalog_rows,
        "extract_rows": filtered_extract_rows,
    }


def _stock_export_query(
    farm_id: int | None = None,
    input_id: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    movement_type: str = "all",
    item_type: str | None = None,
) -> str:
    params = {
        "farm_id": farm_id,
        "input_id": input_id,
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
        "movement_type": movement_type if movement_type and movement_type != "all" else None,
        "item_type": item_type,
    }
    clean = {key: value for key, value in params.items() if value not in (None, "", "all")}
    return urlencode(clean)


def _stock_page_filters_active(
    request: Request,
    *,
    active_farm_id: int | None,
    selected_input_id: int | None,
    movement_type: str,
    normalized_item_type: str | None,
    edit_output_id: int | None,
) -> bool:
    if edit_output_id:
        return True
    qp = request.query_params
    if selected_input_id:
        return True
    if (qp.get("start_date") or "").strip() or (qp.get("end_date") or "").strip():
        return True
    if movement_type and movement_type != "all":
        return True
    if normalized_item_type:
        return True
    if (qp.get("schedule_range") or "").strip():
        return True
    raw_farm = qp.get("farm_id")
    if raw_farm is not None and str(raw_farm).strip() != "":
        try:
            if int(raw_farm) != int(active_farm_id or 0):
                return True
        except (TypeError, ValueError):
            return True
    return False


def _assets_export_query(farm_id: int | None = None, status: str | None = None) -> str:
    params = {"farm_id": farm_id, "status": status}
    clean = {key: value for key, value in params.items() if value not in (None, "", "all")}
    return urlencode(clean)


def _purchased_inputs_export_query(farm_id: int | None = None, item_type: str | None = None) -> str:
    params = {"farm_id": farm_id, "item_type": item_type}
    clean = {key: value for key, value in params.items() if value not in (None, "", "all")}
    return urlencode(clean)


def _format_currency(value) -> str:
    numeric = float(value or 0)
    return f"R$ {numeric:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _format_decimal_br(value, places: int = 2) -> str:
    numeric = float(value or 0)
    return f"{numeric:,.{places}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _stock_report_totals(rows: list[dict]) -> dict:
    entries_total = sum(float(row.get("total_cost") or 0) for row in rows if row.get("kind") == "entrada")
    outputs_total = sum(float(row.get("total_cost") or 0) for row in rows if row.get("kind") == "saida")
    return {
        "entries_total": round(entries_total, 2),
        "outputs_total": round(outputs_total, 2),
        "grand_total": round(entries_total + outputs_total, 2),
        "movements_count": len(rows),
    }


def _filter_equipment_assets_by_status(assets: list[EquipmentAsset], status_value: str | None) -> list[EquipmentAsset]:
    normalized_status = status_value if status_value in {"ativo", "em_manutencao", "baixado"} else None
    if not normalized_status:
        return assets
    return [asset for asset in assets if asset.status == normalized_status]


def _bool_from_form(value) -> bool:
    return str(value or "").lower() in {"1", "true", "on", "yes"}


def _meters_from_cm(value: str | None):
    centimeters = _float_or_none(value)
    return round(centimeters / 100, 4) if centimeters is not None else None


def _normalize_irrigation_type(value: str | None) -> str:
    normalized = (value or "none").strip().lower()
    return normalized if normalized in {"none", "gotejo", "aspersor"} else "none"


def _resolve_geojson(upload: UploadFile | None, fallback_text: str | None, current_value: str | None = None) -> tuple[str | None, bool]:
    if upload and upload.filename:
        raw_bytes = upload.file.read()
        parsed = extract_geojson_file(raw_bytes)
        return parsed, parsed is not None
    normalized = normalize_geojson(fallback_text)
    if normalized is not None:
        return normalized, True
    return current_value, False


def _parse_fertilization_items(values) -> list[dict]:
    input_ids = values.getlist("input_id")
    names = values.getlist("item_name")
    units = values.getlist("item_unit")
    quantities = values.getlist("item_quantity")
    items: list[dict] = []
    for index, name in enumerate(names):
        input_id = input_ids[index] if index < len(input_ids) else ""
        unit = units[index] if index < len(units) else ""
        quantity = quantities[index] if index < len(quantities) else ""
        if not (name or "").strip():
            continue
        items.append(
            {
                "input_id": _int_or_none(input_id),
                "purchased_input_id": None,
                "name": name,
                "unit": unit,
                "quantity": quantity,
            }
        )
    return items


def _legacy_fertilization_items(record: FertilizationRecord | None, plot_area: float | None) -> list[dict]:
    if not record:
        return [{"input_id": "", "purchased_input_id": "", "name": "", "unit": "kg", "quantity": "", "total_quantity": "", "available": ""}]
    if record.items:
        return [
            {
                "input_id": item.input_id or "",
                "purchased_input_id": item.purchased_input_id or "",
                "name": item.name,
                "unit": item.unit,
                "quantity": float(item.quantity_per_hectare),
                "total_quantity": float(item.total_quantity),
                "available": float(item.purchased_input.available_quantity) if item.purchased_input and item.purchased_input.available_quantity is not None else "",
            }
            for item in record.items
        ]
    quantity = ""
    if record.dose:
        quantity = record.dose.split(" ")[0].replace(",", ".")
    quantity_value = _float_or_none(quantity)
    return [
        {
            "input_id": "",
            "purchased_input_id": "",
            "name": record.product,
            "unit": "kg",
            "quantity": quantity_value if quantity_value is not None else "",
            "total_quantity": quantity_value if quantity_value is not None else "",
            "available": "",
        }
    ]


def _parse_recommendation_items(values) -> list[dict]:
    input_ids = values.getlist("input_id")
    units = values.getlist("unit")
    quantities = values.getlist("quantity")
    items: list[dict] = []
    for index, input_id in enumerate(input_ids):
        quantity = quantities[index] if index < len(quantities) else ""
        unit = units[index] if index < len(units) else ""
        if input_id in ("", None) or quantity in ("", None):
            continue
        items.append(
            {
                "input_id": int(input_id),
                "unit": unit,
                "quantity": float(quantity),
            }
        )
    return items


def _build_plot_payload(
    farm: Farm,
    name: str,
    area_hectares: float,
    planting_date: str | None,
    plant_count: int,
    spacing_row_meters: str | None,
    spacing_plant_centimeters: str | None,
    estimated_yield_sacks: str | None,
    variety_id: str | None,
    centroid_lat: str | None,
    centroid_lng: str | None,
    boundary_geojson: str | None,
    notes: str | None,
    irrigation_type: str,
    irrigation_line_count: str | None,
    irrigation_line_length_meters: str | None,
    drip_spacing_centimeters: str | None,
    drip_liters_per_hour: str | None,
    sprinkler_count: str | None,
    sprinkler_liters_per_hour: str | None,
) -> dict:
    auto_lat, auto_lng = estimate_geojson_centroid(boundary_geojson)
    normalized_irrigation_type = _normalize_irrigation_type(irrigation_type)
    payload = {
        "name": name,
        "area_hectares": area_hectares,
        "location": farm.location,
        "planting_date": planting_date,
        "plant_count": plant_count,
        "spacing_row_meters": _float_or_none(spacing_row_meters),
        "spacing_plant_meters": _meters_from_cm(spacing_plant_centimeters),
        "estimated_yield_sacks": _float_or_none(estimated_yield_sacks),
        "variety_id": _int_or_none(variety_id),
        "centroid_lat": _float_or_none(centroid_lat) if centroid_lat not in (None, "") else auto_lat,
        "centroid_lng": _float_or_none(centroid_lng) if centroid_lng not in (None, "") else auto_lng,
        "boundary_geojson": boundary_geojson,
        "notes": notes,
        "farm_id": farm.id,
        "irrigation_type": normalized_irrigation_type,
        "irrigation_line_count": _int_or_none(irrigation_line_count),
        "irrigation_line_length_meters": _float_or_none(irrigation_line_length_meters),
        "drip_spacing_meters": _meters_from_cm(drip_spacing_centimeters),
        "drip_liters_per_hour": _float_or_none(drip_liters_per_hour),
        "sprinkler_count": _int_or_none(sprinkler_count),
        "sprinkler_liters_per_hour": _float_or_none(sprinkler_liters_per_hour),
    }
    if normalized_irrigation_type != "gotejo":
        payload["irrigation_line_length_meters"] = None
        payload["drip_spacing_meters"] = None
        payload["drip_liters_per_hour"] = None
    if normalized_irrigation_type != "aspersor":
        payload["sprinkler_count"] = None
        payload["sprinkler_liters_per_hour"] = None
    if normalized_irrigation_type == "none":
        payload["irrigation_line_count"] = None
    return payload


def _read_upload(upload: UploadFile | None) -> tuple[str | None, str | None, bytes | None]:
    if not upload or not upload.filename:
        return None, None, None
    try:
        upload.file.seek(0)
    except Exception:
        pass
    payload = upload.file.read()
    if not payload:
        return None, None, None
    return upload.filename, upload.content_type or "application/octet-stream", payload


_ALLOWED_ATTACHMENT_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".bmp",
    ".pdf",
    ".txt",
    ".csv",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".odt",
    ".ods",
}
_MAX_ATTACHMENT_SIZE_BYTES = 10 * 1024 * 1024


def _clean_attachment_filename(filename: str | None) -> str:
    cleaned = Path((filename or "arquivo").strip()).name
    return cleaned[:255] or "arquivo"


def _read_attachments(uploads: list[UploadFile] | None) -> list[tuple[str, str, bytes]]:
    attachments: list[tuple[str, str, bytes]] = []
    for upload in uploads or []:
        filename, content_type, payload = _read_upload(upload)
        if not filename or payload is None:
            continue
        extension = Path(filename).suffix.lower()
        if extension not in _ALLOWED_ATTACHMENT_EXTENSIONS and not (content_type or "").startswith("image/"):
            raise ValueError("Envie apenas imagens, PDF ou documentos comuns.")
        if len(payload) > _MAX_ATTACHMENT_SIZE_BYTES:
            raise ValueError("Cada anexo deve ter no maximo 10 MB.")
        attachments.append((_clean_attachment_filename(filename), content_type or "application/octet-stream", payload))
    return attachments


def _save_purchased_input_attachments(
    repo: FarmRepository,
    item: PurchasedInput,
    attachments: list[tuple[str, str, bytes]],
) -> int:
    if not attachments:
        return 0
    repo.db.add_all(
        [
            PurchasedInputAttachment(
                purchased_input_id=item.id,
                filename=filename,
                content_type=content_type,
                file_data=payload,
            )
            for filename, content_type, payload in attachments
        ]
    )
    try:
        repo.db.commit()
    except Exception:
        repo.db.rollback()
        raise
    repo.db.refresh(item)
    return len(attachments)


def _save_equipment_asset_attachments(
    repo: FarmRepository,
    asset: EquipmentAsset,
    attachments: list[tuple[str, str, bytes]],
) -> int:
    if not attachments:
        return 0
    repo.db.add_all(
        [
            EquipmentAssetAttachment(
                equipment_asset_id=asset.id,
                filename=filename,
                content_type=content_type,
                file_data=payload,
            )
            for filename, content_type, payload in attachments
        ]
    )
    try:
        repo.db.commit()
    except Exception:
        repo.db.rollback()
        raise
    repo.db.refresh(asset)
    return len(attachments)


async def _request_attachments(request: Request, field_name: str = "attachments") -> list[UploadFile]:
    form = await request.form()
    uploads: list[UploadFile] = []
    for value in form.getlist(field_name):
        if isinstance(value, (UploadFile, StarletteUploadFile)) and getattr(value, "filename", None):
            uploads.append(value)
    return uploads


def _attachment_response(filename: str, content_type: str, payload: bytes) -> Response:
    safe_name = _clean_attachment_filename(filename).replace('"', "")
    return Response(
        content=payload,
        media_type=content_type or "application/octet-stream",
        headers={"Content-Disposition": f'inline; filename="{safe_name}"'},
    )


def _require_admin(request: Request, user: User) -> RedirectResponse | None:
    if has_admin_access(user):
        return None
    _flash(request, "error", "Apenas administradores podem acessar este modulo.")
    return _redirect("/dashboard")


def _soil_payload(
    farm_id: int,
    plot_id: int,
    analysis_date: str,
    laboratory: str,
    ph: str | None,
    organic_matter: str | None,
    phosphorus: str | None,
    potassium: str | None,
    calcium: str | None,
    magnesium: str | None,
    aluminum: str | None,
    h_al: str | None,
    ctc: str | None,
    base_saturation: str | None,
    observations: str | None,
    pdf_filename: str | None,
    pdf_content_type: str | None,
    pdf_data: bytes | None,
    current_analysis: SoilAnalysis | None = None,
) -> dict:
    payload = {
        "farm_id": farm_id,
        "plot_id": plot_id,
        "analysis_date": analysis_date,
        "laboratory": laboratory,
        "ph": _float_or_none(ph),
        "organic_matter": _float_or_none(organic_matter),
        "phosphorus": _float_or_none(phosphorus),
        "potassium": _float_or_none(potassium),
        "calcium": _float_or_none(calcium),
        "magnesium": _float_or_none(magnesium),
        "aluminum": _float_or_none(aluminum),
        "h_al": _float_or_none(h_al),
        "ctc": _float_or_none(ctc),
        "base_saturation": _float_or_none(base_saturation),
        "observations": observations,
        "pdf_filename": pdf_filename if pdf_filename is not None else (current_analysis.pdf_filename if current_analysis else None),
        "pdf_content_type": pdf_content_type if pdf_content_type is not None else (current_analysis.pdf_content_type if current_analysis else None),
        "pdf_data": pdf_data if pdf_data is not None else (current_analysis.pdf_data if current_analysis else None),
    }
    payload.update(calculate_soil_recommendations(payload))
    return payload


def _apply_soil_ai_recommendation(repo: FarmRepository, analysis: SoilAnalysis) -> SoilAnalysis:
    refreshed = repo.get_soil_analysis(analysis.id) or analysis
    ai_result = gerar_recomendacao_adubacao(refreshed)
    return repo.update(
        refreshed,
        {
            "ai_recommendation": ai_result["recommendation"],
            "ai_status": ai_result["status"],
            "ai_model": ai_result["model"],
            "ai_error": ai_result["error"],
            "ai_generated_at": ai_result["generated_at"],
        },
    )


def _soil_history_chart(analyses: list[SoilAnalysis]) -> str:
    ordered = list(reversed(sorted(analyses, key=lambda item: (item.analysis_date, item.id))))
    return json.dumps(
        {
            "labels": [item.analysis_date.isoformat() for item in ordered],
            "ph": [float(item.ph or 0) for item in ordered],
            "phosphorus": [float(item.phosphorus or 0) for item in ordered],
            "potassium": [float(item.potassium or 0) for item in ordered],
            "calcium": [float(item.calcium or 0) for item in ordered],
            "magnesium": [float(item.magnesium or 0) for item in ordered],
            "base_saturation": [float(item.base_saturation or 0) for item in ordered],
        }
    )


@router.get("/")
def home():
    return _redirect("/dashboard")


@router.post("/contexto")
def update_global_context(
    request: Request,
    csrf_token: str = Form(...),
    farm_id: str | None = Form(None),
    season_id: str | None = Form(None),
    redirect_to: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    validate_csrf(request, csrf_token)
    persist_user_context(request, db, user, _int_or_none(farm_id), _int_or_none(season_id))

    if not redirect_to or not redirect_to.startswith("/"):
        redirect_to = "/dashboard"
    return _redirect(redirect_to)


@router.get("/dashboard")
def dashboard(
    request: Request,
    rain_start_date: str | None = None,
    rain_end_date: str | None = None,
    irrigations_page: int = 1,
    rainfalls_page: int = 1,
    fertilizations_page: int = 1,
    incidents_page: int = 1,
    harvests_page: int = 1,
    forecast_page: int = 1,
    timeline_page: int = 1,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    data = build_dashboard_context(
        repo,
        rain_start_date=_date_or_none(rain_start_date),
        rain_end_date=_date_or_none(rain_end_date),
        farm_id=scope["active_farm_id"],
        season=scope["active_season"],
        pages={
            "irrigations": _page_number(irrigations_page),
            "rainfalls": _page_number(rainfalls_page),
            "fertilizations": _page_number(fertilizations_page),
            "incidents": _page_number(incidents_page),
            "harvests": _page_number(harvests_page),
            "forecast": _page_number(forecast_page),
            "timeline": _page_number(timeline_page),
        },
    )
    return templates.TemplateResponse(
        "dashboard.html",
        _base_context(
            request,
            user,
            csrf_token,
            "dashboard",
            rain_filters={
                "start_date": rain_start_date or "",
                "end_date": rain_end_date or "",
            },
            dashboard_page_values={
                "irrigations_page": _page_number(irrigations_page),
                "rainfalls_page": _page_number(rainfalls_page),
                "fertilizations_page": _page_number(fertilizations_page),
                "incidents_page": _page_number(incidents_page),
                "harvests_page": _page_number(harvests_page),
                "forecast_page": _page_number(forecast_page),
                "timeline_page": _page_number(timeline_page),
            },
            _repo=repo,
            **data,
        ),
    )


@router.get("/talhoes", include_in_schema=False)
@router.get("/setores")
def plots_page(
    request: Request,
    q: str | None = None,
    sort: str = "name",
    edit_id: int | None = None,
    selected_farm_id: int | None = None,
    open_farm_modal: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids = _int_list(request.query_params.getlist("farm_id"))
    variety_ids = _int_list(request.query_params.getlist("variety_id"))
    if not farm_ids and scope["active_farm_id"]:
        farm_ids = [scope["active_farm_id"]]
    if not variety_ids and scope["active_season"] and scope["active_season"].variety_id:
        variety_ids = [scope["active_season"].variety_id]
    edit_plot = repo.get_plot(edit_id) if edit_id else None
    farms, varieties = repo.list_plot_filter_options(farm_ids or None, variety_ids or None)
    return templates.TemplateResponse(
        "plots.html",
        _base_context(
            request,
            user,
            csrf_token,
            "plots",
            plots=repo.list_plots(search=q, farm_ids=farm_ids, variety_ids=variety_ids, sort=sort),
            farms=farms,
            varieties=varieties,
            filters={"q": q or "", "farm_ids": farm_ids, "variety_ids": variety_ids, "sort": sort},
            edit_plot=edit_plot,
            selected_farm_id=selected_farm_id or scope["active_farm_id"],
            open_farm_modal=bool(open_farm_modal),
            filter_links=[
                {"farm_id": plot.farm_id, "variety_id": plot.variety_id}
                for plot in repo.list_plots(farm_ids=farm_ids or None, variety_ids=variety_ids or None)
                if plot.farm_id or plot.variety_id
            ],
            _repo=repo,
        ),
    )


@router.post("/talhoes", include_in_schema=False)
@router.post("/setores")
def create_plot_action(
    request: Request,
    csrf_token: str = Form(...),
    name: str = Form(...),
    area_hectares: float = Form(...),
    farm_id: str | None = Form(None),
    planting_date: str | None = Form(None),
    plant_count: int = Form(...),
    spacing_row_meters: str | None = Form(None),
    spacing_plant_centimeters: str | None = Form(None),
    estimated_yield_sacks: str | None = Form(None),
    variety_id: str | None = Form(None),
    centroid_lat: str | None = Form(None),
    centroid_lng: str | None = Form(None),
    boundary_geojson: str | None = Form(None),
    boundary_geojson_file: UploadFile | None = File(None),
    irrigation_type: str | None = Form("none"),
    irrigation_line_count: str | None = Form(None),
    irrigation_line_length_meters: str | None = Form(None),
    drip_spacing_centimeters: str | None = Form(None),
    drip_liters_per_hour: str | None = Form(None),
    sprinkler_count: str | None = Form(None),
    sprinkler_liters_per_hour: str | None = Form(None),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    selected_farm_id = _int_or_none(farm_id)
    if not selected_farm_id:
        _flash(request, "error", "Selecione uma fazenda antes de salvar o setor.")
        return _redirect("/setores")
    farm = repo.get_farm(selected_farm_id)
    if not farm:
        _flash(request, "error", "A fazenda selecionada nao foi encontrada.")
        return _redirect("/setores")
    geometry, geometry_ok = _resolve_geojson(boundary_geojson_file, boundary_geojson)
    if boundary_geojson_file and boundary_geojson_file.filename and not geometry_ok:
        _flash(request, "error", "O arquivo GeoJSON do setor nao e valido.")
        return _redirect_with_query("/setores", selected_farm_id=selected_farm_id)
    create_plot(
        repo,
        _build_plot_payload(
            farm=farm,
            name=name,
            area_hectares=area_hectares,
            planting_date=planting_date,
            plant_count=plant_count,
            spacing_row_meters=spacing_row_meters,
            spacing_plant_centimeters=spacing_plant_centimeters,
            estimated_yield_sacks=estimated_yield_sacks,
            variety_id=variety_id,
            centroid_lat=centroid_lat,
            centroid_lng=centroid_lng,
            boundary_geojson=geometry,
            notes=notes,
            irrigation_type=irrigation_type,
            irrigation_line_count=irrigation_line_count,
            irrigation_line_length_meters=irrigation_line_length_meters,
            drip_spacing_centimeters=drip_spacing_centimeters,
            drip_liters_per_hour=drip_liters_per_hour,
            sprinkler_count=sprinkler_count,
            sprinkler_liters_per_hour=sprinkler_liters_per_hour,
        ),
    )
    _flash(request, "success", "Setor salvo com sucesso.")
    return _redirect("/setores")


@router.post("/talhoes/{plot_id}/editar", include_in_schema=False)
@router.post("/setores/{plot_id}/editar")
def update_plot_action(
    plot_id: int,
    request: Request,
    csrf_token: str = Form(...),
    name: str = Form(...),
    area_hectares: float = Form(...),
    farm_id: int = Form(...),
    planting_date: str | None = Form(None),
    plant_count: int = Form(...),
    spacing_row_meters: str | None = Form(None),
    spacing_plant_centimeters: str | None = Form(None),
    estimated_yield_sacks: str | None = Form(None),
    variety_id: str | None = Form(None),
    centroid_lat: str | None = Form(None),
    centroid_lng: str | None = Form(None),
    boundary_geojson: str | None = Form(None),
    boundary_geojson_file: UploadFile | None = File(None),
    irrigation_type: str | None = Form("none"),
    irrigation_line_count: str | None = Form(None),
    irrigation_line_length_meters: str | None = Form(None),
    drip_spacing_centimeters: str | None = Form(None),
    drip_liters_per_hour: str | None = Form(None),
    sprinkler_count: str | None = Form(None),
    sprinkler_liters_per_hour: str | None = Form(None),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    plot = repo.get_plot(plot_id)
    if not plot:
        _flash(request, "error", "Setor nao encontrado.")
        return _redirect("/setores")
    farm = repo.get_farm(farm_id)
    if not farm:
        _flash(request, "error", "A fazenda selecionada nao foi encontrada.")
        return _redirect("/setores")
    geometry, geometry_ok = _resolve_geojson(boundary_geojson_file, boundary_geojson, plot.boundary_geojson)
    if boundary_geojson_file and boundary_geojson_file.filename and not geometry_ok:
        _flash(request, "error", "O arquivo GeoJSON do setor nao e valido.")
        return _redirect_with_query("/setores", edit_id=plot_id)
    update_plot(
        repo,
        plot,
        _build_plot_payload(
            farm=farm,
            name=name,
            area_hectares=area_hectares,
            planting_date=planting_date,
            plant_count=plant_count,
            spacing_row_meters=spacing_row_meters,
            spacing_plant_centimeters=spacing_plant_centimeters,
            estimated_yield_sacks=estimated_yield_sacks,
            variety_id=variety_id,
            centroid_lat=centroid_lat,
            centroid_lng=centroid_lng,
            boundary_geojson=geometry,
            notes=notes,
            irrigation_type=irrigation_type,
            irrigation_line_count=irrigation_line_count,
            irrigation_line_length_meters=irrigation_line_length_meters,
            drip_spacing_centimeters=drip_spacing_centimeters,
            drip_liters_per_hour=drip_liters_per_hour,
            sprinkler_count=sprinkler_count,
            sprinkler_liters_per_hour=sprinkler_liters_per_hour,
        ),
    )
    _flash(request, "success", "Setor atualizado com sucesso.")
    return _redirect("/setores")


@router.post("/talhoes/{plot_id}/excluir", include_in_schema=False)
@router.post("/setores/{plot_id}/excluir")
def delete_plot_action(
    plot_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    plot = repo.get_plot(plot_id)
    if not plot:
        _flash(request, "error", "Setor nao encontrado.")
        return _redirect("/setores")
    repo.delete(plot)
    _flash(request, "success", "Setor excluido com sucesso.")
    return _redirect("/setores")


@router.get("/fazendas")
def farms_page(
    request: Request,
    background_tasks: BackgroundTasks,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    edit_farm = repo.get_farm(edit_id) if edit_id else None
    farms = repo.list_farms()
    farm_preview_ready: dict[int, bool] = {}
    farm_preview_fingerprint: dict[int, str] = {}
    for farm in farms:
        if not farm.boundary_geojson:
            continue
        farm_preview_fingerprint[farm.id] = hashlib.sha256(farm.boundary_geojson.encode("utf-8")).hexdigest()[:14]
        preview_path = farm_preview_fs_path(farm.id)
        farm_preview_ready[farm.id] = preview_path.is_file() and preview_path.stat().st_size > 0
        if not farm_preview_ready[farm.id]:
            background_tasks.add_task(generate_farm_preview_image, farm.id, farm.boundary_geojson)
    farm_boundary_by_id: dict[str, object] = {}
    for farm in farms:
        if not farm.boundary_geojson:
            continue
        try:
            farm_boundary_by_id[str(farm.id)] = json.loads(farm.boundary_geojson)
        except json.JSONDecodeError:
            pass
    farm_boundary_geometries_json = json.dumps(farm_boundary_by_id)
    edit_farm_geometry_json = "null"
    if edit_farm and edit_farm.boundary_geojson:
        try:
            edit_farm_geometry_json = json.dumps(json.loads(edit_farm.boundary_geojson))
        except json.JSONDecodeError:
            edit_farm_geometry_json = "null"
    google_maps_web_key = (get_settings().google_maps_api_key or "").strip()
    return templates.TemplateResponse(
        "farms.html",
        _base_context(
            request,
            user,
            csrf_token,
            "farms",
            farms=farms,
            edit_farm=edit_farm,
            farm_preview_ready=farm_preview_ready,
            farm_preview_fingerprint=farm_preview_fingerprint,
            farm_boundary_geometries_json=farm_boundary_geometries_json,
            edit_farm_geometry_json=edit_farm_geometry_json,
            google_maps_web_key=google_maps_web_key,
            _repo=repo,
        ),
    )


@router.get("/fazendas/geocodificar")
def geocode_farm_location(
    q: str = "",
    user: User = Depends(get_current_user_web),
):
    """Geocodifica texto de localização (Nominatim/OSM) para centralizar o mapa do cadastro."""
    del user
    query = (q or "").strip()
    if len(query) < 2:
        return JSONResponse({"ok": False, "code": "empty"}, status_code=400)
    params = urlencode({"format": "json", "limit": "1", "q": query})
    url = f"https://nominatim.openstreetmap.org/search?{params}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "FazendaBelaVista/1.0 (+https://github.com/renan087/fazenda_bela_vista)",
            "Accept-Language": "pt-BR,pt;q=0.9",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        logging.getLogger(__name__).warning("Nominatim HTTP %s", exc.code)
        return JSONResponse({"ok": False, "code": "upstream"}, status_code=502)
    except Exception:
        logging.getLogger(__name__).exception("geocode nominatim")
        return JSONResponse({"ok": False, "code": "upstream"}, status_code=502)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return JSONResponse({"ok": False, "code": "upstream"}, status_code=502)
    if not data:
        return JSONResponse({"ok": False, "code": "not_found"})
    row = data[0]
    try:
        lat = float(row["lat"])
        lng = float(row["lon"])
    except (KeyError, TypeError, ValueError):
        return JSONResponse({"ok": False, "code": "not_found"})
    return JSONResponse({"ok": True, "lat": lat, "lng": lng})


@router.post("/fazendas")
def create_farm_action(
    request: Request,
    csrf_token: str = Form(...),
    name: str = Form(...),
    location: str = Form(...),
    total_area: float = Form(...),
    boundary_geojson: str | None = Form(None),
    boundary_geojson_file: UploadFile | None = File(None),
    notes: str | None = Form(None),
    redirect_to: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    geometry, geometry_ok = _resolve_geojson(boundary_geojson_file, boundary_geojson)
    if boundary_geojson_file and boundary_geojson_file.filename and not geometry_ok:
        _flash(request, "error", "O arquivo GeoJSON da fazenda nao e valido.")
        return _redirect_with_query("/setores", open_farm_modal=1) if redirect_to == "/setores" else _redirect("/fazendas")
    farm = create_farm(
        _repository(db),
        {
            "name": name,
            "location": location,
            "total_area": total_area,
            "boundary_geojson": geometry,
            "notes": notes,
        },
    )
    if geometry:
        try:
            generate_farm_preview_image(farm.id, geometry)
        except Exception:
            logging.getLogger(__name__).exception("Falha ao gerar imagem de satelite da fazenda %s", farm.id)
    _flash(request, "success", "Fazenda cadastrada com sucesso.")
    if redirect_to == "/setores":
        return _redirect_with_query("/setores", selected_farm_id=farm.id)
    return _redirect("/fazendas")


@router.post("/fazendas/{farm_id}/editar")
def update_farm_action(
    farm_id: int,
    request: Request,
    csrf_token: str = Form(...),
    name: str = Form(...),
    location: str = Form(...),
    total_area: float = Form(...),
    boundary_geojson: str | None = Form(None),
    boundary_geojson_file: UploadFile | None = File(None),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    farm = repo.get_farm(farm_id)
    if not farm:
        _flash(request, "error", "Fazenda nao encontrada.")
        return _redirect("/fazendas")
    geometry, geometry_ok = _resolve_geojson(boundary_geojson_file, boundary_geojson, farm.boundary_geojson)
    if boundary_geojson_file and boundary_geojson_file.filename and not geometry_ok:
        _flash(request, "error", "O arquivo GeoJSON da fazenda nao e valido.")
        return _redirect_with_query("/fazendas", edit_id=farm_id)
    update_farm(
        repo,
        farm,
        {"name": name, "location": location, "total_area": total_area, "boundary_geojson": geometry, "notes": notes},
    )
    if geometry:
        try:
            generate_farm_preview_image(farm.id, geometry)
        except Exception:
            logging.getLogger(__name__).exception("Falha ao gerar imagem de satelite da fazenda %s", farm.id)
    else:
        remove_farm_preview_image(farm.id)
    _flash(request, "success", "Fazenda atualizada com sucesso.")
    return _redirect("/fazendas")


@router.post("/fazendas/{farm_id}/geometria")
def update_farm_geometry_only(
    farm_id: int,
    request: Request,
    csrf_token: str = Form(...),
    boundary_geojson: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    farm = repo.get_farm(farm_id)
    if not farm:
        _flash(request, "error", "Fazenda nao encontrada.")
        return _redirect("/fazendas")
    normalized = normalize_geojson(boundary_geojson)
    if not normalized:
        _flash(request, "error", "A geometria enviada nao e valida.")
        return _redirect("/fazendas")
    update_farm(repo, farm, {"boundary_geojson": normalized})
    try:
        generate_farm_preview_image(farm.id, normalized)
    except Exception:
        logging.getLogger(__name__).exception("Falha ao gerar imagem de satelite da fazenda %s", farm.id)
    _flash(request, "success", "Geometria da fazenda atualizada.")
    return _redirect("/fazendas")


@router.post("/fazendas/{farm_id}/excluir")
def delete_farm_action(
    farm_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    farm = repo.get_farm(farm_id)
    if not farm:
        _flash(request, "error", "Fazenda nao encontrada.")
        return _redirect("/fazendas")
    if farm.plots:
        _flash(request, "error", "Nao e possivel excluir a fazenda enquanto houver setores vinculados.")
        return _redirect("/fazendas")
    remove_farm_preview_image(farm_id)
    repo.delete(farm)
    _flash(request, "success", "Fazenda excluida com sucesso.")
    return _redirect("/fazendas")


@router.get("/usuarios")
def users_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    denied = _require_admin(request, user)
    if denied:
        return denied
    repo = _repository(db)
    pending_super_admin_two_factor_disable = None
    pending_super_admin_two_factor_disable_confirmed = False
    pending_payload = _get_super_admin_2fa_disable_pending(request)
    if pending_payload and int(pending_payload.get("actor_user_id") or 0) == int(user.id):
        pending_target_user_id = int(pending_payload.get("target_user_id") or 0)
        pending_confirmed = bool(pending_payload.get("confirmed"))
        if not pending_confirmed and not get_active_login_code(db, user.id):
            _clear_super_admin_2fa_disable_pending(request)
        elif edit_id and pending_target_user_id == edit_id:
            pending_super_admin_two_factor_disable = pending_payload
            pending_super_admin_two_factor_disable_confirmed = pending_confirmed
    return templates.TemplateResponse(
        "users.html",
        _base_context(
            request,
            user,
            csrf_token,
            "users",
            title="Administracao de Usuarios",
            users=repo.list_users(),
            edit_user=repo.get_user(edit_id) if edit_id else None,
            format_app_datetime=format_app_datetime,
            super_admin_email=(settings.super_admin_email or settings.admin_email or "").strip().lower(),
            pending_super_admin_two_factor_disable=pending_super_admin_two_factor_disable,
            pending_super_admin_two_factor_disable_confirmed=pending_super_admin_two_factor_disable_confirmed,
            _repo=repo,
        ),
    )


@router.get("/usuarios/{user_id}/avatar")
def user_avatar_view(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    denied = _require_admin(request, user)
    if denied:
        return denied
    repo = _repository(db)
    target_user = repo.get_user(user_id)
    if not target_user or not target_user.avatar_data:
        return Response(status_code=404)
    return Response(
        content=target_user.avatar_data,
        media_type=target_user.avatar_content_type or "image/jpeg",
    )


@router.post("/usuarios")
def create_user_action(
    request: Request,
    csrf_token: str = Form(...),
    name: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    is_active: str | None = Form(None),
    is_admin: str | None = Form(None),
    is_two_factor_enabled: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    denied = _require_admin(request, user)
    if denied:
        return denied
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    existing = db.query(User).filter(User.email == email.strip().lower()).first()
    if existing:
        _flash(request, "error", "Ja existe um usuario com este email.")
        return _redirect("/usuarios")
    create_user(
        repo,
        {
            "name": name,
            "email": email,
            "password": password,
            "is_active": _bool_from_form(is_active),
            "is_admin": _bool_from_form(is_admin),
            "is_two_factor_enabled": _bool_from_form(is_two_factor_enabled),
        },
    )
    _flash(request, "success", "Usuario criado com sucesso.")
    return _redirect("/usuarios")


@router.post("/usuarios/{user_id}/editar")
def update_user_action(
    user_id: int,
    request: Request,
    csrf_token: str = Form(...),
    name: str = Form(...),
    email: str = Form(...),
    password: str | None = Form(None),
    is_active: str | None = Form(None),
    is_admin: str | None = Form(None),
    is_two_factor_enabled: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    denied = _require_admin(request, user)
    if denied:
        return denied
    try:
        validate_csrf(request, csrf_token)
        normalized_name = (name or "").strip()
        normalized_email = (email or "").strip().lower()
        if not normalized_name or not normalized_email:
            _flash(request, "error", "Nome e email sao obrigatorios para atualizar o usuario.")
            return _redirect_with_query("/usuarios", edit_id=user_id)

        repo = _repository(db)
        target_user = repo.get_user(user_id)
        if not target_user:
            _flash(request, "error", "Usuario nao encontrado.")
            return _redirect("/usuarios")

        existing = db.query(User).filter(User.email == normalized_email, User.id != user_id).first()
        if existing:
            _flash(request, "error", "Ja existe outro usuario com este email.")
            return _redirect_with_query("/usuarios", edit_id=user_id)

        requested_is_admin = _bool_from_form(is_admin)
        requested_is_two_factor_enabled = _bool_from_form(is_two_factor_enabled)
        # Só exige fluxo de código ao desligar 2FA que ainda está ativo no banco (evita bloquear edição quando já está desligado).
        pending_super_admin_two_factor_disable = (
            is_super_admin_email(normalized_email)
            and not requested_is_two_factor_enabled
            and bool(target_user.is_two_factor_enabled)
        )
        confirmed_super_admin_two_factor_disable = _super_admin_2fa_disable_pending_confirmed(
            request,
            target_user_id=user_id,
            actor_user_id=user.id,
        )

        updated_user = update_user(
            repo,
            target_user,
            {
                "name": normalized_name,
                "email": normalized_email,
                "password": password,
                "is_active": _bool_from_form(is_active),
                "is_admin": requested_is_admin,
                "is_two_factor_enabled": requested_is_two_factor_enabled,
                "allow_super_admin_two_factor_disable": pending_super_admin_two_factor_disable and confirmed_super_admin_two_factor_disable,
            },
        )

        if pending_super_admin_two_factor_disable and confirmed_super_admin_two_factor_disable:
            _discard_super_admin_2fa_disable_pending(request, db, user.id)
        elif _has_super_admin_2fa_disable_pending(request, target_user_id=updated_user.id, actor_user_id=user.id):
            _discard_super_admin_2fa_disable_pending(request, db, user.id)

        if (password or "").strip():
            revoke_user_trusted_browsers(db, updated_user.id)
        if not updated_user.is_two_factor_enabled:
            revoke_active_login_codes(db, updated_user.id)

        if updated_user.id == user.id:
            request.session["user_email"] = updated_user.email
            if not has_admin_access(updated_user):
                _flash(request, "success", "Usuario atualizado com sucesso.")
                return _redirect("/dashboard")

        if pending_super_admin_two_factor_disable and not confirmed_super_admin_two_factor_disable:
            _flash(
                request,
                "error",
                "Confirme antes a desabilitacao do 2FA para concluir essa alteracao.",
            )
            return _redirect_with_query("/usuarios", edit_id=user_id)

        _flash(request, "success", "Usuario atualizado com sucesso.")
        return _redirect("/usuarios")
    except Exception:
        db.rollback()
        _flash(request, "error", "Nao foi possivel atualizar o usuario agora. Revise os dados e tente novamente.")
        return _redirect_with_query("/usuarios", edit_id=user_id)


@router.post("/usuarios/{user_id}/confirmar-desabilitar-2fa")
def confirm_super_admin_two_factor_disable_action(
    user_id: int,
    request: Request,
    csrf_token: str = Form(...),
    code: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    denied = _require_admin(request, user)
    if denied:
        return denied
    validate_csrf(request, csrf_token)

    if not _has_super_admin_2fa_disable_pending(request, target_user_id=user_id, actor_user_id=user.id):
        return JSONResponse({"ok": False, "message": "Nao ha confirmacao pendente para desabilitar o 2FA deste usuario."}, status_code=400)

    normalized_code = "".join(char for char in (code or "") if char.isdigit())[:6]
    if len(normalized_code) != 6:
        _flash(request, "error", "Informe um codigo numerico de 6 digitos.")
        return _redirect_with_query("/usuarios", edit_id=user_id)

    valid, message = verify_login_code(db, user.id, normalized_code)
    if not valid:
        if not get_active_login_code(db, user.id):
            _clear_super_admin_2fa_disable_pending(request)
        return JSONResponse({"ok": False, "message": message}, status_code=400)

    repo = _repository(db)
    target_user = repo.get_user(user_id)
    if not target_user:
        _discard_super_admin_2fa_disable_pending(request, db, user.id)
        return JSONResponse({"ok": False, "message": "Usuario nao encontrado."}, status_code=404)
    if not is_super_admin_email(target_user.email):
        _discard_super_admin_2fa_disable_pending(request, db, user.id)
        return JSONResponse({"ok": False, "message": "A confirmacao pendente nao e mais valida para este usuario."}, status_code=400)

    _mark_super_admin_2fa_disable_confirmed(request, target_user_id=user_id, actor_user_id=user.id)
    return JSONResponse({"ok": True, "message": "Codigo confirmado. Agora clique em Salvar para concluir a alteracao."})


@router.post("/usuarios/{user_id}/iniciar-desabilitar-2fa")
def start_super_admin_two_factor_disable_action(
    user_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    denied = _require_admin(request, user)
    if denied:
        return denied
    validate_csrf(request, csrf_token)

    repo = _repository(db)
    target_user = repo.get_user(user_id)
    if not target_user or not is_super_admin_email(target_user.email):
        _discard_super_admin_2fa_disable_pending(request, db, user.id)
        return JSONResponse({"ok": False, "message": "Usuario nao encontrado para esta confirmacao."}, status_code=404)

    try:
        code = issue_login_verification_code(db, user)
        send_access_code_email(user.email, code)
    except Exception:
        db.rollback()
        _discard_super_admin_2fa_disable_pending(request, db, user.id)
        return JSONResponse(
            {"ok": False, "message": "Nao foi possivel enviar o codigo de confirmacao agora. Tente novamente."},
            status_code=500,
        )

    _mark_super_admin_2fa_disable_pending(request, target_user_id=user_id, actor_user_id=user.id)
    return JSONResponse({"ok": True, "message": "Enviamos um codigo de 6 digitos para o seu email."})


@router.post("/usuarios/{user_id}/cancelar-desabilitar-2fa")
def cancel_super_admin_two_factor_disable_action(
    user_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    denied = _require_admin(request, user)
    if denied:
        return denied
    validate_csrf(request, csrf_token)

    if _has_super_admin_2fa_disable_pending(request, target_user_id=user_id, actor_user_id=user.id):
        _discard_super_admin_2fa_disable_pending(request, db, user.id)

    return JSONResponse({"ok": True})


@router.post("/usuarios/{user_id}/excluir")
def delete_user_action(
    user_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    denied = _require_admin(request, user)
    if denied:
        return denied
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    target_user = repo.get_user(user_id)
    if not target_user:
        _flash(request, "error", "Usuario nao encontrado.")
        return _redirect("/usuarios")
    if target_user.id == user.id:
        _flash(request, "error", "Nao e permitido excluir o usuario atualmente logado.")
        return _redirect("/usuarios")
    repo.delete(target_user)
    _flash(request, "success", "Usuario excluido com sucesso.")
    return _redirect("/usuarios")


@router.get("/backups")
def backups_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    denied = _require_admin(request, user)
    if denied:
        return denied
    repo = _repository(db)
    page_param = request.query_params.get("page", "1")
    try:
        page = max(1, int(page_param or "1"))
    except (TypeError, ValueError):
        page = 1
    per_page = 10
    total_runs = repo.count_backup_runs()
    total_pages = max(1, (total_runs + per_page - 1) // per_page)
    page = min(page, total_pages)
    offset = (page - 1) * per_page
    history = [
        {"run": item, "details": _backup_details(item.details_json)}
        for item in repo.list_backup_runs(limit=per_page, offset=offset)
    ]
    return templates.TemplateResponse(
        "backups.html",
        _base_context(
            request,
            user,
            csrf_token,
            "backups",
            title="Backups do Sistema",
            backup_history=history,
            backup_db_bucket=settings.supabase_bucket_db,
            backup_files_bucket=settings.supabase_bucket_files,
            backup_page=page,
            backup_total_pages=total_pages,
            backup_total_runs=total_runs,
            backup_has_prev=page > 1,
            backup_has_next=page < total_pages,
            _repo=repo,
        ),
    )


@router.post("/backups/executar")
def execute_backup_action(
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    denied = _require_admin(request, user)
    if denied:
        return denied
    validate_csrf(request, csrf_token)
    run = execute_backup(db, initiated_by=user, trigger_source="web_manual")
    if run.status == "success":
        _flash(request, "success", "Backup concluido com sucesso no Supabase Storage.")
    elif run.status == "partial":
        _flash(request, "error", "Backup executado parcialmente. Revise o historico para ver os detalhes.")
    else:
        _flash(request, "error", "Backup nao concluido. Revise o historico para ver o erro detalhado.")
    return _redirect("/backups")


@router.post("/backups/{backup_run_id}/excluir")
def delete_backup_action(
    backup_run_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    denied = _require_admin(request, user)
    if denied:
        return denied
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    run = repo.get_backup_run(backup_run_id)
    page = request.query_params.get("page")
    if not run:
        _flash(request, "error", "Backup nao encontrado.")
        return _redirect_with_query("/backups", page=page)

    try:
        warnings = delete_backup_run(db, run)
    except Exception as exc:
        logger.exception("Falha ao excluir backup. run_id=%s", backup_run_id)
        _flash(request, "error", f"Nao foi possivel excluir o backup agora. {exc}")
        return _redirect_with_query("/backups", page=page)

    if warnings:
        _flash(request, "success", f"Backup removido do historico. {' '.join(warnings)}")
    else:
        _flash(request, "success", "Backup excluido com sucesso.")
    return _redirect_with_query("/backups", page=page)


@router.get("/meu-perfil")
def profile_page(
    request: Request,
    aba: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    active_profile_tab = "security" if (aba or "").strip().lower() == "seguranca" else "profile"
    pending_password_change = None
    if active_profile_tab == "security":
        if _has_password_change_pending(request, user.id):
            pending_password_change = get_active_password_change_verification(db, user.id)
            if not pending_password_change:
                _clear_password_change_pending(request)
        else:
            stale_password_change = get_active_password_change_verification(db, user.id)
            if stale_password_change:
                _discard_password_change_pending(request, db, user.id)
    elif _has_password_change_pending(request, user.id):
        _discard_password_change_pending(request, db, user.id)
    return _render_profile_page(
        request,
        user,
        csrf_token,
        repo,
        pending_password_change=pending_password_change,
        active_profile_tab=active_profile_tab,
    )


@router.post("/meu-perfil")
async def update_profile_action(
    request: Request,
    csrf_token: str = Form(...),
    name: str = Form(...),
    display_name: str | None = Form(None),
    gender: str | None = Form(None),
    birth_date: str | None = Form(None),
    phone: str | None = Form(None),
    email: str = Form(...),
    job_title: str | None = Form(None),
    notes: str | None = Form(None),
    avatar: UploadFile | None = File(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    validate_csrf(request, csrf_token)
    repo = _repository(db)

    normalized_name = _clean_text(name)
    normalized_email = (email or "").strip().lower()
    normalized_gender = (gender or "").strip()
    if normalized_gender and normalized_gender not in {option[0] for option in _profile_gender_options()}:
        normalized_gender = None

    if not normalized_name or not normalized_email:
        _flash(request, "error", "Nome completo e email sao obrigatorios.")
        return _redirect("/meu-perfil?aba=perfil")

    existing = db.query(User).filter(User.email == normalized_email, User.id != user.id).first()
    if existing:
        _flash(request, "error", "Ja existe outro usuario com este email.")
        return _redirect("/meu-perfil?aba=perfil")

    try:
        normalized_birth_date = _date_or_none(birth_date)
    except ValueError:
        _flash(request, "error", "Informe uma data de nascimento valida.")
        return _redirect("/meu-perfil?aba=perfil")

    avatar_payload, avatar_error = await _read_avatar_upload(avatar)
    if avatar_error:
        _flash(request, "error", avatar_error)
        return _redirect("/meu-perfil?aba=perfil")

    payload = {
        "name": normalized_name,
        "email": normalized_email,
        "display_name": _clean_text(display_name),
        "gender": normalized_gender or None,
        "birth_date": normalized_birth_date,
        "phone": _clean_text(phone),
        "job_title": _clean_text(job_title),
        "notes": (notes or "").strip() or None,
    }
    if avatar_payload:
        payload.update(avatar_payload)

    updated_user = repo.update(user, payload)
    request.session["user_email"] = updated_user.email
    _flash(request, "success", "Perfil atualizado com sucesso.")
    return _redirect("/meu-perfil?aba=perfil")


@router.post("/meu-perfil/avatar/remover")
def remove_profile_avatar_action(
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    repo.update(
        user,
        {
            "avatar_filename": None,
            "avatar_content_type": None,
            "avatar_data": None,
        },
    )
    _flash(request, "success", "Foto removida com sucesso.")
    return _redirect("/meu-perfil?aba=perfil")


@router.get("/meu-perfil/avatar")
def profile_avatar_view(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    current_user = _repository(db).get_user(user.id)
    if not current_user or not current_user.avatar_data:
        return Response(status_code=status.HTTP_404_NOT_FOUND)
    return Response(
        content=current_user.avatar_data,
        media_type=current_user.avatar_content_type or "image/jpeg",
        headers={"Cache-Control": "no-store"},
    )


@router.post("/meu-perfil/alterar-senha")
def change_own_password_action(
    request: Request,
    csrf_token: str = Form(...),
    current_password: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    validate_csrf(request, csrf_token)
    normalized_current_password = (current_password or "").strip()
    normalized_new_password = (new_password or "").strip()
    normalized_confirm_password = (confirm_password or "").strip()

    if not verify_password(normalized_current_password, user.hashed_password):
        _flash(request, "error", "A senha atual informada nao confere.")
        return _redirect("/meu-perfil?aba=seguranca")
    if not normalized_new_password:
        _flash(request, "error", "Informe a nova senha.")
        return _redirect("/meu-perfil?aba=seguranca")
    if normalized_new_password != normalized_confirm_password:
        _flash(request, "error", "A confirmacao da nova senha nao confere.")
        return _redirect("/meu-perfil?aba=seguranca")
    if verify_password(normalized_new_password, user.hashed_password):
        _flash(request, "error", "A nova senha precisa ser diferente da senha atual.")
        return _redirect("/meu-perfil?aba=seguranca")

    try:
        code = issue_password_change_verification(db, user, get_password_hash(normalized_new_password))
        send_password_change_code_email(user.email, code, get_settings().two_factor_code_minutes)
    except RuntimeError as exc:
        _discard_password_change_pending(request, db, user.id)
        _flash(request, "error", str(exc))
        return _redirect("/meu-perfil?aba=seguranca")
    except Exception:
        db.rollback()
        _discard_password_change_pending(request, db, user.id)
        _flash(request, "error", "Nao foi possivel enviar o codigo de confirmacao agora. Tente novamente.")
        return _redirect("/meu-perfil?aba=seguranca")

    _mark_password_change_pending(request, user.id)
    _flash(request, "success", "Enviamos um codigo de confirmacao para o seu email. Informe o codigo para concluir a alteracao da senha.")
    return _redirect("/meu-perfil?aba=seguranca")


@router.post("/meu-perfil/alterar-senha/confirmar")
def confirm_own_password_change_action(
    request: Request,
    csrf_token: str = Form(...),
    code: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    validate_csrf(request, csrf_token)
    normalized_code = "".join(char for char in (code or "") if char.isdigit())[:6]
    if len(normalized_code) != 6:
        _flash(request, "error", "Informe um codigo numerico de 6 digitos.")
        return _redirect("/meu-perfil?aba=seguranca")

    valid, message, verification = verify_password_change_code(db, user.id, normalized_code)
    if not valid or not verification:
        if not get_active_password_change_verification(db, user.id):
            _clear_password_change_pending(request)
        _flash(request, "error", message)
        return _redirect("/meu-perfil?aba=seguranca")

    repo = _repository(db)
    repo.update(user, {"hashed_password": verification.new_password_hash})
    revoke_password_change_verifications(db, user.id)
    _clear_password_change_pending(request)
    revoke_user_password_reset_tokens(db, user.id)
    revoke_user_trusted_browsers(db, user.id)
    revoke_active_login_codes(db, user.id)
    _flash(request, "success", "Senha atualizada com sucesso.")
    return _redirect("/meu-perfil?aba=seguranca")


@router.post("/meu-perfil/alterar-senha/cancelar")
async def cancel_own_password_change_action(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    form = await request.form()
    validate_csrf(request, str(form.get("csrf_token") or ""))
    _discard_password_change_pending(request, db, user.id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/insumos/comprados")
def purchased_inputs_page(
    request: Request,
    edit_id: int | None = None,
    item_type: str | None = None,
    farm_id: int | None = None,
    purchased_tab: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    effective_farm_id = farm_id or scope["active_farm_id"]
    edit_input = repo.get_purchased_input(edit_id) if edit_id else None
    selected_item_type = item_type if item_type in {"insumo_agricola", "combustivel", "all"} else None
    normalized_item_type = item_type if item_type in {"insumo_agricola", "combustivel"} else None
    if not normalized_item_type and edit_input and edit_input.input_catalog:
        normalized_item_type = edit_input.input_catalog.item_type
    if not normalized_item_type and selected_item_type != "all":
        normalized_item_type = "insumo_agricola"
    stock_context = _build_stock_context(repo, farm_id=effective_farm_id, item_type=normalized_item_type)
    purchase_entries = _sort_collection_desc(
        stock_context["purchase_entries"],
        lambda item: item.purchase_date,
        lambda item: item.id,
    )
    stock_outputs = _sort_collection_desc(
        stock_context["stock_outputs"],
        lambda item: item.movement_date,
        lambda item: item.id,
    )
    purchase_entries_pagination = _paginate_collection(request, purchase_entries, "entries_page")
    stock_outputs_pagination = _paginate_collection(request, stock_outputs, "outputs_page")
    selected_purchased_tab = str(request.query_params.get("purchased_tab") or purchased_tab or "entries")
    if selected_purchased_tab not in {"entries", "outputs"}:
        selected_purchased_tab = "entries"
    return templates.TemplateResponse(
        "purchased_inputs.html",
        _base_context(
            request,
            user,
            csrf_token,
            "purchased_inputs",
            _repo=repo,
            title="Gestão de Compras",
            farms=repo.list_farms(),
            selected_item_type=selected_item_type or normalized_item_type or "insumo_agricola",
            selected_farm_id=effective_farm_id,
            inputs=purchase_entries_pagination["items"],
            inputs_pagination=purchase_entries_pagination,
            inputs_catalog=stock_context["catalog_inputs"],
            input_stock=stock_context["input_stock"],
            stock_outputs=stock_outputs_pagination["items"],
            stock_outputs_pagination=stock_outputs_pagination,
            purchased_inputs_export_query=_purchased_inputs_export_query(
                farm_id=effective_farm_id,
                item_type=selected_item_type,
            ),
            edit_input=edit_input,
            selected_purchased_tab=selected_purchased_tab,
            purchased_tab_urls={
                "entries": _url_with_query(request, purchased_tab="entries"),
                "outputs": _url_with_query(request, purchased_tab="outputs"),
            },
        ),
    )


@router.post("/insumos/comprados")
async def create_purchased_input_action(
    request: Request,
    csrf_token: str = Form(...),
    farm_id: str | None = Form(None),
    item_type: str = Form("insumo_agricola"),
    name: str = Form(...),
    quantity_purchased: float = Form(...),
    package_size: float = Form(...),
    package_unit: str = Form(...),
    unit_price: float = Form(...),
    purchase_date: str | None = Form(None),
    low_stock_threshold: str | None = Form(None),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    scope, denied = _launch_scope_or_redirect(request, repo, "/insumos/comprados")
    if denied:
        return denied
    try:
        attachment_payloads = _read_attachments(await _request_attachments(request))
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect_with_query("/insumos/comprados", item_type=item_type)
    item = create_purchased_input(
        repo,
        {
            "farm_id": scope["active_farm_id"],
            "item_type": item_type,
            "name": name,
            "quantity_purchased": quantity_purchased,
            "package_size": package_size,
            "package_unit": package_unit,
            "unit_price": unit_price,
            "purchase_date": purchase_date,
            "low_stock_threshold": low_stock_threshold,
            "notes": notes,
        },
    )
    try:
        saved_attachments = _save_purchased_input_attachments(repo, item, attachment_payloads)
    except Exception:
        _flash(request, "error", "O lancamento foi salvo, mas nao foi possivel gravar os anexos agora.")
        return _redirect_with_query("/insumos/comprados", edit_id=item.id, item_type=item_type)
    if saved_attachments:
        _flash(request, "success", f"Insumo comprado cadastrado com sucesso. {saved_attachments} anexo(s) salvo(s).")
        return _redirect_with_query("/insumos/comprados", edit_id=item.id, item_type=item_type)
    _flash(request, "success", "Insumo comprado cadastrado com sucesso.")
    return _redirect_with_query("/insumos/comprados", item_type=item_type)


@router.post("/insumos/comprados/{input_id}/editar")
async def update_purchased_input_action(
    input_id: int,
    request: Request,
    csrf_token: str = Form(...),
    farm_id: str | None = Form(None),
    item_type: str = Form("insumo_agricola"),
    name: str = Form(...),
    quantity_purchased: float = Form(...),
    package_size: float = Form(...),
    package_unit: str = Form(...),
    unit_price: float = Form(...),
    purchase_date: str | None = Form(None),
    low_stock_threshold: str | None = Form(None),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    scope, denied = _launch_scope_or_redirect(request, repo, "/insumos/comprados")
    if denied:
        return denied
    item = repo.get_purchased_input(input_id)
    if not item:
        _flash(request, "error", "Insumo comprado nao encontrado.")
        return _redirect_for_request(request, "/insumos/comprados")
    if not _farm_matches_scope(item.farm_id, scope):
        _flash(request, "error", "Este lancamento de entrada nao pertence ao contexto ativo.")
        return _redirect_for_request(request, "/insumos/comprados")
    try:
        attachment_payloads = _read_attachments(await _request_attachments(request))
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect_for_request(request, "/insumos/comprados", edit_id=input_id, item_type=item_type)
    update_purchased_input(
        repo,
        item,
        {
            "farm_id": scope["active_farm_id"],
            "item_type": item_type,
            "name": name,
            "quantity_purchased": quantity_purchased,
            "package_size": package_size,
            "package_unit": package_unit,
            "unit_price": unit_price,
            "purchase_date": purchase_date,
            "low_stock_threshold": low_stock_threshold,
            "notes": notes,
        },
    )
    try:
        saved_attachments = _save_purchased_input_attachments(repo, item, attachment_payloads)
    except Exception:
        _flash(request, "error", "As alteracoes foram salvas, mas nao foi possivel incluir os novos anexos.")
        return _redirect_for_request(request, "/insumos/comprados", edit_id=input_id, item_type=item_type)
    if saved_attachments:
        _flash(request, "success", f"Alteracoes salvas com sucesso. {saved_attachments} novo(s) anexo(s) adicionado(s).")
        return _redirect_for_request(request, "/insumos/comprados", edit_id=input_id, item_type=item_type)
    _flash(request, "success", "Insumo comprado atualizado com sucesso.")
    return _redirect_for_request(request, "/insumos/comprados", item_type=item_type)


@router.post("/insumos/comprados/{input_id}/excluir")
def delete_purchased_input_action(
    input_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    item = repo.get_purchased_input(input_id)
    if not item:
        _flash(request, "error", "Insumo comprado nao encontrado.")
        return _redirect_for_request(request, "/insumos/comprados")
    if item.recommendations:
        _flash(request, "error", "Nao e possivel excluir o insumo enquanto houver recomendacoes vinculadas.")
        return _redirect("/insumos/comprados")
    if item.recommendation_items or item.schedule_items or item.stock_allocations or item.stock_outputs:
        _flash(request, "error", "Nao e possivel excluir o insumo enquanto houver recomendacoes, agendamentos ou aplicacoes vinculadas.")
        return _redirect("/insumos/comprados")
    repo.delete(item)
    _flash(request, "success", "Insumo comprado excluido com sucesso.")
    return _redirect("/insumos/comprados")


@router.get("/insumos/comprados/anexos/{attachment_id}")
def open_purchased_input_attachment(
    attachment_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    repo = _repository(db)
    attachment = repo.get_purchased_input_attachment(attachment_id)
    if not attachment or not attachment.purchased_input:
        _flash(request, "error", "Anexo nao encontrado.")
        return _redirect("/insumos/comprados")
    scope = _global_scope_context(request, repo)
    item = attachment.purchased_input
    if not _farm_matches_scope(item.farm_id, scope):
        _flash(request, "error", "Este anexo nao pertence ao contexto ativo.")
        return _redirect("/insumos/comprados")
    return _attachment_response(attachment.filename, attachment.content_type, attachment.file_data)


@router.post("/insumos/comprados/{input_id}/anexos/{attachment_id}/excluir")
def delete_purchased_input_attachment_action(
    input_id: int,
    attachment_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    item = repo.get_purchased_input(input_id)
    if not item:
        _flash(request, "error", "Insumo comprado nao encontrado.")
        return _redirect("/insumos/comprados")
    scope = _global_scope_context(request, repo)
    if not _farm_matches_scope(item.farm_id, scope):
        _flash(request, "error", "Este lancamento de entrada nao pertence ao contexto ativo.")
        return _redirect_for_request(request, "/insumos/comprados")
    attachment = repo.get_purchased_input_attachment(attachment_id)
    if not attachment or attachment.purchased_input_id != item.id:
        _flash(request, "error", "Anexo nao encontrado.")
        return _redirect_for_request(request, "/insumos/comprados", edit_id=input_id)
    repo.delete(attachment)
    if repo.get_purchased_input_attachment(attachment_id):
        _flash(request, "error", "Nao foi possivel remover o anexo agora. Tente novamente.")
        return _redirect_for_request(request, "/insumos/comprados", edit_id=input_id)
    _flash(request, "success", "Anexo removido com sucesso.")
    item_type = item.input_catalog.item_type if item.input_catalog else None
    return _redirect_for_request(request, "/insumos/comprados", edit_id=input_id, item_type=item_type)


@router.get("/insumos/comprados/exportar.xlsx")
def export_purchased_inputs_xlsx(
    request: Request,
    farm_id: str | None = None,
    item_type: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    repo = _repository(db)
    selected_farm_id = _int_or_none(farm_id) or _active_farm_id(request)
    normalized_item_type = item_type if item_type in {"insumo_agricola", "combustivel"} else None
    stock_context = _build_stock_context(repo, farm_id=selected_farm_id, item_type=normalized_item_type)
    purchase_entries = _sort_collection_desc(
        stock_context["purchase_entries"],
        lambda item: item.purchase_date,
        lambda item: item.id,
    )
    stock_outputs = _sort_collection_desc(
        stock_context["stock_outputs"],
        lambda item: item.movement_date,
        lambda item: item.id,
    )

    workbook = Workbook()
    entries_sheet = workbook.active
    entries_sheet.title = "Entradas"
    entries_sheet.append(["Data", "Insumo", "Tipo", "Fazenda", "Quantidade", "Saldo", "Valor", "Observações"])
    for item in purchase_entries:
        entries_sheet.append([
            item.purchase_date.isoformat() if item.purchase_date else "",
            item.input_catalog.name if item.input_catalog else item.name,
            "Combustível" if item.input_catalog and item.input_catalog.item_type == "combustivel" else "Insumo agrícola",
            item.farm.name if item.farm else "",
            float(item.total_quantity or 0),
            float(item.available_quantity or 0),
            float(item.total_cost or 0),
            item.notes or "",
        ])
    for index, width in enumerate([14, 30, 18, 24, 16, 16, 16, 40], start=1):
        entries_sheet.column_dimensions[get_column_letter(index)].width = width

    outputs_sheet = workbook.create_sheet("Saídas")
    outputs_sheet.append(["Data", "Insumo", "Tipo", "Origem", "Fazenda / Setor", "Quantidade", "Custo", "Observações"])
    for output in stock_outputs:
        outputs_sheet.append([
            output.movement_date.isoformat() if output.movement_date else "",
            output.input_catalog.name if output.input_catalog else "Insumo removido",
            "Combustível" if output.input_catalog and output.input_catalog.item_type == "combustivel" else "Insumo agrícola",
            output.origin or "",
            f"{output.farm.name if output.farm else ''}{(' / ' + output.plot.name) if output.plot else ''}",
            float(output.quantity or 0),
            float(output.total_cost or 0),
            output.notes or "",
        ])
    for index, width in enumerate([14, 30, 18, 18, 28, 16, 16, 40], start=1):
        outputs_sheet.column_dimensions[get_column_letter(index)].width = width

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="gestao_compras.xlsx"'},
    )


@router.get("/insumos/comprados/exportar.pdf")
def export_purchased_inputs_pdf(
    request: Request,
    farm_id: str | None = None,
    item_type: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    repo = _repository(db)
    selected_farm_id = _int_or_none(farm_id) or _active_farm_id(request)
    selected_farm = repo.get_farm(selected_farm_id) if selected_farm_id else None
    normalized_item_type = item_type if item_type in {"insumo_agricola", "combustivel"} else None
    stock_context = _build_stock_context(repo, farm_id=selected_farm_id, item_type=normalized_item_type)
    purchase_entries = _sort_collection_desc(
        stock_context["purchase_entries"],
        lambda item: item.purchase_date,
        lambda item: item.id,
    )
    stock_outputs = _sort_collection_desc(
        stock_context["stock_outputs"],
        lambda item: item.movement_date,
        lambda item: item.id,
    )

    generated_at = app_now()
    generated_by = user.display_name or user.name or user.email
    farm_name = selected_farm.name if selected_farm else "Fazenda Bela Vista"
    item_type_label = {
        "insumo_agricola": "Insumos agrícolas",
        "combustivel": "Combustíveis",
    }.get(normalized_item_type or "", "Todos os itens")
    entries_total = sum(float(item.total_cost or 0) for item in purchase_entries)
    outputs_total = sum(float(output.total_cost or 0) for output in stock_outputs)
    grand_total = round(entries_total + outputs_total, 2)

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), leftMargin=28, rightMargin=28, topMargin=32, bottomMargin=34)
    styles = getSampleStyleSheet()
    farm_header_style = ParagraphStyle("PurchasedPdfFarmHeader", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=18, leading=22, alignment=TA_RIGHT, textColor=colors.HexColor("#1e293b"))
    meta_label_style = ParagraphStyle("PurchasedPdfMetaLabel", parent=styles["BodyText"], fontName="Helvetica-Bold", fontSize=8, leading=10, textColor=colors.HexColor("#446a36"), spaceAfter=2)
    meta_value_style = ParagraphStyle("PurchasedPdfMetaValue", parent=styles["BodyText"], fontName="Helvetica", fontSize=10, leading=13, textColor=colors.HexColor("#334155"))
    cell_style = ParagraphStyle("PurchasedPdfCell", parent=styles["BodyText"], fontName="Helvetica", fontSize=8.2, leading=10.4, textColor=colors.HexColor("#0f172a"))
    cell_muted_style = ParagraphStyle("PurchasedPdfCellMuted", parent=cell_style, textColor=colors.HexColor("#475569"))
    cell_numeric_style = ParagraphStyle("PurchasedPdfCellNumeric", parent=cell_style, alignment=TA_RIGHT)
    summary_value_style = ParagraphStyle("PurchasedPdfSummaryValue", parent=styles["BodyText"], fontName="Helvetica-Bold", fontSize=11, leading=14, textColor=colors.HexColor("#0f172a"))

    logo_path = Path("app/static/images/logo.png")
    logo_flowable = Image(str(logo_path), width=92.8, height=73.6) if logo_path.exists() else Spacer(92.8, 73.6)
    header_table = Table([[logo_flowable, Paragraph(farm_name, farm_header_style)]], colWidths=[76, doc.width - 76], hAlign="LEFT")
    header_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (1, 0), (1, 0), "RIGHT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))

    summary_table = Table([
        [
            [Paragraph("FAZENDA", meta_label_style), Paragraph(farm_name, meta_value_style)],
            [Paragraph("ESCOPO", meta_label_style), Paragraph(item_type_label, meta_value_style)],
            [Paragraph("ENTRADAS", meta_label_style), Paragraph(str(len(purchase_entries)), summary_value_style)],
        ],
        [
            [Paragraph("SAÍDAS", meta_label_style), Paragraph(str(len(stock_outputs)), summary_value_style)],
            [Paragraph("TOTAL DE REGISTROS", meta_label_style), Paragraph(str(len(purchase_entries) + len(stock_outputs)), summary_value_style)],
            [Paragraph("TOTAL FINANCEIRO", meta_label_style), Paragraph(_format_currency(grand_total), summary_value_style)],
        ],
    ], colWidths=[doc.width / 3] * 3, hAlign="LEFT")
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc")),
        ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#dbe5dd")),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))

    elements = [header_table, Spacer(1, 16), summary_table, Spacer(1, 14)]
    table_data = [["Mov.", "Data", "Insumo", "Tipo", "Origem / Fazenda", "Quantidade", "Valor", "Observações"]]
    report_rows = []
    for item in purchase_entries:
        report_rows.append({
            "kind": "Entrada",
            "date": item.purchase_date,
            "name": item.input_catalog.name if item.input_catalog else item.name,
            "type": "Combustível" if item.input_catalog and item.input_catalog.item_type == "combustivel" else "Insumo agrícola",
            "origin": item.farm.name if item.farm else "Sem fazenda vinculada",
            "quantity": f"{_format_decimal_br(item.total_quantity, 2)} {item.package_unit}",
            "value": float(item.total_cost or 0),
            "notes": item.notes or "-",
            "sort_key": (item.purchase_date or today_in_app_timezone(), 0, item.id),
        })
    for output in stock_outputs:
        report_rows.append({
            "kind": "Saída",
            "date": output.movement_date,
            "name": output.input_catalog.name if output.input_catalog else "Insumo removido",
            "type": "Combustível" if output.input_catalog and output.input_catalog.item_type == "combustivel" else "Insumo agrícola",
            "origin": f"{output.origin} • {output.farm.name if output.farm else 'Sem fazenda'}{(' / ' + output.plot.name) if output.plot else ''}",
            "quantity": f"{_format_decimal_br(output.quantity, 2)} {output.unit}",
            "value": float(output.total_cost or 0),
            "notes": output.notes or "-",
            "sort_key": (output.movement_date or today_in_app_timezone(), 1, output.id),
        })
    report_rows.sort(key=lambda row: row["sort_key"], reverse=True)

    for row in report_rows:
        movement_color = "#166534" if row["kind"] == "Entrada" else "#be123c"
        table_data.append([
            Paragraph(f'<font color="{movement_color}"><b>{row["kind"]}</b></font>', cell_style),
            Paragraph(row["date"].strftime("%d/%m/%Y") if row["date"] else "-", cell_style),
            Paragraph(row["name"], cell_style),
            Paragraph(row["type"], cell_muted_style),
            Paragraph(row["origin"], cell_muted_style),
            Paragraph(row["quantity"], cell_numeric_style),
            Paragraph(_format_currency(row["value"]), cell_numeric_style),
            Paragraph(row["notes"][:90], cell_muted_style),
        ])

    column_weights = [7, 8, 18, 12, 20, 11, 12, 20]
    weight_total = sum(column_weights)
    table_col_widths = [doc.width * (weight / weight_total) for weight in column_weights[:-1]]
    table_col_widths.append(doc.width - sum(table_col_widths))
    table = Table(table_data, repeatRows=1, colWidths=table_col_widths, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#446a36")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("LINEBELOW", (0, 0), (-1, 0), 0.8, colors.HexColor("#36552a")),
        ("LINEBELOW", (0, 1), (-1, -1), 0.35, colors.HexColor("#e2e8f0")),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.2),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("ALIGN", (5, 1), (6, -1), "RIGHT"),
    ]))
    elements.append(table)
    elements.append(Spacer(1, 12))

    footer_summary = Table([[
        Paragraph(f"<b>Entradas:</b> {_format_currency(entries_total)}", meta_value_style),
        Paragraph(f"<b>Saídas:</b> {_format_currency(outputs_total)}", meta_value_style),
        Paragraph(f"<b>Total geral:</b> {_format_currency(grand_total)}", summary_value_style),
    ]], colWidths=[doc.width * 0.35, doc.width * 0.25, doc.width * 0.40], hAlign="LEFT")
    footer_summary.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#eef6ee")),
        ("BOX", (0, 0), (-1, -1), 0.7, colors.HexColor("#cfe1d0")),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
    ]))
    elements.append(footer_summary)

    generated_at_label = generated_at.strftime("%d/%m/%Y %H:%M")

    def _draw_footer(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#e2e8f0"))
        canvas.line(doc.leftMargin, 22, landscape(A4)[0] - doc.rightMargin, 22)
        canvas.setFont("Helvetica", 8.2)
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.drawString(doc.leftMargin, 10, f"Gerado por: {generated_by}")
        canvas.drawRightString(
            landscape(A4)[0] - doc.rightMargin,
            10,
            f"Emitido em {generated_at_label} • Página {canvas.getPageNumber()}",
        )
        canvas.restoreState()

    doc.build(elements, onFirstPage=_draw_footer, onLaterPages=_draw_footer)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="gestao_compras.pdf"'},
    )


@router.get("/insumos/estoque")
def stock_page(
    request: Request,
    edit_output_id: int | None = None,
    farm_id: str | None = None,
    input_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    movement_type: str = "all",
    item_type: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    selected_farm_id = _int_or_none(farm_id) or scope["active_farm_id"]
    selected_input_id = _int_or_none(input_id)
    start = _date_or_none(start_date)
    end = _date_or_none(end_date)
    normalized_item_type = item_type if item_type in {"insumo_agricola", "combustivel"} else None
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    stock_context = _build_stock_context(
        repo,
        farm_id=selected_farm_id,
        input_id=selected_input_id,
        start_date=start,
        end_date=end,
        movement_type=movement_type,
        item_type=normalized_item_type,
    )
    raw_stock_tab = request.query_params.get("stock_tab")
    if raw_stock_tab in {"entries", "outputs", "extract"}:
        selected_stock_tab = raw_stock_tab
    else:
        selected_stock_tab = "entries"
        if movement_type == "entrada":
            selected_stock_tab = "entries"
        elif movement_type == "saida":
            selected_stock_tab = "outputs"
    if selected_stock_tab not in {"entries", "outputs", "extract"}:
        selected_stock_tab = "entries"
    purchase_entries = _sort_collection_desc(
        stock_context["purchase_entries"],
        lambda item: item.purchase_date,
        lambda item: item.id,
    )
    stock_outputs = _sort_collection_desc(
        stock_context["stock_outputs"],
        lambda item: item.movement_date,
        lambda item: item.id,
    )
    extract_rows = _sort_collection_desc(
        stock_context["extract_rows"],
        lambda row: row.get("date"),
        lambda row: row.get("reference"),
    )
    purchase_entries_pagination = _paginate_collection(request, purchase_entries, "entries_page")
    stock_outputs_pagination = _paginate_collection(request, stock_outputs, "outputs_page")
    extract_rows_pagination = _paginate_collection(request, extract_rows, "extract_page")
    edit_output = repo.get_stock_output(edit_output_id) if edit_output_id else None
    edit_output_mode = None
    edit_fertilization_item = None
    edit_fertilization_record = None
    edit_inputs_catalog = repo.list_input_catalog()
    if edit_output:
        if edit_output.reference_type == "manual_stock_output":
            edit_output_mode = "manual"
        elif edit_output.reference_type in {"fertilization_item", "fertilization_record"}:
            edit_fertilization_item, edit_fertilization_record = _resolve_fertilization_output_context(repo, db, edit_output)
            if edit_fertilization_record:
                edit_output_mode = "fertilization"
                edit_inputs_catalog = repo.list_input_catalog(item_type="insumo_agricola")
            else:
                edit_output = None
        else:
            edit_output = None
    stock_filters_active = _stock_page_filters_active(
        request,
        active_farm_id=scope["active_farm_id"],
        selected_input_id=selected_input_id,
        movement_type=movement_type,
        normalized_item_type=normalized_item_type,
        edit_output_id=edit_output_id,
    )
    return templates.TemplateResponse(
        "stock.html",
        _base_context(
            request,
            user,
            csrf_token,
            "stock",
            _repo=repo,
            title="Estoque de Insumos",
            farms=repo.list_farms(),
            plots=repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids),
            selected_farm_id=selected_farm_id,
            selected_input_id=selected_input_id,
            selected_start_date=start_date,
            selected_end_date=end_date,
            selected_movement_type=movement_type,
            selected_item_type=normalized_item_type or "",
            stock_filters_active=stock_filters_active,
            stock_export_query=_stock_export_query(
                farm_id=selected_farm_id,
                input_id=selected_input_id,
                start_date=start,
                end_date=end,
                movement_type=movement_type,
                item_type=normalized_item_type,
            ),
            inputs_catalog=stock_context["catalog_inputs"],
            input_stock=stock_context["input_stock"],
            stock_catalog_rows=stock_context["stock_catalog_rows"],
            purchase_entries=purchase_entries_pagination["items"],
            purchase_entries_pagination=purchase_entries_pagination,
            stock_outputs=stock_outputs_pagination["items"],
            stock_outputs_pagination=stock_outputs_pagination,
            extract_rows=extract_rows_pagination["items"],
            extract_rows_pagination=extract_rows_pagination,
            selected_stock_tab=selected_stock_tab,
            stock_tab_urls={
                "entries": _url_with_query(request, stock_tab="entries"),
                "outputs": _url_with_query(request, stock_tab="outputs"),
                "extract": _url_with_query(request, stock_tab="extract"),
            },
            edit_output=edit_output,
            edit_output_mode=edit_output_mode,
            edit_fertilization_item=edit_fertilization_item,
            edit_fertilization_record=edit_fertilization_record,
            edit_inputs_catalog=edit_inputs_catalog,
        ),
    )


@router.post("/insumos/estoque/saida-manual")
def create_manual_stock_output_action(
    request: Request,
    csrf_token: str = Form(...),
    farm_id: str | None = Form(None),
    plot_id: str | None = Form(None),
    input_id: int = Form(...),
    movement_date: str | None = Form(None),
    quantity: float = Form(...),
    unit: str = Form(...),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    plot, scope, denied = _resolve_optional_plot_in_scope(
        request,
        repo,
        _int_or_none(plot_id),
        "/insumos/estoque",
    )
    if denied:
        return denied
    try:
        create_manual_stock_output(
            repo,
            {
                "farm_id": scope["active_farm_id"],
                "plot_id": plot.id if plot else None,
                "input_id": input_id,
                "season_id": scope["active_season_id"],
                "movement_date": movement_date,
                "quantity": quantity,
                "unit": unit,
                "notes": notes,
            },
        )
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect("/insumos/estoque")
    _flash(request, "success", "Saida manual registrada com sucesso.")
    return _redirect("/insumos/estoque")


@router.get("/insumos/estoque/saidas/{output_id}/editar")
def edit_stock_output_entry(
    output_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    repo = _repository(db)
    output = repo.get_stock_output(output_id)
    if not output:
        _flash(request, "error", "Lancamento de saida nao encontrado.")
        return _redirect("/insumos/estoque")
    if output.reference_type in {"manual_stock_output", "fertilization_item", "fertilization_record"}:
        return _redirect_with_query("/insumos/estoque", edit_output_id=output_id)
    _flash(request, "error", f"Este lancamento ainda nao possui edicao integrada para o modulo {output.origin}.")
    return _redirect("/insumos/estoque")


@router.post("/insumos/estoque/saidas/{output_id}/editar")
async def update_stock_output_entry_action(
    output_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    form = await request.form()
    csrf_token = str(form.get("csrf_token") or "")
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    output = repo.get_stock_output(output_id)
    if not output:
        _flash(request, "error", "Lancamento de saida nao encontrado.")
        return _redirect("/insumos/estoque")
    try:
        if output.reference_type == "manual_stock_output":
            scope, denied = _launch_scope_or_redirect(request, repo, "/insumos/estoque")
            if denied:
                return denied
            if not _farm_matches_scope(output.farm_id, scope) or (output.plot_id and not _plot_matches_scope(output.plot, scope)):
                _flash(request, "error", "Este lancamento de saida nao pertence ao contexto ativo.")
                return _redirect("/insumos/estoque")
            plot, _, invalid_plot = _resolve_optional_plot_in_scope(
                request,
                repo,
                _int_or_none(form.get("plot_id")),
                "/insumos/estoque",
            )
            if invalid_plot:
                return invalid_plot
            update_manual_stock_output(
                repo,
                output,
                {
                    "farm_id": scope["active_farm_id"],
                    "plot_id": plot.id if plot else None,
                    "movement_date": str(form.get("movement_date") or ""),
                    "quantity": float(form.get("quantity") or 0),
                    "unit": str(form.get("unit") or ""),
                    "notes": str(form.get("notes") or "") or None,
                },
            )
        elif output.reference_type in {"fertilization_item", "fertilization_record"}:
            fertilization_item, fertilization = _resolve_fertilization_output_context(repo, db, output)
            if not fertilization_item or not fertilization:
                raise ValueError("Nao foi possivel localizar o item de fertilizacao vinculado.")
            plot, scope, invalid_plot = _resolve_plot_in_scope(
                request,
                repo,
                int(fertilization.plot_id or 0),
                "/insumos/estoque",
            )
            if invalid_plot:
                return invalid_plot
            input_catalog = repo.get_input_catalog(int(fertilization_item.input_id or 0))
            if not input_catalog or input_catalog.item_type != "insumo_agricola" or not input_catalog.is_active:
                raise ValueError("Selecione apenas insumos agrícolas válidos para a fertilização.")
            quantity = float(form.get("quantity") or 0)
            if quantity <= 0:
                raise ValueError("Informe uma quantidade valida para a saida.")
            unit = str(form.get("unit") or input_catalog.default_unit or fertilization_item.unit or "kg").strip()
            if not unit:
                raise ValueError("Informe uma unidade valida para a saida.")

            items = []
            for item in fertilization.items:
                if item.id == fertilization_item.id:
                    items.append(
                        {
                            "input_id": input_catalog.id,
                            "purchased_input_id": None,
                            "name": input_catalog.name,
                            "unit": unit,
                            "quantity": quantity,
                        }
                    )
                else:
                    items.append(
                        {
                            "input_id": item.input_id,
                            "purchased_input_id": None,
                            "name": item.name,
                            "unit": item.unit,
                            "quantity": float(item.total_quantity or 0),
                        }
                    )
            update_fertilization(
                repo,
                fertilization,
                {
                    "plot_id": plot.id,
                    "application_date": str(form.get("movement_date") or fertilization.application_date.isoformat()),
                    "season_id": scope["active_season_id"],
                    "notes": str(form.get("notes") or "") or None,
                    "items": items,
                },
            )
        else:
            raise ValueError(f"Este lancamento ainda nao possui edicao integrada para o modulo {output.origin}.")
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect_with_query("/insumos/estoque", edit_output_id=output_id)
    _flash(request, "success", "Lancamento atualizado com sucesso.")
    return _redirect("/insumos/estoque")


@router.post("/insumos/estoque/saidas/{output_id}/excluir")
def delete_stock_output_entry_action(
    output_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    output = repo.get_stock_output(output_id)
    if not output:
        _flash(request, "error", "Lancamento de saida nao encontrado.")
        return _redirect("/insumos/estoque")
    try:
        delete_manual_stock_output(repo, output)
    except ValueError:
        _flash(request, "error", f"Este lancamento esta vinculado ao modulo {output.origin}. Exclua por la.")
        return _redirect("/insumos/estoque")
    _flash(request, "success", "Saida manual excluida com sucesso.")
    return _redirect("/insumos/estoque")


@router.get("/insumos/estoque/exportar.xlsx")
def export_stock_extract_xlsx(
    request: Request,
    farm_id: str | None = None,
    input_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    movement_type: str = "all",
    item_type: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    repo = _repository(db)
    selected_farm_id = _int_or_none(farm_id) or _active_farm_id(request)
    selected_input_id = _int_or_none(input_id)
    rows = _build_stock_context(
        repo,
        farm_id=selected_farm_id,
        input_id=selected_input_id,
        start_date=_date_or_none(start_date),
        end_date=_date_or_none(end_date),
        movement_type=movement_type,
        item_type=item_type if item_type in {"insumo_agricola", "combustivel"} else None,
    )["extract_rows"]
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Extrato de estoque"
    headers = [
        "Data",
        "Insumo",
        "Tipo da movimentacao",
        "Origem",
        "Quantidade",
        "Unidade",
        "Custo unitario",
        "Custo total",
        "Saldo apos movimentacao",
        "Observacoes",
    ]
    sheet.append(headers)
    for row in rows:
        sheet.append(
            [
                row["date"].isoformat() if row["date"] else "",
                row["input_name"],
                row["kind"],
                row["origin"],
                float(row["quantity"] or 0),
                row["unit"],
                float(row.get("unit_cost") or 0),
                float(row.get("total_cost") or 0),
                float(row.get("balance_after") or 0),
                row.get("notes") or "",
            ]
        )
    for index, width in enumerate([14, 30, 18, 20, 14, 12, 16, 16, 20, 42], start=1):
        sheet.column_dimensions[get_column_letter(index)].width = width
    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="extrato_estoque.xlsx"'},
    )


@router.get("/insumos/estoque/exportar.pdf")
def export_stock_extract_pdf(
    request: Request,
    farm_id: str | None = None,
    input_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    movement_type: str = "all",
    item_type: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    repo = _repository(db)
    selected_farm_id = _int_or_none(farm_id) or _active_farm_id(request)
    selected_input_id = _int_or_none(input_id)
    normalized_item_type = item_type if item_type in {"insumo_agricola", "combustivel"} else None
    rows = _build_stock_context(
        repo,
        farm_id=selected_farm_id,
        input_id=selected_input_id,
        start_date=_date_or_none(start_date),
        end_date=_date_or_none(end_date),
        movement_type=movement_type,
        item_type=normalized_item_type,
    )["extract_rows"]
    totals = _stock_report_totals(rows)
    selected_farm = repo.get_farm(selected_farm_id) if selected_farm_id else None
    generated_at = app_now()
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), leftMargin=28, rightMargin=28, topMargin=32, bottomMargin=34)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "StockPdfTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=20,
        leading=24,
        textColor=colors.HexColor("#1e293b"),
        spaceAfter=2,
    )
    subtitle_style = ParagraphStyle(
        "StockPdfSubtitle",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10.5,
        leading=14,
        textColor=colors.HexColor("#64748b"),
    )
    farm_header_style = ParagraphStyle(
        "StockPdfFarmHeader",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        alignment=TA_RIGHT,
        textColor=colors.HexColor("#1e293b"),
    )
    meta_label_style = ParagraphStyle(
        "StockPdfMetaLabel",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=8,
        leading=10,
        textColor=colors.HexColor("#446a36"),
        spaceAfter=2,
    )
    meta_value_style = ParagraphStyle(
        "StockPdfMetaValue",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10,
        leading=13,
        textColor=colors.HexColor("#334155"),
    )
    cell_style = ParagraphStyle(
        "StockPdfCell",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=8.3,
        leading=10.5,
        textColor=colors.HexColor("#0f172a"),
    )
    cell_muted_style = ParagraphStyle(
        "StockPdfCellMuted",
        parent=cell_style,
        textColor=colors.HexColor("#475569"),
    )
    cell_numeric_style = ParagraphStyle(
        "StockPdfCellNumeric",
        parent=cell_style,
        alignment=TA_RIGHT,
    )
    summary_value_style = ParagraphStyle(
        "StockPdfSummaryValue",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=11,
        leading=14,
        textColor=colors.HexColor("#0f172a"),
    )

    logo_path = Path("app/static/images/logo.png")
    logo_flowable = Image(str(logo_path), width=92.8, height=73.6) if logo_path.exists() else Spacer(92.8, 73.6)
    farm_name = selected_farm.name if selected_farm else "Fazenda Bela Vista"
    movement_label = {
        "entrada": "Somente entradas",
        "saida": "Somente saídas",
        "all": "Entradas, saídas e extrato consolidado",
    }.get(movement_type, "Entradas, saídas e extrato consolidado")
    item_type_label = {
        "insumo_agricola": "Insumos agrícolas",
        "combustivel": "Combustíveis",
    }.get(normalized_item_type or "", "Todos os consumíveis")
    period_label = "Período completo"
    if start_date or end_date:
        period_label = f"{start_date or 'Início'} até {end_date or 'Hoje'}"

    header_table = Table(
        [[
            logo_flowable,
            Paragraph(farm_name, farm_header_style),
        ]],
        colWidths=[76, doc.width - 76],
        hAlign="LEFT",
    )
    header_table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (1, 0), (1, 0), "RIGHT"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )

    summary_table = Table(
        [
            [
                [Paragraph("FAZENDA", meta_label_style), Paragraph(farm_name, meta_value_style)],
                [Paragraph("PERÍODO", meta_label_style), Paragraph(period_label, meta_value_style)],
                [Paragraph("ESCOPO", meta_label_style), Paragraph(item_type_label, meta_value_style)],
            ],
            [
                [Paragraph("MOVIMENTAÇÕES", meta_label_style), Paragraph(movement_label, meta_value_style)],
                [Paragraph("TOTAL DE LANÇAMENTOS", meta_label_style), Paragraph(str(totals["movements_count"]), summary_value_style)],
                [Paragraph("TOTAL MOVIMENTADO", meta_label_style), Paragraph(_format_currency(totals["grand_total"]), summary_value_style)],
            ],
        ],
        colWidths=[doc.width / 3] * 3,
        hAlign="LEFT",
    )
    summary_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc")),
                ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#dbe5dd")),
                ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )

    elements = [header_table, Spacer(1, 16), summary_table, Spacer(1, 14)]
    data = [[
        "Data",
        "Insumo",
        "Mov.",
        "Origem / Ref.",
        "Qtd.",
        "Un.",
        "Custo un.",
        "Custo total",
        "Saldo apos",
        "Observacoes",
    ]]
    for row in rows:
        movement_color = "#166534" if row["kind"] == "entrada" else "#be123c"
        data.append([
            Paragraph(row["date"].strftime("%d/%m/%Y") if row["date"] else "-", cell_style),
            Paragraph(row["input_name"], cell_style),
            Paragraph(f'<font color="{movement_color}"><b>{str(row["kind"]).title()}</b></font>', cell_style),
            Paragraph(f'{row["origin"]} • {row["reference"]}', cell_muted_style),
            Paragraph(_format_decimal_br(row["quantity"], 2), cell_numeric_style),
            Paragraph(str(row["unit"] or "-"), cell_style),
            Paragraph(_format_currency(row.get("unit_cost") or 0), cell_numeric_style),
            Paragraph(_format_currency(row.get("total_cost") or 0), cell_numeric_style),
            Paragraph(_format_decimal_br(row.get("balance_after") or 0, 2), cell_numeric_style),
            Paragraph((row.get("notes") or "-")[:90], cell_muted_style),
        ])
    data.append([
        "",
        "",
        "",
        Paragraph("<b>Total geral</b>", cell_style),
        "",
        "",
        "",
        Paragraph(f"<b>{_format_currency(totals['grand_total'])}</b>", cell_numeric_style),
        "",
        "",
    ])

    column_weights = [6, 14, 6, 19, 7, 5, 9, 10, 8, 16]
    weight_total = sum(column_weights)
    table_col_widths = [doc.width * (weight / weight_total) for weight in column_weights[:-1]]
    table_col_widths.append(doc.width - sum(table_col_widths))
    table = Table(data, repeatRows=1, colWidths=table_col_widths, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#446a36")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("LINEBELOW", (0, 0), (-1, 0), 0.8, colors.HexColor("#36552a")),
                ("LINEBELOW", (0, 1), (-1, -2), 0.35, colors.HexColor("#e2e8f0")),
                ("LINEABOVE", (0, -1), (-1, -1), 0.8, colors.HexColor("#cbd5e1")),
                ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#f8fafc")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8.2),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -2), [colors.white, colors.HexColor("#f8fafc")]),
                ("LEFTPADDING", (0, 0), (-1, -1), 7),
                ("RIGHTPADDING", (0, 0), (-1, -1), 7),
                ("TOPPADDING", (0, 0), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                ("ALIGN", (4, 1), (4, -1), "RIGHT"),
                ("ALIGN", (6, 1), (8, -1), "RIGHT"),
            ]
        )
    )
    elements.append(table)
    elements.append(Spacer(1, 12))
    footer_col_widths = [
        sum(table_col_widths[:4]),
        sum(table_col_widths[4:7]),
        sum(table_col_widths[7:]),
    ]
    footer_summary = Table(
        [[
            Paragraph(f"<b>Entradas:</b> {_format_currency(totals['entries_total'])}", meta_value_style),
            Paragraph(f"<b>Saídas:</b> {_format_currency(totals['outputs_total'])}", meta_value_style),
            Paragraph(f"<b>Total geral:</b> {_format_currency(totals['grand_total'])}", summary_value_style),
        ]],
        colWidths=footer_col_widths,
        hAlign="LEFT",
    )
    footer_summary.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#eef6ee")),
                ("BOX", (0, 0), (-1, -1), 0.7, colors.HexColor("#cfe1d0")),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ]
        )
    )
    elements.append(footer_summary)

    generated_by = user.display_name or user.name or user.email
    generated_at_label = generated_at.strftime("%d/%m/%Y %H:%M")

    def _draw_footer(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#e2e8f0"))
        canvas.line(doc.leftMargin, 22, landscape(A4)[0] - doc.rightMargin, 22)
        canvas.setFont("Helvetica", 8.2)
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.drawString(doc.leftMargin, 10, f"Gerado por: {generated_by}")
        canvas.drawRightString(
            landscape(A4)[0] - doc.rightMargin,
            10,
            f"Emitido em {generated_at_label} • Página {canvas.getPageNumber()}",
        )
        canvas.restoreState()

    doc.build(elements, onFirstPage=_draw_footer, onLaterPages=_draw_footer)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="extrato_estoque.pdf"'},
    )


@router.get("/insumos/patrimonio/exportar.xlsx")
def export_equipment_assets_xlsx(
    request: Request,
    farm_id: str | None = None,
    status: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    repo = _repository(db)
    selected_farm_id = _int_or_none(farm_id) or _active_farm_id(request)
    selected_status = status if status in {"ativo", "em_manutencao", "baixado"} else None
    assets = _sort_collection_desc(
        _filter_equipment_assets_by_status(repo.list_equipment_assets(farm_id=selected_farm_id), selected_status),
        lambda item: item.acquisition_date,
        lambda item: item.id,
    )

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Patrimônio"
    headers = [
        "Nome",
        "Categoria",
        "Fazenda",
        "Fabricante",
        "Modelo",
        "Ano",
        "Identificação",
        "Data de aquisição",
        "Valor de aquisição",
        "Status",
        "Observações",
    ]
    sheet.append(headers)
    for asset in assets:
        sheet.append(
            [
                asset.name or "",
                asset.category or "",
                asset.farm.name if asset.farm else "",
                asset.manufacturer or "",
                asset.brand_model or "",
                asset.manufacture_year or "",
                asset.asset_code or "",
                asset.acquisition_date.isoformat() if asset.acquisition_date else "",
                float(asset.acquisition_value or 0),
                asset.status.replace("_", " ").title() if asset.status else "",
                asset.notes or "",
            ]
        )
    for index, width in enumerate([28, 18, 24, 20, 20, 10, 18, 15, 18, 18, 38], start=1):
        sheet.column_dimensions[get_column_letter(index)].width = width
    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="patrimonio_equipamentos.xlsx"'},
    )


@router.get("/insumos/patrimonio/exportar.pdf")
def export_equipment_assets_pdf(
    request: Request,
    farm_id: str | None = None,
    status: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    repo = _repository(db)
    selected_farm_id = _int_or_none(farm_id) or _active_farm_id(request)
    selected_farm = repo.get_farm(selected_farm_id) if selected_farm_id else None
    selected_status = status if status in {"ativo", "em_manutencao", "baixado"} else None
    assets = _sort_collection_desc(
        _filter_equipment_assets_by_status(repo.list_equipment_assets(farm_id=selected_farm_id), selected_status),
        lambda item: item.acquisition_date,
        lambda item: item.id,
    )
    generated_at = app_now()
    generated_by = user.display_name or user.name or user.email
    total_assets = len(assets)
    total_acquisition = round(sum(float(asset.acquisition_value or 0) for asset in assets), 2)
    active_assets = sum(1 for asset in assets if asset.status == "ativo")
    maintenance_assets = sum(1 for asset in assets if asset.status == "em_manutencao")

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), leftMargin=28, rightMargin=28, topMargin=32, bottomMargin=34)
    styles = getSampleStyleSheet()
    farm_header_style = ParagraphStyle(
        "AssetPdfFarmHeader",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        alignment=TA_RIGHT,
        textColor=colors.HexColor("#1e293b"),
    )
    meta_label_style = ParagraphStyle(
        "AssetPdfMetaLabel",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=8,
        leading=10,
        textColor=colors.HexColor("#446a36"),
        spaceAfter=2,
    )
    meta_value_style = ParagraphStyle(
        "AssetPdfMetaValue",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10,
        leading=13,
        textColor=colors.HexColor("#334155"),
    )
    cell_style = ParagraphStyle(
        "AssetPdfCell",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=8.3,
        leading=10.5,
        textColor=colors.HexColor("#0f172a"),
    )
    cell_muted_style = ParagraphStyle(
        "AssetPdfCellMuted",
        parent=cell_style,
        textColor=colors.HexColor("#475569"),
    )
    cell_numeric_style = ParagraphStyle(
        "AssetPdfCellNumeric",
        parent=cell_style,
        alignment=TA_RIGHT,
    )
    summary_value_style = ParagraphStyle(
        "AssetPdfSummaryValue",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=11,
        leading=14,
        textColor=colors.HexColor("#0f172a"),
    )

    logo_path = Path("app/static/images/logo.png")
    logo_flowable = Image(str(logo_path), width=92.8, height=73.6) if logo_path.exists() else Spacer(92.8, 73.6)
    farm_name = selected_farm.name if selected_farm else "Fazenda Bela Vista"
    scope_label = selected_farm.name if selected_farm else "Todas as fazendas"

    header_table = Table(
        [[logo_flowable, Paragraph(farm_name, farm_header_style)]],
        colWidths=[76, doc.width - 76],
        hAlign="LEFT",
    )
    header_table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (1, 0), (1, 0), "RIGHT"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )

    summary_table = Table(
        [
            [
                [Paragraph("FAZENDA", meta_label_style), Paragraph(farm_name, meta_value_style)],
                [Paragraph("ESCOPO", meta_label_style), Paragraph(scope_label, meta_value_style)],
                [Paragraph("TOTAL DE BENS", meta_label_style), Paragraph(str(total_assets), summary_value_style)],
            ],
            [
                [Paragraph("ATIVOS", meta_label_style), Paragraph(str(active_assets), summary_value_style)],
                [Paragraph("EM MANUTENÇÃO", meta_label_style), Paragraph(str(maintenance_assets), summary_value_style)],
                [Paragraph("VALOR TOTAL", meta_label_style), Paragraph(_format_currency(total_acquisition), summary_value_style)],
            ],
        ],
        colWidths=[doc.width / 3] * 3,
        hAlign="LEFT",
    )
    summary_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc")),
                ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#dbe5dd")),
                ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )

    elements = [header_table, Spacer(1, 16), summary_table, Spacer(1, 14)]
    data = [[
        "Nome",
        "Categoria / Fazenda",
        "Fabricante / Modelo",
        "Ano / ID",
        "Aquisição",
        "Valor",
        "Status",
        "Observações",
    ]]
    for asset in assets:
        status_color = {
            "ativo": "#166534",
            "em_manutencao": "#b45309",
            "baixado": "#be123c",
        }.get(asset.status or "", "#334155")
        category_farm = asset.category or "-"
        if asset.farm:
            category_farm = f"{category_farm} • {asset.farm.name}"
        maker_model = asset.manufacturer or "-"
        if asset.brand_model:
            maker_model = f"{maker_model} • {asset.brand_model}" if maker_model != "-" else asset.brand_model
        year_code = asset.manufacture_year or "-"
        if asset.asset_code:
            year_code = f"{year_code} • {asset.asset_code}" if year_code != "-" else asset.asset_code

        data.append([
            Paragraph(asset.name or "-", cell_style),
            Paragraph(category_farm, cell_muted_style),
            Paragraph(maker_model, cell_muted_style),
            Paragraph(year_code, cell_muted_style),
            Paragraph(asset.acquisition_date.strftime("%d/%m/%Y") if asset.acquisition_date else "-", cell_style),
            Paragraph(_format_currency(asset.acquisition_value or 0), cell_numeric_style),
            Paragraph(f'<font color="{status_color}"><b>{(asset.status or "-").replace("_", " ").title()}</b></font>', cell_style),
            Paragraph((asset.notes or "-")[:90], cell_muted_style),
        ])
    data.append([
        "",
        "",
        "",
        Paragraph("<b>Total geral</b>", cell_style),
        "",
        Paragraph(f"<b>{_format_currency(total_acquisition)}</b>", cell_numeric_style),
        "",
        "",
    ])

    column_weights = [16, 17, 17, 12, 9, 11, 8, 18]
    weight_total = sum(column_weights)
    table_col_widths = [doc.width * (weight / weight_total) for weight in column_weights[:-1]]
    table_col_widths.append(doc.width - sum(table_col_widths))
    table = Table(data, repeatRows=1, colWidths=table_col_widths, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#446a36")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("LINEBELOW", (0, 0), (-1, 0), 0.8, colors.HexColor("#36552a")),
                ("LINEBELOW", (0, 1), (-1, -2), 0.35, colors.HexColor("#e2e8f0")),
                ("LINEABOVE", (0, -1), (-1, -1), 0.8, colors.HexColor("#cbd5e1")),
                ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#f8fafc")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8.2),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -2), [colors.white, colors.HexColor("#f8fafc")]),
                ("LEFTPADDING", (0, 0), (-1, -1), 7),
                ("RIGHTPADDING", (0, 0), (-1, -1), 7),
                ("TOPPADDING", (0, 0), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                ("ALIGN", (5, 1), (5, -1), "RIGHT"),
            ]
        )
    )
    elements.append(table)
    elements.append(Spacer(1, 12))

    footer_col_widths = [
        sum(table_col_widths[:3]),
        sum(table_col_widths[3:5]),
        sum(table_col_widths[5:]),
    ]
    footer_summary = Table(
        [[
            Paragraph(f"<b>Total de bens:</b> {total_assets}", meta_value_style),
            Paragraph(f"<b>Ativos:</b> {active_assets} • <b>Em manutenção:</b> {maintenance_assets}", meta_value_style),
            Paragraph(f"<b>Valor total:</b> {_format_currency(total_acquisition)}", summary_value_style),
        ]],
        colWidths=footer_col_widths,
        hAlign="LEFT",
    )
    footer_summary.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#eef6ee")),
                ("BOX", (0, 0), (-1, -1), 0.7, colors.HexColor("#cfe1d0")),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ]
        )
    )
    elements.append(footer_summary)

    generated_at_label = generated_at.strftime("%d/%m/%Y %H:%M")

    def _draw_footer(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#e2e8f0"))
        canvas.line(doc.leftMargin, 22, landscape(A4)[0] - doc.rightMargin, 22)
        canvas.setFont("Helvetica", 8.2)
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.drawString(doc.leftMargin, 10, f"Gerado por: {generated_by}")
        canvas.drawRightString(
            landscape(A4)[0] - doc.rightMargin,
            10,
            f"Emitido em {generated_at_label} • Página {canvas.getPageNumber()}",
        )
        canvas.restoreState()

    doc.build(elements, onFirstPage=_draw_footer, onLaterPages=_draw_footer)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="patrimonio_equipamentos.pdf"'},
    )


@router.get("/insumos/patrimonio")
def equipment_assets_page(
    request: Request,
    edit_id: int | None = None,
    farm_id: int | None = None,
    status: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    effective_farm_id = farm_id or _active_farm_id(request)
    selected_status = status if status in {"ativo", "em_manutencao", "baixado"} else None
    assets = _sort_collection_desc(
        _filter_equipment_assets_by_status(repo.list_equipment_assets(farm_id=effective_farm_id), selected_status),
        lambda item: item.acquisition_date,
        lambda item: item.id,
    )
    assets_pagination = _paginate_collection(request, assets, "assets_page", per_page=5)
    return templates.TemplateResponse(
        "equipment_assets.html",
        _base_context(
            request,
            user,
            csrf_token,
            "assets",
            _repo=repo,
            title="Patrimonio e Equipamentos",
            farms=repo.list_farms(),
            selected_farm_id=effective_farm_id,
            selected_asset_status=selected_status or "",
            assets_export_query=_assets_export_query(farm_id=effective_farm_id, status=selected_status),
            asset_category_options=EQUIPMENT_ASSET_CATEGORY_OPTIONS,
            current_year=today_in_app_timezone().year,
            assets=assets_pagination["items"],
            assets_pagination=assets_pagination,
            edit_asset=repo.get_equipment_asset(edit_id) if edit_id else None,
        ),
    )


@router.post("/insumos/patrimonio")
async def create_equipment_asset_action(
    request: Request,
    csrf_token: str = Form(...),
    farm_id: str | None = Form(None),
    name: str = Form(...),
    category: str = Form(...),
    manufacturer: str | None = Form(None),
    manufacture_year: str | None = Form(None),
    brand_model: str | None = Form(None),
    asset_code: str | None = Form(None),
    acquisition_date: str | None = Form(None),
    acquisition_value: str | None = Form(None),
    status_value: str = Form("ativo", alias="status"),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    scope, denied = _launch_scope_or_redirect(request, repo, "/insumos/patrimonio")
    if denied:
        return denied
    normalized_category = _clean_text(category)
    if normalized_category not in EQUIPMENT_ASSET_CATEGORY_OPTIONS:
        _flash(request, "error", "Selecione uma categoria valida para o patrimonio.")
        return _redirect("/insumos/patrimonio")
    try:
        normalized_manufacture_year = _parse_equipment_asset_manufacture_year(manufacture_year)
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect("/insumos/patrimonio")
    try:
        attachment_payloads = _read_attachments(await _request_attachments(request))
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect("/insumos/patrimonio")
    asset = create_equipment_asset(
        repo,
        {
            "farm_id": scope["active_farm_id"],
            "name": name,
            "category": normalized_category,
            "manufacturer": _clean_text(manufacturer),
            "manufacture_year": normalized_manufacture_year,
            "brand_model": _clean_text(brand_model),
            "asset_code": _clean_text(asset_code),
            "acquisition_date": acquisition_date,
            "acquisition_value": _float_or_none(acquisition_value),
            "status": status_value,
            "notes": notes,
        },
    )
    try:
        saved_attachments = _save_equipment_asset_attachments(repo, asset, attachment_payloads)
    except Exception:
        _flash(request, "error", "O patrimonio foi salvo, mas nao foi possivel gravar os anexos agora.")
        return _redirect_with_query("/insumos/patrimonio", edit_id=asset.id)
    if saved_attachments:
        _flash(request, "success", f"Patrimonio cadastrado com sucesso. {saved_attachments} anexo(s) salvo(s).")
        return _redirect_with_query("/insumos/patrimonio", edit_id=asset.id)
    _flash(request, "success", "Patrimonio cadastrado com sucesso.")
    return _redirect("/insumos/patrimonio")


@router.post("/insumos/patrimonio/{asset_id}/editar")
async def update_equipment_asset_action(
    asset_id: int,
    request: Request,
    csrf_token: str = Form(...),
    farm_id: str | None = Form(None),
    name: str = Form(...),
    category: str = Form(...),
    manufacturer: str | None = Form(None),
    manufacture_year: str | None = Form(None),
    brand_model: str | None = Form(None),
    asset_code: str | None = Form(None),
    acquisition_date: str | None = Form(None),
    acquisition_value: str | None = Form(None),
    status_value: str = Form("ativo", alias="status"),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    scope, denied = _launch_scope_or_redirect(request, repo, "/insumos/patrimonio")
    if denied:
        return denied
    asset = repo.get_equipment_asset(asset_id)
    if not asset:
        _flash(request, "error", "Patrimonio nao encontrado.")
        return _redirect_for_request(request, "/insumos/patrimonio")
    if not _farm_matches_scope(asset.farm_id, scope):
        _flash(request, "error", "Este patrimonio nao pertence ao contexto ativo.")
        return _redirect_for_request(request, "/insumos/patrimonio")
    normalized_category = _clean_text(category)
    if normalized_category not in EQUIPMENT_ASSET_CATEGORY_OPTIONS and normalized_category != asset.category:
        _flash(request, "error", "Selecione uma categoria valida para o patrimonio.")
        return _redirect_for_request(request, "/insumos/patrimonio", edit_id=asset_id)
    try:
        normalized_manufacture_year = _parse_equipment_asset_manufacture_year(manufacture_year)
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect_for_request(request, "/insumos/patrimonio", edit_id=asset_id)
    try:
        attachment_payloads = _read_attachments(await _request_attachments(request))
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect_for_request(request, "/insumos/patrimonio", edit_id=asset_id)
    update_equipment_asset(
        repo,
        asset,
        {
            "farm_id": scope["active_farm_id"],
            "name": name,
            "category": normalized_category,
            "manufacturer": _clean_text(manufacturer),
            "manufacture_year": normalized_manufacture_year,
            "brand_model": _clean_text(brand_model),
            "asset_code": _clean_text(asset_code),
            "acquisition_date": acquisition_date,
            "acquisition_value": _float_or_none(acquisition_value),
            "status": status_value,
            "notes": notes,
        },
    )
    try:
        saved_attachments = _save_equipment_asset_attachments(repo, asset, attachment_payloads)
    except Exception:
        _flash(request, "error", "As alteracoes foram salvas, mas nao foi possivel incluir os novos anexos.")
        return _redirect_for_request(request, "/insumos/patrimonio", edit_id=asset_id)
    if saved_attachments:
        _flash(request, "success", f"Alteracoes salvas com sucesso. {saved_attachments} novo(s) anexo(s) adicionado(s).")
        return _redirect_for_request(request, "/insumos/patrimonio", edit_id=asset_id)
    _flash(request, "success", "Patrimonio atualizado com sucesso.")
    return _redirect_for_request(request, "/insumos/patrimonio")


@router.post("/insumos/patrimonio/{asset_id}/excluir")
def delete_equipment_asset_action(
    asset_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    asset = repo.get_equipment_asset(asset_id)
    if not asset:
        _flash(request, "error", "Patrimonio nao encontrado.")
        return _redirect_for_request(request, "/insumos/patrimonio")
    repo.delete(asset)
    _flash(request, "success", "Patrimonio excluido com sucesso.")
    return _redirect("/insumos/patrimonio")


@router.get("/insumos/patrimonio/anexos/{attachment_id}")
def open_equipment_asset_attachment(
    attachment_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    repo = _repository(db)
    attachment = repo.get_equipment_asset_attachment(attachment_id)
    if not attachment or not attachment.equipment_asset:
        _flash(request, "error", "Anexo nao encontrado.")
        return _redirect("/insumos/patrimonio")
    scope = _global_scope_context(request, repo)
    asset = attachment.equipment_asset
    if not _farm_matches_scope(asset.farm_id, scope):
        _flash(request, "error", "Este anexo nao pertence ao contexto ativo.")
        return _redirect("/insumos/patrimonio")
    return _attachment_response(attachment.filename, attachment.content_type, attachment.file_data)


@router.post("/insumos/patrimonio/{asset_id}/anexos/{attachment_id}/excluir")
def delete_equipment_asset_attachment_action(
    asset_id: int,
    attachment_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    asset = repo.get_equipment_asset(asset_id)
    if not asset:
        _flash(request, "error", "Patrimonio nao encontrado.")
        return _redirect("/insumos/patrimonio")
    scope = _global_scope_context(request, repo)
    if not _farm_matches_scope(asset.farm_id, scope):
        _flash(request, "error", "Este patrimonio nao pertence ao contexto ativo.")
        return _redirect_for_request(request, "/insumos/patrimonio")
    attachment = repo.get_equipment_asset_attachment(attachment_id)
    if not attachment or attachment.equipment_asset_id != asset.id:
        _flash(request, "error", "Anexo nao encontrado.")
        return _redirect_for_request(request, "/insumos/patrimonio", edit_id=asset_id)
    repo.delete(attachment)
    if repo.get_equipment_asset_attachment(attachment_id):
        _flash(request, "error", "Nao foi possivel remover o anexo agora. Tente novamente.")
        return _redirect_for_request(request, "/insumos/patrimonio", edit_id=asset_id)
    _flash(request, "success", "Anexo removido com sucesso.")
    return _redirect_for_request(request, "/insumos/patrimonio", edit_id=asset_id)


@router.get("/insumos/recomendacao")
def input_recommendations_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    plots = repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids)
    plot_ids = {plot.id for plot in plots}
    catalog_inputs = repo.list_input_catalog(item_type="insumo_agricola")
    purchase_entries = repo.list_purchased_inputs()
    input_stock = {
        item.id: {
            "available": round(
                sum(float(entry.available_quantity or 0) for entry in purchase_entries if entry.input_id == item.id),
                2,
            ),
            "unit": item.default_unit,
        }
        for item in catalog_inputs
    }
    recommendations = [
        recommendation
        for recommendation in repo.list_input_recommendations()
        if (
            (not scope["active_farm_id"] or recommendation.farm_id == scope["active_farm_id"])
            and (not plot_ids or recommendation.plot_id is None or recommendation.plot_id in plot_ids)
        )
    ]
    recommendations = _sort_collection_desc(
        recommendations,
        lambda item: item.id,
    )
    recommendations_pagination = _paginate_collection(request, recommendations, "recommendations_page")
    return templates.TemplateResponse(
        "input_recommendations.html",
        _base_context(
            request,
            user,
            csrf_token,
            "input_recommendations",
            _repo=repo,
            title="Recomendacao de Insumos",
            farms=repo.list_farms(),
            plots=plots,
            inputs_catalog=catalog_inputs,
            input_stock=input_stock,
            recommendations=recommendations_pagination["items"],
            recommendations_pagination=recommendations_pagination,
            edit_recommendation=repo.get_input_recommendation(edit_id) if edit_id else None,
        ),
    )


@router.post("/insumos/recomendacao")
async def create_input_recommendation_action(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    form = await request.form()
    validate_csrf(request, str(form.get("csrf_token") or ""))
    application_name = str(form.get("application_name") or "").strip()
    items = _parse_recommendation_items(form)
    if not application_name or not items:
        _flash(request, "error", "Informe a aplicacao e adicione ao menos um insumo.")
        return _redirect("/insumos/recomendacao")
    repo = _repository(db)
    plot, scope, denied = _resolve_optional_plot_in_scope(
        request,
        repo,
        _int_or_none(form.get("plot_id")),
        "/insumos/recomendacao",
    )
    if denied:
        return denied
    notes = str(form.get("notes") or "") or None
    create_input_recommendation(
        repo,
        {
            "farm_id": scope["active_farm_id"],
            "plot_id": plot.id if plot else None,
            "application_name": application_name,
            "items": items,
            "notes": notes,
        },
    )
    _flash(request, "success", "Recomendacao cadastrada com sucesso.")
    return _redirect("/insumos/recomendacao")


@router.post("/insumos/recomendacao/{recommendation_id}/editar")
async def update_input_recommendation_action(
    recommendation_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    form = await request.form()
    validate_csrf(request, str(form.get("csrf_token") or ""))
    repo = _repository(db)
    plot, scope, denied = _resolve_optional_plot_in_scope(
        request,
        repo,
        _int_or_none(form.get("plot_id")),
        "/insumos/recomendacao",
    )
    if denied:
        return denied
    recommendation = repo.get_input_recommendation(recommendation_id)
    if not recommendation:
        _flash(request, "error", "Recomendacao nao encontrada.")
        return _redirect_for_request(request, "/insumos/recomendacao")
    if not _farm_matches_scope(recommendation.farm_id, scope):
        _flash(request, "error", "Esta recomendacao nao pertence ao contexto ativo.")
        return _redirect_for_request(request, "/insumos/recomendacao")
    if recommendation.plot_id and not _plot_matches_scope(recommendation.plot, scope):
        _flash(request, "error", "Esta recomendacao nao pertence ao contexto ativo.")
        return _redirect_for_request(request, "/insumos/recomendacao")
    application_name = str(form.get("application_name") or "").strip()
    items = _parse_recommendation_items(form)
    if not application_name or not items:
        _flash(request, "error", "Informe a aplicacao e adicione ao menos um insumo.")
        return _redirect_for_request(request, "/insumos/recomendacao", edit_id=recommendation_id)
    update_input_recommendation(
        repo,
        recommendation,
        {
            "farm_id": scope["active_farm_id"],
            "plot_id": plot.id if plot else None,
            "application_name": application_name,
            "items": items,
            "notes": str(form.get("notes") or "") or None,
        },
    )
    _flash(request, "success", "Recomendacao atualizada com sucesso.")
    return _redirect_for_request(request, "/insumos/recomendacao")


@router.post("/insumos/recomendacao/{recommendation_id}/excluir")
def delete_input_recommendation_action(
    recommendation_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    recommendation = repo.get_input_recommendation(recommendation_id)
    if not recommendation:
        _flash(request, "error", "Recomendacao nao encontrada.")
        return _redirect("/insumos/recomendacao")
    repo.delete(recommendation)
    _flash(request, "success", "Recomendacao excluida com sucesso.")
    return _redirect("/insumos/recomendacao")


@router.get("/variedades")
def varieties_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    return templates.TemplateResponse(
        "varieties.html",
        _base_context(
            request,
            user,
            csrf_token,
            "varieties",
            _repo=repo,
            varieties=repo.list_varieties(),
            edit_variety=repo.get_variety(edit_id) if edit_id else None,
        ),
    )


@router.post("/variedades")
def create_variety_action(
    request: Request,
    csrf_token: str = Form(...),
    name: str = Form(...),
    species: str = Form(...),
    maturation_cycle: str = Form(...),
    flavor_profile: str | None = Form(None),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    create_variety(
        _repository(db),
        {
            "name": name,
            "species": species,
            "maturation_cycle": maturation_cycle,
            "flavor_profile": flavor_profile,
            "notes": notes,
        },
    )
    _flash(request, "success", "Variedade cadastrada com sucesso.")
    return _redirect("/variedades")


@router.post("/variedades/{variety_id}/editar")
def update_variety_action(
    variety_id: int,
    request: Request,
    csrf_token: str = Form(...),
    name: str = Form(...),
    species: str = Form(...),
    maturation_cycle: str = Form(...),
    flavor_profile: str | None = Form(None),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    variety = repo.get_variety(variety_id)
    if not variety:
        _flash(request, "error", "Variedade nao encontrada.")
        return _redirect("/variedades")
    update_variety(
        repo,
        variety,
        {
            "name": name,
            "species": species,
            "maturation_cycle": maturation_cycle,
            "flavor_profile": flavor_profile,
            "notes": notes,
        },
    )
    _flash(request, "success", "Variedade atualizada com sucesso.")
    return _redirect("/variedades")


@router.post("/variedades/{variety_id}/excluir")
def delete_variety_action(
    variety_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    variety = repo.get_variety(variety_id)
    if not variety:
        _flash(request, "error", "Variedade nao encontrada.")
        return _redirect("/variedades")
    if variety.plots:
        _flash(request, "error", "Nao e possivel excluir a variedade enquanto houver setores vinculados.")
        return _redirect("/variedades")
    repo.delete(variety)
    _flash(request, "success", "Variedade excluida com sucesso.")
    return _redirect("/variedades")


@router.get("/safras")
def crop_seasons_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    effective_farm_id = scope["active_farm_id"]
    crop_seasons = repo.list_crop_seasons(farm_id=effective_farm_id)
    fertilizations = repo.list_fertilizations()
    stock_outputs = repo.list_stock_outputs(farm_id=effective_farm_id) if effective_farm_id else repo.list_stock_outputs()
    edit_season = repo.get_crop_season(edit_id) if edit_id else None
    if edit_season and not _farm_matches_scope(edit_season.farm_id, scope):
        _flash(request, "error", "Esta safra nao pertence ao contexto ativo.")
        edit_season = None
    season_costs: dict[int, dict] = {}
    for season in crop_seasons:
        season_outputs = [output for output in stock_outputs if output.season_id == season.id]
        season_fertilizations = [record for record in fertilizations if record.season_id == season.id]
        input_cost = round(
            sum(
                float(output.total_cost or 0)
                for output in season_outputs
                if output.input_catalog and output.input_catalog.item_type != "combustivel"
            ),
            2,
        )
        fuel_cost = round(
            sum(
                float(output.total_cost or 0)
                for output in season_outputs
                if output.input_catalog and output.input_catalog.item_type == "combustivel"
            ),
            2,
        )
        application_cost = round(sum(float(record.cost or 0) for record in season_fertilizations), 2)
        consumed_inputs_cost = round(input_cost + fuel_cost, 2)
        operational_total = round(consumed_inputs_cost + application_cost, 2)
        cultivated_area = float(season.cultivated_area or 0)
        season_costs[season.id] = {
            "input_cost": input_cost,
            "fuel_cost": fuel_cost,
            "application_cost": application_cost,
            "consumed_inputs_cost": consumed_inputs_cost,
            "operational_total": operational_total,
            "cost_per_hectare": round(operational_total / cultivated_area, 2) if cultivated_area else 0,
            "farm_cost": operational_total,
            "culture_cost": operational_total,
            "variety_cost": operational_total if season.variety_id else 0,
        }
    return templates.TemplateResponse(
        "seasons.html",
        _base_context(
            request,
            user,
            csrf_token,
            "seasons",
            _repo=repo,
            title="Safras",
            farms=repo.list_farms(),
            varieties=repo.list_varieties(),
            crop_seasons=crop_seasons,
            season_costs=season_costs,
            edit_season=edit_season,
        ),
    )


@router.post("/safras")
def create_crop_season_action(
    request: Request,
    csrf_token: str = Form(...),
    name: str | None = Form(None),
    start_date: str = Form(...),
    end_date: str = Form(...),
    culture: str = Form(...),
    variety_id: str | None = Form(None),
    cultivated_area: str | None = Form(None),
    area_unit: str = Form("ha"),
    notes: str | None = Form(None),
    status_value: str = Form(..., alias="status"),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    scope, denied = _launch_scope_or_redirect(request, repo, "/safras", require_season=False)
    if denied:
        return denied
    try:
        parsed_start_date = date.fromisoformat(start_date)
        parsed_end_date = date.fromisoformat(end_date)
    except ValueError:
        _flash(request, "error", "Informe datas validas para a safra.")
        return _redirect("/safras")
    try:
        normalized_area = str(cultivated_area or "").strip().replace(",", ".")
        parsed_cultivated_area = float(normalized_area)
    except ValueError:
        _flash(request, "error", "Informe uma area cultivada valida.")
        return _redirect("/safras")
    if parsed_cultivated_area <= 0:
        _flash(request, "error", "A area cultivada deve ser maior que zero.")
        return _redirect("/safras")
    if parsed_end_date < parsed_start_date:
        _flash(request, "error", "A data final da safra nao pode ser anterior a data inicial.")
        return _redirect("/safras")
    try:
        create_crop_season(
            repo,
            {
                "farm_id": scope["active_farm_id"],
                "name": name,
                "start_date": start_date,
                "end_date": end_date,
                "culture": culture,
                "variety_id": _int_or_none(variety_id),
                "cultivated_area": parsed_cultivated_area,
                "area_unit": area_unit,
                "notes": notes,
                "status": status_value,
            },
        )
    except Exception:
        logger.exception("Falha ao criar safra", extra={"farm_id": scope["active_farm_id"], "start_date": start_date, "end_date": end_date})
        _flash(request, "error", "Nao foi possivel salvar a safra agora. Revise os dados e tente novamente.")
        return _redirect("/safras")
    _flash(request, "success", "Safra cadastrada com sucesso.")
    return _redirect("/safras")


@router.post("/safras/{season_id}/editar")
def update_crop_season_action(
    season_id: int,
    request: Request,
    csrf_token: str = Form(...),
    name: str | None = Form(None),
    start_date: str = Form(...),
    end_date: str = Form(...),
    culture: str = Form(...),
    variety_id: str | None = Form(None),
    cultivated_area: str | None = Form(None),
    area_unit: str = Form("ha"),
    notes: str | None = Form(None),
    status_value: str = Form(..., alias="status"),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    scope, denied = _launch_scope_or_redirect(request, repo, "/safras", require_season=False)
    if denied:
        return denied
    crop_season = repo.get_crop_season(season_id)
    if not crop_season:
        _flash(request, "error", "Safra nao encontrada.")
        return _redirect("/safras")
    if not _farm_matches_scope(crop_season.farm_id, scope):
        _flash(request, "error", "Esta safra nao pertence ao contexto ativo.")
        return _redirect("/safras")
    try:
        parsed_start_date = date.fromisoformat(start_date)
        parsed_end_date = date.fromisoformat(end_date)
    except ValueError:
        _flash(request, "error", "Informe datas validas para a safra.")
        return _redirect("/safras")
    try:
        normalized_area = str(cultivated_area or "").strip().replace(",", ".")
        parsed_cultivated_area = float(normalized_area)
    except ValueError:
        _flash(request, "error", "Informe uma area cultivada valida.")
        return _redirect("/safras")
    if parsed_cultivated_area <= 0:
        _flash(request, "error", "A area cultivada deve ser maior que zero.")
        return _redirect("/safras")
    if parsed_end_date < parsed_start_date:
        _flash(request, "error", "A data final da safra nao pode ser anterior a data inicial.")
        return _redirect("/safras")
    try:
        update_crop_season(
            repo,
            crop_season,
            {
                "farm_id": scope["active_farm_id"],
                "name": name,
                "start_date": start_date,
                "end_date": end_date,
                "culture": culture,
                "variety_id": _int_or_none(variety_id),
                "cultivated_area": parsed_cultivated_area,
                "area_unit": area_unit,
                "notes": notes,
                "status": status_value,
            },
        )
    except Exception:
        logger.exception("Falha ao atualizar safra", extra={"season_id": season_id, "farm_id": scope["active_farm_id"]})
        _flash(request, "error", "Nao foi possivel atualizar a safra agora. Revise os dados e tente novamente.")
        return _redirect("/safras")
    _flash(request, "success", "Safra atualizada com sucesso.")
    return _redirect("/safras")


@router.post("/safras/{season_id}/excluir")
def delete_crop_season_action(
    season_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    crop_season = repo.get_crop_season(season_id)
    if not crop_season:
        _flash(request, "error", "Safra nao encontrada.")
        return _redirect("/safras")
    repo.delete(crop_season)
    _flash(request, "success", "Safra excluida com sucesso.")
    return _redirect("/safras")


@router.get("/irrigacao")
def irrigation_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    start_date, end_date = _scoped_dates(scope["active_season"])
    plots = repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids)
    plot_ids = {plot.id for plot in plots}
    irrigations = [
        irrigation
        for irrigation in repo.list_irrigations()
        if irrigation.plot_id in plot_ids and _within_scope(irrigation.irrigation_date, start_date, end_date)
    ]
    irrigations = _sort_collection_desc(
        irrigations,
        lambda item: item.irrigation_date,
        lambda item: item.id,
    )
    irrigations_pagination = _paginate_collection(request, irrigations, "irrigations_page")
    return templates.TemplateResponse(
        "irrigation.html",
        _base_context(
            request,
            user,
            csrf_token,
            "irrigation",
            _repo=repo,
            plots=plots,
            irrigations=irrigations_pagination["items"],
            irrigations_pagination=irrigations_pagination,
            edit_irrigation=repo.get_irrigation(edit_id) if edit_id else None,
            plot_irrigation_configs=[
                {
                    "id": plot.id,
                    "name": plot.name,
                    "type": plot.irrigation_type,
                    "line_count": plot.irrigation_line_count,
                    "line_length": float(plot.irrigation_line_length_meters) if plot.irrigation_line_length_meters is not None else None,
                    "drip_spacing_cm": round(float(plot.drip_spacing_meters) * 100, 2) if plot.drip_spacing_meters is not None else None,
                    "drip_lph": float(plot.drip_liters_per_hour) if plot.drip_liters_per_hour is not None else None,
                    "sprinkler_count": plot.sprinkler_count,
                    "sprinkler_lph": float(plot.sprinkler_liters_per_hour) if plot.sprinkler_liters_per_hour is not None else None,
                }
                for plot in plots
            ],
        ),
    )


@router.post("/irrigacao")
def create_irrigation_action(
    request: Request,
    csrf_token: str = Form(...),
    plot_id: int = Form(...),
    irrigation_date: str = Form(...),
    volume_liters: str | None = Form(None),
    duration_minutes: int = Form(...),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    plot, _, denied = _resolve_plot_in_scope(request, repo, plot_id, "/irrigacao")
    if denied:
        return denied
    calculated_volume = calculate_irrigation_volume(plot, duration_minutes)
    manual_volume = _float_or_none(volume_liters)
    if calculated_volume is None and manual_volume is None:
        _flash(request, "error", "Informe o volume manual em litros ou cadastre os dados de irrigacao no setor.")
        return _redirect("/irrigacao")
    create_irrigation(
        repo,
        {
            "plot_id": plot_id,
            "irrigation_date": irrigation_date,
            "volume_liters": calculated_volume if calculated_volume is not None else manual_volume,
            "duration_minutes": duration_minutes,
            "notes": notes,
        },
    )
    _flash(request, "success", "Irrigacao registrada com sucesso.")
    return _redirect("/irrigacao")


@router.post("/irrigacao/{record_id}/editar")
def update_irrigation_action(
    record_id: int,
    request: Request,
    csrf_token: str = Form(...),
    plot_id: int = Form(...),
    irrigation_date: str = Form(...),
    volume_liters: str | None = Form(None),
    duration_minutes: int = Form(...),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    irrigation = repo.get_irrigation(record_id)
    if not irrigation:
        _flash(request, "error", "Registro de irrigacao nao encontrado.")
        return _redirect_for_request(request, "/irrigacao")
    plot, scope, denied = _resolve_plot_in_scope(request, repo, plot_id, "/irrigacao")
    if denied:
        return denied
    if not _plot_matches_scope(irrigation.plot, scope):
        _flash(request, "error", "Este registro de irrigacao nao pertence ao contexto ativo.")
        return _redirect_for_request(request, "/irrigacao")
    calculated_volume = calculate_irrigation_volume(plot, duration_minutes)
    manual_volume = _float_or_none(volume_liters)
    if calculated_volume is None and manual_volume is None:
        _flash(request, "error", "Informe o volume manual em litros ou cadastre os dados de irrigacao no setor.")
        return _redirect_for_request(request, "/irrigacao", edit_id=record_id)
    update_irrigation(
        repo,
        irrigation,
        {
            "plot_id": plot.id,
            "irrigation_date": irrigation_date,
            "volume_liters": calculated_volume if calculated_volume is not None else manual_volume,
            "duration_minutes": duration_minutes,
            "notes": notes,
        },
    )
    _flash(request, "success", "Irrigacao atualizada com sucesso.")
    return _redirect_for_request(request, "/irrigacao")


@router.post("/irrigacao/{record_id}/excluir")
def delete_irrigation_action(
    record_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    irrigation = repo.get_irrigation(record_id)
    if not irrigation:
        _flash(request, "error", "Registro de irrigacao nao encontrado.")
        return _redirect("/irrigacao")
    repo.delete(irrigation)
    _flash(request, "success", "Irrigacao excluida com sucesso.")
    return _redirect("/irrigacao")


@router.get("/pluviometria")
def rainfall_page(
    request: Request,
    farm_id: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    effective_farm_id = farm_id or _active_farm_id(request)
    rainfalls = _sort_collection_desc(
        repo.list_rainfalls(
            farm_id=effective_farm_id,
            start_date=_date_or_none(start_date),
            end_date=_date_or_none(end_date),
        ),
        lambda item: item.rainfall_date,
        lambda item: item.id,
    )
    rainfalls_pagination = _paginate_collection(request, rainfalls, "rainfalls_page")
    return templates.TemplateResponse(
        "rainfall.html",
        _base_context(
            request,
            user,
            csrf_token,
            "rainfall",
            _repo=repo,
            farms=repo.list_farms(),
            rainfalls=rainfalls_pagination["items"],
            rainfalls_pagination=rainfalls_pagination,
            edit_rainfall=repo.get_rainfall(edit_id) if edit_id else None,
            filters={
                "farm_id": effective_farm_id,
                "start_date": start_date or "",
                "end_date": end_date or "",
            },
        ),
    )


@router.post("/pluviometria")
def create_rainfall_action(
    request: Request,
    csrf_token: str = Form(...),
    rainfall_date: str = Form(...),
    millimeters: float = Form(...),
    source: str | None = Form(None),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    scope, denied = _launch_scope_or_redirect(request, repo, "/pluviometria")
    if denied:
        return denied
    create_rainfall(
        repo,
        {
            "farm_id": scope["active_farm_id"],
            "rainfall_date": rainfall_date,
            "millimeters": millimeters,
            "source": source,
            "notes": notes,
        },
    )
    _flash(request, "success", "Pluviometria registrada com sucesso.")
    return _redirect("/pluviometria")


@router.post("/pluviometria/{record_id}/editar")
def update_rainfall_action(
    record_id: int,
    request: Request,
    csrf_token: str = Form(...),
    rainfall_date: str = Form(...),
    millimeters: float = Form(...),
    source: str | None = Form(None),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    scope, denied = _launch_scope_or_redirect(request, repo, "/pluviometria")
    if denied:
        return denied
    rainfall = repo.get_rainfall(record_id)
    if not rainfall:
        _flash(request, "error", "Registro de pluviometria nao encontrado.")
        return _redirect_for_request(request, "/pluviometria")
    if not _farm_matches_scope(rainfall.farm_id, scope):
        _flash(request, "error", "Este registro de pluviometria nao pertence ao contexto ativo.")
        return _redirect_for_request(request, "/pluviometria")
    update_rainfall(
        repo,
        rainfall,
        {
            "farm_id": scope["active_farm_id"],
            "rainfall_date": rainfall_date,
            "millimeters": millimeters,
            "source": source,
            "notes": notes,
        },
    )
    _flash(request, "success", "Pluviometria atualizada com sucesso.")
    return _redirect_for_request(request, "/pluviometria")


@router.post("/pluviometria/{record_id}/excluir")
def delete_rainfall_action(
    record_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    rainfall = repo.get_rainfall(record_id)
    if not rainfall:
        _flash(request, "error", "Registro de pluviometria nao encontrado.")
        return _redirect("/pluviometria")
    repo.delete(rainfall)
    _flash(request, "success", "Pluviometria excluida com sucesso.")
    return _redirect("/pluviometria")


@router.get("/fertilizacao")
def fertilization_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    start_date, end_date, filter_start_str, filter_end_str = _schedule_filter_date_bounds(
        request, scope["active_season"], flash_invalid=True
    )
    selected_fertilization_range = _fertilization_filter_range_preset(
        request.query_params.get("schedule_range"),
        filter_start_str,
        filter_end_str,
    )
    plots = repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids)
    plot_ids = {plot.id for plot in plots}
    edit_fertilization = repo.get_fertilization(edit_id) if edit_id else None
    recommendation_groups: dict[str, list[dict]] = {}
    consolidated_inputs = repo.list_input_catalog(item_type="insumo_agricola")
    purchased_inputs = repo.list_purchased_inputs()
    input_stock = {
        item.id: {
            "available": round(
                sum(
                    float(entry.available_quantity or 0)
                    for entry in purchased_inputs
                    if entry.input_id == item.id
                ),
                2,
            ),
            "unit": item.default_unit,
        }
        for item in consolidated_inputs
    }
    fertilization_filter_clear_url = _url_with_query(
        request,
        start_date=None,
        end_date=None,
        schedule_range=None,
        fertilizations_page=None,
    )
    search_q = (request.query_params.get("search") or "").strip() or None
    fertilizations = _filter_fertilization_records(repo, plot_ids, start_date, end_date, search=search_q)
    fertilizations_pagination = _paginate_collection(request, fertilizations, "fertilizations_page")
    schedules = [
        item
        for item in repo.list_fertilization_schedules()
        if item.plot_id in plot_ids and _within_scope(item.scheduled_date, start_date, end_date)
    ]
    recommendations = [
        recommendation
        for recommendation in repo.list_input_recommendations()
        if (
            (not scope["active_farm_id"] or recommendation.farm_id == scope["active_farm_id"])
            and (not plot_ids or recommendation.plot_id is None or recommendation.plot_id in plot_ids)
        )
    ]
    for recommendation in recommendations:
        bucket = recommendation_groups.setdefault(recommendation.application_name, [])
        for item in recommendation.items:
            bucket.append(
                {
                    "input_id": item.input_id,
                    "name": item.input_catalog.name if item.input_catalog else (item.purchased_input.name if item.purchased_input else "Insumo removido"),
                    "unit": item.unit,
                    "quantity": float(item.quantity or 0),
                    "available": input_stock.get(item.input_id or 0, {}).get("available", 0),
                }
            )
    return templates.TemplateResponse(
        "fertilization.html",
        _base_context(
            request,
            user,
            csrf_token,
            "fertilization",
            _repo=repo,
            plots=plots,
            inputs_catalog=consolidated_inputs,
            input_stock=input_stock,
            fertilizations=fertilizations_pagination["items"],
            fertilizations_pagination=fertilizations_pagination,
            fertilization_filter_start_date=filter_start_str or None,
            fertilization_filter_end_date=filter_end_str or None,
            fertilization_filter_clear_url=fertilization_filter_clear_url,
            selected_fertilization_range=selected_fertilization_range,
            schedules=schedules,
            recommendation_groups=recommendation_groups,
            edit_fertilization=edit_fertilization,
            edit_fertilization_items=_legacy_fertilization_items(
                edit_fertilization,
                float(edit_fertilization.plot.area_hectares) if edit_fertilization and edit_fertilization.plot else None,
            ),
        ),
    )


@router.post("/fertilizacao")
async def create_fertilization_action(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    form = await request.form()
    csrf_token = str(form.get("csrf_token") or "")
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    scope, denied = _launch_scope_or_redirect(request, repo, "/fertilizacao")
    if denied:
        return denied
    selected_plot_ids: list[int] = []
    for raw_plot_id in form.getlist("plot_id"):
        parsed_plot_id = _int_or_none(raw_plot_id)
        if parsed_plot_id and parsed_plot_id not in selected_plot_ids:
            selected_plot_ids.append(parsed_plot_id)
    if not selected_plot_ids:
        _flash(request, "error", "Selecione ao menos um setor.")
        return _redirect("/fertilizacao")
    selected_plots = []
    for plot_id in selected_plot_ids:
        plot, _, denied = _resolve_plot_in_scope(request, repo, plot_id, "/fertilizacao")
        if denied:
            return denied
        selected_plots.append(plot)
    items = _parse_fertilization_items(form)
    if not items:
        _flash(request, "error", "Adicione ao menos um insumo na atividade.")
        return _redirect("/fertilizacao")
    try:
        for plot in selected_plots:
            create_fertilization(
                repo,
                {
                    "plot_id": plot.id,
                    "application_date": str(form.get("application_date") or ""),
                    "season_id": scope["active_season_id"],
                    "notes": str(form.get("notes") or "") or None,
                    "items": items,
                },
            )
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect("/fertilizacao")
    _flash(
        request,
        "success",
        "Fertilizacao registrada com sucesso."
        if len(selected_plots) == 1
        else f"Fertilizacao registrada com sucesso para {len(selected_plots)} setores.",
    )
    return _redirect("/fertilizacao")


@router.post("/fertilizacao/{record_id}/editar")
async def update_fertilization_action(
    record_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    form = await request.form()
    csrf_token = str(form.get("csrf_token") or "")
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    fertilization = repo.get_fertilization(record_id)
    if not fertilization:
        _flash(request, "error", "Registro de fertilizacao nao encontrado.")
        return _redirect_for_request(request, "/fertilizacao")
    plot, scope, denied = _resolve_plot_in_scope(request, repo, int(form.get("plot_id") or 0), "/fertilizacao")
    if denied:
        return denied
    if not _plot_matches_scope(fertilization.plot, scope):
        _flash(request, "error", "Este registro de fertilizacao nao pertence ao contexto ativo.")
        return _redirect_for_request(request, "/fertilizacao")
    items = _parse_fertilization_items(form)
    if not items:
        _flash(request, "error", "Adicione ao menos um insumo na atividade.")
        return _redirect_for_request(request, "/fertilizacao", edit_id=record_id)
    try:
        update_fertilization(
            repo,
            fertilization,
            {
                "plot_id": plot.id,
                "application_date": str(form.get("application_date") or ""),
                "season_id": scope["active_season_id"],
                "notes": str(form.get("notes") or "") or None,
                "items": items,
            },
        )
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect_for_request(request, "/fertilizacao", edit_id=record_id)
    _flash(request, "success", "Fertilizacao atualizada com sucesso.")
    return _redirect_for_request(request, "/fertilizacao")


@router.post("/fertilizacao/{record_id}/excluir")
def delete_fertilization_action(
    record_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    fertilization = repo.get_fertilization(record_id)
    if not fertilization:
        _flash(request, "error", "Registro de fertilizacao nao encontrado.")
        return _redirect("/fertilizacao")
    delete_fertilization(repo, fertilization)
    _flash(request, "success", "Fertilizacao excluida com sucesso.")
    return _redirect("/fertilizacao")


@router.get("/fertilizacao/exportar.xlsx")
def export_fertilization_xlsx(
    request: Request,
    search: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    start_date, end_date, _, _ = _schedule_filter_date_bounds(request, scope["active_season"], flash_invalid=False)
    plots = repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids)
    plot_ids = {plot.id for plot in plots}
    fertilizations = _filter_fertilization_records(repo, plot_ids, start_date, end_date, search=search)

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Fertilizacao"
    sheet.append(["Data", "Setor", "Produto", "Custo", "Insumos Aplicados", "Observações"])
    for item in fertilizations:
        sheet.append([
            item.application_date.isoformat() if item.application_date else "",
            item.plot.name if item.plot else "Setor removido",
            item.product or "",
            _format_currency(item.cost),
            " | ".join(
                f"{detail.name} ({_format_decimal_br(detail.total_quantity, 2)} {detail.unit})"
                for detail in item.items
            ) or (item.dose or ""),
            item.notes or "",
        ])
    for index, width in enumerate([14, 28, 26, 16, 54, 40], start=1):
        sheet.column_dimensions[get_column_letter(index)].width = width

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="fertilizacao.xlsx"'},
    )


@router.get("/fertilizacao/exportar.pdf")
def export_fertilization_pdf(
    request: Request,
    search: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    start_date, end_date, raw_start, raw_end = _schedule_filter_date_bounds(request, scope["active_season"], flash_invalid=False)
    plots = repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids)
    plot_ids = {plot.id for plot in plots}
    fertilizations = _filter_fertilization_records(repo, plot_ids, start_date, end_date, search=search)

    generated_at = app_now()
    generated_by = user.display_name or user.name or user.email
    farm_name = scope["active_farm"].name if scope.get("active_farm") else "Fazenda Bela Vista"
    season_label = scope["active_season"].name if scope.get("active_season") else "Safra ativa"
    period_label = "Safra ativa"
    if raw_start or raw_end:
        period_label = f"{(raw_start or '--').replace('-', '/')} a {(raw_end or '--').replace('-', '/')}"
    total_cost = sum(float(item.cost or 0) for item in fertilizations)
    total_items = sum(len(item.items or []) for item in fertilizations)

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), leftMargin=28, rightMargin=28, topMargin=32, bottomMargin=34)
    styles = getSampleStyleSheet()
    farm_header_style = ParagraphStyle("FertilizationPdfFarmHeader", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=18, leading=22, alignment=TA_RIGHT, textColor=colors.HexColor("#1e293b"))
    meta_label_style = ParagraphStyle("FertilizationPdfMetaLabel", parent=styles["BodyText"], fontName="Helvetica-Bold", fontSize=8, leading=10, textColor=colors.HexColor("#446a36"), spaceAfter=2)
    meta_value_style = ParagraphStyle("FertilizationPdfMetaValue", parent=styles["BodyText"], fontName="Helvetica", fontSize=10, leading=13, textColor=colors.HexColor("#334155"))
    cell_style = ParagraphStyle("FertilizationPdfCell", parent=styles["BodyText"], fontName="Helvetica", fontSize=8.2, leading=10.4, textColor=colors.HexColor("#0f172a"))
    cell_muted_style = ParagraphStyle("FertilizationPdfCellMuted", parent=cell_style, textColor=colors.HexColor("#475569"))
    summary_value_style = ParagraphStyle("FertilizationPdfSummaryValue", parent=styles["BodyText"], fontName="Helvetica-Bold", fontSize=11, leading=14, textColor=colors.HexColor("#0f172a"))

    logo_path = Path("app/static/images/logo.png")
    logo_flowable = Image(str(logo_path), width=92.8, height=73.6) if logo_path.exists() else Spacer(92.8, 73.6)
    header_table = Table([[logo_flowable, Paragraph(farm_name, farm_header_style)]], colWidths=[76, doc.width - 76], hAlign="LEFT")
    header_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (1, 0), (1, 0), "RIGHT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))

    summary_table = Table([
        [
            [Paragraph("FAZENDA", meta_label_style), Paragraph(farm_name, meta_value_style)],
            [Paragraph("SAFRA", meta_label_style), Paragraph(season_label, meta_value_style)],
            [Paragraph("PERÍODO", meta_label_style), Paragraph(period_label, meta_value_style)],
        ],
        [
            [Paragraph("LANÇAMENTOS", meta_label_style), Paragraph(str(len(fertilizations)), summary_value_style)],
            [Paragraph("INSUMOS APLICADOS", meta_label_style), Paragraph(str(total_items), summary_value_style)],
            [Paragraph("CUSTO TOTAL", meta_label_style), Paragraph(_format_currency(total_cost), summary_value_style)],
        ],
    ], colWidths=[doc.width / 3] * 3, hAlign="LEFT")
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc")),
        ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#dbe5dd")),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))

    elements = [header_table, Spacer(1, 16), summary_table, Spacer(1, 14)]
    data = [["Data", "Setor", "Produto", "Custo", "Insumos Aplicados", "Observações"]]
    for item in fertilizations:
        items_label = " • ".join(
            f"{detail.name} ({_format_decimal_br(detail.total_quantity, 2)} {detail.unit})"
            for detail in item.items
        ) or (item.dose or "-")
        data.append([
            Paragraph(item.application_date.strftime("%d/%m/%Y") if item.application_date else "-", cell_style),
            Paragraph(item.plot.name if item.plot else "Setor removido", cell_style),
            Paragraph(item.product or "-", cell_style),
            Paragraph(_format_currency(item.cost), cell_style),
            Paragraph(items_label, cell_muted_style),
            Paragraph(item.notes or "-", cell_muted_style),
        ])

    table = Table(data, colWidths=[60, 110, 110, 76, 260, doc.width - 616], repeatRows=1, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#446a36")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("LEADING", (0, 0), (-1, -1), 10),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
        ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#dbe5dd")),
        ("INNERGRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#e2e8f0")),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    elements.append(table)
    elements.append(Spacer(1, 14))

    footer_summary = Table([
        ["Total de lançamentos", str(len(fertilizations))],
        ["Total de insumos aplicados", str(total_items)],
        ["Custo total", _format_currency(total_cost)],
    ], colWidths=[doc.width * 0.28, doc.width * 0.18], hAlign="LEFT")
    footer_summary.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc")),
        ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#dbe5dd")),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
        ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#0f172a")),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    elements.append(footer_summary)

    generated_at_label = format_app_datetime(generated_at)

    def _draw_footer(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#e2e8f0"))
        canvas.line(doc.leftMargin, 22, landscape(A4)[0] - doc.rightMargin, 22)
        canvas.setFont("Helvetica", 8.2)
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.drawString(doc.leftMargin, 10, f"Gerado por: {generated_by}")
        canvas.drawRightString(
            landscape(A4)[0] - doc.rightMargin,
            10,
            f"Emitido em {generated_at_label} • Página {canvas.getPageNumber()}",
        )
        canvas.restoreState()

    doc.build(elements, onFirstPage=_draw_footer, onLaterPages=_draw_footer)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="fertilizacao.pdf"'},
    )


@router.get("/fertilizacao/agendamentos")
def fertilization_schedules_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    selected_schedule_tab = str(request.query_params.get("schedule_tab") or "active")
    if selected_schedule_tab not in {"active", "completed"}:
        selected_schedule_tab = "active"
    start_date, end_date, filter_start_str, filter_end_str = _schedule_filter_date_bounds(
        request, scope["active_season"], flash_invalid=True
    )
    selected_schedule_range = _schedule_tab_filter_range_preset(
        request.query_params.get("schedule_range"),
        filter_start_str,
        filter_end_str,
        schedule_tab=selected_schedule_tab,
    )
    plots = repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids)
    plot_ids = {plot.id for plot in plots}
    edit_schedule = repo.get_fertilization_schedule(edit_id) if edit_id else None
    schedules = [
        schedule
        for schedule in repo.list_fertilization_schedules()
        if schedule.plot_id in plot_ids and _within_scope(schedule.scheduled_date, start_date, end_date)
    ]
    schedules.sort(key=lambda schedule: (schedule.scheduled_date, schedule.id), reverse=True)
    schedule_validations = {schedule.id: validate_schedule_stock(repo, schedule) for schedule in schedules}
    schedule_filter_clear_url = _url_with_query(
        request,
        start_date=None,
        end_date=None,
        schedule_range=None,
        active_page=None,
        completed_page=None,
        schedule_tab=selected_schedule_tab,
    )
    active_schedules = [schedule for schedule in schedules if schedule.status != "completed"]
    completed_schedules = [schedule for schedule in schedules if schedule.status == "completed"]
    active_schedules_pagination = _paginate_collection(request, active_schedules, "active_page")
    completed_schedules_pagination = _paginate_collection(request, completed_schedules, "completed_page")
    recommendations = [
        recommendation
        for recommendation in repo.list_input_recommendations()
        if (
            (not scope["active_farm_id"] or recommendation.farm_id == scope["active_farm_id"])
            and (not plot_ids or recommendation.plot_id is None or recommendation.plot_id in plot_ids)
        )
    ]
    consolidated_inputs = repo.list_input_catalog(item_type="insumo_agricola")
    purchased_inputs = repo.list_purchased_inputs()
    input_stock = {
        item.id: {
            "available": round(
                sum(
                    float(entry.available_quantity or 0)
                    for entry in purchased_inputs
                    if entry.input_id == item.id
                ),
                2,
            ),
            "unit": item.default_unit,
        }
        for item in consolidated_inputs
    }
    edit_schedule_items = (
        [
            {
                "input_id": item.input_id,
                "unit": item.unit,
                "quantity": float(item.quantity or 0),
            }
            for item in edit_schedule.items
        ]
        if edit_schedule and edit_schedule.items
        else [{"input_id": "", "unit": "kg", "quantity": ""}]
    )
    schedule_recommendations = [
        {
            "id": recommendation.id,
            "application_name": recommendation.application_name,
            "plot_name": recommendation.plot.name if recommendation.plot else None,
            "farm_name": recommendation.farm.name if recommendation.farm else None,
            "items": [
                {
                    "input_id": item.input_id,
                    "unit": item.unit,
                    "quantity": float(item.quantity or 0),
                }
                for item in recommendation.items
                if item.input_id
            ],
        }
        for recommendation in recommendations
        if recommendation.items
    ]
    return templates.TemplateResponse(
        "fertilization_schedule.html",
        _base_context(
            request,
            user,
            csrf_token,
            "fertilization_schedules",
            _repo=repo,
            title="Agendamento de Fertilizacao",
            plots=plots,
            inputs_catalog=consolidated_inputs,
            input_stock=input_stock,
            active_schedules=active_schedules_pagination["items"],
            active_schedules_pagination=active_schedules_pagination,
            completed_schedules=completed_schedules_pagination["items"],
            completed_schedules_pagination=completed_schedules_pagination,
            selected_schedule_tab=selected_schedule_tab,
            schedule_tab_urls={
                "active": _url_with_query(request, schedule_tab="active"),
                "completed": _url_with_query(request, schedule_tab="completed"),
            },
            schedule_filter_start_date=filter_start_str or None,
            schedule_filter_end_date=filter_end_str or None,
            selected_schedule_range=selected_schedule_range,
            schedule_filter_clear_url=schedule_filter_clear_url,
            schedule_validations=schedule_validations,
            edit_schedule=edit_schedule,
            edit_schedule_items=edit_schedule_items,
            schedule_recommendations=schedule_recommendations,
            today=today_in_app_timezone(),
        ),
    )


@router.get("/fertilizacao/agendamentos/exportar.xlsx")
def export_fertilization_schedules_xlsx(
    request: Request,
    schedule_tab: str | None = None,
    search: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    start_date, end_date, _, _ = _schedule_filter_date_bounds(request, scope["active_season"], flash_invalid=False)
    plots = repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids)
    plot_ids = {plot.id for plot in plots}
    schedules = [
        schedule
        for schedule in repo.list_fertilization_schedules()
        if schedule.plot_id in plot_ids and _within_scope(schedule.scheduled_date, start_date, end_date)
    ]
    schedules.sort(key=lambda schedule: (schedule.scheduled_date, schedule.id), reverse=True)
    selected_schedule_tab = schedule_tab if schedule_tab in {"active", "completed"} else "active"
    search_query = (search or "").strip().lower()
    if search_query:
        schedules = [
            schedule
            for schedule in schedules
            if search_query in (
                f"{schedule.plot.name if schedule.plot else ''} "
                f"{schedule.scheduled_date or ''} "
                f"{schedule.status or ''} "
                f"{schedule.notes or ''} "
                + " ".join(item.input_catalog.name if item.input_catalog else item.name for item in schedule.items)
            ).lower()
        ]
    filtered_schedules = [schedule for schedule in schedules if schedule.status == "completed"] if selected_schedule_tab == "completed" else [schedule for schedule in schedules if schedule.status != "completed"]

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Concluídos" if selected_schedule_tab == "completed" else "Ativos"
    sheet.append(["Data", "Setor", "Status", "Itens Programados", "Observações"])
    for schedule in filtered_schedules:
        sheet.append([
            schedule.scheduled_date.isoformat() if schedule.scheduled_date else "",
            schedule.plot.name if schedule.plot else "Setor removido",
            "Concluído" if schedule.status == "completed" else "Agendado",
            " | ".join(
                f"{item.input_catalog.name if item.input_catalog else item.name} ({_format_decimal_br(item.quantity, 2)} {item.unit})"
                for item in schedule.items
            ),
            schedule.notes or "",
        ])
    for index, width in enumerate([14, 28, 16, 54, 40], start=1):
        sheet.column_dimensions[get_column_letter(index)].width = width

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="planejamento_agendamentos.xlsx"'},
    )


@router.get("/fertilizacao/agendamentos/exportar.pdf")
def export_fertilization_schedules_pdf(
    request: Request,
    schedule_tab: str | None = None,
    search: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    start_date, end_date, _, _ = _schedule_filter_date_bounds(request, scope["active_season"], flash_invalid=False)
    plots = repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids)
    plot_ids = {plot.id for plot in plots}
    schedules = [
        schedule
        for schedule in repo.list_fertilization_schedules()
        if schedule.plot_id in plot_ids and _within_scope(schedule.scheduled_date, start_date, end_date)
    ]
    schedules.sort(key=lambda schedule: (schedule.scheduled_date, schedule.id), reverse=True)
    selected_schedule_tab = schedule_tab if schedule_tab in {"active", "completed"} else "active"
    search_query = (search or "").strip().lower()
    if search_query:
        schedules = [
            schedule
            for schedule in schedules
            if search_query in (
                f"{schedule.plot.name if schedule.plot else ''} "
                f"{schedule.scheduled_date or ''} "
                f"{schedule.status or ''} "
                f"{schedule.notes or ''} "
                + " ".join(item.input_catalog.name if item.input_catalog else item.name for item in schedule.items)
            ).lower()
        ]
    schedules = [schedule for schedule in schedules if schedule.status == "completed"] if selected_schedule_tab == "completed" else [schedule for schedule in schedules if schedule.status != "completed"]

    generated_at = app_now()
    generated_by = user.display_name or user.name or user.email
    farm_name = scope["active_farm"].name if scope.get("active_farm") else "Fazenda Bela Vista"
    active_count = sum(1 for schedule in schedules if schedule.status != "completed")
    completed_count = sum(1 for schedule in schedules if schedule.status == "completed")
    total_items = sum(len(schedule.items or []) for schedule in schedules)
    season_label = scope["active_season"].name if scope.get("active_season") else "Safra ativa"
    scope_label = "Concluídos" if selected_schedule_tab == "completed" else "Ativos"

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), leftMargin=28, rightMargin=28, topMargin=32, bottomMargin=34)
    styles = getSampleStyleSheet()
    farm_header_style = ParagraphStyle("SchedulePdfFarmHeader", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=18, leading=22, alignment=TA_RIGHT, textColor=colors.HexColor("#1e293b"))
    meta_label_style = ParagraphStyle("SchedulePdfMetaLabel", parent=styles["BodyText"], fontName="Helvetica-Bold", fontSize=8, leading=10, textColor=colors.HexColor("#446a36"), spaceAfter=2)
    meta_value_style = ParagraphStyle("SchedulePdfMetaValue", parent=styles["BodyText"], fontName="Helvetica", fontSize=10, leading=13, textColor=colors.HexColor("#334155"))
    cell_style = ParagraphStyle("SchedulePdfCell", parent=styles["BodyText"], fontName="Helvetica", fontSize=8.2, leading=10.4, textColor=colors.HexColor("#0f172a"))
    cell_muted_style = ParagraphStyle("SchedulePdfCellMuted", parent=cell_style, textColor=colors.HexColor("#475569"))
    summary_value_style = ParagraphStyle("SchedulePdfSummaryValue", parent=styles["BodyText"], fontName="Helvetica-Bold", fontSize=11, leading=14, textColor=colors.HexColor("#0f172a"))

    logo_path = Path("app/static/images/logo.png")
    logo_flowable = Image(str(logo_path), width=92.8, height=73.6) if logo_path.exists() else Spacer(92.8, 73.6)
    header_table = Table([[logo_flowable, Paragraph(farm_name, farm_header_style)]], colWidths=[76, doc.width - 76], hAlign="LEFT")
    header_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (1, 0), (1, 0), "RIGHT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))

    summary_table = Table([
        [
            [Paragraph("FAZENDA", meta_label_style), Paragraph(farm_name, meta_value_style)],
            [Paragraph("SAFRA", meta_label_style), Paragraph(season_label, meta_value_style)],
            [Paragraph("LISTA", meta_label_style), Paragraph(scope_label, meta_value_style)],
        ],
        [
            [Paragraph("CONCLUÍDOS", meta_label_style), Paragraph(str(completed_count), summary_value_style)],
            [Paragraph("TOTAL DE AGENDAMENTOS", meta_label_style), Paragraph(str(len(schedules)), summary_value_style)],
            [Paragraph("ITENS PROGRAMADOS", meta_label_style), Paragraph(str(total_items), summary_value_style)],
        ],
    ], colWidths=[doc.width / 3] * 3, hAlign="LEFT")
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc")),
        ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#dbe5dd")),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))

    elements = [header_table, Spacer(1, 16), summary_table, Spacer(1, 14)]
    column_weights = [10, 18, 12, 34, 26]
    weight_total = sum(column_weights)
    table_col_widths = [doc.width * (weight / weight_total) for weight in column_weights[:-1]]
    table_col_widths.append(doc.width - sum(table_col_widths))
    notes_col_width = table_col_widths[-1] - 14
    notes_max_height = cell_muted_style.leading * 4 + 2

    data = [["Data", "Setor", "Situação", "Itens Programados", "Observações"]]
    for schedule in schedules:
        items_label = " • ".join(
            f"{item.input_catalog.name if item.input_catalog else item.name} ({_format_decimal_br(item.quantity, 2)} {item.unit})"
            for item in schedule.items
        ) or "-"
        status_label = "Concluído" if schedule.status == "completed" else "Agendado"
        status_color = "#166534" if schedule.status == "completed" else "#0369a1"
        data.append([
            Paragraph(schedule.scheduled_date.strftime("%d/%m/%Y") if schedule.scheduled_date else "-", cell_style),
            Paragraph(schedule.plot.name if schedule.plot else "Setor removido", cell_style),
            Paragraph(f'<font color="{status_color}"><b>{status_label}</b></font>', cell_style),
            Paragraph(items_label, cell_muted_style),
            KeepInFrame(notes_col_width, notes_max_height, [Paragraph(schedule.notes or "-", cell_muted_style)], mode="truncate"),
        ])

    table = Table(data, repeatRows=1, colWidths=table_col_widths, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#446a36")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("LINEBELOW", (0, 0), (-1, 0), 0.8, colors.HexColor("#36552a")),
        ("LINEBELOW", (0, 1), (-1, -1), 0.35, colors.HexColor("#e2e8f0")),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.2),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
    ]))
    elements.append(table)
    elements.append(Spacer(1, 12))

    footer_summary = Table([[
        Paragraph(f"<b>Ativos:</b> {active_count}", meta_value_style),
        Paragraph(f"<b>Concluídos:</b> {completed_count}", meta_value_style),
        Paragraph(f"<b>Total de agendamentos:</b> {len(schedules)} • <b>Itens:</b> {total_items}", summary_value_style),
    ]], colWidths=[doc.width * 0.25, doc.width * 0.25, doc.width * 0.50], hAlign="LEFT")
    footer_summary.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#eef6ee")),
        ("BOX", (0, 0), (-1, -1), 0.7, colors.HexColor("#cfe1d0")),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
    ]))
    elements.append(footer_summary)

    generated_at_label = generated_at.strftime("%d/%m/%Y %H:%M")

    def _draw_footer(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#e2e8f0"))
        canvas.line(doc.leftMargin, 22, landscape(A4)[0] - doc.rightMargin, 22)
        canvas.setFont("Helvetica", 8.2)
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.drawString(doc.leftMargin, 10, f"Gerado por: {generated_by}")
        canvas.drawRightString(
            landscape(A4)[0] - doc.rightMargin,
            10,
            f"Emitido em {generated_at_label} • Página {canvas.getPageNumber()}",
        )
        canvas.restoreState()

    doc.build(elements, onFirstPage=_draw_footer, onLaterPages=_draw_footer)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="planejamento_agendamentos.pdf"'},
    )


@router.post("/fertilizacao/agendamentos")
async def create_fertilization_schedule_action(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    form = await request.form()
    validate_csrf(request, str(form.get("csrf_token") or ""))
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    allowed_plots = {
        plot.id: plot
        for plot in repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids)
    }
    selected_plot_ids = []
    for raw_plot_id in form.getlist("plot_id"):
        parsed_plot_id = _int_or_none(raw_plot_id)
        if parsed_plot_id and parsed_plot_id not in selected_plot_ids:
            selected_plot_ids.append(parsed_plot_id)
    selected_plots = [allowed_plots[plot_id] for plot_id in selected_plot_ids if plot_id in allowed_plots]
    items = _parse_recommendation_items(form)
    if not selected_plots or not items:
        _flash(request, "error", "Selecione ao menos um setor e adicione ao menos um insumo.")
        return _redirect("/fertilizacao/agendamentos")
    for plot in selected_plots:
        create_fertilization_schedule(
            repo,
            {
                "plot_id": plot.id,
                "scheduled_date": str(form.get("scheduled_date") or ""),
                "season_id": scope["active_season_id"],
                "status": str(form.get("status") or "scheduled"),
                "notes": str(form.get("notes") or "") or None,
                "items": items,
            },
        )
    if len(selected_plots) == 1:
        _flash(request, "success", "Agendamento salvo com sucesso.")
    else:
        _flash(request, "success", f"Agendamentos salvos com sucesso para {len(selected_plots)} setores.")
    return _redirect("/fertilizacao/agendamentos")


@router.post("/fertilizacao/agendamentos/{schedule_id}/editar")
async def update_fertilization_schedule_action(
    schedule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    form = await request.form()
    validate_csrf(request, str(form.get("csrf_token") or ""))
    repo = _repository(db)
    plot, scope, denied = _resolve_plot_in_scope(
        request,
        repo,
        int(form.get("plot_id") or 0),
        "/fertilizacao/agendamentos",
    )
    if denied:
        return denied
    schedule = repo.get_fertilization_schedule(schedule_id)
    if not schedule:
        _flash(request, "error", "Agendamento nao encontrado.")
        return _redirect_for_request(request, "/fertilizacao/agendamentos")
    if not _plot_matches_scope(schedule.plot, scope):
        _flash(request, "error", "Este agendamento nao pertence ao contexto ativo.")
        return _redirect_for_request(request, "/fertilizacao/agendamentos")
    items = _parse_recommendation_items(form)
    if not items:
        _flash(request, "error", "Adicione ao menos um insumo ao agendamento.")
        return _redirect_for_request(request, "/fertilizacao/agendamentos", edit_id=schedule_id)
    update_fertilization_schedule(
        repo,
        schedule,
        {
            "plot_id": plot.id,
            "scheduled_date": str(form.get("scheduled_date") or ""),
            "season_id": scope["active_season_id"],
            "status": str(form.get("status") or schedule.status),
            "notes": str(form.get("notes") or "") or None,
            "items": items,
        },
    )
    _flash(request, "success", "Agendamento atualizado com sucesso.")
    return _redirect_for_request(request, "/fertilizacao/agendamentos")


@router.post("/fertilizacao/agendamentos/{schedule_id}/concluir")
def conclude_fertilization_schedule_action(
    schedule_id: int,
    request: Request,
    csrf_token: str = Form(...),
    application_date: str | None = Form(None),
    redirect_to: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    target_url = redirect_to if redirect_to and redirect_to.startswith("/") else "/fertilizacao/agendamentos"
    repo = _repository(db)
    schedule = repo.get_fertilization_schedule(schedule_id)
    if not schedule:
        _flash(request, "error", "Agendamento nao encontrado.")
        return _redirect(target_url)
    validation = validate_schedule_stock(repo, schedule)
    if not validation["ok"]:
        first = validation["shortages"][0]
        _flash(request, "error", f"Estoque insuficiente. Necessario comprar {first['missing']} {first['unit']} de {first['name']}.")
        return _redirect(target_url)
    try:
        conclude_fertilization_schedule(repo, schedule, application_date)
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect(target_url)
    _flash(request, "success", "Agendamento concluido e aplicacao registrada.")
    return _redirect(target_url)


@router.post("/fertilizacao/agendamentos/{schedule_id}/excluir")
def delete_fertilization_schedule_action(
    schedule_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    schedule = repo.get_fertilization_schedule(schedule_id)
    if not schedule:
        _flash(request, "error", "Agendamento nao encontrado.")
        return _redirect("/fertilizacao/agendamentos")
    delete_fertilization_schedule(repo, schedule)
    _flash(request, "success", "Agendamento excluido com sucesso.")
    return _redirect("/fertilizacao/agendamentos")


@router.get("/producao")
def production_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    start_date, end_date = _scoped_dates(scope["active_season"])
    plots = repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids)
    plot_ids = {plot.id for plot in plots}
    harvests = [
        harvest
        for harvest in repo.list_harvests()
        if harvest.plot_id in plot_ids and _within_scope(harvest.harvest_date, start_date, end_date)
    ]
    harvests = _sort_collection_desc(
        harvests,
        lambda item: item.harvest_date,
        lambda item: item.id,
    )
    harvests_pagination = _paginate_collection(request, harvests, "harvests_page")
    return templates.TemplateResponse(
        "production.html",
        _base_context(
            request,
            user,
            csrf_token,
            "production",
            _repo=repo,
            plots=plots,
            harvests=harvests_pagination["items"],
            harvests_pagination=harvests_pagination,
            edit_harvest=repo.get_harvest(edit_id) if edit_id else None,
        ),
    )


@router.post("/producao")
def create_harvest_action(
    request: Request,
    csrf_token: str = Form(...),
    plot_id: int = Form(...),
    harvest_date: str = Form(...),
    sacks_produced: float = Form(...),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    plot, _, denied = _resolve_plot_in_scope(request, repo, plot_id, "/producao")
    if denied:
        return denied
    area = float(plot.area_hectares) if plot else 0
    create_harvest(
        repo,
        {
            "plot_id": plot.id,
            "harvest_date": harvest_date,
            "sacks_produced": sacks_produced,
            "notes": notes,
        },
        area,
    )
    _flash(request, "success", "Colheita registrada com sucesso.")
    return _redirect("/producao")


@router.post("/producao/{record_id}/editar")
def update_harvest_action(
    record_id: int,
    request: Request,
    csrf_token: str = Form(...),
    plot_id: int = Form(...),
    harvest_date: str = Form(...),
    sacks_produced: float = Form(...),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    plot, scope, denied = _resolve_plot_in_scope(request, repo, plot_id, "/producao")
    if denied:
        return denied
    harvest = repo.get_harvest(record_id)
    if not harvest:
        _flash(request, "error", "Registro de producao nao encontrado.")
        return _redirect_for_request(request, "/producao")
    if not _plot_matches_scope(harvest.plot, scope):
        _flash(request, "error", "Este registro de producao nao pertence ao contexto ativo.")
        return _redirect_for_request(request, "/producao")
    area = float(plot.area_hectares) if plot else 0
    update_harvest(
        repo,
        harvest,
        {
            "plot_id": plot.id,
            "harvest_date": harvest_date,
            "sacks_produced": sacks_produced,
            "notes": notes,
        },
        area,
    )
    _flash(request, "success", "Colheita atualizada com sucesso.")
    return _redirect_for_request(request, "/producao")


@router.post("/producao/{record_id}/excluir")
def delete_harvest_action(
    record_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    harvest = repo.get_harvest(record_id)
    if not harvest:
        _flash(request, "error", "Registro de producao nao encontrado.")
        return _redirect("/producao")
    repo.delete(harvest)
    _flash(request, "success", "Colheita excluida com sucesso.")
    return _redirect("/producao")


@router.get("/pragas")
def pests_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    start_date, end_date = _scoped_dates(scope["active_season"])
    plots = repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids)
    plot_ids = {plot.id for plot in plots}
    incidents = [
        incident
        for incident in repo.list_pest_incidents()
        if incident.plot_id in plot_ids and _within_scope(incident.occurrence_date, start_date, end_date)
    ]
    incidents = _sort_collection_desc(
        incidents,
        lambda item: item.occurrence_date,
        lambda item: item.id,
    )
    incidents_pagination = _paginate_collection(request, incidents, "incidents_page")
    return templates.TemplateResponse(
        "pests.html",
        _base_context(
            request,
            user,
            csrf_token,
            "pests",
            _repo=repo,
            plots=plots,
            incidents=incidents_pagination["items"],
            incidents_pagination=incidents_pagination,
            edit_incident=repo.get_pest_incident(edit_id) if edit_id else None,
        ),
    )


@router.post("/pragas")
def create_pest_action(
    request: Request,
    csrf_token: str = Form(...),
    plot_id: int = Form(...),
    occurrence_date: str = Form(...),
    category: str = Form(...),
    name: str = Form(...),
    severity: int = Form(...),
    treatment: str | None = Form(None),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    plot, _, denied = _resolve_plot_in_scope(request, repo, plot_id, "/pragas")
    if denied:
        return denied
    create_pest_incident(
        repo,
        {
            "plot_id": plot.id,
            "occurrence_date": occurrence_date,
            "category": category,
            "name": name,
            "severity": severity,
            "treatment": treatment,
            "notes": notes,
        },
    )
    _flash(request, "success", "Ocorrencia registrada com sucesso.")
    return _redirect("/pragas")


@router.post("/pragas/{record_id}/editar")
def update_pest_action(
    record_id: int,
    request: Request,
    csrf_token: str = Form(...),
    plot_id: int = Form(...),
    occurrence_date: str = Form(...),
    category: str = Form(...),
    name: str = Form(...),
    severity: int = Form(...),
    treatment: str | None = Form(None),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    plot, scope, denied = _resolve_plot_in_scope(request, repo, plot_id, "/pragas")
    if denied:
        return denied
    incident = repo.get_pest_incident(record_id)
    if not incident:
        _flash(request, "error", "Ocorrencia nao encontrada.")
        return _redirect_for_request(request, "/pragas")
    if not _plot_matches_scope(incident.plot, scope):
        _flash(request, "error", "Esta ocorrencia nao pertence ao contexto ativo.")
        return _redirect_for_request(request, "/pragas")
    update_pest_incident(
        repo,
        incident,
        {
            "plot_id": plot.id,
            "occurrence_date": occurrence_date,
            "category": category,
            "name": name,
            "severity": severity,
            "treatment": treatment,
            "notes": notes,
        },
    )
    _flash(request, "success", "Ocorrencia atualizada com sucesso.")
    return _redirect_for_request(request, "/pragas")


@router.post("/pragas/{record_id}/excluir")
def delete_pest_action(
    record_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    incident = repo.get_pest_incident(record_id)
    if not incident:
        _flash(request, "error", "Ocorrencia nao encontrada.")
        return _redirect("/pragas")
    repo.delete(incident)
    _flash(request, "success", "Ocorrencia excluida com sucesso.")
    return _redirect("/pragas")


@router.get("/perfil-agronomico")
def agronomic_profiles_page(
    request: Request,
    edit_farm_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    del edit_farm_id
    scope = _global_scope_context(request, repo)
    selected_farm = scope["active_farm"]
    return templates.TemplateResponse(
        "agronomic_profiles.html",
        _base_context(
            request,
            user,
            csrf_token,
            "agronomic_profiles",
            _repo=repo,
            title="Perfil Agronomico",
            farms=repo.list_farms(),
            profiles=[
                profile
                for profile in repo.list_agronomic_profiles()
                if not scope["active_farm_id"] or profile.farm_id == scope["active_farm_id"]
            ],
            edit_farm=selected_farm,
            edit_profile=repo.get_agronomic_profile_by_farm(scope["active_farm_id"]) if scope["active_farm_id"] else None,
        ),
    )


@router.post("/perfil-agronomico")
def save_agronomic_profile_action(
    request: Request,
    csrf_token: str = Form(...),
    culture: str = Form(...),
    region: str = Form(...),
    climate: str | None = Form(None),
    soil_type: str | None = Form(None),
    irrigation_system: str | None = Form(None),
    plant_spacing: str | None = Form(None),
    drip_spacing: str | None = Form(None),
    fertilizers_used: str | None = Form(None),
    crop_stage: str | None = Form(None),
    common_pests: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    scope, denied = _launch_scope_or_redirect(request, repo, "/perfil-agronomico")
    if denied:
        return denied
    farm_id = scope["active_farm_id"]
    existing = repo.get_agronomic_profile_by_farm(farm_id)
    payload = {
        "farm_id": farm_id,
        "culture": culture,
        "region": region,
        "climate": climate,
        "soil_type": soil_type,
        "irrigation_system": irrigation_system,
        "plant_spacing": plant_spacing,
        "drip_spacing": drip_spacing,
        "fertilizers_used": fertilizers_used,
        "crop_stage": crop_stage,
        "common_pests": common_pests,
    }
    if existing:
        update_agronomic_profile(repo, existing, payload)
        _flash(request, "success", "Perfil agronomico atualizado com sucesso.")
    else:
        create_agronomic_profile(repo, payload)
        _flash(request, "success", "Perfil agronomico salvo com sucesso.")
    return _redirect("/perfil-agronomico")


@router.post("/perfil-agronomico/{farm_id}/excluir")
def delete_agronomic_profile_action(
    farm_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    scope, denied = _launch_scope_or_redirect(request, repo, "/perfil-agronomico")
    if denied:
        return denied
    if farm_id != scope["active_farm_id"]:
        _flash(request, "error", "Este perfil agronomico nao pertence ao contexto ativo.")
        return _redirect("/perfil-agronomico")
    profile = repo.get_agronomic_profile_by_farm(farm_id)
    if not profile:
        _flash(request, "error", "Perfil agronomico nao encontrado.")
        return _redirect("/perfil-agronomico")
    repo.delete(profile)
    _flash(request, "success", "Perfil agronomico removido com sucesso.")
    return _redirect("/perfil-agronomico")


@router.get("/analise-solo")
def soil_analysis_page(
    request: Request,
    farm_id: int | None = None,
    plot_id: int | None = None,
    edit_id: int | None = None,
    compare_plot_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    effective_farm_id = farm_id or scope["active_farm_id"]
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    start_date, end_date = _scoped_dates(scope["active_season"])
    plots = repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids)
    plot_ids = {plot.id for plot in plots}
    effective_plot_id = plot_id if plot_id else None
    analyses = [
        analysis
        for analysis in repo.list_soil_analyses(farm_id=effective_farm_id, plot_id=effective_plot_id)
        if (not plot_ids or analysis.plot_id in plot_ids) and _within_scope(analysis.analysis_date, start_date, end_date)
    ]
    analyses = _sort_collection_desc(
        analyses,
        lambda item: item.analysis_date,
        lambda item: item.id,
    )
    analyses_pagination = _paginate_collection(request, analyses, "analyses_page")
    compare_target = compare_plot_id or effective_plot_id
    compare_analyses = (
        [
            analysis
            for analysis in repo.list_soil_analyses(plot_id=compare_target)
            if _within_scope(analysis.analysis_date, start_date, end_date)
        ]
        if compare_target
        else analyses[:6]
    )
    return templates.TemplateResponse(
        "soil_analyses.html",
        _base_context(
            request,
            user,
            csrf_token,
            "soil_analyses",
            _repo=repo,
            title="Analise de Solo",
            farms=repo.list_farms(),
            plots=plots,
            analyses=analyses_pagination["items"],
            analyses_pagination=analyses_pagination,
            edit_analysis=repo.get_soil_analysis(edit_id) if edit_id else None,
            filters={"farm_id": effective_farm_id, "plot_id": effective_plot_id, "compare_plot_id": compare_target},
            compare_chart=_soil_history_chart(compare_analyses),
            compare_analyses=compare_analyses,
            latest_recommendations=[item for item in analyses if item.ai_recommendation][:4],
        ),
    )


@router.post("/analise-solo")
def create_soil_analysis_action(
    request: Request,
    csrf_token: str = Form(...),
    plot_id: int = Form(...),
    analysis_date: str = Form(...),
    laboratory: str = Form(...),
    ph: str | None = Form(None),
    organic_matter: str | None = Form(None),
    phosphorus: str | None = Form(None),
    potassium: str | None = Form(None),
    calcium: str | None = Form(None),
    magnesium: str | None = Form(None),
    aluminum: str | None = Form(None),
    h_al: str | None = Form(None),
    ctc: str | None = Form(None),
    base_saturation: str | None = Form(None),
    observations: str | None = Form(None),
    analysis_pdf: UploadFile | None = File(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    plot, scope, denied = _resolve_plot_in_scope(request, repo, plot_id, "/analise-solo")
    if denied:
        return denied
    pdf_filename, pdf_content_type, pdf_data = _read_upload(analysis_pdf)
    analysis = create_soil_analysis(
        repo,
        _soil_payload(
            farm_id=scope["active_farm_id"],
            plot_id=plot.id,
            analysis_date=analysis_date,
            laboratory=laboratory,
            ph=ph,
            organic_matter=organic_matter,
            phosphorus=phosphorus,
            potassium=potassium,
            calcium=calcium,
            magnesium=magnesium,
            aluminum=aluminum,
            h_al=h_al,
            ctc=ctc,
            base_saturation=base_saturation,
            observations=observations,
            pdf_filename=pdf_filename,
            pdf_content_type=pdf_content_type,
            pdf_data=pdf_data,
        ),
    )
    analysis = _apply_soil_ai_recommendation(repo, analysis)
    if analysis.ai_status == "generated":
        _flash(request, "success", "Analise de solo salva e recomendacao da IA gerada com sucesso.")
    elif analysis.ai_status == "skipped":
        _flash(request, "success", "Analise de solo salva. Configure a OPENAI_API_KEY para gerar recomendacoes personalizadas.")
    else:
        _flash(request, "error", "Analise de solo salva, mas a recomendacao da IA falhou nesta tentativa.")
    return _redirect_with_query("/analise-solo", plot_id=plot.id, compare_plot_id=plot.id)


@router.post("/analise-solo/{analysis_id}/editar")
def update_soil_analysis_action(
    analysis_id: int,
    request: Request,
    csrf_token: str = Form(...),
    plot_id: int = Form(...),
    analysis_date: str = Form(...),
    laboratory: str = Form(...),
    ph: str | None = Form(None),
    organic_matter: str | None = Form(None),
    phosphorus: str | None = Form(None),
    potassium: str | None = Form(None),
    calcium: str | None = Form(None),
    magnesium: str | None = Form(None),
    aluminum: str | None = Form(None),
    h_al: str | None = Form(None),
    ctc: str | None = Form(None),
    base_saturation: str | None = Form(None),
    observations: str | None = Form(None),
    analysis_pdf: UploadFile | None = File(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    plot, scope, denied = _resolve_plot_in_scope(request, repo, plot_id, "/analise-solo")
    if denied:
        return denied
    analysis = repo.get_soil_analysis(analysis_id)
    if not analysis:
        _flash(request, "error", "Analise de solo nao encontrada.")
        return _redirect_for_request(request, "/analise-solo")
    if not _farm_matches_scope(analysis.farm_id, scope) or not _plot_matches_scope(analysis.plot, scope):
        _flash(request, "error", "Esta analise de solo nao pertence ao contexto ativo.")
        return _redirect_for_request(request, "/analise-solo")
    pdf_filename, pdf_content_type, pdf_data = _read_upload(analysis_pdf)
    updated = update_soil_analysis(
        repo,
        analysis,
        _soil_payload(
            farm_id=scope["active_farm_id"],
            plot_id=plot.id,
            analysis_date=analysis_date,
            laboratory=laboratory,
            ph=ph,
            organic_matter=organic_matter,
            phosphorus=phosphorus,
            potassium=potassium,
            calcium=calcium,
            magnesium=magnesium,
            aluminum=aluminum,
            h_al=h_al,
            ctc=ctc,
            base_saturation=base_saturation,
            observations=observations,
            pdf_filename=pdf_filename,
            pdf_content_type=pdf_content_type,
            pdf_data=pdf_data,
            current_analysis=analysis,
        ),
    )
    updated = _apply_soil_ai_recommendation(repo, updated)
    if updated.ai_status == "generated":
        _flash(request, "success", "Analise de solo atualizada e recomendacao regenerada.")
    elif updated.ai_status == "skipped":
        _flash(request, "success", "Analise de solo atualizada. Configure a OPENAI_API_KEY para gerar recomendacoes personalizadas.")
    else:
        _flash(request, "error", "Analise de solo atualizada, mas a recomendacao da IA falhou nesta tentativa.")
    return _redirect_for_request(request, "/analise-solo", plot_id=plot.id, compare_plot_id=plot.id)


@router.post("/analise-solo/{analysis_id}/regenerar")
def regenerate_soil_recommendation_action(
    analysis_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    analysis = repo.get_soil_analysis(analysis_id)
    if not analysis:
        _flash(request, "error", "Analise de solo nao encontrada.")
        return _redirect("/analise-solo")
    updated = _apply_soil_ai_recommendation(repo, analysis)
    if updated.ai_status == "generated":
        _flash(request, "success", "Recomendacao agronomica regenerada com sucesso.")
    elif updated.ai_status == "skipped":
        _flash(request, "error", "OPENAI_API_KEY nao configurada para gerar a recomendacao.")
    else:
        _flash(request, "error", "Nao foi possivel gerar a recomendacao agronomica nesta tentativa.")
    return _redirect_with_query("/analise-solo", edit_id=analysis_id, plot_id=analysis.plot_id, compare_plot_id=analysis.plot_id)


@router.post("/analise-solo/{analysis_id}/excluir")
def delete_soil_analysis_action(
    analysis_id: int,
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    analysis = repo.get_soil_analysis(analysis_id)
    if not analysis:
        _flash(request, "error", "Analise de solo nao encontrada.")
        return _redirect("/analise-solo")
    plot_id = analysis.plot_id
    repo.delete(analysis)
    _flash(request, "success", "Analise de solo excluida com sucesso.")
    return _redirect_with_query("/analise-solo", compare_plot_id=plot_id)


@router.get("/analise-solo/{analysis_id}/pdf")
def soil_analysis_pdf_download(
    analysis_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    analysis = _repository(db).get_soil_analysis(analysis_id)
    if not analysis or not analysis.pdf_data:
        return Response(status_code=status.HTTP_404_NOT_FOUND)
    headers = {"Content-Disposition": f'inline; filename="{analysis.pdf_filename or "analise-solo.pdf"}"'}
    return Response(content=analysis.pdf_data, media_type=analysis.pdf_content_type or "application/pdf", headers=headers)


@router.get("/mapa")
def map_page(
    request: Request,
    edit_plot_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    geojson = build_dashboard_context(
        repo,
        farm_id=scope["active_farm_id"],
        season=scope["active_season"],
    )["map_geojson"]
    edit_plot = repo.get_plot(edit_plot_id) if edit_plot_id else None
    return templates.TemplateResponse(
        "map.html",
        _base_context(
            request,
            user,
            csrf_token,
            "map",
            _repo=repo,
            map_geojson=geojson,
            plots=repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids),
            edit_plot=edit_plot,
            edit_plot_geometry=edit_plot.boundary_geojson if edit_plot and edit_plot.boundary_geojson else None,
        ),
    )


@router.post("/mapa/setores/{plot_id}/salvar")
def save_plot_map_geometry(
    plot_id: int,
    request: Request,
    csrf_token: str = Form(...),
    boundary_geojson: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    repo = _repository(db)
    plot = repo.get_plot(plot_id)
    if not plot:
        _flash(request, "error", "Setor nao encontrado para salvar a edicao no mapa.")
        return _redirect("/mapa")
    normalized = normalize_geojson(boundary_geojson)
    if not normalized:
        _flash(request, "error", "A geometria enviada nao e valida.")
        return _redirect_with_query("/mapa", edit_plot_id=plot_id)
    area_hectares = calculate_geojson_area_hectares(normalized)
    centroid_lat, centroid_lng = estimate_geojson_centroid(normalized)
    repo.update(
        plot,
        {
            "boundary_geojson": normalized,
            "area_hectares": area_hectares if area_hectares is not None else plot.area_hectares,
            "centroid_lat": centroid_lat if centroid_lat is not None else plot.centroid_lat,
            "centroid_lng": centroid_lng if centroid_lng is not None else plot.centroid_lng,
        },
    )
    _flash(request, "success", "Poligono do setor atualizado com sucesso no mapa.")
    return _redirect_with_query("/mapa", edit_plot_id=plot_id)


@router.get("/mobile")
def mobile_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    scope = _global_scope_context(request, repo)
    farm_ids, variety_ids = _scoped_plot_filters(request, scope["active_season"])
    start_date, end_date = _scoped_dates(scope["active_season"])
    plots = repo.list_plots(farm_ids=farm_ids, variety_ids=variety_ids)
    plot_ids = {plot.id for plot in plots}
    return templates.TemplateResponse(
        "mobile.html",
        _base_context(
            request,
            user,
            csrf_token,
            "mobile",
            _repo=repo,
            plots=plots,
            quick_irrigations=[
                irrigation
                for irrigation in repo.list_irrigations(limit=20)
                if irrigation.plot_id in plot_ids and _within_scope(irrigation.irrigation_date, start_date, end_date)
            ][:3],
            quick_incidents=[
                incident
                for incident in repo.list_pest_incidents(limit=20)
                if incident.plot_id in plot_ids and _within_scope(incident.occurrence_date, start_date, end_date)
            ][:3],
        ),
    )
