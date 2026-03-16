from datetime import datetime
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile, status
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.core.csrf import validate_csrf
from app.core.deps import get_csrf_token, get_current_user_web
from app.db.session import get_db
from app.models import CoffeeVariety, Farm, FertilizationRecord, HarvestRecord, IrrigationRecord, PestIncident, Plot, User
from app.repositories.farm import FarmRepository
from app.services.dashboard import build_dashboard_context
from app.services.forms import (
    calculate_irrigation_volume,
    create_farm,
    create_fertilization,
    create_harvest,
    create_irrigation,
    create_pest_incident,
    create_plot,
    create_variety,
    estimate_geojson_centroid,
    extract_geojson_file,
    normalize_geojson,
    update_farm,
    update_fertilization,
    update_harvest,
    update_irrigation,
    update_pest_incident,
    update_plot,
    update_variety,
)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=status.HTTP_303_SEE_OTHER)


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


def _base_context(request: Request, user: User, csrf_token: str, page: str, **kwargs):
    flash = request.session.pop("flash", None)
    context = {
        "request": request,
        "user": user,
        "csrf_token": csrf_token,
        "page": page,
        "flash": flash,
    }
    context.update(kwargs)
    return context


def _repository(db: Session) -> FarmRepository:
    return FarmRepository(db)


def _flash(request: Request, kind: str, message: str) -> None:
    request.session["flash"] = {"kind": kind, "message": message}


def _float_or_none(value: str | None):
    return float(value) if value not in (None, "") else None


def _int_or_none(value: str | None):
    return int(value) if value not in (None, "") else None


def _int_list(values: list[str]) -> list[int]:
    parsed: list[int] = []
    for value in values:
        if value in (None, ""):
            continue
        parsed.append(int(value))
    return parsed


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


@router.get("/")
def home():
    return _redirect("/dashboard")


@router.get("/dashboard")
def dashboard(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    data = build_dashboard_context(_repository(db))
    return templates.TemplateResponse(
        "dashboard.html",
        _base_context(request, user, csrf_token, "dashboard", **data),
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
    farm_ids = _int_list(request.query_params.getlist("farm_id"))
    variety_ids = _int_list(request.query_params.getlist("variety_id"))
    edit_plot = repo.get_plot(edit_id) if edit_id else None
    return templates.TemplateResponse(
        "plots.html",
        _base_context(
            request,
            user,
            csrf_token,
            "plots",
            plots=repo.list_plots(search=q, farm_ids=farm_ids, variety_ids=variety_ids, sort=sort),
            farms=repo.list_farms(),
            varieties=repo.list_varieties(),
            filters={"q": q or "", "farm_ids": farm_ids, "variety_ids": variety_ids, "sort": sort},
            edit_plot=edit_plot,
            selected_farm_id=selected_farm_id,
            open_farm_modal=bool(open_farm_modal),
            filter_links=[
                {"farm_id": plot.farm_id, "variety_id": plot.variety_id}
                for plot in repo.list_plots()
                if plot.farm_id or plot.variety_id
            ],
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
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    edit_farm = repo.get_farm(edit_id) if edit_id else None
    return templates.TemplateResponse(
        "farms.html",
        _base_context(
            request,
            user,
            csrf_token,
            "farms",
            farms=repo.list_farms(),
            edit_farm=edit_farm,
        ),
    )


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
    _flash(request, "success", "Fazenda atualizada com sucesso.")
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
    repo.delete(farm)
    _flash(request, "success", "Fazenda excluida com sucesso.")
    return _redirect("/fazendas")


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


@router.get("/irrigacao")
def irrigation_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    return templates.TemplateResponse(
        "irrigation.html",
        _base_context(
            request,
            user,
            csrf_token,
            "irrigation",
            plots=repo.list_plots(),
            irrigations=repo.list_irrigations(),
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
                for plot in repo.list_plots()
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
    plot = repo.get_plot(plot_id)
    if not plot:
        _flash(request, "error", "Setor nao encontrado para registrar irrigacao.")
        return _redirect("/irrigacao")
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
        return _redirect("/irrigacao")
    plot = repo.get_plot(plot_id)
    if not plot:
        _flash(request, "error", "Setor nao encontrado para atualizar irrigacao.")
        return _redirect("/irrigacao")
    calculated_volume = calculate_irrigation_volume(plot, duration_minutes)
    manual_volume = _float_or_none(volume_liters)
    if calculated_volume is None and manual_volume is None:
        _flash(request, "error", "Informe o volume manual em litros ou cadastre os dados de irrigacao no setor.")
        return _redirect_with_query("/irrigacao", edit_id=record_id)
    update_irrigation(
        repo,
        irrigation,
        {
            "plot_id": plot_id,
            "irrigation_date": irrigation_date,
            "volume_liters": calculated_volume if calculated_volume is not None else manual_volume,
            "duration_minutes": duration_minutes,
            "notes": notes,
        },
    )
    _flash(request, "success", "Irrigacao atualizada com sucesso.")
    return _redirect("/irrigacao")


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


@router.get("/fertilizacao")
def fertilization_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    return templates.TemplateResponse(
        "fertilization.html",
        _base_context(
            request,
            user,
            csrf_token,
            "fertilization",
            plots=repo.list_plots(),
            fertilizations=repo.list_fertilizations(),
            edit_fertilization=repo.get_fertilization(edit_id) if edit_id else None,
        ),
    )


@router.post("/fertilizacao")
def create_fertilization_action(
    request: Request,
    csrf_token: str = Form(...),
    plot_id: int = Form(...),
    application_date: str = Form(...),
    product: str = Form(...),
    dose: str = Form(...),
    cost: float = Form(...),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    create_fertilization(
        _repository(db),
        {
            "plot_id": plot_id,
            "application_date": application_date,
            "product": product,
            "dose": dose,
            "cost": cost,
            "notes": notes,
        },
    )
    _flash(request, "success", "Fertilizacao registrada com sucesso.")
    return _redirect("/fertilizacao")


@router.post("/fertilizacao/{record_id}/editar")
def update_fertilization_action(
    record_id: int,
    request: Request,
    csrf_token: str = Form(...),
    plot_id: int = Form(...),
    application_date: str = Form(...),
    product: str = Form(...),
    dose: str = Form(...),
    cost: float = Form(...),
    notes: str | None = Form(None),
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
    update_fertilization(
        repo,
        fertilization,
        {
            "plot_id": plot_id,
            "application_date": application_date,
            "product": product,
            "dose": dose,
            "cost": cost,
            "notes": notes,
        },
    )
    _flash(request, "success", "Fertilizacao atualizada com sucesso.")
    return _redirect("/fertilizacao")


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
    repo.delete(fertilization)
    _flash(request, "success", "Fertilizacao excluida com sucesso.")
    return _redirect("/fertilizacao")


@router.get("/producao")
def production_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    return templates.TemplateResponse(
        "production.html",
        _base_context(
            request,
            user,
            csrf_token,
            "production",
            plots=repo.list_plots(),
            harvests=repo.list_harvests(),
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
    plot = db.query(Plot).filter(Plot.id == plot_id).first()
    area = float(plot.area_hectares) if plot else 0
    create_harvest(
        _repository(db),
        {
            "plot_id": plot_id,
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
    harvest = repo.get_harvest(record_id)
    if not harvest:
        _flash(request, "error", "Registro de producao nao encontrado.")
        return _redirect("/producao")
    plot = db.query(Plot).filter(Plot.id == plot_id).first()
    area = float(plot.area_hectares) if plot else 0
    update_harvest(
        repo,
        harvest,
        {
            "plot_id": plot_id,
            "harvest_date": harvest_date,
            "sacks_produced": sacks_produced,
            "notes": notes,
        },
        area,
    )
    _flash(request, "success", "Colheita atualizada com sucesso.")
    return _redirect("/producao")


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
    return templates.TemplateResponse(
        "pests.html",
        _base_context(
            request,
            user,
            csrf_token,
            "pests",
            plots=repo.list_plots(),
            incidents=repo.list_pest_incidents(),
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
    create_pest_incident(
        _repository(db),
        {
            "plot_id": plot_id,
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
    incident = repo.get_pest_incident(record_id)
    if not incident:
        _flash(request, "error", "Ocorrencia nao encontrada.")
        return _redirect("/pragas")
    update_pest_incident(
        repo,
        incident,
        {
            "plot_id": plot_id,
            "occurrence_date": occurrence_date,
            "category": category,
            "name": name,
            "severity": severity,
            "treatment": treatment,
            "notes": notes,
        },
    )
    _flash(request, "success", "Ocorrencia atualizada com sucesso.")
    return _redirect("/pragas")


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


@router.get("/mapa")
def map_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    geojson = build_dashboard_context(_repository(db))["map_geojson"]
    return templates.TemplateResponse(
        "map.html",
        _base_context(request, user, csrf_token, "map", map_geojson=geojson),
    )


@router.get("/mobile")
def mobile_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    return templates.TemplateResponse(
        "mobile.html",
        _base_context(
            request,
            user,
            csrf_token,
            "mobile",
            plots=repo.list_plots(),
            quick_irrigations=repo.list_irrigations(limit=3),
            quick_incidents=repo.list_pest_incidents(limit=3),
        ),
    )
