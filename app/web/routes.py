from datetime import date, datetime
from urllib.parse import urlencode

import json

from fastapi import APIRouter, Depends, File, Form, Request, Response, UploadFile, status
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.core.csrf import validate_csrf
from app.core.deps import get_csrf_token, get_current_user_web
from app.core.security import get_password_hash
from app.db.session import get_db
from app.models import (
    AgronomicProfile,
    CoffeeVariety,
    Farm,
    FertilizationSchedule,
    FertilizationRecord,
    HarvestRecord,
    InputRecommendation,
    IrrigationRecord,
    PestIncident,
    Plot,
    PurchasedInput,
    RainfallRecord,
    SoilAnalysis,
    User,
)
from app.repositories.farm import FarmRepository
from app.services.dashboard import build_dashboard_context
from app.services.forms import (
    calculate_geojson_area_hectares,
    calculate_irrigation_volume,
    calculate_soil_recommendations,
    create_agronomic_profile,
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
    validate_schedule_stock,
)
from app.services.openai_service import gerar_recomendacao_adubacao

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


def _date_or_none(value: str | None):
    return date.fromisoformat(value) if value not in (None, "") else None


def _page_number(value: str | int | None, default: int = 1) -> int:
    try:
        return max(int(value or default), 1)
    except (TypeError, ValueError):
        return default


def _build_stock_context(repo: FarmRepository, farm_id: int | None = None, input_id: int | None = None):
    catalog_inputs = repo.list_input_catalog()
    if input_id:
        catalog_inputs = [item for item in catalog_inputs if item.id == input_id]

    purchase_entries = repo.list_purchased_inputs()
    if input_id:
        purchase_entries = [entry for entry in purchase_entries if entry.input_id == input_id]
    if farm_id:
        purchase_entries = [entry for entry in purchase_entries if entry.farm_id in (None, farm_id)]

    stock_outputs = repo.list_stock_outputs()
    if input_id:
        stock_outputs = [output for output in stock_outputs if output.input_id == input_id]
    if farm_id:
        stock_outputs = [output for output in stock_outputs if output.farm_id in (None, farm_id)]

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
                    "sort_key": (entry.purchase_date or date.today(), 0, entry.id),
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
                    "sort_key": (output.movement_date or date.today(), 1, output.id),
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
                    "value": event["value"],
                    "balance_after": running_balance,
                    "sort_key": event["sort_key"],
                }
            )

    stock_catalog_rows.sort(key=lambda row: row["name"].lower())
    extract_rows.sort(key=lambda row: row["sort_key"], reverse=True)
    return {
        "catalog_inputs": catalog_inputs,
        "purchase_entries": purchase_entries,
        "stock_outputs": stock_outputs,
        "input_stock": input_stock,
        "stock_catalog_rows": stock_catalog_rows,
        "extract_rows": extract_rows,
    }


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
    payload = upload.file.read()
    if not payload:
        return None, None, None
    return upload.filename, upload.content_type or "application/octet-stream", payload


def _require_admin(request: Request, user: User) -> RedirectResponse | None:
    if user.is_admin:
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
    data = build_dashboard_context(
        _repository(db),
        rain_start_date=_date_or_none(rain_start_date),
        rain_end_date=_date_or_none(rain_end_date),
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
        ),
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

        updated_user = update_user(
            repo,
            target_user,
            {
                "name": normalized_name,
                "email": normalized_email,
                "password": password,
                "is_active": _bool_from_form(is_active),
                "is_admin": _bool_from_form(is_admin),
            },
        )

        if updated_user.id == user.id:
            request.session["user_email"] = updated_user.email
            if not updated_user.is_admin:
                _flash(request, "success", "Usuario atualizado com sucesso.")
                return _redirect("/dashboard")

        _flash(request, "success", "Usuario atualizado com sucesso.")
        return _redirect("/usuarios")
    except Exception:
        db.rollback()
        _flash(request, "error", "Nao foi possivel atualizar o usuario agora. Revise os dados e tente novamente.")
        return _redirect_with_query("/usuarios", edit_id=user_id)


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


@router.get("/insumos/comprados")
def purchased_inputs_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    stock_context = _build_stock_context(repo)
    return templates.TemplateResponse(
        "purchased_inputs.html",
        _base_context(
            request,
            user,
            csrf_token,
            "purchased_inputs",
            title="Insumos Comprados",
            farms=repo.list_farms(),
            inputs=stock_context["purchase_entries"],
            inputs_catalog=stock_context["catalog_inputs"],
            input_stock=stock_context["input_stock"],
            stock_outputs=stock_context["stock_outputs"],
            stock_catalog_rows=stock_context["stock_catalog_rows"],
            edit_input=repo.get_purchased_input(edit_id) if edit_id else None,
        ),
    )


@router.post("/insumos/comprados")
def create_purchased_input_action(
    request: Request,
    csrf_token: str = Form(...),
    farm_id: str | None = Form(None),
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
    create_purchased_input(
        _repository(db),
        {
            "farm_id": _int_or_none(farm_id),
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
    _flash(request, "success", "Insumo comprado cadastrado com sucesso.")
    return _redirect("/insumos/comprados")


@router.post("/insumos/comprados/{input_id}/editar")
def update_purchased_input_action(
    input_id: int,
    request: Request,
    csrf_token: str = Form(...),
    farm_id: str | None = Form(None),
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
    item = repo.get_purchased_input(input_id)
    if not item:
        _flash(request, "error", "Insumo comprado nao encontrado.")
        return _redirect("/insumos/comprados")
    update_purchased_input(
        repo,
        item,
        {
            "farm_id": _int_or_none(farm_id),
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
    _flash(request, "success", "Insumo comprado atualizado com sucesso.")
    return _redirect("/insumos/comprados")


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
        return _redirect("/insumos/comprados")
    if item.recommendations:
        _flash(request, "error", "Nao e possivel excluir o insumo enquanto houver recomendacoes vinculadas.")
        return _redirect("/insumos/comprados")
    if item.recommendation_items or item.schedule_items or item.stock_allocations or item.stock_outputs:
        _flash(request, "error", "Nao e possivel excluir o insumo enquanto houver recomendacoes, agendamentos ou aplicacoes vinculadas.")
        return _redirect("/insumos/comprados")
    repo.delete(item)
    _flash(request, "success", "Insumo comprado excluido com sucesso.")
    return _redirect("/insumos/comprados")


@router.get("/insumos/estoque")
def stock_page(
    request: Request,
    farm_id: int | None = None,
    input_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    stock_context = _build_stock_context(repo, farm_id=farm_id, input_id=input_id)
    return templates.TemplateResponse(
        "stock.html",
        _base_context(
            request,
            user,
            csrf_token,
            "stock",
            title="Estoque de Insumos",
            farms=repo.list_farms(),
            plots=repo.list_plots(),
            selected_farm_id=farm_id,
            selected_input_id=input_id,
            inputs_catalog=stock_context["catalog_inputs"],
            input_stock=stock_context["input_stock"],
            stock_catalog_rows=stock_context["stock_catalog_rows"],
            purchase_entries=stock_context["purchase_entries"],
            stock_outputs=stock_context["stock_outputs"],
            extract_rows=stock_context["extract_rows"],
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
    try:
        create_manual_stock_output(
            _repository(db),
            {
                "farm_id": _int_or_none(farm_id),
                "plot_id": _int_or_none(plot_id),
                "input_id": input_id,
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


@router.get("/insumos/recomendacao")
def input_recommendations_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    catalog_inputs = repo.list_input_catalog()
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
    return templates.TemplateResponse(
        "input_recommendations.html",
        _base_context(
            request,
            user,
            csrf_token,
            "input_recommendations",
            title="Recomendacao de Insumos",
            farms=repo.list_farms(),
            plots=repo.list_plots(),
            inputs_catalog=catalog_inputs,
            input_stock=input_stock,
            recommendations=repo.list_input_recommendations(),
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
    farm_id = _int_or_none(form.get("farm_id"))
    plot_id = _int_or_none(form.get("plot_id"))
    notes = str(form.get("notes") or "") or None
    create_input_recommendation(
        repo,
        {
            "farm_id": farm_id,
            "plot_id": plot_id,
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
    recommendation = repo.get_input_recommendation(recommendation_id)
    if not recommendation:
        _flash(request, "error", "Recomendacao nao encontrada.")
        return _redirect("/insumos/recomendacao")
    application_name = str(form.get("application_name") or "").strip()
    items = _parse_recommendation_items(form)
    if not application_name or not items:
        _flash(request, "error", "Informe a aplicacao e adicione ao menos um insumo.")
        return _redirect_with_query("/insumos/recomendacao", edit_id=recommendation_id)
    update_input_recommendation(
        repo,
        recommendation,
        {
            "farm_id": _int_or_none(form.get("farm_id")),
            "plot_id": _int_or_none(form.get("plot_id")),
            "application_name": application_name,
            "items": items,
            "notes": str(form.get("notes") or "") or None,
        },
    )
    _flash(request, "success", "Recomendacao atualizada com sucesso.")
    return _redirect("/insumos/recomendacao")


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
    return templates.TemplateResponse(
        "rainfall.html",
        _base_context(
            request,
            user,
            csrf_token,
            "rainfall",
            farms=repo.list_farms(),
            rainfalls=repo.list_rainfalls(
                farm_id=farm_id,
                start_date=_date_or_none(start_date),
                end_date=_date_or_none(end_date),
            ),
            edit_rainfall=repo.get_rainfall(edit_id) if edit_id else None,
            filters={
                "farm_id": farm_id,
                "start_date": start_date or "",
                "end_date": end_date or "",
            },
        ),
    )


@router.post("/pluviometria")
def create_rainfall_action(
    request: Request,
    csrf_token: str = Form(...),
    farm_id: int = Form(...),
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
    farm = repo.get_farm(farm_id)
    if not farm:
        _flash(request, "error", "Fazenda nao encontrada para registrar a pluviometria.")
        return _redirect("/pluviometria")
    create_rainfall(
        repo,
        {
            "farm_id": farm_id,
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
    farm_id: int = Form(...),
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
    rainfall = repo.get_rainfall(record_id)
    if not rainfall:
        _flash(request, "error", "Registro de pluviometria nao encontrado.")
        return _redirect("/pluviometria")
    farm = repo.get_farm(farm_id)
    if not farm:
        _flash(request, "error", "Fazenda nao encontrada para atualizar a pluviometria.")
        return _redirect("/pluviometria")
    update_rainfall(
        repo,
        rainfall,
        {
            "farm_id": farm_id,
            "rainfall_date": rainfall_date,
            "millimeters": millimeters,
            "source": source,
            "notes": notes,
        },
    )
    _flash(request, "success", "Pluviometria atualizada com sucesso.")
    return _redirect("/pluviometria")


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
    edit_fertilization = repo.get_fertilization(edit_id) if edit_id else None
    recommendation_groups: dict[str, list[dict]] = {}
    consolidated_inputs = repo.list_input_catalog()
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
    for recommendation in repo.list_input_recommendations():
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
            plots=repo.list_plots(),
            inputs_catalog=consolidated_inputs,
            input_stock=input_stock,
            fertilizations=repo.list_fertilizations(),
            schedules=repo.list_fertilization_schedules(),
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
    plot_id = int(form.get("plot_id") or 0)
    repo = _repository(db)
    plot = repo.get_plot(plot_id)
    if not plot:
        _flash(request, "error", "Setor nao encontrado para registrar a fertilizacao.")
        return _redirect("/fertilizacao")
    items = _parse_fertilization_items(form)
    if not items:
        _flash(request, "error", "Adicione ao menos um insumo na atividade.")
        return _redirect("/fertilizacao")
    try:
        create_fertilization(
            repo,
            {
                "plot_id": plot_id,
                "application_date": str(form.get("application_date") or ""),
                "notes": str(form.get("notes") or "") or None,
                "items": items,
            },
        )
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect("/fertilizacao")
    _flash(request, "success", "Fertilizacao registrada com sucesso.")
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
        return _redirect("/fertilizacao")
    plot_id = int(form.get("plot_id") or 0)
    plot = repo.get_plot(plot_id)
    if not plot:
        _flash(request, "error", "Setor nao encontrado para atualizar a fertilizacao.")
        return _redirect_with_query("/fertilizacao", edit_id=record_id)
    items = _parse_fertilization_items(form)
    if not items:
        _flash(request, "error", "Adicione ao menos um insumo na atividade.")
        return _redirect_with_query("/fertilizacao", edit_id=record_id)
    try:
        update_fertilization(
            repo,
            fertilization,
            {
                "plot_id": plot_id,
                "application_date": str(form.get("application_date") or ""),
                "notes": str(form.get("notes") or "") or None,
                "items": items,
            },
        )
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect_with_query("/fertilizacao", edit_id=record_id)
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
    delete_fertilization(repo, fertilization)
    _flash(request, "success", "Fertilizacao excluida com sucesso.")
    return _redirect("/fertilizacao")


@router.get("/fertilizacao/agendamentos")
def fertilization_schedules_page(
    request: Request,
    edit_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    edit_schedule = repo.get_fertilization_schedule(edit_id) if edit_id else None
    schedules = repo.list_fertilization_schedules()
    schedule_validations = {schedule.id: validate_schedule_stock(repo, schedule) for schedule in schedules}
    consolidated_inputs = repo.list_input_catalog()
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
    return templates.TemplateResponse(
        "fertilization_schedule.html",
        _base_context(
            request,
            user,
            csrf_token,
            "fertilization_schedules",
            title="Agendamento de Fertilizacao",
            plots=repo.list_plots(),
            inputs_catalog=consolidated_inputs,
            input_stock=input_stock,
            schedules=schedules,
            schedule_validations=schedule_validations,
            edit_schedule=edit_schedule,
            edit_schedule_items=edit_schedule_items,
            today=date.today(),
        ),
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
    plot_id = int(form.get("plot_id") or 0)
    items = _parse_recommendation_items(form)
    if not plot_id or not items:
        _flash(request, "error", "Selecione o setor e adicione ao menos um insumo.")
        return _redirect("/fertilizacao/agendamentos")
    create_fertilization_schedule(
        _repository(db),
        {
            "plot_id": plot_id,
            "scheduled_date": str(form.get("scheduled_date") or ""),
            "status": str(form.get("status") or "scheduled"),
            "notes": str(form.get("notes") or "") or None,
            "items": items,
        },
    )
    _flash(request, "success", "Agendamento salvo com sucesso.")
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
    schedule = repo.get_fertilization_schedule(schedule_id)
    if not schedule:
        _flash(request, "error", "Agendamento nao encontrado.")
        return _redirect("/fertilizacao/agendamentos")
    items = _parse_recommendation_items(form)
    if not items:
        _flash(request, "error", "Adicione ao menos um insumo ao agendamento.")
        return _redirect_with_query("/fertilizacao/agendamentos", edit_id=schedule_id)
    update_fertilization_schedule(
        repo,
        schedule,
        {
            "plot_id": int(form.get("plot_id") or 0),
            "scheduled_date": str(form.get("scheduled_date") or ""),
            "status": str(form.get("status") or schedule.status),
            "notes": str(form.get("notes") or "") or None,
            "items": items,
        },
    )
    _flash(request, "success", "Agendamento atualizado com sucesso.")
    return _redirect("/fertilizacao/agendamentos")


@router.post("/fertilizacao/agendamentos/{schedule_id}/concluir")
def conclude_fertilization_schedule_action(
    schedule_id: int,
    request: Request,
    csrf_token: str = Form(...),
    application_date: str | None = Form(None),
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
    validation = validate_schedule_stock(repo, schedule)
    if not validation["ok"]:
        first = validation["shortages"][0]
        _flash(request, "error", f"Estoque insuficiente. Necessario comprar {first['missing']} {first['unit']} de {first['name']}.")
        return _redirect("/fertilizacao/agendamentos")
    try:
        conclude_fertilization_schedule(repo, schedule, application_date)
    except ValueError as exc:
        _flash(request, "error", str(exc))
        return _redirect("/fertilizacao/agendamentos")
    _flash(request, "success", "Agendamento concluido e aplicacao registrada.")
    return _redirect("/fertilizacao/agendamentos")


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


@router.get("/perfil-agronomico")
def agronomic_profiles_page(
    request: Request,
    edit_farm_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    selected_farm = repo.get_farm(edit_farm_id) if edit_farm_id else None
    return templates.TemplateResponse(
        "agronomic_profiles.html",
        _base_context(
            request,
            user,
            csrf_token,
            "agronomic_profiles",
            title="Perfil Agronomico",
            farms=repo.list_farms(),
            profiles=repo.list_agronomic_profiles(),
            edit_farm=selected_farm,
            edit_profile=repo.get_agronomic_profile_by_farm(edit_farm_id) if edit_farm_id else None,
        ),
    )


@router.post("/perfil-agronomico")
def save_agronomic_profile_action(
    request: Request,
    csrf_token: str = Form(...),
    farm_id: int = Form(...),
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
    return _redirect_with_query("/perfil-agronomico", edit_farm_id=farm_id)


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
    analyses = repo.list_soil_analyses(farm_id=farm_id, plot_id=plot_id)
    compare_target = compare_plot_id or plot_id
    compare_analyses = repo.list_soil_analyses(plot_id=compare_target) if compare_target else analyses[:6]
    return templates.TemplateResponse(
        "soil_analyses.html",
        _base_context(
            request,
            user,
            csrf_token,
            "soil_analyses",
            title="Analise de Solo",
            farms=repo.list_farms(),
            plots=repo.list_plots(),
            analyses=analyses,
            edit_analysis=repo.get_soil_analysis(edit_id) if edit_id else None,
            filters={"farm_id": farm_id, "plot_id": plot_id, "compare_plot_id": compare_target},
            compare_chart=_soil_history_chart(compare_analyses),
            compare_analyses=compare_analyses,
            latest_recommendations=[item for item in analyses if item.ai_recommendation][:4],
        ),
    )


@router.post("/analise-solo")
def create_soil_analysis_action(
    request: Request,
    csrf_token: str = Form(...),
    farm_id: int = Form(...),
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
    pdf_filename, pdf_content_type, pdf_data = _read_upload(analysis_pdf)
    analysis = create_soil_analysis(
        repo,
        _soil_payload(
            farm_id=farm_id,
            plot_id=plot_id,
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
    return _redirect_with_query("/analise-solo", plot_id=plot_id, compare_plot_id=plot_id)


@router.post("/analise-solo/{analysis_id}/editar")
def update_soil_analysis_action(
    analysis_id: int,
    request: Request,
    csrf_token: str = Form(...),
    farm_id: int = Form(...),
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
    analysis = repo.get_soil_analysis(analysis_id)
    if not analysis:
        _flash(request, "error", "Analise de solo nao encontrada.")
        return _redirect("/analise-solo")
    pdf_filename, pdf_content_type, pdf_data = _read_upload(analysis_pdf)
    updated = update_soil_analysis(
        repo,
        analysis,
        _soil_payload(
            farm_id=farm_id,
            plot_id=plot_id,
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
    return _redirect_with_query("/analise-solo", plot_id=plot_id, compare_plot_id=plot_id)


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
    geojson = build_dashboard_context(repo)["map_geojson"]
    edit_plot = repo.get_plot(edit_plot_id) if edit_plot_id else None
    return templates.TemplateResponse(
        "map.html",
        _base_context(
            request,
            user,
            csrf_token,
            "map",
            map_geojson=geojson,
            plots=repo.list_plots(),
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
