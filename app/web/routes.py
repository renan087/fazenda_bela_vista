import json

from fastapi import APIRouter, Depends, Form, Request, status
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.core.csrf import validate_csrf
from app.core.deps import get_csrf_token, get_current_user_web
from app.db.session import get_db
from app.models import Plot, User
from app.repositories.farm import FarmRepository
from app.services.dashboard import build_dashboard_context
from app.services.forms import (
    create_fertilization,
    create_harvest,
    create_irrigation,
    create_pest_incident,
    create_plot,
    create_variety,
    normalize_geojson,
)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=status.HTTP_303_SEE_OTHER)


def _base_context(request: Request, user: User, csrf_token: str, page: str, **kwargs):
    context = {
        "request": request,
        "user": user,
        "csrf_token": csrf_token,
        "page": page,
    }
    context.update(kwargs)
    return context


def _repository(db: Session) -> FarmRepository:
    return FarmRepository(db)


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


@router.get("/talhoes")
def plots_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    return templates.TemplateResponse(
        "plots.html",
        _base_context(
            request,
            user,
            csrf_token,
            "plots",
            plots=repo.list_plots(),
            varieties=repo.list_varieties(),
        ),
    )


@router.post("/talhoes")
def create_plot_action(
    request: Request,
    csrf_token: str = Form(...),
    name: str = Form(...),
    area_hectares: float = Form(...),
    location: str = Form(...),
    planting_year: int | None = Form(None),
    plant_count: int = Form(...),
    spacing_row_meters: float | None = Form(None),
    spacing_plant_meters: float | None = Form(None),
    estimated_yield_sacks: float | None = Form(None),
    variety_id: int | None = Form(None),
    centroid_lat: float | None = Form(None),
    centroid_lng: float | None = Form(None),
    boundary_geojson: str | None = Form(None),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    create_plot(
        _repository(db),
        {
            "name": name,
            "area_hectares": area_hectares,
            "location": location,
            "planting_year": planting_year,
            "plant_count": plant_count,
            "spacing_row_meters": spacing_row_meters,
            "spacing_plant_meters": spacing_plant_meters,
            "estimated_yield_sacks": estimated_yield_sacks,
            "variety_id": variety_id,
            "centroid_lat": centroid_lat,
            "centroid_lng": centroid_lng,
            "boundary_geojson": normalize_geojson(boundary_geojson),
            "notes": notes,
        },
    )
    return _redirect("/talhoes")


@router.get("/variedades")
def varieties_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
    csrf_token: str = Depends(get_csrf_token),
):
    repo = _repository(db)
    return templates.TemplateResponse(
        "varieties.html",
        _base_context(request, user, csrf_token, "varieties", varieties=repo.list_varieties()),
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
    return _redirect("/variedades")


@router.get("/irrigacao")
def irrigation_page(
    request: Request,
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
        ),
    )


@router.post("/irrigacao")
def create_irrigation_action(
    request: Request,
    csrf_token: str = Form(...),
    plot_id: int = Form(...),
    irrigation_date: str = Form(...),
    volume_liters: float = Form(...),
    duration_minutes: int = Form(...),
    notes: str | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    del user
    validate_csrf(request, csrf_token)
    create_irrigation(
        _repository(db),
        {
            "plot_id": plot_id,
            "irrigation_date": irrigation_date,
            "volume_liters": volume_liters,
            "duration_minutes": duration_minutes,
            "notes": notes,
        },
    )
    return _redirect("/irrigacao")


@router.get("/fertilizacao")
def fertilization_page(
    request: Request,
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
    return _redirect("/fertilizacao")


@router.get("/producao")
def production_page(
    request: Request,
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
    return _redirect("/producao")


@router.get("/pragas")
def pests_page(
    request: Request,
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
