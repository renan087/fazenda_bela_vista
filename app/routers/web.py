from collections.abc import Callable
from datetime import date

from fastapi import APIRouter, Depends, Form, Request, status
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.core.deps import get_current_user_web
from app.crud.resources import fertilizations, harvests, irrigations, pesticides, plots, varieties
from app.db.session import get_db
from app.models.user import User

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _redirect(path: str) -> RedirectResponse:
    return RedirectResponse(url=path, status_code=status.HTTP_303_SEE_OTHER)


def _safe_create(creator: Callable, db: Session, data: dict, path: str) -> RedirectResponse:
    try:
        creator(db, data)
    except ValueError:
        return _redirect(path)
    return _redirect(path)


@router.get("/")
def root():
    return _redirect("/dashboard")


@router.get("/dashboard")
def dashboard(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_web),
):
    plot_list = plots.get_multi(db)
    harvest_list = harvests.get_multi(db)
    total_sacks = sum(float(item.sacks_produced) for item in harvest_list)
    total_area = sum(float(item.area_hectares) for item in plot_list)
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": current_user,
            "plot_count": len(plot_list),
            "variety_count": len(varieties.get_multi(db)),
            "harvest_count": len(harvest_list),
            "total_sacks": f"{total_sacks:.2f}",
            "total_area": f"{total_area:.2f}",
            "recent_harvests": harvest_list[:5],
        },
    )


@router.get("/talhoes")
def plot_page(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_web),
):
    return templates.TemplateResponse(
        "plots.html",
        {
            "request": request,
            "user": current_user,
            "plots": plots.get_multi(db),
            "varieties": varieties.get_multi(db),
        },
    )


@router.post("/talhoes")
def create_plot(
    name: str = Form(...),
    area_hectares: float = Form(...),
    location: str = Form(...),
    plant_count: int = Form(...),
    notes: str = Form(""),
    variety_id: int | None = Form(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_web),
):
    del current_user
    return _safe_create(
        plots.create,
        db,
        {
            "name": name,
            "area_hectares": area_hectares,
            "location": location,
            "plant_count": plant_count,
            "notes": notes or None,
            "variety_id": variety_id,
        },
        "/talhoes",
    )


@router.get("/variedades")
def variety_page(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_web),
):
    return templates.TemplateResponse(
        "varieties.html",
        {
            "request": request,
            "user": current_user,
            "varieties": varieties.get_multi(db),
        },
    )


@router.post("/variedades")
def create_variety(
    name: str = Form(...),
    species: str = Form(...),
    maturation_cycle: str = Form(...),
    notes: str = Form(""),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_web),
):
    del current_user
    return _safe_create(
        varieties.create,
        db,
        {
            "name": name,
            "species": species,
            "maturation_cycle": maturation_cycle,
            "notes": notes or None,
        },
        "/variedades",
    )


@router.get("/operacoes")
def operations_page(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_web),
):
    return templates.TemplateResponse(
        "operations.html",
        {
            "request": request,
            "user": current_user,
            "plots": plots.get_multi(db),
            "irrigations": irrigations.get_multi(db),
            "fertilizations": fertilizations.get_multi(db),
            "pesticides": pesticides.get_multi(db),
            "harvests": harvests.get_multi(db),
        },
    )


def _create_operation(
    creator: Callable,
    data: dict,
    db: Session,
    path: str = "/operacoes",
) -> RedirectResponse:
    return _safe_create(creator, db, data, path)


@router.post("/operacoes/irrigacao")
def create_irrigation(
    plot_id: int = Form(...),
    irrigation_date: str = Form(...),
    water_volume_mm: float = Form(...),
    method: str = Form(...),
    notes: str = Form(""),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_web),
):
    del current_user
    return _create_operation(
        irrigations.create,
        {
            "plot_id": plot_id,
            "irrigation_date": date.fromisoformat(irrigation_date),
            "water_volume_mm": water_volume_mm,
            "method": method,
            "notes": notes or None,
        },
        db,
    )


@router.post("/operacoes/adubacao")
def create_fertilization(
    plot_id: int = Form(...),
    application_date: str = Form(...),
    product: str = Form(...),
    dose: str = Form(...),
    cost: float = Form(...),
    notes: str = Form(""),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_web),
):
    del current_user
    return _create_operation(
        fertilizations.create,
        {
            "plot_id": plot_id,
            "application_date": date.fromisoformat(application_date),
            "product": product,
            "dose": dose,
            "cost": cost,
            "notes": notes or None,
        },
        db,
    )


@router.post("/operacoes/defensivos")
def create_pesticide(
    plot_id: int = Form(...),
    application_date: str = Form(...),
    product: str = Form(...),
    target_pest: str = Form(...),
    cost: float = Form(...),
    notes: str = Form(""),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_web),
):
    del current_user
    return _create_operation(
        pesticides.create,
        {
            "plot_id": plot_id,
            "application_date": date.fromisoformat(application_date),
            "product": product,
            "target_pest": target_pest,
            "cost": cost,
            "notes": notes or None,
        },
        db,
    )


@router.post("/operacoes/colheita")
def create_harvest(
    plot_id: int = Form(...),
    harvest_date: str = Form(...),
    sacks_produced: float = Form(...),
    notes: str = Form(""),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_web),
):
    del current_user
    return _create_operation(
        harvests.create,
        {
            "plot_id": plot_id,
            "harvest_date": date.fromisoformat(harvest_date),
            "sacks_produced": sacks_produced,
            "notes": notes or None,
        },
        db,
    )
