from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.deps import get_current_user_api
from app.db.session import get_db
from app.models import User
from app.repositories.farm import FarmRepository
from app.services.dashboard import build_dashboard_context

router = APIRouter(prefix="/api/v1", tags=["farm"])


def _repo(db: Session) -> FarmRepository:
    return FarmRepository(db)


@router.get("/dashboard")
def dashboard_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    data = build_dashboard_context(_repo(db))
    return {
        "kpis": data["kpis"],
        "forecast": data["forecast_plots"],
    }


@router.get("/plots")
def list_plots(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    plots = _repo(db).list_plots()
    return [
        {
            "id": plot.id,
            "name": plot.name,
            "area_hectares": float(plot.area_hectares),
            "location": plot.location,
            "planting_date": plot.planting_date.isoformat() if plot.planting_date else None,
            "plant_count": plot.plant_count,
            "spacing_row_meters": float(plot.spacing_row_meters or 0),
            "spacing_plant_meters": float(plot.spacing_plant_meters or 0),
            "estimated_yield_sacks": float(plot.estimated_yield_sacks or 0),
            "variety": plot.variety.name if plot.variety else None,
        }
        for plot in plots
    ]


@router.get("/irrigations")
def list_irrigations(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return [
        {
            "id": item.id,
            "plot": item.plot.name if item.plot else item.plot_id,
            "date": item.irrigation_date.isoformat(),
            "volume_liters": float(item.volume_liters),
            "duration_minutes": item.duration_minutes,
            "notes": item.notes,
        }
        for item in _repo(db).list_irrigations()
    ]


@router.get("/fertilizations")
def list_fertilizations(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return [
        {
            "id": item.id,
            "plot": item.plot.name if item.plot else item.plot_id,
            "date": item.application_date.isoformat(),
            "product": item.product,
            "dose": item.dose,
            "cost": float(item.cost),
        }
        for item in _repo(db).list_fertilizations()
    ]


@router.get("/harvests")
def list_harvests(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return [
        {
            "id": item.id,
            "plot": item.plot.name if item.plot else item.plot_id,
            "date": item.harvest_date.isoformat(),
            "sacks_produced": float(item.sacks_produced),
            "productivity_per_hectare": float(item.productivity_per_hectare or 0),
        }
        for item in _repo(db).list_harvests()
    ]


@router.get("/pest-incidents")
def list_pest_incidents(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return [
        {
            "id": item.id,
            "plot": item.plot.name if item.plot else item.plot_id,
            "date": item.occurrence_date.isoformat(),
            "category": item.category,
            "name": item.name,
            "severity": item.severity,
            "treatment": item.treatment,
        }
        for item in _repo(db).list_pest_incidents()
    ]
