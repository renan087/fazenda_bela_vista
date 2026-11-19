from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.core.deps import get_current_user_api
from app.core.permissions_catalog import PAGE_DASHBOARD, PAGE_FINANCE, PAGE_IRRIGATION, PAGE_OPERATIONS, PAGE_PESTS, PAGE_PRODUCTION, PAGE_PRODUCTIVE_UNIT
from app.core.timezone import today_in_app_timezone
from app.db.session import get_db
from app.models import Farm, User
from app.repositories.farm import FarmRepository
from app.services.dashboard import build_dashboard_context
from app.services.entitlements import compute_entitlements
from app.services.rbac_service import user_has_permission
from app.services.plan_usage import user_exceeds_plan

# Modulo do plano exigido por cada recurso da API (mesmos codigos do menu web).
API_MODULES = {
    "/api/v1/dashboard": PAGE_DASHBOARD,
    "/api/v1/plots": PAGE_PRODUCTIVE_UNIT,
    "/api/v1/irrigations": PAGE_IRRIGATION,
    "/api/v1/fertilizations": PAGE_OPERATIONS,
    "/api/v1/harvests": PAGE_PRODUCTION,
    "/api/v1/pest-incidents": PAGE_PESTS,
}


def enforce_plan(request: Request, db: Session = Depends(get_db), current_user: User = Depends(get_current_user_api)) -> None:
    """Aplica na API os mesmos direitos das paginas: modulos do plano e modo somente leitura."""
    module = API_MODULES.get(request.url.path.rstrip("/"))
    if module and not user_has_permission(db, current_user, module):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Sem permissão para acessar este recurso.")
    entitlements = compute_entitlements(db, current_user.organization_id, today_in_app_timezone())
    if not entitlements.module_allowed(module):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Este recurso não faz parte do plano da organização.")
    if request.method not in {"GET", "HEAD", "OPTIONS"}:
        if entitlements.read_only:
            raise HTTPException(status_code=status.HTTP_402_PAYMENT_REQUIRED, detail="Organização em modo somente leitura por pagamento em atraso.")
        if entitlements.max_users is not None and user_exceeds_plan(db, entitlements, current_user.id):
            raise HTTPException(status_code=403, detail="Seu usuário está em modo consulta por exceder o limite do plano.")


router = APIRouter(prefix="/api/v1", tags=["farm"], dependencies=[Depends(enforce_plan)])


def _organization_farm_ids(db: Session, user: User) -> list[int]:
    if user.organization_id is None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Usuário sem organização.")
    return [farm_id for (farm_id,) in db.query(Farm.id).filter_by(organization_id=user.organization_id)]


@router.get("/dashboard")
def dashboard_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
    farm_id: int | None = None,
):
    farms = _organization_farm_ids(db, current_user)
    if farm_id is not None and farm_id not in farms:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fazenda não encontrada.")
    data = build_dashboard_context(
        FarmRepository(db), organization_id=current_user.organization_id,
        farm_id=farm_id if farm_id is not None else min(farms, default=-1),
    )
    kpis = data["kpis"]
    entitlements = compute_entitlements(db, current_user.organization_id, today_in_app_timezone())
    if not entitlements.module_allowed(PAGE_FINANCE) or not user_has_permission(db, current_user, PAGE_FINANCE):
        kpis = {key: value for key, value in kpis.items()
                if not key.startswith(("finance_", "cost_", "operational_cost_"))}
    return {
        "kpis": kpis,
        "forecast": data["forecast_plots"],
    }


@router.get("/plots")
def list_plots(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    plots = FarmRepository(db).list_plots(farm_ids=_organization_farm_ids(db, current_user))
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
    return [
        {
            "id": item.id,
            "plot": item.plot.name if item.plot else item.plot_id,
            "date": item.irrigation_date.isoformat(),
            "volume_liters": float(item.volume_liters),
            "duration_minutes": item.duration_minutes,
            "notes": item.notes,
        }
        for item in FarmRepository(db).list_irrigations(farm_ids=_organization_farm_ids(db, current_user))
    ]


@router.get("/fertilizations")
def list_fertilizations(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    return [
        {
            "id": item.id,
            "plot": item.plot.name if item.plot else item.plot_id,
            "date": item.application_date.isoformat(),
            "product": item.product,
            "dose": item.dose,
            "cost": float(item.cost),
        }
        for item in FarmRepository(db).list_fertilizations(farm_ids=_organization_farm_ids(db, current_user))
    ]


@router.get("/harvests")
def list_harvests(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    return [
        {
            "id": item.id,
            "plot": item.plot.name if item.plot else item.plot_id,
            "date": item.harvest_date.isoformat(),
            "sacks_produced": float(item.sacks_produced),
            "productivity_per_hectare": float(item.productivity_per_hectare or 0),
        }
        for item in FarmRepository(db).list_harvests(farm_ids=_organization_farm_ids(db, current_user))
    ]


@router.get("/pest-incidents")
def list_pest_incidents(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
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
        for item in FarmRepository(db).list_pest_incidents(farm_ids=_organization_farm_ids(db, current_user))
    ]
