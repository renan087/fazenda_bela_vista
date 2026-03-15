from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.deps import get_current_user_api
from app.crud.resources import fertilizations, harvests, irrigations, pesticides, plots, varieties
from app.db.session import get_db
from app.models.user import User
from app.schemas.coffee_variety import CoffeeVarietyCreate, CoffeeVarietyRead
from app.schemas.fertilization import FertilizationCreate, FertilizationRead
from app.schemas.harvest import HarvestCreate, HarvestRead
from app.schemas.irrigation import IrrigationCreate, IrrigationRead
from app.schemas.pesticide import PesticideCreate, PesticideRead
from app.schemas.plot import PlotCreate, PlotRead

router = APIRouter(prefix="/api/v1", tags=["resources"])


@router.get("/plots", response_model=list[PlotRead])
def list_plots(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return plots.get_multi(db)


@router.post("/plots", response_model=PlotRead)
def create_plot(
    payload: PlotCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return plots.create(db, payload.model_dump())


@router.get("/varieties", response_model=list[CoffeeVarietyRead])
def list_varieties(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return varieties.get_multi(db)


@router.post("/varieties", response_model=CoffeeVarietyRead)
def create_variety(
    payload: CoffeeVarietyCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return varieties.create(db, payload.model_dump())


@router.get("/irrigations", response_model=list[IrrigationRead])
def list_irrigations(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return irrigations.get_multi(db)


@router.post("/irrigations", response_model=IrrigationRead)
def create_irrigation(
    payload: IrrigationCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return irrigations.create(db, payload.model_dump())


@router.get("/fertilizations", response_model=list[FertilizationRead])
def list_fertilizations(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return fertilizations.get_multi(db)


@router.post("/fertilizations", response_model=FertilizationRead)
def create_fertilization(
    payload: FertilizationCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return fertilizations.create(db, payload.model_dump())


@router.get("/pesticides", response_model=list[PesticideRead])
def list_pesticides(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return pesticides.get_multi(db)


@router.post("/pesticides", response_model=PesticideRead)
def create_pesticide(
    payload: PesticideCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return pesticides.create(db, payload.model_dump())


@router.get("/harvests", response_model=list[HarvestRead])
def list_harvests(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return harvests.get_multi(db)


@router.post("/harvests", response_model=HarvestRead)
def create_harvest(
    payload: HarvestCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user_api),
):
    del current_user
    return harvests.create(db, payload.model_dump())
