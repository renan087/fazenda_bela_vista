from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.models import CoffeeVariety, FertilizationRecord, HarvestRecord, IrrigationRecord, PestIncident, Plot


class FarmRepository:
    def __init__(self, db: Session):
        self.db = db

    def list_plots(self) -> list[Plot]:
        return (
            self.db.query(Plot)
            .options(joinedload(Plot.variety))
            .order_by(Plot.name.asc())
            .all()
        )

    def list_varieties(self) -> list[CoffeeVariety]:
        return self.db.query(CoffeeVariety).order_by(CoffeeVariety.name.asc()).all()

    def list_irrigations(self, limit: int | None = None) -> list[IrrigationRecord]:
        query = (
            self.db.query(IrrigationRecord)
            .options(joinedload(IrrigationRecord.plot))
            .order_by(IrrigationRecord.irrigation_date.desc(), IrrigationRecord.id.desc())
        )
        return query.limit(limit).all() if limit else query.all()

    def list_fertilizations(self, limit: int | None = None) -> list[FertilizationRecord]:
        query = (
            self.db.query(FertilizationRecord)
            .options(joinedload(FertilizationRecord.plot))
            .order_by(FertilizationRecord.application_date.desc(), FertilizationRecord.id.desc())
        )
        return query.limit(limit).all() if limit else query.all()

    def list_harvests(self, limit: int | None = None) -> list[HarvestRecord]:
        query = (
            self.db.query(HarvestRecord)
            .options(joinedload(HarvestRecord.plot))
            .order_by(HarvestRecord.harvest_date.desc(), HarvestRecord.id.desc())
        )
        return query.limit(limit).all() if limit else query.all()

    def list_pest_incidents(self, limit: int | None = None) -> list[PestIncident]:
        query = (
            self.db.query(PestIncident)
            .options(joinedload(PestIncident.plot))
            .order_by(PestIncident.occurrence_date.desc(), PestIncident.id.desc())
        )
        return query.limit(limit).all() if limit else query.all()

    def create(self, instance):
        self.db.add(instance)
        self.db.commit()
        self.db.refresh(instance)
        return instance

    def get_total_area(self) -> float:
        return float(self.db.query(func.coalesce(func.sum(Plot.area_hectares), 0)).scalar() or 0)

    def get_total_production(self) -> float:
        return float(self.db.query(func.coalesce(func.sum(HarvestRecord.sacks_produced), 0)).scalar() or 0)
