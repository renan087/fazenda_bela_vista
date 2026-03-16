from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.models import CoffeeVariety, Farm, FertilizationRecord, HarvestRecord, IrrigationRecord, PestIncident, Plot


class FarmRepository:
    def __init__(self, db: Session):
        self.db = db

    def list_farms(self) -> list[Farm]:
        return self.db.query(Farm).order_by(Farm.name.asc()).all()

    def get_farm(self, farm_id: int) -> Farm | None:
        return self.db.query(Farm).filter(Farm.id == farm_id).first()

    def list_plots(
        self,
        search: str | None = None,
        farm_id: int | None = None,
        variety_id: int | None = None,
        sort: str = "name",
    ) -> list[Plot]:
        query = self.db.query(Plot).options(joinedload(Plot.variety), joinedload(Plot.farm))
        if search:
            query = query.filter(Plot.name.ilike(f"%{search}%"))
        if farm_id:
            query = query.filter(Plot.farm_id == farm_id)
        if variety_id:
            query = query.filter(Plot.variety_id == variety_id)
        order_map = {
            "name": Plot.name.asc(),
            "area_desc": Plot.area_hectares.desc(),
            "area_asc": Plot.area_hectares.asc(),
            "planting_desc": Plot.planting_date.desc(),
            "planting_asc": Plot.planting_date.asc(),
        }
        query = query.order_by(order_map.get(sort, Plot.name.asc()))
        return query.all()

    def get_plot(self, plot_id: int) -> Plot | None:
        return (
            self.db.query(Plot)
            .options(joinedload(Plot.variety), joinedload(Plot.farm))
            .filter(Plot.id == plot_id)
            .first()
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

    def update(self, instance, data: dict):
        for key, value in data.items():
            setattr(instance, key, value)
        self.db.add(instance)
        self.db.commit()
        self.db.refresh(instance)
        return instance

    def delete(self, instance) -> None:
        self.db.delete(instance)
        self.db.commit()

    def get_total_area(self) -> float:
        return float(self.db.query(func.coalesce(func.sum(Plot.area_hectares), 0)).scalar() or 0)

    def get_total_production(self) -> float:
        return float(self.db.query(func.coalesce(func.sum(HarvestRecord.sacks_produced), 0)).scalar() or 0)
