from sqlalchemy import distinct, func
from sqlalchemy.orm import Session, joinedload

from app.models import (
    AgronomicProfile,
    CoffeeVariety,
    CropSeason,
    EquipmentAsset,
    Farm,
    FertilizationItem,
    FertilizationSchedule,
    FertilizationScheduleItem,
    FertilizationStockAllocation,
    FertilizationRecord,
    HarvestRecord,
    InputCatalog,
    InputRecommendation,
    InputRecommendationItem,
    IrrigationRecord,
    PestIncident,
    Plot,
    PurchasedInput,
    RainfallRecord,
    SoilAnalysis,
    StockOutput,
    User,
)


class FarmRepository:
    def __init__(self, db: Session):
        self.db = db

    def list_farms(self) -> list[Farm]:
        return self.db.query(Farm).order_by(Farm.name.asc()).all()

    def list_users(self) -> list[User]:
        return self.db.query(User).order_by(User.name.asc(), User.email.asc()).all()

    def get_user(self, user_id: int) -> User | None:
        return self.db.query(User).filter(User.id == user_id).first()

    def get_farm(self, farm_id: int) -> Farm | None:
        return self.db.query(Farm).filter(Farm.id == farm_id).first()

    def get_agronomic_profile_by_farm(self, farm_id: int) -> AgronomicProfile | None:
        return self.db.query(AgronomicProfile).filter(AgronomicProfile.farm_id == farm_id).first()

    def list_agronomic_profiles(self) -> list[AgronomicProfile]:
        return self.db.query(AgronomicProfile).options(joinedload(AgronomicProfile.farm)).order_by(AgronomicProfile.id.desc()).all()

    def list_plots(
        self,
        search: str | None = None,
        farm_ids: list[int] | None = None,
        variety_ids: list[int] | None = None,
        sort: str = "name",
    ) -> list[Plot]:
        query = self.db.query(Plot).options(joinedload(Plot.variety), joinedload(Plot.farm))
        if search:
            query = query.filter(Plot.name.ilike(f"%{search}%"))
        if farm_ids:
            query = query.filter(Plot.farm_id.in_(farm_ids))
        if variety_ids:
            query = query.filter(Plot.variety_id.in_(variety_ids))
        order_map = {
            "name": Plot.name.asc(),
            "area_desc": Plot.area_hectares.desc(),
            "area_asc": Plot.area_hectares.asc(),
            "planting_desc": Plot.planting_date.desc(),
            "planting_asc": Plot.planting_date.asc(),
        }
        query = query.order_by(order_map.get(sort, Plot.name.asc()))
        return query.all()

    def list_plot_filter_options(
        self,
        selected_farm_ids: list[int] | None = None,
        selected_variety_ids: list[int] | None = None,
    ) -> tuple[list[Farm], list[CoffeeVariety]]:
        farm_query = self.db.query(Farm).order_by(Farm.name.asc())
        variety_query = self.db.query(CoffeeVariety).order_by(CoffeeVariety.name.asc())

        if selected_variety_ids:
            farm_ids = (
                self.db.query(distinct(Plot.farm_id))
                .filter(Plot.farm_id.isnot(None), Plot.variety_id.in_(selected_variety_ids))
                .all()
            )
            farm_query = farm_query.filter(Farm.id.in_([row[0] for row in farm_ids] or [-1]))

        if selected_farm_ids:
            variety_ids = (
                self.db.query(distinct(Plot.variety_id))
                .filter(Plot.variety_id.isnot(None), Plot.farm_id.in_(selected_farm_ids))
                .all()
            )
            variety_query = variety_query.filter(CoffeeVariety.id.in_([row[0] for row in variety_ids] or [-1]))

        return farm_query.all(), variety_query.all()

    def get_plot(self, plot_id: int) -> Plot | None:
        return (
            self.db.query(Plot)
            .options(joinedload(Plot.variety), joinedload(Plot.farm))
            .filter(Plot.id == plot_id)
            .first()
        )

    def list_soil_analyses(self, farm_id: int | None = None, plot_id: int | None = None) -> list[SoilAnalysis]:
        query = (
            self.db.query(SoilAnalysis)
            .options(joinedload(SoilAnalysis.farm), joinedload(SoilAnalysis.plot))
            .order_by(SoilAnalysis.analysis_date.desc(), SoilAnalysis.id.desc())
        )
        if farm_id:
            query = query.filter(SoilAnalysis.farm_id == farm_id)
        if plot_id:
            query = query.filter(SoilAnalysis.plot_id == plot_id)
        return query.all()

    def get_soil_analysis(self, analysis_id: int) -> SoilAnalysis | None:
        return (
            self.db.query(SoilAnalysis)
            .options(
                joinedload(SoilAnalysis.farm).joinedload(Farm.agronomic_profile),
                joinedload(SoilAnalysis.plot),
            )
            .filter(SoilAnalysis.id == analysis_id)
            .first()
        )

    def list_varieties(self) -> list[CoffeeVariety]:
        return self.db.query(CoffeeVariety).order_by(CoffeeVariety.name.asc()).all()

    def get_variety(self, variety_id: int) -> CoffeeVariety | None:
        return self.db.query(CoffeeVariety).filter(CoffeeVariety.id == variety_id).first()

    def list_crop_seasons(self, farm_id: int | None = None) -> list[CropSeason]:
        query = (
            self.db.query(CropSeason)
            .options(joinedload(CropSeason.farm), joinedload(CropSeason.variety))
            .order_by(CropSeason.start_date.desc(), CropSeason.id.desc())
        )
        if farm_id:
            query = query.filter(CropSeason.farm_id == farm_id)
        return query.all()

    def get_crop_season(self, season_id: int) -> CropSeason | None:
        return (
            self.db.query(CropSeason)
            .options(joinedload(CropSeason.farm), joinedload(CropSeason.variety))
            .filter(CropSeason.id == season_id)
            .first()
        )

    def list_irrigations(self, limit: int | None = None) -> list[IrrigationRecord]:
        query = (
            self.db.query(IrrigationRecord)
            .options(joinedload(IrrigationRecord.plot))
            .order_by(IrrigationRecord.irrigation_date.desc(), IrrigationRecord.id.desc())
        )
        return query.limit(limit).all() if limit else query.all()

    def get_irrigation(self, record_id: int) -> IrrigationRecord | None:
        return (
            self.db.query(IrrigationRecord)
            .options(joinedload(IrrigationRecord.plot))
            .filter(IrrigationRecord.id == record_id)
            .first()
        )

    def list_rainfalls(
        self,
        farm_id: int | None = None,
        start_date=None,
        end_date=None,
        limit: int | None = None,
    ) -> list[RainfallRecord]:
        query = (
            self.db.query(RainfallRecord)
            .options(joinedload(RainfallRecord.farm))
            .order_by(RainfallRecord.rainfall_date.desc(), RainfallRecord.id.desc())
        )
        if farm_id:
            query = query.filter(RainfallRecord.farm_id == farm_id)
        if start_date:
            query = query.filter(RainfallRecord.rainfall_date >= start_date)
        if end_date:
            query = query.filter(RainfallRecord.rainfall_date <= end_date)
        return query.limit(limit).all() if limit else query.all()

    def get_rainfall(self, record_id: int) -> RainfallRecord | None:
        return (
            self.db.query(RainfallRecord)
            .options(joinedload(RainfallRecord.farm))
            .filter(RainfallRecord.id == record_id)
            .first()
        )

    def list_fertilizations(self, limit: int | None = None) -> list[FertilizationRecord]:
        query = (
            self.db.query(FertilizationRecord)
            .options(
                joinedload(FertilizationRecord.plot),
                joinedload(FertilizationRecord.season),
                joinedload(FertilizationRecord.items).joinedload(FertilizationItem.input_catalog),
                joinedload(FertilizationRecord.items).joinedload(FertilizationItem.purchased_input),
                joinedload(FertilizationRecord.items).joinedload(FertilizationItem.stock_allocations).joinedload(FertilizationStockAllocation.purchased_input),
                joinedload(FertilizationRecord.schedule),
            )
            .order_by(FertilizationRecord.application_date.desc(), FertilizationRecord.id.desc())
        )
        return query.limit(limit).all() if limit else query.all()

    def list_purchased_inputs(self, item_type: str | None = None) -> list[PurchasedInput]:
        query = (
            self.db.query(PurchasedInput)
            .options(
                joinedload(PurchasedInput.farm),
                joinedload(PurchasedInput.input_catalog),
                joinedload(PurchasedInput.stock_allocations),
            )
            .order_by(PurchasedInput.name.asc(), PurchasedInput.purchase_date.desc(), PurchasedInput.id.desc())
        )
        if item_type:
            query = query.join(PurchasedInput.input_catalog).filter(InputCatalog.item_type == item_type)
        return query.all()

    def get_purchased_input(self, input_id: int) -> PurchasedInput | None:
        return (
            self.db.query(PurchasedInput)
            .options(
                joinedload(PurchasedInput.farm),
                joinedload(PurchasedInput.input_catalog),
                joinedload(PurchasedInput.stock_allocations),
                joinedload(PurchasedInput.recommendation_items),
                joinedload(PurchasedInput.schedule_items),
            )
            .filter(PurchasedInput.id == input_id)
            .first()
        )

    def list_input_catalog(self, item_type: str | None = None) -> list[InputCatalog]:
        query = (
            self.db.query(InputCatalog)
            .options(joinedload(InputCatalog.purchase_entries))
            .order_by(InputCatalog.name.asc())
        )
        if item_type:
            query = query.filter(InputCatalog.item_type == item_type)
        return query.all()

    def get_input_catalog(self, input_id: int) -> InputCatalog | None:
        return (
            self.db.query(InputCatalog)
            .options(joinedload(InputCatalog.purchase_entries))
            .filter(InputCatalog.id == input_id)
            .first()
        )

    def get_input_catalog_by_normalized_name(self, normalized_name: str) -> InputCatalog | None:
        return (
            self.db.query(InputCatalog)
            .filter(InputCatalog.normalized_name == normalized_name)
            .first()
        )

    def list_equipment_assets(self, farm_id: int | None = None) -> list[EquipmentAsset]:
        query = (
            self.db.query(EquipmentAsset)
            .options(joinedload(EquipmentAsset.farm))
            .order_by(EquipmentAsset.name.asc(), EquipmentAsset.id.desc())
        )
        if farm_id:
            query = query.filter(EquipmentAsset.farm_id == farm_id)
        return query.all()

    def get_equipment_asset(self, asset_id: int) -> EquipmentAsset | None:
        return (
            self.db.query(EquipmentAsset)
            .options(joinedload(EquipmentAsset.farm))
            .filter(EquipmentAsset.id == asset_id)
            .first()
        )

    def list_input_recommendations(self) -> list[InputRecommendation]:
        return (
            self.db.query(InputRecommendation)
            .options(
                joinedload(InputRecommendation.farm),
                joinedload(InputRecommendation.plot),
                joinedload(InputRecommendation.items).joinedload(InputRecommendationItem.input_catalog),
                joinedload(InputRecommendation.items).joinedload(InputRecommendationItem.purchased_input),
            )
            .order_by(InputRecommendation.application_name.asc(), InputRecommendation.id.desc())
            .all()
        )

    def get_input_recommendation(self, recommendation_id: int) -> InputRecommendation | None:
        return (
            self.db.query(InputRecommendation)
            .options(
                joinedload(InputRecommendation.farm),
                joinedload(InputRecommendation.plot),
                joinedload(InputRecommendation.items).joinedload(InputRecommendationItem.input_catalog),
                joinedload(InputRecommendation.items).joinedload(InputRecommendationItem.purchased_input),
            )
            .filter(InputRecommendation.id == recommendation_id)
            .first()
        )

    def get_fertilization(self, record_id: int) -> FertilizationRecord | None:
        return (
            self.db.query(FertilizationRecord)
            .options(
                joinedload(FertilizationRecord.plot),
                joinedload(FertilizationRecord.season),
                joinedload(FertilizationRecord.items).joinedload(FertilizationItem.input_catalog),
                joinedload(FertilizationRecord.items).joinedload(FertilizationItem.purchased_input),
                joinedload(FertilizationRecord.items).joinedload(FertilizationItem.stock_allocations).joinedload(FertilizationStockAllocation.purchased_input),
                joinedload(FertilizationRecord.schedule),
            )
            .filter(FertilizationRecord.id == record_id)
            .first()
        )

    def list_fertilization_schedules(self) -> list[FertilizationSchedule]:
        return (
            self.db.query(FertilizationSchedule)
            .options(
                joinedload(FertilizationSchedule.plot).joinedload(Plot.farm),
                joinedload(FertilizationSchedule.season),
                joinedload(FertilizationSchedule.items).joinedload(FertilizationScheduleItem.input_catalog),
                joinedload(FertilizationSchedule.items).joinedload(FertilizationScheduleItem.purchased_input),
                joinedload(FertilizationSchedule.fertilization_record),
            )
            .order_by(FertilizationSchedule.scheduled_date.asc(), FertilizationSchedule.id.desc())
            .all()
        )

    def get_fertilization_schedule(self, schedule_id: int) -> FertilizationSchedule | None:
        return (
            self.db.query(FertilizationSchedule)
            .options(
                joinedload(FertilizationSchedule.plot).joinedload(Plot.farm),
                joinedload(FertilizationSchedule.season),
                joinedload(FertilizationSchedule.items).joinedload(FertilizationScheduleItem.input_catalog),
                joinedload(FertilizationSchedule.items).joinedload(FertilizationScheduleItem.purchased_input),
                joinedload(FertilizationSchedule.fertilization_record),
            )
            .filter(FertilizationSchedule.id == schedule_id)
            .first()
        )

    def list_stock_outputs(self, input_id: int | None = None, farm_id: int | None = None) -> list[StockOutput]:
        query = (
            self.db.query(StockOutput)
            .options(
                joinedload(StockOutput.input_catalog),
                joinedload(StockOutput.farm),
                joinedload(StockOutput.plot),
                joinedload(StockOutput.season),
                joinedload(StockOutput.purchased_input),
            )
            .order_by(StockOutput.movement_date.desc(), StockOutput.id.desc())
        )
        if input_id:
            query = query.filter(StockOutput.input_id == input_id)
        if farm_id:
            query = query.filter(StockOutput.farm_id == farm_id)
        return query.all()

    def get_stock_output(self, output_id: int) -> StockOutput | None:
        return (
            self.db.query(StockOutput)
            .options(
                joinedload(StockOutput.input_catalog),
                joinedload(StockOutput.farm),
                joinedload(StockOutput.plot),
                joinedload(StockOutput.season),
                joinedload(StockOutput.purchased_input),
            )
            .filter(StockOutput.id == output_id)
            .first()
        )

    def list_harvests(self, limit: int | None = None) -> list[HarvestRecord]:
        query = (
            self.db.query(HarvestRecord)
            .options(joinedload(HarvestRecord.plot))
            .order_by(HarvestRecord.harvest_date.desc(), HarvestRecord.id.desc())
        )
        return query.limit(limit).all() if limit else query.all()

    def get_harvest(self, record_id: int) -> HarvestRecord | None:
        return (
            self.db.query(HarvestRecord)
            .options(joinedload(HarvestRecord.plot))
            .filter(HarvestRecord.id == record_id)
            .first()
        )

    def list_pest_incidents(self, limit: int | None = None) -> list[PestIncident]:
        query = (
            self.db.query(PestIncident)
            .options(joinedload(PestIncident.plot))
            .order_by(PestIncident.occurrence_date.desc(), PestIncident.id.desc())
        )
        return query.limit(limit).all() if limit else query.all()

    def get_pest_incident(self, record_id: int) -> PestIncident | None:
        return (
            self.db.query(PestIncident)
            .options(joinedload(PestIncident.plot))
            .filter(PestIncident.id == record_id)
            .first()
        )

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
