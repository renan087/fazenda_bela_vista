import unicodedata
from datetime import date

from sqlalchemy import distinct, func, or_
from sqlalchemy.orm import Session, joinedload

from app.models import (
    AgronomicProfile,
    BackupRun,
    BackupAutomationSetting,
    CoffeeCommercializationRecord,
    CoffeeVariety,
    CropSeason,
    EquipmentAsset,
    EquipmentAssetAttachment,
    Farm,
    FinanceAccount,
    FinanceCreditCard,
    FinanceCustomBank,
    FinanceTransaction,
    FinanceTransactionAttachment,
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
    PlotAttachment,
    PurchasedInput,
    PurchasedInputAttachment,
    RainfallRecord,
    SoilAnalysis,
    StockOutput,
    User,
)

MANUAL_STOCK_OUTPUT_ALLOCATION = "manual_stock_output_allocation"


def _normalize_plot_search_text(value: object) -> str:
    """Minusculas e sem acentos (mesma ideia de _normalize_search_value em routes)."""
    return (
        unicodedata.normalize("NFD", str(value or ""))
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )


class FarmRepository:
    def __init__(self, db: Session):
        self.db = db

    def list_farms(self) -> list[Farm]:
        return self.db.query(Farm).order_by(Farm.name.asc()).all()

    def list_users(self) -> list[User]:
        return self.db.query(User).order_by(User.name.asc(), User.email.asc()).all()

    def count_backup_runs(self) -> int:
        return self.db.query(func.count(BackupRun.id)).scalar() or 0

    def get_backup_automation_setting(self) -> BackupAutomationSetting:
        setting = self.db.query(BackupAutomationSetting).filter(BackupAutomationSetting.id == 1).first()
        if setting:
            return setting
        setting = BackupAutomationSetting(id=1, automatic_enabled=True, interval_days=5, storage_limit_gb=1, scheduled_hour=3, scheduled_minute=0)
        self.db.add(setting)
        self.db.commit()
        self.db.refresh(setting)
        return setting

    def get_backup_run(self, backup_run_id: int) -> BackupRun | None:
        return (
            self.db.query(BackupRun)
            .options(joinedload(BackupRun.initiated_by_user))
            .filter(BackupRun.id == backup_run_id)
            .first()
        )

    def list_backup_runs(self, limit: int = 20, offset: int = 0) -> list[BackupRun]:
        query = (
            self.db.query(BackupRun)
            .options(joinedload(BackupRun.initiated_by_user))
            .order_by(BackupRun.started_at.desc(), BackupRun.id.desc())
        )
        if offset:
            query = query.offset(offset)
        return query.limit(limit).all()

    def summarize_backup_storage_usage(self) -> dict[str, int]:
        database_bytes, files_bytes = (
            self.db.query(
                func.coalesce(func.sum(BackupRun.database_size_bytes), 0),
                func.coalesce(func.sum(BackupRun.files_size_bytes), 0),
            )
            .filter(BackupRun.deleted_from_storage_at.is_(None))
            .one()
        )
        return {
            "database_bytes": int(database_bytes or 0),
            "files_bytes": int(files_bytes or 0),
        }

    def get_oldest_active_backup_run(self) -> BackupRun | None:
        return (
            self.db.query(BackupRun)
            .filter(BackupRun.deleted_from_storage_at.is_(None))
            .filter(or_(BackupRun.database_object_path.is_not(None), BackupRun.files_object_path.is_not(None)))
            .order_by(BackupRun.started_at.asc(), BackupRun.id.asc())
            .first()
        )

    def get_latest_automatic_backup_run(self) -> BackupRun | None:
        return (
            self.db.query(BackupRun)
            .options(joinedload(BackupRun.initiated_by_user))
            .filter(BackupRun.trigger_source == "automatic")
            .order_by(BackupRun.started_at.desc(), BackupRun.id.desc())
            .first()
        )

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
        rows = query.all()
        if search and (needle := _normalize_plot_search_text(search).strip()):
            rows = [p for p in rows if needle in _normalize_plot_search_text(p.name)]
        return rows

    def list_plots_with_boundary_geojson(self, farm_ids: list[int] | None = None) -> list[Plot]:
        """Setores com perímetro salvo, para contexto visual no mapa (sem filtros de busca/variedade)."""
        query = self.db.query(Plot).filter(Plot.boundary_geojson.isnot(None))
        if farm_ids:
            query = query.filter(Plot.farm_id.in_(farm_ids))
        query = query.order_by(Plot.farm_id.asc(), Plot.name.asc())
        rows = query.all()
        return [p for p in rows if (p.boundary_geojson or "").strip()]

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
            .options(joinedload(Plot.variety), joinedload(Plot.farm), joinedload(Plot.attachments))
            .filter(Plot.id == plot_id)
            .first()
        )

    def get_plot_attachment(self, attachment_id: int) -> PlotAttachment | None:
        return (
            self.db.query(PlotAttachment)
            .options(joinedload(PlotAttachment.plot).joinedload(Plot.farm))
            .filter(PlotAttachment.id == attachment_id)
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

    def list_finance_accounts(self, farm_id: int | None = None) -> list[FinanceAccount]:
        query = (
            self.db.query(FinanceAccount)
            .options(joinedload(FinanceAccount.farm), joinedload(FinanceAccount.custom_bank))
            .order_by(FinanceAccount.is_default.desc(), FinanceAccount.account_name.asc(), FinanceAccount.id.desc())
        )
        if farm_id:
            query = query.filter(FinanceAccount.farm_id == farm_id)
        return query.all()

    def get_finance_account(self, account_id: int) -> FinanceAccount | None:
        return (
            self.db.query(FinanceAccount)
            .options(joinedload(FinanceAccount.farm), joinedload(FinanceAccount.custom_bank))
            .filter(FinanceAccount.id == account_id)
            .first()
        )

    def list_finance_credit_cards(self, farm_id: int | None = None) -> list[FinanceCreditCard]:
        query = (
            self.db.query(FinanceCreditCard)
            .options(joinedload(FinanceCreditCard.farm), joinedload(FinanceCreditCard.payment_account))
            .order_by(FinanceCreditCard.is_default.desc(), FinanceCreditCard.is_active.desc(), FinanceCreditCard.card_name.asc(), FinanceCreditCard.id.desc())
        )
        if farm_id:
            query = query.filter(FinanceCreditCard.farm_id == farm_id)
        return query.all()

    def get_finance_credit_card(self, card_id: int) -> FinanceCreditCard | None:
        return (
            self.db.query(FinanceCreditCard)
            .options(joinedload(FinanceCreditCard.farm), joinedload(FinanceCreditCard.payment_account))
            .filter(FinanceCreditCard.id == card_id)
            .first()
        )

    def list_finance_custom_banks(self) -> list[FinanceCustomBank]:
        return self.db.query(FinanceCustomBank).order_by(FinanceCustomBank.bank_name.asc(), FinanceCustomBank.id.desc()).all()

    def get_finance_custom_bank(self, bank_id: int) -> FinanceCustomBank | None:
        return self.db.query(FinanceCustomBank).filter(FinanceCustomBank.id == bank_id).first()

    def list_finance_transactions(self, farm_id: int | None = None, finance_account_id: int | None = None) -> list[FinanceTransaction]:
        query = (
            self.db.query(FinanceTransaction)
            .options(
                joinedload(FinanceTransaction.farm),
                joinedload(FinanceTransaction.finance_account),
                joinedload(FinanceTransaction.credit_card),
                joinedload(FinanceTransaction.attachments),
                joinedload(FinanceTransaction.installments),
            )
            .order_by(FinanceTransaction.launch_date.desc(), FinanceTransaction.id.desc())
        )
        if farm_id:
            query = query.filter(FinanceTransaction.farm_id == farm_id)
        if finance_account_id:
            query = query.filter(FinanceTransaction.finance_account_id == finance_account_id)
        return query.all()

    def get_finance_transaction(self, transaction_id: int) -> FinanceTransaction | None:
        return (
            self.db.query(FinanceTransaction)
            .options(
                joinedload(FinanceTransaction.farm),
                joinedload(FinanceTransaction.finance_account),
                joinedload(FinanceTransaction.credit_card),
                joinedload(FinanceTransaction.attachments),
                joinedload(FinanceTransaction.installments),
            )
            .filter(FinanceTransaction.id == transaction_id)
            .first()
        )

    def count_finance_transactions_for_account(self, finance_account_id: int) -> int:
        return (
            self.db.query(FinanceTransaction)
            .filter(FinanceTransaction.finance_account_id == finance_account_id)
            .count()
        )

    def get_finance_transaction_attachment(self, attachment_id: int) -> FinanceTransactionAttachment | None:
        return (
            self.db.query(FinanceTransactionAttachment)
            .options(joinedload(FinanceTransactionAttachment.transaction).joinedload(FinanceTransaction.farm))
            .filter(FinanceTransactionAttachment.id == attachment_id)
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
                joinedload(PurchasedInput.finance_credit_card),
                joinedload(PurchasedInput.stock_allocations),
                joinedload(PurchasedInput.attachments),
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
                joinedload(PurchasedInput.finance_credit_card),
                joinedload(PurchasedInput.stock_allocations),
                joinedload(PurchasedInput.recommendation_items),
                joinedload(PurchasedInput.schedule_items),
                joinedload(PurchasedInput.attachments),
            )
            .filter(PurchasedInput.id == input_id)
            .first()
        )

    def get_purchased_input_attachment(self, attachment_id: int) -> PurchasedInputAttachment | None:
        return (
            self.db.query(PurchasedInputAttachment)
            .options(joinedload(PurchasedInputAttachment.purchased_input).joinedload(PurchasedInput.farm))
            .filter(PurchasedInputAttachment.id == attachment_id)
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
            .options(joinedload(EquipmentAsset.farm), joinedload(EquipmentAsset.finance_credit_card), joinedload(EquipmentAsset.attachments))
            .order_by(EquipmentAsset.name.asc(), EquipmentAsset.id.desc())
        )
        if farm_id:
            query = query.filter(EquipmentAsset.farm_id == farm_id)
        return query.all()

    def get_equipment_asset(self, asset_id: int) -> EquipmentAsset | None:
        return (
            self.db.query(EquipmentAsset)
            .options(joinedload(EquipmentAsset.farm), joinedload(EquipmentAsset.finance_credit_card), joinedload(EquipmentAsset.attachments))
            .filter(EquipmentAsset.id == asset_id)
            .first()
        )

    def get_equipment_asset_attachment(self, attachment_id: int) -> EquipmentAssetAttachment | None:
        return (
            self.db.query(EquipmentAssetAttachment)
            .options(joinedload(EquipmentAssetAttachment.equipment_asset).joinedload(EquipmentAsset.farm))
            .filter(EquipmentAssetAttachment.id == attachment_id)
            .first()
        )

    def list_input_recommendations(self, farm_id: int | None = None) -> list[InputRecommendation]:
        query = (
            self.db.query(InputRecommendation)
            .options(
                joinedload(InputRecommendation.farm),
                joinedload(InputRecommendation.plot),
                joinedload(InputRecommendation.items).joinedload(InputRecommendationItem.input_catalog),
                joinedload(InputRecommendation.items).joinedload(InputRecommendationItem.purchased_input),
            )
            .order_by(InputRecommendation.application_name.asc(), InputRecommendation.id.desc())
        )
        if farm_id is not None:
            query = query.filter(InputRecommendation.farm_id == farm_id)
        return query.all()

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

    def list_fertilization_schedules_for_scope(
        self,
        plot_ids: list[int] | set[int],
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[FertilizationSchedule]:
        ids = list(plot_ids)
        if not ids:
            return []
        query = (
            self.db.query(FertilizationSchedule)
            .options(
                joinedload(FertilizationSchedule.plot).joinedload(Plot.farm),
                joinedload(FertilizationSchedule.season),
                joinedload(FertilizationSchedule.items).joinedload(FertilizationScheduleItem.input_catalog),
                joinedload(FertilizationSchedule.items).joinedload(FertilizationScheduleItem.purchased_input),
                joinedload(FertilizationSchedule.fertilization_record),
            )
            .filter(FertilizationSchedule.plot_id.in_(ids))
        )
        if start_date is not None:
            query = query.filter(
                or_(
                    FertilizationSchedule.scheduled_date.is_(None),
                    FertilizationSchedule.scheduled_date >= start_date,
                )
            )
        if end_date is not None:
            query = query.filter(
                or_(
                    FertilizationSchedule.scheduled_date.is_(None),
                    FertilizationSchedule.scheduled_date <= end_date,
                )
            )
        return query.order_by(FertilizationSchedule.scheduled_date.asc(), FertilizationSchedule.id.desc()).all()

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
        query = query.filter(
            or_(
                StockOutput.reference_type.is_(None),
                StockOutput.reference_type != MANUAL_STOCK_OUTPUT_ALLOCATION,
            )
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
            .options(joinedload(HarvestRecord.plot).joinedload(Plot.variety), joinedload(HarvestRecord.plot).joinedload(Plot.farm))
            .order_by(HarvestRecord.harvest_date.desc(), HarvestRecord.id.desc())
        )
        return query.limit(limit).all() if limit else query.all()

    def get_harvest(self, record_id: int) -> HarvestRecord | None:
        return (
            self.db.query(HarvestRecord)
            .options(joinedload(HarvestRecord.plot).joinedload(Plot.variety), joinedload(HarvestRecord.plot).joinedload(Plot.farm))
            .filter(HarvestRecord.id == record_id)
            .first()
        )

    def harvest_has_commercializations(self, harvest_id: int) -> bool:
        return (
            self.db.query(CoffeeCommercializationRecord.id)
            .filter(CoffeeCommercializationRecord.harvest_id == harvest_id)
            .limit(1)
            .first()
            is not None
        )

    def harvest_ids_linked_to_commercializations(self) -> set[int]:
        rows = (
            self.db.query(CoffeeCommercializationRecord.harvest_id)
            .filter(CoffeeCommercializationRecord.harvest_id.isnot(None))
            .distinct()
            .all()
        )
        return {row[0] for row in rows}

    def list_coffee_commercializations(self, farm_id: int | None = None) -> list[CoffeeCommercializationRecord]:
        query = (
            self.db.query(CoffeeCommercializationRecord)
            .options(
                joinedload(CoffeeCommercializationRecord.harvest).joinedload(HarvestRecord.plot).joinedload(Plot.variety),
                joinedload(CoffeeCommercializationRecord.harvest).joinedload(HarvestRecord.plot).joinedload(Plot.farm),
                joinedload(CoffeeCommercializationRecord.finance_account),
                joinedload(CoffeeCommercializationRecord.finance_transaction),
            )
            .order_by(CoffeeCommercializationRecord.sale_date.desc(), CoffeeCommercializationRecord.id.desc())
        )
        if farm_id:
            query = query.filter(CoffeeCommercializationRecord.farm_id == farm_id)
        return query.all()

    def get_coffee_commercialization(self, record_id: int) -> CoffeeCommercializationRecord | None:
        return (
            self.db.query(CoffeeCommercializationRecord)
            .options(
                joinedload(CoffeeCommercializationRecord.harvest).joinedload(HarvestRecord.plot).joinedload(Plot.variety),
                joinedload(CoffeeCommercializationRecord.harvest).joinedload(HarvestRecord.plot).joinedload(Plot.farm),
                joinedload(CoffeeCommercializationRecord.finance_account),
                joinedload(CoffeeCommercializationRecord.finance_transaction),
            )
            .filter(CoffeeCommercializationRecord.id == record_id)
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
