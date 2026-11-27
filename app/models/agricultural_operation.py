from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class AgriculturalOperation(Base):
    __tablename__ = "agricultural_operations"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id", ondelete="CASCADE"), nullable=False, index=True)
    season_id: Mapped[int] = mapped_column(ForeignKey("crop_seasons.id", ondelete="SET NULL"), nullable=True, index=True)
    finance_transaction_id: Mapped[int] = mapped_column(ForeignKey("finance_transactions.id", ondelete="SET NULL"), nullable=True, index=True)
    equipment_asset_id: Mapped[int | None] = mapped_column(ForeignKey("equipment_assets.id", ondelete="SET NULL"), nullable=True, index=True)
    pest_incident_id: Mapped[int | None] = mapped_column(ForeignKey("pest_incidents.id", ondelete="SET NULL"), nullable=True, index=True)
    operation_date: Mapped[date] = mapped_column(Date, nullable=False)
    operation_type: Mapped[str] = mapped_column(String(80), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="realizada")
    responsible_name: Mapped[str] = mapped_column(String(160), nullable=False)
    equipment_hours: Mapped[float | None] = mapped_column(Numeric(8, 2), nullable=True)
    spray_volume_liters: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    withdrawal_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    target_pest: Mapped[str | None] = mapped_column(String(140), nullable=True)
    pruning_subtype: Mapped[str | None] = mapped_column(String(60), nullable=True)
    inputs_cost: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True, default=0)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    plot = relationship("Plot")
    season = relationship("CropSeason")
    finance_transaction = relationship("FinanceTransaction")
    equipment_asset = relationship("EquipmentAsset")
    pest_incident = relationship("PestIncident")
