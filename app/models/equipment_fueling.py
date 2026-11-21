from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class EquipmentFueling(Base):
    """Abastecimento de uma máquina: liga o combustível do estoque (ou posto) ao horímetro."""

    __tablename__ = "equipment_fuelings"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("equipment_assets.id", ondelete="CASCADE"), nullable=False, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="SET NULL"), nullable=True, index=True)
    season_id: Mapped[int] = mapped_column(ForeignKey("crop_seasons.id", ondelete="SET NULL"), nullable=True)
    input_id: Mapped[int] = mapped_column(ForeignKey("input_catalog.id", ondelete="SET NULL"), nullable=True)
    stock_output_id: Mapped[int] = mapped_column(ForeignKey("stock_outputs.id", ondelete="SET NULL"), nullable=True)
    fueling_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    liters: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    meter_reading: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)
    meter_unit: Mapped[str] = mapped_column(String(10), nullable=False, default="h")
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="estoque")
    total_cost: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)
    responsible_name: Mapped[str] = mapped_column(String(120), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    created_by: Mapped[int] = mapped_column(Integer, nullable=True)

    asset = relationship("EquipmentAsset")
    input_catalog = relationship("InputCatalog")
