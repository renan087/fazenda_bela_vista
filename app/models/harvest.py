from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, Numeric, String, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class HarvestRecord(Base):
    __tablename__ = "harvest_records"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id"), nullable=False)
    season_id: Mapped[int | None] = mapped_column(ForeignKey("crop_seasons.id", ondelete="SET NULL"), nullable=True)
    harvest_date: Mapped[date] = mapped_column(Date, nullable=False)
    lot_code: Mapped[str] = mapped_column(String(40), nullable=True)
    sacks_produced: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    productivity_per_hectare: Mapped[float] = mapped_column(Numeric(10, 2), nullable=True)
    harvest_type: Mapped[str] = mapped_column(String(40), nullable=True)
    coffee_stage: Mapped[str] = mapped_column(String(40), nullable=True)
    initial_destination: Mapped[str] = mapped_column(String(60), nullable=True)
    responsible_name: Mapped[str] = mapped_column(String(160), nullable=True)
    work_shift: Mapped[str] = mapped_column(String(30), nullable=True)
    maturation_percentage: Mapped[float] = mapped_column(Numeric(5, 2), nullable=True)
    impurity_percentage: Mapped[float] = mapped_column(Numeric(5, 2), nullable=True)
    input_moisture_percentage: Mapped[float] = mapped_column(Numeric(5, 2), nullable=True)
    volume_count: Mapped[int] = mapped_column(Integer, nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    measure_unit: Mapped[str | None] = mapped_column(String(20), nullable=True)
    measured_quantity: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    yield_factor: Mapped[float | None] = mapped_column(Numeric(10, 3), nullable=True)
    sacks_estimated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    lot_stage: Mapped[str | None] = mapped_column(String(30), nullable=True)
    storage_location: Mapped[str | None] = mapped_column(String(120), nullable=True)
    warehouse_receipt: Mapped[str | None] = mapped_column(String(60), nullable=True)
    processed_on: Mapped[date | None] = mapped_column(Date, nullable=True)

    plot = relationship("Plot", back_populates="harvests")
    events = relationship("HarvestLotEvent", back_populates="harvest", cascade="all, delete-orphan",
                          order_by="HarvestLotEvent.event_date")
    season = relationship("CropSeason")
    commercializations = relationship("CoffeeCommercializationRecord", back_populates="harvest")


class HarvestLotEvent(Base):
    """Acontecimentos do lote depois da colheita (secagem, tulha, benefício, armazém) para a rastreabilidade."""

    __tablename__ = "harvest_lot_events"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    harvest_id: Mapped[int] = mapped_column(ForeignKey("harvest_records.id", ondelete="CASCADE"), nullable=False, index=True)
    event_date: Mapped[date] = mapped_column(Date, nullable=False)
    kind: Mapped[str] = mapped_column(String(20), nullable=False, default="etapa")
    stage: Mapped[str | None] = mapped_column(String(30), nullable=True)
    title: Mapped[str] = mapped_column(String(160), nullable=False)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    harvest = relationship("HarvestRecord", back_populates="events")
