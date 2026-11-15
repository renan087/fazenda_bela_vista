from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class PlotPlanting(Base):
    """Cada lavoura plantada num setor ao longo do tempo. O plantio ativo e o que tem ended_on vazio."""

    __tablename__ = "plot_plantings"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id", ondelete="CASCADE"), nullable=False, index=True)
    culture: Mapped[str] = mapped_column(String(80), nullable=False, default="Café")
    variety_id: Mapped[int | None] = mapped_column(ForeignKey("coffee_varieties.id", ondelete="SET NULL"), nullable=True)
    planting_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    plant_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    spacing_row_meters: Mapped[float | None] = mapped_column(Numeric(8, 2), nullable=True)
    spacing_plant_meters: Mapped[float | None] = mapped_column(Numeric(8, 2), nullable=True)
    estimated_yield_sacks: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    ended_on: Mapped[date | None] = mapped_column(Date, nullable=True)
    end_reason: Mapped[str | None] = mapped_column(String(40), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    plot = relationship("Plot", back_populates="plantings")
    variety = relationship("CoffeeVariety")

    @property
    def is_active(self) -> bool:
        return self.ended_on is None
