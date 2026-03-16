from datetime import date

from sqlalchemy import Date, ForeignKey, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class IrrigationRecord(Base):
    __tablename__ = "irrigation_records"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id"), nullable=False)
    irrigation_date: Mapped[date] = mapped_column(Date, nullable=False)
    water_volume_mm: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    method: Mapped[str] = mapped_column(String(80), nullable=False)
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    plot = relationship("Plot", back_populates="irrigations")
