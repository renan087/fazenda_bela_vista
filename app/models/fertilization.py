from datetime import date

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class FertilizationRecord(Base):
    __tablename__ = "fertilization_records"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id"), nullable=False)
    season_id: Mapped[int] = mapped_column(ForeignKey("crop_seasons.id"), nullable=True)
    application_date: Mapped[date] = mapped_column(Date, nullable=False)
    product: Mapped[str] = mapped_column(String(120), nullable=False)
    dose: Mapped[str] = mapped_column(String(80), nullable=False)
    cost: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    duration_minutes: Mapped[int] = mapped_column(Integer, nullable=True)
    application_method: Mapped[str] = mapped_column(String(40), nullable=False, default="fertirrigacao")
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    plot = relationship("Plot", back_populates="fertilizations")
    season = relationship("CropSeason", back_populates="fertilizations")
    items = relationship("FertilizationItem", back_populates="fertilization", cascade="all, delete-orphan")
    schedule = relationship("FertilizationSchedule", back_populates="fertilization_record", uselist=False)
