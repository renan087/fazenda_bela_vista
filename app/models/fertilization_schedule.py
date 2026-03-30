from datetime import date

from sqlalchemy import Date, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class FertilizationSchedule(Base):
    __tablename__ = "fertilization_schedules"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id"), nullable=False)
    season_id: Mapped[int] = mapped_column(ForeignKey("crop_seasons.id"), nullable=True)
    scheduled_date: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="scheduled")
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    fertilization_record_id: Mapped[int] = mapped_column(ForeignKey("fertilization_records.id"), nullable=True)

    plot = relationship("Plot", back_populates="fertilization_schedules")
    season = relationship("CropSeason", back_populates="fertilization_schedules")
    fertilization_record = relationship("FertilizationRecord", back_populates="schedule")
    items = relationship("FertilizationScheduleItem", back_populates="schedule", cascade="all, delete-orphan")
