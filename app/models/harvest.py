from datetime import date

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class HarvestRecord(Base):
    __tablename__ = "harvest_records"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id"), nullable=False)
    harvest_date: Mapped[date] = mapped_column(Date, nullable=False)
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

    plot = relationship("Plot", back_populates="harvests")
