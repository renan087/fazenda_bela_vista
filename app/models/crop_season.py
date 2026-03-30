from sqlalchemy import Date, ForeignKey, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class CropSeason(Base):
    __tablename__ = "crop_seasons"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"), nullable=False, index=True)
    variety_id: Mapped[int] = mapped_column(ForeignKey("coffee_varieties.id", ondelete="SET NULL"), nullable=True, index=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    start_date: Mapped[Date] = mapped_column(Date, nullable=False)
    end_date: Mapped[Date] = mapped_column(Date, nullable=False)
    culture: Mapped[str] = mapped_column(String(120), nullable=False, default="Cafe")
    cultivated_area: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    area_unit: Mapped[str] = mapped_column(String(20), nullable=False, default="ha")
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="planejada")

    farm = relationship("Farm", back_populates="crop_seasons")
    variety = relationship("CoffeeVariety", back_populates="crop_seasons")
    fertilizations = relationship("FertilizationRecord", back_populates="season")
    fertilization_schedules = relationship("FertilizationSchedule", back_populates="season")
    stock_outputs = relationship("StockOutput", back_populates="season")
