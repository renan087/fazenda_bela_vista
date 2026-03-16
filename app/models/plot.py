from datetime import date

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Plot(Base):
    __tablename__ = "plots"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    area_hectares: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    location: Mapped[str] = mapped_column(String(120), nullable=True)
    planting_date: Mapped[date] = mapped_column(Date, nullable=True)
    plant_count: Mapped[int] = mapped_column(nullable=False)
    spacing_row_meters: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    spacing_plant_meters: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    estimated_yield_sacks: Mapped[float] = mapped_column(Numeric(10, 2), nullable=True)
    centroid_lat: Mapped[float] = mapped_column(Numeric(10, 6), nullable=True)
    centroid_lng: Mapped[float] = mapped_column(Numeric(10, 6), nullable=True)
    boundary_geojson: Mapped[str] = mapped_column(Text, nullable=True)
    irrigation_type: Mapped[str] = mapped_column(String(40), nullable=False, default="none")
    irrigation_line_count: Mapped[int] = mapped_column(Integer, nullable=True)
    irrigation_line_length_meters: Mapped[float] = mapped_column(Numeric(10, 2), nullable=True)
    drip_spacing_meters: Mapped[float] = mapped_column(Numeric(8, 3), nullable=True)
    drip_liters_per_hour: Mapped[float] = mapped_column(Numeric(10, 2), nullable=True)
    sprinkler_count: Mapped[int] = mapped_column(Integer, nullable=True)
    sprinkler_liters_per_hour: Mapped[float] = mapped_column(Numeric(10, 2), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id"), nullable=True)
    variety_id: Mapped[int] = mapped_column(ForeignKey("coffee_varieties.id"), nullable=True)

    farm = relationship("Farm", back_populates="plots")
    variety = relationship("CoffeeVariety", back_populates="plots")
    irrigations = relationship("IrrigationRecord", back_populates="plot", cascade="all, delete-orphan")
    fertilizations = relationship("FertilizationRecord", back_populates="plot", cascade="all, delete-orphan")
    harvests = relationship("HarvestRecord", back_populates="plot", cascade="all, delete-orphan")
    pest_incidents = relationship("PestIncident", back_populates="plot", cascade="all, delete-orphan")
    soil_analyses = relationship("SoilAnalysis", back_populates="plot", cascade="all, delete-orphan")
