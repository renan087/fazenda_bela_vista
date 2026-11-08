from datetime import date

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class AgronomicProfile(Base):
    __tablename__ = "agronomic_profiles"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id"), nullable=False, unique=True)
    culture: Mapped[str] = mapped_column(String(120), nullable=False)
    region: Mapped[str] = mapped_column(String(180), nullable=False)
    climate: Mapped[str] = mapped_column(String(180), nullable=True)
    soil_type: Mapped[str] = mapped_column(String(180), nullable=True)
    irrigation_system: Mapped[str] = mapped_column(String(120), nullable=True)
    plant_spacing: Mapped[str] = mapped_column(String(120), nullable=True)
    drip_spacing: Mapped[str] = mapped_column(String(120), nullable=True)
    fertilizers_used: Mapped[str] = mapped_column(Text, nullable=True)
    crop_stage: Mapped[str] = mapped_column(Text, nullable=True)
    common_pests: Mapped[str] = mapped_column(Text, nullable=True)
    cultivar: Mapped[str] = mapped_column(String(120), nullable=True)
    production_system: Mapped[str] = mapped_column(String(120), nullable=True)
    altitude_meters: Mapped[int] = mapped_column(Integer, nullable=True)
    terrain_slope: Mapped[str] = mapped_column(String(80), nullable=True)
    sun_exposure: Mapped[str] = mapped_column(String(80), nullable=True)
    target_productivity_sacks_per_ha: Mapped[float] = mapped_column(Numeric(10, 2), nullable=True)
    last_liming_date: Mapped[date] = mapped_column(Date, nullable=True)
    last_gypsum_date: Mapped[date] = mapped_column(Date, nullable=True)
    last_organic_matter_date: Mapped[date] = mapped_column(Date, nullable=True)
    pruning_history: Mapped[str] = mapped_column(Text, nullable=True)
    current_limitations: Mapped[str] = mapped_column(Text, nullable=True)

    farm = relationship("Farm", back_populates="agronomic_profile")
