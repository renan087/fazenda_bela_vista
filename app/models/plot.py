from sqlalchemy import ForeignKey, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Plot(Base):
    __tablename__ = "plots"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    area_hectares: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    location: Mapped[str] = mapped_column(String(120), nullable=False)
    plant_count: Mapped[int] = mapped_column(nullable=False)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    variety_id: Mapped[int] = mapped_column(ForeignKey("coffee_varieties.id"), nullable=True)

    variety = relationship("CoffeeVariety", back_populates="plots")
    irrigations = relationship("IrrigationRecord", back_populates="plot", cascade="all, delete-orphan")
    fertilizations = relationship(
        "FertilizationRecord",
        back_populates="plot",
        cascade="all, delete-orphan",
    )
    pesticide_applications = relationship(
        "PesticideApplication",
        back_populates="plot",
        cascade="all, delete-orphan",
    )
    harvests = relationship("HarvestRecord", back_populates="plot", cascade="all, delete-orphan")
