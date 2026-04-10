from sqlalchemy import Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Farm(Base):
    __tablename__ = "farms"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(160), unique=True, nullable=False)
    location: Mapped[str] = mapped_column(String(180), nullable=False)
    total_area: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    boundary_geojson: Mapped[str] = mapped_column(Text, nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    plots = relationship("Plot", back_populates="farm")
    agronomic_profile = relationship("AgronomicProfile", back_populates="farm", uselist=False, cascade="all, delete-orphan")
    soil_analyses = relationship("SoilAnalysis", back_populates="farm", cascade="all, delete-orphan")
    rainfalls = relationship("RainfallRecord", back_populates="farm", cascade="all, delete-orphan")
    crop_seasons = relationship("CropSeason", back_populates="farm", cascade="all, delete-orphan")
    purchased_inputs = relationship("PurchasedInput", back_populates="farm", cascade="all, delete-orphan")
    input_recommendations = relationship("InputRecommendation", back_populates="farm", cascade="all, delete-orphan")
    stock_outputs = relationship("StockOutput", back_populates="farm", cascade="all, delete-orphan")
    equipment_assets = relationship("EquipmentAsset", back_populates="farm", cascade="all, delete-orphan")
    finance_accounts = relationship("FinanceAccount", back_populates="farm", cascade="all, delete-orphan")
