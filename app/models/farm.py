from sqlalchemy import Boolean, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Farm(Base):
    __tablename__ = "farms"
    # Nome unico dentro da organizacao (contas diferentes podem ter fazendas com o mesmo nome).
    __table_args__ = (UniqueConstraint("organization_id", "name", name="uq_farms_organization_name"),)

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id", ondelete="SET NULL"), nullable=True, index=True)
    operations_paused: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"), nullable=False)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    location: Mapped[str] = mapped_column(String(180), nullable=False)
    total_area: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    car_registration_number: Mapped[str | None] = mapped_column(String(140), nullable=True, index=True)
    ccir_number: Mapped[str | None] = mapped_column(String(80), nullable=True)
    itr_registration_number: Mapped[str | None] = mapped_column(String(100), nullable=True)
    property_registration_number: Mapped[str | None] = mapped_column(String(140), nullable=True)
    registry_office: Mapped[str | None] = mapped_column(String(180), nullable=True)
    boundary_geojson: Mapped[str] = mapped_column(Text, nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    organization = relationship("Organization", back_populates="farms")
    attachments = relationship("FarmAttachment", back_populates="farm", cascade="all, delete-orphan")
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
    finance_credit_cards = relationship("FinanceCreditCard", back_populates="farm", cascade="all, delete-orphan")
    finance_transactions = relationship("FinanceTransaction", back_populates="farm", cascade="all, delete-orphan")
