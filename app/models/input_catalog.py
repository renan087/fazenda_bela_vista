from sqlalchemy import Boolean, ForeignKey, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class InputCatalog(Base):
    __tablename__ = "input_catalog"
    __table_args__ = (
        UniqueConstraint("organization_id", "normalized_name", name="uq_input_catalog_organization_normalized_name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    organization_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    normalized_name: Mapped[str] = mapped_column(String(180), index=True, nullable=False)
    item_type: Mapped[str] = mapped_column(String(40), nullable=False, default="insumo_agricola")
    category: Mapped[str] = mapped_column(String(120), nullable=False, default="Geral")
    default_unit: Mapped[str] = mapped_column(String(20), nullable=False, default="kg")
    low_stock_threshold: Mapped[float] = mapped_column(Numeric(10, 2), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    organization = relationship("Organization")
    purchase_entries = relationship("PurchasedInput", back_populates="input_catalog")
    recommendation_items = relationship("InputRecommendationItem", back_populates="input_catalog")
    fertilization_items = relationship("FertilizationItem", back_populates="input_catalog")
    schedule_items = relationship("FertilizationScheduleItem", back_populates="input_catalog")
    stock_outputs = relationship("StockOutput", back_populates="input_catalog")
