from datetime import date

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class EquipmentAsset(Base):
    __tablename__ = "equipment_assets"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id"), nullable=True)
    finance_account_id: Mapped[int] = mapped_column(ForeignKey("finance_accounts.id"), nullable=True)
    finance_transaction_id: Mapped[int] = mapped_column(ForeignKey("finance_transactions.id", ondelete="SET NULL"), nullable=True)
    name: Mapped[str] = mapped_column(String(180), nullable=False)
    category: Mapped[str] = mapped_column(String(120), nullable=False)
    subtype: Mapped[str] = mapped_column(String(120), nullable=True)
    manufacturer: Mapped[str] = mapped_column(String(180), nullable=True)
    manufacture_year: Mapped[int] = mapped_column(Integer, nullable=True)
    brand_model: Mapped[str] = mapped_column(String(180), nullable=True)
    asset_code: Mapped[str] = mapped_column(String(120), nullable=True)
    measurement_label: Mapped[str] = mapped_column(String(120), nullable=True)
    measurement_value: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)
    measurement_unit: Mapped[str] = mapped_column(String(30), nullable=True)
    acquisition_date: Mapped[date] = mapped_column(Date, nullable=True)
    acquisition_value: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)
    status: Mapped[str] = mapped_column(String(60), nullable=False, default="ativo")
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    payment_condition: Mapped[str] = mapped_column(String(20), nullable=False, default="a_vista")
    payment_method: Mapped[str] = mapped_column(String(80), nullable=True)
    installment_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    installment_frequency: Mapped[str] = mapped_column(String(20), nullable=True)
    first_installment_date: Mapped[date] = mapped_column(Date, nullable=True)

    farm = relationship("Farm", back_populates="equipment_assets")
    finance_account = relationship("FinanceAccount", back_populates="equipment_assets")
    finance_transaction = relationship("FinanceTransaction", back_populates="equipment_assets")
    attachments = relationship("EquipmentAssetAttachment", back_populates="equipment_asset", cascade="all, delete-orphan")
