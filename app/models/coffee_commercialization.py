from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class CoffeeCommercializationRecord(Base):
    __tablename__ = "coffee_commercialization_records"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"), nullable=False, index=True)
    harvest_id: Mapped[int] = mapped_column(ForeignKey("harvest_records.id", ondelete="SET NULL"), nullable=True, index=True)
    buyer_id: Mapped[int | None] = mapped_column(ForeignKey("coffee_buyers.id", ondelete="SET NULL"), nullable=True, index=True)
    finance_account_id: Mapped[int] = mapped_column(ForeignKey("finance_accounts.id", ondelete="SET NULL"), nullable=True, index=True)
    finance_transaction_id: Mapped[int] = mapped_column(ForeignKey("finance_transactions.id", ondelete="SET NULL"), nullable=True, index=True)
    sale_date: Mapped[date] = mapped_column(Date, nullable=False)
    buyer_name: Mapped[str] = mapped_column(String(180), nullable=False)
    lot_label: Mapped[str] = mapped_column(String(220), nullable=False)
    plot_name: Mapped[str] = mapped_column(String(160), nullable=True)
    variety_name: Mapped[str] = mapped_column(String(160), nullable=True)
    harvest_date_snapshot: Mapped[date] = mapped_column(Date, nullable=True)
    coffee_type: Mapped[str] = mapped_column(String(60), nullable=True)
    sale_unit: Mapped[str] = mapped_column(String(20), nullable=False, default="sc_60")
    quantity_sold: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False)
    equivalent_sacks: Mapped[float] = mapped_column(Numeric(14, 4), nullable=False)
    unit_price: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False)
    total_value: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False)
    status: Mapped[str] = mapped_column(String(40), nullable=False, default="negociado")
    payment_method: Mapped[str] = mapped_column(String(80), nullable=True)
    payment_condition: Mapped[str] = mapped_column(String(20), nullable=False, default="a_vista")
    installment_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    installment_frequency: Mapped[str] = mapped_column(String(20), nullable=True)
    first_installment_date: Mapped[date] = mapped_column(Date, nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    sale_code: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    contract_number: Mapped[str | None] = mapped_column(String(60), nullable=True)
    delivery_due_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    delivered_on: Mapped[date | None] = mapped_column(Date, nullable=True)
    received_on: Mapped[date | None] = mapped_column(Date, nullable=True)
    funrural_percent: Mapped[float | None] = mapped_column(Numeric(6, 3), nullable=True)
    brokerage_percent: Mapped[float | None] = mapped_column(Numeric(6, 3), nullable=True)
    freight_value: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    discount_value: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    net_value: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    farm = relationship("Farm")
    buyer = relationship("CoffeeBuyer")
    harvest = relationship("HarvestRecord", back_populates="commercializations")
    finance_account = relationship("FinanceAccount")
    finance_transaction = relationship("FinanceTransaction", back_populates="commercializations")
