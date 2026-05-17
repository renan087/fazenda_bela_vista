from datetime import date, datetime

from sqlalchemy import Date, DateTime, Numeric, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class CoffeeQuote(Base):
    __tablename__ = "coffee_quotes"
    __table_args__ = (
        UniqueConstraint("quote_type", "quote_date", "source", name="uq_coffee_quotes_type_date_source"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    quote_type: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    quote_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    price_brl: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False)
    variation_day: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    variation_month: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    price_usd: Mapped[float] = mapped_column(Numeric(14, 2), nullable=True)
    source: Mapped[str] = mapped_column(String(80), nullable=False, default="CEPEA/ESALQ")
    source_url: Mapped[str] = mapped_column(String(500), nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
