from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class CoffeeBuyer(Base):
    """Comprador de café (cadastro reutilizável, com dados para emissão de NF-e)."""

    __tablename__ = "coffee_buyers"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    organization_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=True, index=True
    )
    person_type: Mapped[str] = mapped_column(String(2), nullable=False, default="pj")
    name: Mapped[str] = mapped_column(String(180), nullable=False)
    trade_name: Mapped[str | None] = mapped_column(String(180), nullable=True)
    document: Mapped[str | None] = mapped_column(String(14), nullable=True, index=True)
    state_registration: Mapped[str | None] = mapped_column(String(30), nullable=True)
    ie_indicator: Mapped[str | None] = mapped_column(String(20), nullable=True)
    ie_exempt: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    rural_producer: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    postal_code: Mapped[str | None] = mapped_column(String(8), nullable=True)
    street: Mapped[str | None] = mapped_column(String(180), nullable=True)
    number: Mapped[str | None] = mapped_column(String(20), nullable=True)
    complement: Mapped[str | None] = mapped_column(String(120), nullable=True)
    district: Mapped[str | None] = mapped_column(String(120), nullable=True)
    city: Mapped[str | None] = mapped_column(String(120), nullable=True)
    municipality_code: Mapped[str | None] = mapped_column(String(7), nullable=True)
    state: Mapped[str | None] = mapped_column(String(2), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(40), nullable=True)
    email: Mapped[str | None] = mapped_column(String(180), nullable=True)
    contact_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
