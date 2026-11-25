from datetime import datetime

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class MercadoPagoEvent(Base):
    __tablename__ = "mercadopago_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    delivery_key: Mapped[str] = mapped_column(String(64), unique=True)
    request_key: Mapped[str] = mapped_column(String(64), unique=True)
    topic: Mapped[str] = mapped_column(String(48))
    resource_id: Mapped[str] = mapped_column(String(80))
    state: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    result: Mapped[str | None] = mapped_column(String(100))
    retry_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), index=True)
    received_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
