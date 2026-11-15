from datetime import datetime

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class SystemMaintenance(Base):
    """Linha unica (id=1) com o estado do modo manutencao. Fica no banco para sobreviver a deploys e reinicios."""

    __tablename__ = "system_maintenance"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="off")  # off | leitura | fora_do_ar
    message: Mapped[str | None] = mapped_column(String(300), nullable=True)
    announce_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)  # manutencao programada
    ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)  # previsao de volta / fim automatico
    updated_by_email: Mapped[str | None] = mapped_column(String(254), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
