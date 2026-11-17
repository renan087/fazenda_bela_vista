from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class WhatsAppLink(Base):
    """Vinculo de um numero de WhatsApp a um usuario, fazenda e safra (comprovado por codigo enviado do proprio numero)."""

    __tablename__ = "whatsapp_links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), unique=True, index=True)
    organization_id: Mapped[int | None] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"))
    season_id: Mapped[int] = mapped_column(ForeignKey("crop_seasons.id", ondelete="CASCADE"))
    wa_id: Mapped[str | None] = mapped_column(String(32), unique=True, nullable=True)
    thread_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    code_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    code_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_inbound_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class WhatsAppInboundMessage(Base):
    """Mensagem recebida pelo webhook. O wamid unico impede processar a mesma mensagem duas vezes."""

    __tablename__ = "whatsapp_inbound_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wamid: Mapped[str] = mapped_column(String(160), unique=True)
    wa_id: Mapped[str] = mapped_column(String(32), index=True)
    msg_type: Mapped[str] = mapped_column(String(24))
    payload_json: Mapped[str] = mapped_column(Text, default="{}")
    status: Mapped[str] = mapped_column(String(20), default="received", index=True)
    error: Mapped[str | None] = mapped_column(String(300), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class WhatsAppRecordAccess(Base):
    """Referencia temporaria, de uso unico, para consulta de um unico registro."""

    __tablename__ = "whatsapp_record_access"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    binding_hash: Mapped[str] = mapped_column(String(64))
    link_id: Mapped[int] = mapped_column(ForeignKey("whatsapp_links.id", ondelete="CASCADE"), index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"))
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"))
    season_id: Mapped[int] = mapped_column(ForeignKey("crop_seasons.id", ondelete="CASCADE"))
    kind: Mapped[str] = mapped_column(String(24))
    record_id: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
