from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Index, Integer, String, func, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class OrganizationCardMigration(Base):
    """Tentativa persistente; nao armazena dados de cartao."""

    __tablename__ = 'organization_card_migrations'
    __table_args__ = (
        Index('uq_card_migration_open', 'organization_id', unique=True,
              postgresql_where=text('is_open'), sqlite_where=text('is_open = 1')),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey('organizations.id', ondelete='RESTRICT'), nullable=False, index=True)
    old_subscription_id: Mapped[int] = mapped_column(ForeignKey('organization_subscriptions.id', ondelete='RESTRICT'), nullable=False)
    new_subscription_id: Mapped[int | None] = mapped_column(ForeignKey('organization_subscriptions.id', ondelete='RESTRICT'), nullable=True)
    checkout_id: Mapped[str | None] = mapped_column(String(80), unique=True, nullable=True)
    first_due_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    status: Mapped[str] = mapped_column(String(40), nullable=False, default='preparando')
    last_check_error: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_open: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text('true'))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
