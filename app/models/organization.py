from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, func, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Organization(Base):
    __tablename__ = "organizations"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    slug: Mapped[str] = mapped_column(String(180), unique=True, index=True, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    onboarding_dismissed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, server_default=text("false"))
    # Tour guiado dos primeiros passos: "" (nunca iniciado), active, paused, ended ou done.
    onboarding_tour: Mapped[str] = mapped_column(String(12), default="", nullable=False, server_default="")
    onboarding_plot_skipped: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, server_default=text("false"))
    # Onde a pessoa parou no tour (rótulo do passo, ex.: "Fazenda · CAR") e quando: usado nas boas-vindas de volta.
    onboarding_last_step: Mapped[str] = mapped_column(String(60), default="", nullable=False, server_default="")
    onboarding_last_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    primary_user_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    users = relationship("User", back_populates="organization", foreign_keys="User.organization_id")
    primary_user = relationship("User", foreign_keys=[primary_user_id], post_update=True)
    farms = relationship("Farm", back_populates="organization")
    roles = relationship("Role", back_populates="organization", cascade="all, delete-orphan", passive_deletes=True)
