from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, DateTime, ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class StockAssistantDraft(Base):
    __tablename__ = "stock_assistant_drafts"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    organization_id: Mapped[int | None] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"))
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"))
    season_id: Mapped[int] = mapped_column(ForeignKey("crop_seasons.id", ondelete="CASCADE"))
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id", ondelete="CASCADE"))
    input_id: Mapped[int] = mapped_column(ForeignKey("input_catalog.id", ondelete="CASCADE"))
    movement_date: Mapped[date] = mapped_column(Date)
    quantity: Mapped[Decimal] = mapped_column(Numeric(12, 2))
    unit: Mapped[str] = mapped_column(String(20))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), default="pending")
