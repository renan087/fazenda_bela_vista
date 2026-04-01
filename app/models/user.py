from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, LargeBinary, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    email: Mapped[str] = mapped_column(String(120), unique=True, index=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    active_farm_id: Mapped[int] = mapped_column(Integer, ForeignKey("farms.id", ondelete="SET NULL"), nullable=True, index=True)
    active_season_id: Mapped[int] = mapped_column(Integer, ForeignKey("crop_seasons.id", ondelete="SET NULL"), nullable=True, index=True)
    display_name: Mapped[str] = mapped_column(String(120), nullable=True)
    gender: Mapped[str] = mapped_column(String(30), nullable=True)
    birth_date: Mapped[date] = mapped_column(Date, nullable=True)
    phone: Mapped[str] = mapped_column(String(40), nullable=True)
    job_title: Mapped[str] = mapped_column(String(120), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    avatar_filename: Mapped[str] = mapped_column(String(255), nullable=True)
    avatar_content_type: Mapped[str] = mapped_column(String(120), nullable=True)
    avatar_data: Mapped[bytes] = mapped_column(LargeBinary, nullable=True)
    last_login_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
