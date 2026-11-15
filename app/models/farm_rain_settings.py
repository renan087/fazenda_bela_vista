from sqlalchemy import Boolean, ForeignKey, Integer, Numeric
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class FarmRainSettings(Base):
    """Configuracoes da pluviometria por fazenda (limites e alertas de chuva forte)."""

    __tablename__ = "farm_rain_settings"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    heavy_rain_mm: Mapped[float] = mapped_column(Numeric(6, 1), nullable=False, default=30)
    rainy_day_mm: Mapped[float] = mapped_column(Numeric(6, 1), nullable=False, default=1)
    alert_registered: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    alert_forecast: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    notify_dashboard: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    notify_bell: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    average_since_year: Mapped[int] = mapped_column(Integer, nullable=True)
