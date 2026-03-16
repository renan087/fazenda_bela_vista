from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, LargeBinary, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class SoilAnalysis(Base):
    __tablename__ = "soil_analyses"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id"), nullable=False)
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id"), nullable=False)
    analysis_date: Mapped[date] = mapped_column(Date, nullable=False)
    laboratory: Mapped[str] = mapped_column(String(180), nullable=False)
    ph: Mapped[float] = mapped_column(Numeric(6, 2), nullable=True)
    organic_matter: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    phosphorus: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    potassium: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    calcium: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    magnesium: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    aluminum: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    h_al: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    ctc: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    base_saturation: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    observations: Mapped[str] = mapped_column(Text, nullable=True)
    pdf_filename: Mapped[str] = mapped_column(String(255), nullable=True)
    pdf_content_type: Mapped[str] = mapped_column(String(120), nullable=True)
    pdf_data: Mapped[bytes] = mapped_column(LargeBinary, nullable=True)
    liming_need_t_ha: Mapped[float] = mapped_column(Numeric(8, 2), nullable=True)
    npk_recommendation: Mapped[str] = mapped_column(Text, nullable=True)
    micronutrient_recommendation: Mapped[str] = mapped_column(Text, nullable=True)
    ai_recommendation: Mapped[str] = mapped_column(Text, nullable=True)
    ai_status: Mapped[str] = mapped_column(String(40), nullable=True)
    ai_model: Mapped[str] = mapped_column(String(120), nullable=True)
    ai_error: Mapped[str] = mapped_column(Text, nullable=True)
    ai_generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)

    farm = relationship("Farm", back_populates="soil_analyses")
    plot = relationship("Plot", back_populates="soil_analyses")
