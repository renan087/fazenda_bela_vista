from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class CostClassification(Base):
    """Categoria de custo confirmada ou corrigida pelo usuario.

    ``entry_key`` identifica o lancamento (ex.: ``fin:12`` ou ``stock:40``) e
    ``learn_key`` guarda o texto normalizado do item, usado para classificar
    lancamentos parecidos da mesma fazenda.
    """

    __tablename__ = "cost_classifications"
    __table_args__ = (UniqueConstraint("farm_id", "entry_key", name="uq_cost_classification_entry"),)

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"), nullable=False, index=True)
    entry_key: Mapped[str] = mapped_column(String(40), nullable=False)
    learn_key: Mapped[str] = mapped_column(String(200), nullable=True, index=True)
    group_code: Mapped[str] = mapped_column(String(40), nullable=False)
    subgroup_code: Mapped[str] = mapped_column(String(40), nullable=False)
    user_id: Mapped[int] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
