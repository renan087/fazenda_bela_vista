from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import get_password_hash
from app.db.base import Base
from app.db.session import engine
from app.models import (
    CoffeeVariety,
    FertilizationRecord,
    HarvestRecord,
    IrrigationRecord,
    Plot,
    PesticideApplication,
    User,
)


def create_tables() -> None:
    Base.metadata.create_all(bind=engine)


def seed_admin(db: Session) -> None:
    settings = get_settings()
    existing = db.query(User).filter(User.email == settings.admin_email).first()
    if existing:
        return

    admin = User(
        name="Administrador",
        email=settings.admin_email,
        hashed_password=get_password_hash(settings.admin_password),
        is_active=True,
    )
    db.add(admin)
    db.commit()
