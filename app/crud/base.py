from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session


class CRUDBase:
    def __init__(self, model):
        self.model = model

    def get(self, db: Session, obj_id: int):
        return db.query(self.model).filter(self.model.id == obj_id).first()

    def get_multi(self, db: Session):
        return db.query(self.model).order_by(self.model.id.desc()).all()

    def create(self, db: Session, data: dict[str, Any]):
        obj = self.model(**data)
        db.add(obj)
        try:
            db.commit()
            db.refresh(obj)
            return obj
        except IntegrityError as exc:
            db.rollback()
            raise ValueError("Nao foi possivel salvar o registro.") from exc
