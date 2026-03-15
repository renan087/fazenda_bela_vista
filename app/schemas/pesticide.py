from datetime import date

from pydantic import BaseModel, ConfigDict


class PesticideBase(BaseModel):
    plot_id: int
    application_date: date
    product: str
    target_pest: str
    cost: float
    notes: str | None = None


class PesticideCreate(PesticideBase):
    pass


class PesticideRead(PesticideBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
