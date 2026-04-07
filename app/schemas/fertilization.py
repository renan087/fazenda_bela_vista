from datetime import date

from pydantic import BaseModel, ConfigDict


class FertilizationBase(BaseModel):
    plot_id: int
    application_date: date
    product: str
    dose: str
    cost: float
    application_method: str | None = "fertirrigacao"
    notes: str | None = None


class FertilizationCreate(FertilizationBase):
    pass


class FertilizationRead(FertilizationBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
