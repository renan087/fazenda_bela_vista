from datetime import date

from pydantic import BaseModel, ConfigDict


class IrrigationBase(BaseModel):
    plot_id: int
    irrigation_date: date
    water_volume_mm: float
    method: str
    notes: str | None = None


class IrrigationCreate(IrrigationBase):
    pass


class IrrigationRead(IrrigationBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
