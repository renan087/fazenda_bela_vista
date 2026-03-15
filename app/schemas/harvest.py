from datetime import date

from pydantic import BaseModel, ConfigDict


class HarvestBase(BaseModel):
    plot_id: int
    harvest_date: date
    sacks_produced: float
    notes: str | None = None


class HarvestCreate(HarvestBase):
    pass


class HarvestRead(HarvestBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
