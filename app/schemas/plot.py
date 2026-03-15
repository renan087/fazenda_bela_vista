from pydantic import BaseModel, ConfigDict


class PlotBase(BaseModel):
    name: str
    area_hectares: float
    location: str
    plant_count: int
    notes: str | None = None
    variety_id: int | None = None


class PlotCreate(PlotBase):
    pass


class PlotRead(PlotBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
