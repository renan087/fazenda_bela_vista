from pydantic import BaseModel, ConfigDict


class CoffeeVarietyBase(BaseModel):
    name: str
    species: str
    maturation_cycle: str
    notes: str | None = None


class CoffeeVarietyCreate(CoffeeVarietyBase):
    pass


class CoffeeVarietyRead(CoffeeVarietyBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
