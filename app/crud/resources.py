from app.crud.base import CRUDBase
from app.models import (
    CoffeeVariety,
    FertilizationRecord,
    HarvestRecord,
    IrrigationRecord,
    PesticideApplication,
    Plot,
)

plots = CRUDBase(Plot)
varieties = CRUDBase(CoffeeVariety)
irrigations = CRUDBase(IrrigationRecord)
fertilizations = CRUDBase(FertilizationRecord)
pesticides = CRUDBase(PesticideApplication)
harvests = CRUDBase(HarvestRecord)
