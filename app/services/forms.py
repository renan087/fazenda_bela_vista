import json
from datetime import date

from app.models import CoffeeVariety, Farm, FertilizationRecord, HarvestRecord, IrrigationRecord, PestIncident, Plot
from app.repositories.farm import FarmRepository


def create_farm(repository: FarmRepository, form: dict) -> Farm:
    return repository.create(
        Farm(
            name=form["name"],
            location=form["location"],
            total_area=form["total_area"],
            notes=form.get("notes"),
        )
    )


def create_plot(repository: FarmRepository, form: dict) -> Plot:
    plot = Plot(
        name=form["name"],
        area_hectares=form["area_hectares"],
        location=form.get("location"),
        planting_date=date.fromisoformat(form["planting_date"]) if form.get("planting_date") else None,
        plant_count=form["plant_count"],
        spacing_row_meters=form.get("spacing_row_meters"),
        spacing_plant_meters=form.get("spacing_plant_meters"),
        estimated_yield_sacks=form.get("estimated_yield_sacks"),
        centroid_lat=form.get("centroid_lat"),
        centroid_lng=form.get("centroid_lng"),
        boundary_geojson=form.get("boundary_geojson"),
        notes=form.get("notes"),
        farm_id=form.get("farm_id"),
        variety_id=form.get("variety_id"),
    )
    return repository.create(plot)


def update_plot(repository: FarmRepository, plot: Plot, form: dict) -> Plot:
    return repository.update(
        plot,
        {
            "name": form["name"],
            "area_hectares": form["area_hectares"],
            "location": form.get("location"),
            "planting_date": date.fromisoformat(form["planting_date"]) if form.get("planting_date") else None,
            "plant_count": form["plant_count"],
            "spacing_row_meters": form.get("spacing_row_meters"),
            "spacing_plant_meters": form.get("spacing_plant_meters"),
            "estimated_yield_sacks": form.get("estimated_yield_sacks"),
            "centroid_lat": form.get("centroid_lat"),
            "centroid_lng": form.get("centroid_lng"),
            "boundary_geojson": form.get("boundary_geojson"),
            "notes": form.get("notes"),
            "farm_id": form.get("farm_id"),
            "variety_id": form.get("variety_id"),
        },
    )


def update_farm(repository: FarmRepository, farm: Farm, form: dict) -> Farm:
    return repository.update(
        farm,
        {
            "name": form["name"],
            "location": form["location"],
            "total_area": form["total_area"],
            "notes": form.get("notes"),
        },
    )


def create_variety(repository: FarmRepository, form: dict) -> CoffeeVariety:
    return repository.create(
        CoffeeVariety(
            name=form["name"],
            species=form["species"],
            maturation_cycle=form["maturation_cycle"],
            flavor_profile=form.get("flavor_profile"),
            notes=form.get("notes"),
        )
    )


def update_variety(repository: FarmRepository, variety: CoffeeVariety, form: dict) -> CoffeeVariety:
    return repository.update(
        variety,
        {
            "name": form["name"],
            "species": form["species"],
            "maturation_cycle": form["maturation_cycle"],
            "flavor_profile": form.get("flavor_profile"),
            "notes": form.get("notes"),
        },
    )


def create_irrigation(repository: FarmRepository, form: dict) -> IrrigationRecord:
    return repository.create(
        IrrigationRecord(
            plot_id=form["plot_id"],
            irrigation_date=date.fromisoformat(form["irrigation_date"]),
            volume_liters=form["volume_liters"],
            duration_minutes=form["duration_minutes"],
            notes=form.get("notes"),
        )
    )


def update_irrigation(repository: FarmRepository, irrigation: IrrigationRecord, form: dict) -> IrrigationRecord:
    return repository.update(
        irrigation,
        {
            "plot_id": form["plot_id"],
            "irrigation_date": date.fromisoformat(form["irrigation_date"]),
            "volume_liters": form["volume_liters"],
            "duration_minutes": form["duration_minutes"],
            "notes": form.get("notes"),
        },
    )


def create_fertilization(repository: FarmRepository, form: dict) -> FertilizationRecord:
    return repository.create(
        FertilizationRecord(
            plot_id=form["plot_id"],
            application_date=date.fromisoformat(form["application_date"]),
            product=form["product"],
            dose=form["dose"],
            cost=form["cost"],
            notes=form.get("notes"),
        )
    )


def update_fertilization(repository: FarmRepository, fertilization: FertilizationRecord, form: dict) -> FertilizationRecord:
    return repository.update(
        fertilization,
        {
            "plot_id": form["plot_id"],
            "application_date": date.fromisoformat(form["application_date"]),
            "product": form["product"],
            "dose": form["dose"],
            "cost": form["cost"],
            "notes": form.get("notes"),
        },
    )


def create_harvest(repository: FarmRepository, form: dict, area_hectares: float) -> HarvestRecord:
    sacks = float(form["sacks_produced"])
    productivity = sacks / area_hectares if area_hectares else 0
    return repository.create(
        HarvestRecord(
            plot_id=form["plot_id"],
            harvest_date=date.fromisoformat(form["harvest_date"]),
            sacks_produced=sacks,
            productivity_per_hectare=round(productivity, 2),
            notes=form.get("notes"),
        )
    )


def update_harvest(
    repository: FarmRepository,
    harvest: HarvestRecord,
    form: dict,
    area_hectares: float,
) -> HarvestRecord:
    sacks = float(form["sacks_produced"])
    productivity = sacks / area_hectares if area_hectares else 0
    return repository.update(
        harvest,
        {
            "plot_id": form["plot_id"],
            "harvest_date": date.fromisoformat(form["harvest_date"]),
            "sacks_produced": sacks,
            "productivity_per_hectare": round(productivity, 2),
            "notes": form.get("notes"),
        },
    )


def create_pest_incident(repository: FarmRepository, form: dict) -> PestIncident:
    return repository.create(
        PestIncident(
            plot_id=form["plot_id"],
            occurrence_date=date.fromisoformat(form["occurrence_date"]),
            category=form["category"],
            name=form["name"],
            severity=form["severity"],
            treatment=form.get("treatment"),
            notes=form.get("notes"),
        )
    )


def update_pest_incident(repository: FarmRepository, incident: PestIncident, form: dict) -> PestIncident:
    return repository.update(
        incident,
        {
            "plot_id": form["plot_id"],
            "occurrence_date": date.fromisoformat(form["occurrence_date"]),
            "category": form["category"],
            "name": form["name"],
            "severity": form["severity"],
            "treatment": form.get("treatment"),
            "notes": form.get("notes"),
        },
    )


def normalize_geojson(raw_text: str | None) -> str | None:
    if not raw_text:
        return None
    normalized = raw_text.strip()
    if not normalized:
        return None
    try:
        parsed = json.loads(normalized)
    except json.JSONDecodeError:
        return None
    return json.dumps(parsed)
