import json
from datetime import date

from app.models import CoffeeVariety, FertilizationRecord, HarvestRecord, IrrigationRecord, PestIncident, Plot
from app.repositories.farm import FarmRepository


def create_plot(repository: FarmRepository, form: dict) -> Plot:
    plot = Plot(
        name=form["name"],
        area_hectares=form["area_hectares"],
        location=form["location"],
        planting_year=form.get("planting_year"),
        plant_count=form["plant_count"],
        spacing_row_meters=form.get("spacing_row_meters"),
        spacing_plant_meters=form.get("spacing_plant_meters"),
        estimated_yield_sacks=form.get("estimated_yield_sacks"),
        centroid_lat=form.get("centroid_lat"),
        centroid_lng=form.get("centroid_lng"),
        boundary_geojson=form.get("boundary_geojson"),
        notes=form.get("notes"),
        variety_id=form.get("variety_id"),
    )
    return repository.create(plot)


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


def normalize_geojson(raw_text: str | None) -> str | None:
    if not raw_text:
        return None
    parsed = json.loads(raw_text)
    return json.dumps(parsed)
