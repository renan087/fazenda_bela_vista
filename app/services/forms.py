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
            boundary_geojson=form.get("boundary_geojson"),
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
        irrigation_type=form.get("irrigation_type") or "none",
        irrigation_line_count=form.get("irrigation_line_count"),
        irrigation_line_length_meters=form.get("irrigation_line_length_meters"),
        drip_spacing_meters=form.get("drip_spacing_meters"),
        drip_liters_per_hour=form.get("drip_liters_per_hour"),
        sprinkler_count=form.get("sprinkler_count"),
        sprinkler_liters_per_hour=form.get("sprinkler_liters_per_hour"),
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
            "irrigation_type": form.get("irrigation_type") or "none",
            "irrigation_line_count": form.get("irrigation_line_count"),
            "irrigation_line_length_meters": form.get("irrigation_line_length_meters"),
            "drip_spacing_meters": form.get("drip_spacing_meters"),
            "drip_liters_per_hour": form.get("drip_liters_per_hour"),
            "sprinkler_count": form.get("sprinkler_count"),
            "sprinkler_liters_per_hour": form.get("sprinkler_liters_per_hour"),
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
            "boundary_geojson": form.get("boundary_geojson"),
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
    if isinstance(parsed, dict) and parsed.get("type") == "FeatureCollection":
        features = parsed.get("features") or []
        if not features:
            return None
        first_feature = features[0]
        if isinstance(first_feature, dict):
            geometry = first_feature.get("geometry")
            return json.dumps(geometry) if geometry else None
        return None
    if isinstance(parsed, dict) and parsed.get("type") == "Feature":
        geometry = parsed.get("geometry")
        return json.dumps(geometry) if geometry else None
    return json.dumps(parsed)


def extract_geojson_file(raw_bytes: bytes | None) -> str | None:
    if not raw_bytes:
        return None
    try:
        decoded = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        decoded = raw_bytes.decode("latin-1")
    return normalize_geojson(decoded)


def estimate_geojson_centroid(geojson_text: str | None) -> tuple[float | None, float | None]:
    if not geojson_text:
        return None, None
    try:
        geometry = json.loads(geojson_text)
    except json.JSONDecodeError:
        return None, None

    coordinates = _flatten_coordinates(geometry.get("coordinates"))
    if not coordinates:
        return None, None

    longitudes = [point[0] for point in coordinates]
    latitudes = [point[1] for point in coordinates]
    return round(sum(latitudes) / len(latitudes), 6), round(sum(longitudes) / len(longitudes), 6)


def calculate_irrigation_volume(plot: Plot, duration_minutes: int) -> float | None:
    if not plot or not duration_minutes:
        return None
    hours = duration_minutes / 60

    if plot.irrigation_type == "gotejo":
        if not (
            plot.irrigation_line_count
            and plot.irrigation_line_length_meters
            and plot.drip_spacing_meters
            and plot.drip_liters_per_hour
        ):
            return None
        emitters_per_line = float(plot.irrigation_line_length_meters) / float(plot.drip_spacing_meters)
        total_emitters = float(plot.irrigation_line_count) * emitters_per_line
        return round(total_emitters * float(plot.drip_liters_per_hour) * hours, 2)

    if plot.irrigation_type == "aspersor":
        if not (plot.sprinkler_count and plot.sprinkler_liters_per_hour):
            return None
        return round(float(plot.sprinkler_count) * float(plot.sprinkler_liters_per_hour) * hours, 2)

    return None


def _flatten_coordinates(value) -> list[tuple[float, float]]:
    if not isinstance(value, list):
        return []
    if len(value) >= 2 and all(isinstance(item, (int, float)) for item in value[:2]):
        return [(float(value[0]), float(value[1]))]

    coordinates: list[tuple[float, float]] = []
    for item in value:
        coordinates.extend(_flatten_coordinates(item))
    return coordinates
