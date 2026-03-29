import json
import math
from datetime import date

from app.models import CoffeeVariety, Farm, FertilizationItem, FertilizationRecord, HarvestRecord, IrrigationRecord, PestIncident, Plot, RainfallRecord
from app.models import AgronomicProfile, SoilAnalysis
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


def create_rainfall(repository: FarmRepository, form: dict) -> RainfallRecord:
    return repository.create(
        RainfallRecord(
            farm_id=form["farm_id"],
            rainfall_date=date.fromisoformat(form["rainfall_date"]),
            millimeters=form["millimeters"],
            source=form.get("source"),
            notes=form.get("notes"),
        )
    )


def update_rainfall(repository: FarmRepository, rainfall: RainfallRecord, form: dict) -> RainfallRecord:
    return repository.update(
        rainfall,
        {
            "farm_id": form["farm_id"],
            "rainfall_date": date.fromisoformat(form["rainfall_date"]),
            "millimeters": form["millimeters"],
            "source": form.get("source"),
            "notes": form.get("notes"),
        },
    )


def create_fertilization(repository: FarmRepository, form: dict) -> FertilizationRecord:
    items = _normalize_fertilization_items(form.get("items"), form.get("area_hectares"))
    product, dose = _fertilization_summary(items)
    record = FertilizationRecord(
        plot_id=form["plot_id"],
        application_date=date.fromisoformat(form["application_date"]),
        product=product,
        dose=dose,
        cost=form["cost"],
        notes=form.get("notes"),
    )
    repository.db.add(record)
    repository.db.flush()
    for item in items:
        repository.db.add(
            FertilizationItem(
                fertilization_record_id=record.id,
                name=item["name"],
                unit=item["unit"],
                quantity_per_hectare=item["quantity_per_hectare"],
                total_quantity=item["total_quantity"],
            )
        )
    repository.db.commit()
    repository.db.refresh(record)
    return record


def update_fertilization(repository: FarmRepository, fertilization: FertilizationRecord, form: dict) -> FertilizationRecord:
    items = _normalize_fertilization_items(form.get("items"), form.get("area_hectares"))
    product, dose = _fertilization_summary(items)
    fertilization.plot_id = form["plot_id"]
    fertilization.application_date = date.fromisoformat(form["application_date"])
    fertilization.product = product
    fertilization.dose = dose
    fertilization.cost = form["cost"]
    fertilization.notes = form.get("notes")
    fertilization.items.clear()
    repository.db.flush()
    for item in items:
        fertilization.items.append(
            FertilizationItem(
                name=item["name"],
                unit=item["unit"],
                quantity_per_hectare=item["quantity_per_hectare"],
                total_quantity=item["total_quantity"],
            )
        )
    repository.db.add(fertilization)
    repository.db.commit()
    repository.db.refresh(fertilization)
    return fertilization


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


def create_agronomic_profile(repository: FarmRepository, form: dict) -> AgronomicProfile:
    return repository.create(
        AgronomicProfile(
            farm_id=form["farm_id"],
            culture=form["culture"],
            region=form["region"],
            climate=form.get("climate"),
            soil_type=form.get("soil_type"),
            irrigation_system=form.get("irrigation_system"),
            plant_spacing=form.get("plant_spacing"),
            drip_spacing=form.get("drip_spacing"),
            fertilizers_used=form.get("fertilizers_used"),
            crop_stage=form.get("crop_stage"),
            common_pests=form.get("common_pests"),
        )
    )


def update_agronomic_profile(repository: FarmRepository, profile: AgronomicProfile, form: dict) -> AgronomicProfile:
    return repository.update(
        profile,
        {
            "farm_id": form["farm_id"],
            "culture": form["culture"],
            "region": form["region"],
            "climate": form.get("climate"),
            "soil_type": form.get("soil_type"),
            "irrigation_system": form.get("irrigation_system"),
            "plant_spacing": form.get("plant_spacing"),
            "drip_spacing": form.get("drip_spacing"),
            "fertilizers_used": form.get("fertilizers_used"),
            "crop_stage": form.get("crop_stage"),
            "common_pests": form.get("common_pests"),
        },
    )


def create_soil_analysis(repository: FarmRepository, form: dict) -> SoilAnalysis:
    return repository.create(
        SoilAnalysis(
            farm_id=form["farm_id"],
            plot_id=form["plot_id"],
            analysis_date=date.fromisoformat(form["analysis_date"]),
            laboratory=form["laboratory"],
            ph=form.get("ph"),
            organic_matter=form.get("organic_matter"),
            phosphorus=form.get("phosphorus"),
            potassium=form.get("potassium"),
            calcium=form.get("calcium"),
            magnesium=form.get("magnesium"),
            aluminum=form.get("aluminum"),
            h_al=form.get("h_al"),
            ctc=form.get("ctc"),
            base_saturation=form.get("base_saturation"),
            observations=form.get("observations"),
            pdf_filename=form.get("pdf_filename"),
            pdf_content_type=form.get("pdf_content_type"),
            pdf_data=form.get("pdf_data"),
            liming_need_t_ha=form.get("liming_need_t_ha"),
            npk_recommendation=form.get("npk_recommendation"),
            micronutrient_recommendation=form.get("micronutrient_recommendation"),
            ai_recommendation=form.get("ai_recommendation"),
            ai_status=form.get("ai_status"),
            ai_model=form.get("ai_model"),
            ai_error=form.get("ai_error"),
            ai_generated_at=form.get("ai_generated_at"),
        )
    )


def update_soil_analysis(repository: FarmRepository, analysis: SoilAnalysis, form: dict) -> SoilAnalysis:
    return repository.update(
        analysis,
        {
            "farm_id": form["farm_id"],
            "plot_id": form["plot_id"],
            "analysis_date": date.fromisoformat(form["analysis_date"]),
            "laboratory": form["laboratory"],
            "ph": form.get("ph"),
            "organic_matter": form.get("organic_matter"),
            "phosphorus": form.get("phosphorus"),
            "potassium": form.get("potassium"),
            "calcium": form.get("calcium"),
            "magnesium": form.get("magnesium"),
            "aluminum": form.get("aluminum"),
            "h_al": form.get("h_al"),
            "ctc": form.get("ctc"),
            "base_saturation": form.get("base_saturation"),
            "observations": form.get("observations"),
            "pdf_filename": form.get("pdf_filename"),
            "pdf_content_type": form.get("pdf_content_type"),
            "pdf_data": form.get("pdf_data"),
            "liming_need_t_ha": form.get("liming_need_t_ha"),
            "npk_recommendation": form.get("npk_recommendation"),
            "micronutrient_recommendation": form.get("micronutrient_recommendation"),
            "ai_recommendation": form.get("ai_recommendation"),
            "ai_status": form.get("ai_status"),
            "ai_model": form.get("ai_model"),
            "ai_error": form.get("ai_error"),
            "ai_generated_at": form.get("ai_generated_at"),
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


def calculate_geojson_area_hectares(geojson_text: str | None) -> float | None:
    if not geojson_text:
        return None
    try:
        geometry = json.loads(geojson_text)
    except json.JSONDecodeError:
        return None

    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates") or []
    rings: list[list[tuple[float, float]]] = []
    if geometry_type == "Polygon":
        if coordinates:
            rings.append(_flatten_coordinates(coordinates[0]))
    elif geometry_type == "MultiPolygon":
        for polygon in coordinates:
            if polygon:
                rings.append(_flatten_coordinates(polygon[0]))

    areas = [_polygon_area_hectares(ring) for ring in rings if len(ring) >= 3]
    valid_areas = [area for area in areas if area > 0]
    return round(sum(valid_areas), 4) if valid_areas else None


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


def calculate_soil_recommendations(form: dict) -> dict:
    ph = _float(form.get("ph"))
    phosphorus = _float(form.get("phosphorus"))
    potassium = _float(form.get("potassium"))
    organic_matter = _float(form.get("organic_matter"))
    ctc = _float(form.get("ctc"))
    base_saturation = _float(form.get("base_saturation"))

    liming_need = None
    if ctc is not None and base_saturation is not None:
        liming_need = max(0.0, ((60 - base_saturation) / 100) * ctc * 2)
    elif ph is not None and ph < 5.5:
        liming_need = round((5.5 - ph) * 1.6, 2)

    npk_parts = []
    if phosphorus is not None:
        if phosphorus < 12:
            npk_parts.append("Elevar fosforo com formulacao rica em P, priorizando MAP ou fosfatado de alta solubilidade.")
        elif phosphorus < 20:
            npk_parts.append("Manutencao moderada de fosforo, ajustando pela meta produtiva do setor.")
        else:
            npk_parts.append("Fosforo em faixa satisfatoria para manutencao.")
    if potassium is not None:
        if potassium < 120:
            npk_parts.append("Reforcar potassio com NPK de cobertura ou fertirrigacao potassica.")
        elif potassium < 180:
            npk_parts.append("Potassio em faixa intermediaria, manter reposicao parcelada.")
        else:
            npk_parts.append("Potassio adequado para manutencao.")
    if organic_matter is not None:
        if organic_matter < 2.5:
            npk_parts.append("Associar materia organica e nitrogenio de arranque para estimular raiz e brotacao.")
        else:
            npk_parts.append("Materia organica favorece resposta a adubacao nitrogenada parcelada.")

    micronutrients = []
    if ph is not None and ph < 5.3:
        micronutrients.append("Monitorar Boro e Zinco apos correcao de acidez.")
    if ph is not None and ph > 6.4:
        micronutrients.append("Atencao a possiveis limitacoes de Zinco, Boro e Manganes em pH mais alto.")
    if organic_matter is not None and organic_matter < 2.0:
        micronutrients.append("Considerar programa com micronutrientes foliares e fontes organicas.")
    if not micronutrients:
        micronutrients.append("Micronutrientes em manutencao, com foco em Boro e Zinco conforme diagnostico foliar.")

    return {
        "liming_need_t_ha": round(liming_need, 2) if liming_need is not None else None,
        "npk_recommendation": " ".join(npk_parts) if npk_parts else "Definir NPK conforme produtividade alvo e historico do setor.",
        "micronutrient_recommendation": " ".join(micronutrients),
    }


def _flatten_coordinates(value) -> list[tuple[float, float]]:
    if not isinstance(value, list):
        return []
    if len(value) >= 2 and all(isinstance(item, (int, float)) for item in value[:2]):
        return [(float(value[0]), float(value[1]))]

    coordinates: list[tuple[float, float]] = []
    for item in value:
        coordinates.extend(_flatten_coordinates(item))
    return coordinates


def _float(value):
    return float(value) if value is not None else None


def _normalize_fertilization_items(items: list[dict] | None, area_hectares: float | None) -> list[dict]:
    normalized: list[dict] = []
    area = float(area_hectares or 0)
    for item in items or []:
        name = (item.get("name") or "").strip()
        unit = (item.get("unit") or "").strip()
        quantity = item.get("quantity_per_hectare")
        if not name or not unit or quantity in (None, ""):
            continue
        quantity_value = round(float(quantity), 2)
        normalized.append(
            {
                "name": name,
                "unit": unit,
                "quantity_per_hectare": quantity_value,
                "total_quantity": round(quantity_value * area, 2),
            }
        )
    return normalized


def _fertilization_summary(items: list[dict]) -> tuple[str, str]:
    if not items:
        return "Aplicacao sem itens", "-"
    first = items[0]
    if len(items) == 1:
        return first["name"], f'{first["quantity_per_hectare"]:.2f} {first["unit"]}/ha'
    return f'{len(items)} insumos aplicados', f'{first["quantity_per_hectare"]:.2f} {first["unit"]}/ha + {len(items) - 1} item(ns)'


def _polygon_area_hectares(points: list[tuple[float, float]]) -> float:
    if len(points) < 3:
        return 0.0
    if points[0] == points[-1]:
        points = points[:-1]
    avg_lat = sum(lat for _, lat in points) / len(points)
    meters_per_degree_lat = 111_320
    meters_per_degree_lng = 111_320 * math.cos(math.radians(avg_lat))
    projected = [(lng * meters_per_degree_lng, lat * meters_per_degree_lat) for lng, lat in points]
    area = 0.0
    for index, (x1, y1) in enumerate(projected):
        x2, y2 = projected[(index + 1) % len(projected)]
        area += (x1 * y2) - (x2 * y1)
    return abs(area) / 2 / 10_000
