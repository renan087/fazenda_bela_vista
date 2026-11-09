"""Sugestoes tecnicas do perfil agronomico baseadas em dados ja cadastrados."""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from datetime import date
from typing import Iterable

import httpx

from app.core.config import get_settings
from app.models import Farm, Plot

logger = logging.getLogger(__name__)
settings = get_settings()
GOOGLE_ELEVATION_URL = "https://maps.googleapis.com/maps/api/elevation/json"
MAX_ELEVATION_POINTS = 12


def build_agronomic_profile_suggestions(farm: Farm, plots: Iterable[Plot]) -> dict[str, object]:
    """Monta somente sugestoes rastreaveis; lacunas permanecem sem valor."""
    plot_rows = list(plots)
    suggestions: dict[str, dict[str, str]] = {}

    if (location := (farm.location or "").strip()):
        suggestions["region"] = _suggestion(location, "confirmed", "Localização cadastrada para a fazenda.")

    _append_plot_suggestions(suggestions, plot_rows)
    coordinates = _farm_coordinate_samples(farm, plot_rows)
    _append_elevation_suggestions(suggestions, coordinates)

    return {
        "farm_id": farm.id,
        "suggestions": suggestions,
        "unavailable": _unavailable_fields(suggestions),
    }


def _append_plot_suggestions(suggestions: dict[str, dict[str, str]], plots: list[Plot]) -> None:
    weighted_varieties: dict[str, float] = defaultdict(float)
    weighted_species: dict[str, float] = defaultdict(float)
    irrigation_weights: dict[str, float] = defaultdict(float)
    spacing_weights: dict[str, float] = defaultdict(float)
    drip_weights: dict[str, float] = defaultdict(float)
    stage_weights: dict[str, float] = defaultdict(float)
    total_area = 0.0

    for plot in plots:
        area = max(float(plot.area_hectares or 0), 0.01)
        total_area += area
        if plot.variety and (name := (plot.variety.name or "").strip()):
            weighted_varieties[name] += area
            species = (plot.variety.species or "").strip()
            if species:
                weighted_species[species] += area
        irrigation = _irrigation_label(plot.irrigation_type)
        if irrigation:
            irrigation_weights[irrigation] += area
        if plot.spacing_row_meters and plot.spacing_plant_meters:
            spacing_weights[_format_spacing(plot.spacing_row_meters, plot.spacing_plant_meters)] += area
        if plot.drip_spacing_meters:
            drip_weights[_format_meters(plot.drip_spacing_meters)] += area
        if plot.planting_date:
            stage_weights[_crop_stage_from_date(plot.planting_date)] += area

    if not total_area:
        return
    dominant_variety, variety_share = _dominant(weighted_varieties, total_area)
    if dominant_variety and variety_share >= 0.5:
        suggestions["cultivar"] = _suggestion(
            dominant_variety,
            "confirmed",
            f"Cultivar presente em {variety_share:.0%} da área dos setores cadastrados.",
        )
    dominant_species, species_share = _dominant(weighted_species, total_area)
    culture = _culture_label(dominant_species) if dominant_species and species_share >= 0.5 else None
    if culture:
        suggestions["culture"] = _suggestion(
            culture,
            "confirmed",
            f"Espécie registrada em {species_share:.0%} da área dos setores cadastrados.",
        )

    productive_system, irrigation_system = _production_system(irrigation_weights, total_area)
    if productive_system:
        suggestions["production_system"] = _suggestion(productive_system, "confirmed", "Composição dos sistemas registrados nos setores.")
    if irrigation_system:
        suggestions["irrigation_system"] = _suggestion(irrigation_system, "confirmed", "Sistema predominante registrado nos setores.")

    dominant_spacing, spacing_share = _dominant(spacing_weights, total_area)
    if dominant_spacing and spacing_share >= 0.5:
        suggestions["plant_spacing"] = _suggestion(dominant_spacing, "confirmed", "Espaçamento predominante cadastrado nos setores.")
    dominant_drip, drip_share = _dominant(drip_weights, total_area)
    if dominant_drip and drip_share >= 0.5:
        suggestions["drip_spacing"] = _suggestion(dominant_drip, "confirmed", "Espaçamento de gotejo predominante cadastrado nos setores.")

    dominant_stage, stage_share = _dominant(stage_weights, total_area)
    if dominant_stage and stage_share >= 0.7:
        suggestions["crop_stage"] = _suggestion(
            dominant_stage,
            "estimate",
            f"Estimativa pela idade de plantio em {stage_share:.0%} da área; confirme em campo.",
        )


def _append_elevation_suggestions(
    suggestions: dict[str, dict[str, str]], coordinates: list[tuple[float, float]]
) -> None:
    if not settings.google_maps_api_key or not coordinates:
        return
    samples = _fetch_elevation_samples(coordinates)
    if not samples:
        return
    elevations = [sample[2] for sample in samples]
    suggestions["altitude_meters"] = _suggestion(
        str(round(sum(elevations) / len(elevations))),
        "estimate",
        "Média estimada pela Google Elevation API a partir das coordenadas da fazenda.",
    )
    terrain = _terrain_estimate(samples)
    if terrain:
        suggestions["terrain_slope"] = _suggestion(
            terrain["slope_label"],
            "estimate",
            f"Estimativa topográfica: aproximadamente {terrain['slope_percent']:.1f}% de declive; confirme em levantamento de campo.",
        )
        if terrain.get("sun_exposure"):
            suggestions["sun_exposure"] = _suggestion(
                str(terrain["sun_exposure"]),
                "estimate",
                "Orientação estimada pelo sentido do declive; confirme em campo.",
            )


def _farm_coordinate_samples(farm: Farm, plots: list[Plot]) -> list[tuple[float, float]]:
    points = _geojson_points(farm.boundary_geojson)
    if not points:
        points = [(float(plot.centroid_lat), float(plot.centroid_lng)) for plot in plots if plot.centroid_lat is not None and plot.centroid_lng is not None]
    if not points:
        return []
    unique: list[tuple[float, float]] = []
    for point in points:
        if point not in unique:
            unique.append(point)
    if len(unique) <= MAX_ELEVATION_POINTS:
        return unique
    step = (len(unique) - 1) / (MAX_ELEVATION_POINTS - 1)
    return [unique[round(index * step)] for index in range(MAX_ELEVATION_POINTS)]


def _geojson_points(raw_geometry: str | None) -> list[tuple[float, float]]:
    if not raw_geometry:
        return []
    try:
        geometry = json.loads(raw_geometry)
    except (TypeError, json.JSONDecodeError):
        return []

    def visit(value: object) -> list[tuple[float, float]]:
        if not isinstance(value, list):
            return []
        if len(value) >= 2 and all(isinstance(item, (int, float)) for item in value[:2]):
            longitude, latitude = float(value[0]), float(value[1])
            if -180 <= longitude <= 180 and -90 <= latitude <= 90:
                return [(latitude, longitude)]
            return []
        points: list[tuple[float, float]] = []
        for item in value:
            points.extend(visit(item))
        return points

    return visit(geometry.get("coordinates") if isinstance(geometry, dict) else None)


def _fetch_elevation_samples(coordinates: list[tuple[float, float]]) -> list[tuple[float, float, float]]:
    locations = "|".join(f"{latitude:.6f},{longitude:.6f}" for latitude, longitude in coordinates)
    try:
        with httpx.Client(timeout=httpx.Timeout(6.0, connect=3.0)) as client:
            response = client.get(
                GOOGLE_ELEVATION_URL,
                params={"locations": locations, "key": settings.google_maps_api_key},
            )
        payload = response.json() if response.status_code == 200 else {}
    except (httpx.RequestError, ValueError) as exc:
        logger.info("Nao foi possivel consultar elevacao geografica: %s", exc)
        return []
    if payload.get("status") != "OK" or not isinstance(payload.get("results"), list):
        logger.info("Google Elevation API indisponivel para sugestoes: %s", payload.get("status"))
        return []
    samples: list[tuple[float, float, float]] = []
    for result in payload["results"]:
        location = result.get("location") if isinstance(result, dict) else None
        if not isinstance(location, dict):
            continue
        try:
            samples.append((float(location["lat"]), float(location["lng"]), float(result["elevation"])))
        except (KeyError, TypeError, ValueError):
            continue
    return samples


def _terrain_estimate(samples: list[tuple[float, float, float]]) -> dict[str, object] | None:
    if len(samples) < 3:
        return None
    lowest = min(samples, key=lambda item: item[2])
    highest = max(samples, key=lambda item: item[2])
    horizontal_distance = _haversine_meters(highest[0], highest[1], lowest[0], lowest[1])
    elevation_difference = highest[2] - lowest[2]
    if horizontal_distance < 40 or elevation_difference < 2:
        return {"slope_percent": 0.0, "slope_label": "Plano", "sun_exposure": None}
    slope_percent = elevation_difference * 100 / horizontal_distance
    if slope_percent < 3:
        slope_label = "Plano"
    elif slope_percent < 8:
        slope_label = "Suave ondulado"
    elif slope_percent < 20:
        slope_label = "Ondulado"
    elif slope_percent < 45:
        slope_label = "Forte ondulado"
    else:
        slope_label = "Montanhoso"
    direction = _cardinal_direction(_bearing_degrees(highest[0], highest[1], lowest[0], lowest[1]))
    return {"slope_percent": slope_percent, "slope_label": f"{slope_label} ({slope_percent:.1f}%)", "sun_exposure": f"Face {direction.title()}"}


def _haversine_meters(lat_a: float, lng_a: float, lat_b: float, lng_b: float) -> float:
    radius = 6_371_000
    lat_delta = math.radians(lat_b - lat_a)
    lng_delta = math.radians(lng_b - lng_a)
    base = math.sin(lat_delta / 2) ** 2 + math.cos(math.radians(lat_a)) * math.cos(math.radians(lat_b)) * math.sin(lng_delta / 2) ** 2
    return radius * 2 * math.atan2(math.sqrt(base), math.sqrt(1 - base))


def _bearing_degrees(lat_a: float, lng_a: float, lat_b: float, lng_b: float) -> float:
    lng_delta = math.radians(lng_b - lng_a)
    lat_a_radians, lat_b_radians = math.radians(lat_a), math.radians(lat_b)
    x = math.sin(lng_delta) * math.cos(lat_b_radians)
    y = math.cos(lat_a_radians) * math.sin(lat_b_radians) - math.sin(lat_a_radians) * math.cos(lat_b_radians) * math.cos(lng_delta)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _cardinal_direction(bearing: float) -> str:
    directions = ("norte", "nordeste", "leste", "sudeste", "sul", "sudoeste", "oeste", "noroeste")
    return directions[round(bearing / 45) % len(directions)]


def _dominant(values: dict[str, float], total_area: float) -> tuple[str | None, float]:
    if not values or total_area <= 0:
        return None, 0.0
    value, area = max(values.items(), key=lambda item: (item[1], item[0]))
    return value, area / total_area


def _irrigation_label(value: str | None) -> str | None:
    normalized = (value or "").strip().lower()
    return {
        "drip": "Gotejamento",
        "gotejamento": "Gotejamento",
        "micro": "Microaspersão",
        "microaspersao": "Microaspersão",
        "sprinkler": "Aspersão",
        "pivot": "Pivô central",
        "none": "Sequeiro",
        "sequeiro": "Sequeiro",
    }.get(normalized)


def _production_system(irrigation_weights: dict[str, float], total_area: float) -> tuple[str | None, str | None]:
    if not irrigation_weights:
        return None, None
    irrigated_area = sum(area for name, area in irrigation_weights.items() if name != "Sequeiro")
    dominant, dominant_share = _dominant(irrigation_weights, total_area)
    if irrigated_area <= 0:
        return "Sequeiro", "Sequeiro"
    if irrigated_area >= total_area * 0.95:
        system = f"Irrigado por {dominant}" if dominant and dominant != "Sequeiro" else "Irrigado"
        return system, dominant
    return "Misto: irrigado e sequeiro", dominant if dominant_share >= 0.5 else "Misto"


def _culture_label(species: str) -> str | None:
    normalized = species.lower()
    if "arab" in normalized:
        return "Café arábica"
    if "conilon" in normalized or "robusta" in normalized:
        return "Café conilon/robusta"
    return f"Café {species}" if species else None


def _format_spacing(row: object, plant: object) -> str:
    return f"{_format_meters(row)} × {_format_meters(plant)}"


def _format_meters(value: object) -> str:
    return f"{float(value):.2f}".replace(".", ",") + " m"


def _crop_stage_from_date(planting_date: date) -> str:
    age_days = max((date.today() - planting_date).days, 0)
    if age_days < 365:
        return "Implantação"
    if age_days < 3 * 365:
        return "Formação"
    return "Produção"


def _suggestion(value: str, source: str, detail: str) -> dict[str, str]:
    return {"value": value, "source": source, "detail": detail}


def _unavailable_fields(suggestions: dict[str, dict[str, str]]) -> list[str]:
    all_fields = ("climate", "soil_type", "fertilizers_used", "common_pests", "target_productivity_sacks_per_ha", "pruning_history", "current_limitations")
    return [field for field in all_fields if field not in suggestions]
