"""Exportacao do perimetro salvo de uma fazenda em KML."""

from __future__ import annotations

import json
from typing import Any
from xml.sax.saxutils import escape


def farm_boundary_to_kml(farm_name: str, boundary_geojson: str) -> bytes:
    """Converte Polygon/MultiPolygon GeoJSON do cadastro em um KML baixavel."""
    try:
        geometry = _extract_geometry(json.loads(boundary_geojson))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("O perimetro salvo da fazenda nao esta em um formato compativel com KML.") from exc

    polygons = _polygons_from_geometry(geometry)
    if not polygons:
        raise ValueError("O perimetro salvo da fazenda nao possui coordenadas validas para exportacao.")

    escaped_name = escape(farm_name.strip() or "Fazenda")
    placemarks = "".join(
        _polygon_placemark(escaped_name if len(polygons) == 1 else f"{escaped_name} {index}", polygon)
        for index, polygon in enumerate(polygons, start=1)
    )
    document = f'''<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>{escaped_name}</name>
    <Style id="farm-boundary"><LineStyle><color>ff38761d</color><width>3</width></LineStyle><PolyStyle><color>5538761d</color></PolyStyle></Style>
    {placemarks}
  </Document>
</kml>
'''
    return document.encode("utf-8")


def _extract_geometry(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("Invalid geometry")
    if value.get("type") == "Feature":
        value = value.get("geometry")
    if not isinstance(value, dict) or value.get("type") not in {"Polygon", "MultiPolygon"}:
        raise ValueError("Unsupported geometry")
    return value


def _polygons_from_geometry(geometry: dict[str, Any]) -> list[list[list[list[object]]]]:
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list):
        return []
    if geometry.get("type") == "Polygon":
        return [coordinates]
    return [polygon for polygon in coordinates if isinstance(polygon, list)]


def _polygon_placemark(name: str, rings: list[list[list[object]]]) -> str:
    valid_rings = [_coordinates(ring) for ring in rings if isinstance(ring, list)]
    valid_rings = [ring for ring in valid_rings if ring]
    if not valid_rings:
        return ""
    outer = valid_rings[0]
    inner = "".join(
        f"<innerBoundaryIs><LinearRing><coordinates>{ring}</coordinates></LinearRing></innerBoundaryIs>"
        for ring in valid_rings[1:]
    )
    return (
        f"<Placemark><name>{name}</name><styleUrl>#farm-boundary</styleUrl><Polygon>"
        f"<outerBoundaryIs><LinearRing><coordinates>{outer}</coordinates></LinearRing></outerBoundaryIs>"
        f"{inner}</Polygon></Placemark>"
    )


def _coordinates(ring: list[object]) -> str:
    formatted: list[str] = []
    for point in ring:
        if not isinstance(point, list | tuple) or len(point) < 2:
            continue
        try:
            longitude = float(point[0])
            latitude = float(point[1])
            altitude = float(point[2]) if len(point) > 2 else None
        except (TypeError, ValueError):
            continue
        coordinate = f"{longitude:.8f},{latitude:.8f}"
        if altitude is not None:
            coordinate += f",{altitude:.2f}"
        formatted.append(coordinate)
    return " ".join(formatted) if len(formatted) >= 3 else ""
