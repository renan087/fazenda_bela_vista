"""Consulta de imoveis pelo numero do CAR no geosservico publico do SICAR.

O servico e publico, nao autenticado e pode ficar indisponivel. Por isso este
modulo nunca e usado como fonte obrigatoria: o usuario sempre confirma e pode
ajustar o perimetro retornado antes de salvar a fazenda.
"""

from __future__ import annotations

import re
from typing import Any

import httpx


SICAR_WFS_URL = "https://geoserver.car.gov.br/geoserver/sicar/wfs"
CAR_NUMBER_PATTERN = re.compile(r"^(?P<uf>[A-Z]{2})-[A-Z0-9][A-Z0-9.\-]{5,118}$")
SUPPORTED_UFS = {
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT",
    "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
}


class CarPublicLookupError(ValueError):
    """Erro seguro para apresentar ao usuario durante a consulta publica."""


def normalize_car_number(value: str | None) -> str:
    normalized = re.sub(r"\s+", "", str(value or "").upper())
    match = CAR_NUMBER_PATTERN.fullmatch(normalized)
    if not match:
        raise CarPublicLookupError("Informe um numero de CAR valido, no formato UF-codigo do imovel.")
    if match.group("uf") not in SUPPORTED_UFS:
        raise CarPublicLookupError("A UF informada nao esta disponivel no geosservico publico do CAR.")
    return normalized


def lookup_public_car(car_number: str | None) -> dict[str, Any]:
    """Retorna o imovel publico e seu poligono GeoJSON, quando disponivel."""
    normalized = normalize_car_number(car_number)
    uf = normalized[:2].lower()
    escaped_number = normalized.replace("'", "''")
    params = {
        "service": "WFS",
        "version": "1.0.0",
        "request": "GetFeature",
        "typeName": f"sicar:sicar_imoveis_{uf}",
        "CQL_FILTER": f"cod_imovel='{escaped_number}'",
        "outputFormat": "application/json",
        "maxFeatures": "2",
    }
    try:
        with httpx.Client(timeout=httpx.Timeout(12.0, connect=4.0), follow_redirects=True) as client:
            response = client.get(SICAR_WFS_URL, params=params)
            response.raise_for_status()
            payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise CarPublicLookupError("Nao foi possivel consultar o geosservico publico do CAR agora. Tente novamente mais tarde.") from exc

    features = payload.get("features") if isinstance(payload, dict) else None
    if not isinstance(features, list) or not features:
        raise CarPublicLookupError("Nenhum imovel publico foi encontrado para este numero do CAR.")
    feature = features[0] if isinstance(features[0], dict) else None
    geometry = _normalize_geometry(feature.get("geometry") if feature else None)
    properties = feature.get("properties") if feature and isinstance(feature.get("properties"), dict) else {}
    if geometry is None:
        raise CarPublicLookupError("O CAR foi localizado, mas nao possui um perimetro geografico publico utilizavel.")

    return {
        "car_number": str(properties.get("cod_imovel") or normalized),
        "geometry": geometry,
        "name": None,
        "location": _format_location(properties.get("municipio"), properties.get("uf")),
        "total_area": _number_or_none(properties.get("area")),
        "status": _clean_text(properties.get("status_imovel")),
        "condition": _clean_text(properties.get("condicao")),
        "source": "Consulta Publica SICAR",
    }


def _normalize_geometry(value: object) -> dict[str, Any] | None:
    """Aceita Polygon e MultiPolygon com uma unica parte, que o editor suporta."""
    if not isinstance(value, dict):
        return None
    coordinates = value.get("coordinates")
    if value.get("type") == "Polygon" and isinstance(coordinates, list) and coordinates:
        return {"type": "Polygon", "coordinates": coordinates}
    if value.get("type") == "MultiPolygon" and isinstance(coordinates, list) and len(coordinates) == 1 and coordinates[0]:
        return {"type": "Polygon", "coordinates": coordinates[0]}
    return None


def _format_location(municipio: object, uf: object) -> str | None:
    city = _clean_text(municipio)
    state = _clean_text(uf)
    if city and state:
        return f"{city.title()}, {state.upper()}"
    return city or state


def _clean_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _number_or_none(value: object) -> float | None:
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None
