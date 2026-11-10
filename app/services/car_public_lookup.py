"""Consulta de imoveis pelo numero do CAR no geosservico publico do SICAR.

O servico e publico, nao autenticado e pode ficar indisponivel. Por isso este
modulo nunca e usado como fonte obrigatoria: o usuario sempre confirma e pode
ajustar o perimetro retornado antes de salvar a fazenda.
"""

from __future__ import annotations

import re
import json
import logging
import shutil
import subprocess
from typing import Any

import httpx


SICAR_WFS_URL = "https://geoserver.car.gov.br/geoserver/sicar/wfs"
CAR_NUMBER_PATTERN = re.compile(r"^(?P<uf>[A-Z]{2})-(?P<municipality>\d{7})-(?P<code>[A-Z0-9]{6,120})$")
SUPPORTED_UFS = {
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT",
    "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
}
logger = logging.getLogger(__name__)


class CarPublicLookupError(ValueError):
    """Erro seguro para apresentar ao usuario durante a consulta publica."""


def normalize_car_number(value: str | None) -> str:
    # O recibo do CAR costuma ser copiado com pontos em diferentes agrupamentos.
    # O GeoServer publico compara o codigo sem pontuacao interna.
    compact = re.sub(r"[^A-Z0-9]", "", str(value or "").upper())
    normalized = f"{compact[:2]}-{compact[2:9]}-{compact[9:]}" if len(compact) > 9 else compact
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
    payload = _fetch_public_payload(params)

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


def _fetch_public_payload(params: dict[str, str]) -> dict[str, Any]:
    """Busca o WFS com fallback para a pilha TLS que o servidor publico aceita."""
    try:
        with httpx.Client(timeout=httpx.Timeout(12.0, connect=4.0), follow_redirects=True) as client:
            response = client.get(SICAR_WFS_URL, params=params)
            response.raise_for_status()
            payload = response.json()
        if isinstance(payload, dict):
            return payload
    except (httpx.HTTPError, ValueError) as exc:
        # O GeoServer do SICAR por vezes recusa o handshake TLS do OpenSSL usado
        # pelo Python, embora aceite o curl com validacao de certificado normal.
        logger.info("Consulta CAR via httpx falhou; tentando cliente TLS alternativo: %s", exc)

    curl_path = shutil.which("curl")
    if not curl_path:
        raise CarPublicLookupError("Nao foi possivel consultar o geosservico publico do CAR agora. Tente novamente mais tarde.")
    command = [
        curl_path,
        "--fail",
        "--silent",
        "--show-error",
        "--max-time",
        "12",
        "--get",
        SICAR_WFS_URL,
    ]
    for key, value in params.items():
        command.extend(["--data-urlencode", f"{key}={value}"])
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=14, check=False)
        if result.returncode != 0:
            logger.info("Consulta CAR via curl falhou: %s", (result.stderr or "").strip()[:200])
            raise ValueError("curl failed")
        payload = json.loads(result.stdout)
        if isinstance(payload, dict):
            return payload
    except (OSError, subprocess.TimeoutExpired, ValueError, json.JSONDecodeError) as exc:
        logger.info("Cliente TLS alternativo do CAR indisponivel: %s", exc)
    raise CarPublicLookupError("Nao foi possivel consultar o geosservico publico do CAR agora. Tente novamente mais tarde.")


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
