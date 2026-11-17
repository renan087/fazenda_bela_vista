"""Previsao do tempo da fazenda pela API publica do Open-Meteo.

Consulta ECMWF (principal), GFS e ICON numa unica chamada, guarda o resultado em cache
de memoria por 3 horas por coordenada e nunca levanta excecao para quem chama: em falha
devolve o ultimo dado bom (marcado como desatualizado) ou um status de erro.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import date, datetime, timezone

import httpx

logger = logging.getLogger(__name__)

API_URL = "https://api.open-meteo.com/v1/forecast"
FORECAST_DAYS = 7
CACHE_TTL_SECONDS = 3 * 60 * 60
ERROR_RETRY_SECONDS = 10 * 60
DAILY_VARIABLES = ("temperature_2m_max", "temperature_2m_min", "precipitation_sum", "precipitation_probability_max",
                   "weather_code")
# Ordem = prioridade: o primeiro e a previsao principal; os demais cobrem dados ausentes.
MODELS = (
    ("ecmwf_ifs025", "ECMWF"),
    ("gfs_seamless", "GFS"),
    ("icon_seamless", "ICON"),
)
PRIMARY_MODEL = MODELS[0][0]
WEEKDAYS = ("Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom")

_cache: dict[tuple[float, float], dict] = {}
_lock = threading.Lock()


# ---------------------------------------------------------------- coordenadas
def _collect_points(node, points: list[tuple[float, float]]) -> None:
    if isinstance(node, dict):
        if node.get("type") == "FeatureCollection":
            for feature in node.get("features") or []:
                _collect_points(feature, points)
        elif node.get("type") == "Feature":
            _collect_points(node.get("geometry"), points)
        elif node.get("type") == "GeometryCollection":
            for geometry in node.get("geometries") or []:
                _collect_points(geometry, points)
        else:
            _collect_points(node.get("coordinates"), points)
    elif isinstance(node, (list, tuple)):
        if len(node) >= 2 and all(isinstance(value, (int, float)) for value in node[:2]):
            points.append((float(node[0]), float(node[1])))
        else:
            for item in node:
                _collect_points(item, points)


def _geojson_center(text: str | None) -> tuple[float, float] | None:
    if not text:
        return None
    try:
        points: list[tuple[float, float]] = []
        _collect_points(json.loads(text), points)
    except (TypeError, ValueError):
        return None
    points = [(lng, lat) for lng, lat in points if -180 <= lng <= 180 and -90 <= lat <= 90]
    if not points:
        return None
    return sum(p[1] for p in points) / len(points), sum(p[0] for p in points) / len(points)


def farm_coordinates(farm) -> tuple[float, float, str] | None:
    """(lat, lng, origem) da fazenda: contorno da fazenda; senao, centro dos setores."""
    center = _geojson_center(getattr(farm, "boundary_geojson", None))
    if center:
        return center[0], center[1], "contorno da fazenda"
    plot_points = []
    for plot in getattr(farm, "plots", None) or []:
        if plot.centroid_lat is not None and plot.centroid_lng is not None:
            plot_points.append((float(plot.centroid_lat), float(plot.centroid_lng)))
        else:
            plot_center = _geojson_center(getattr(plot, "boundary_geojson", None))
            if plot_center:
                plot_points.append(plot_center)
    if plot_points:
        return (sum(p[0] for p in plot_points) / len(plot_points),
                sum(p[1] for p in plot_points) / len(plot_points), "setores da fazenda")
    return None


# ---------------------------------------------------------------- condicao do dia
def weather_condition(code: int | None, precip: float | None = None, prob: int | None = None) -> tuple[str, str]:
    """(emoji, descricao) pelo codigo WMO do Open-Meteo; sem codigo, estima pela chuva prevista."""
    if code is None:
        if precip is not None and precip >= 10:
            return "🌧️", "chuva"
        if (precip or 0) >= 1 or (prob or 0) >= 50:
            return "🌦️", "possibilidade de chuva"
        return "🌤️", "sem chuva prevista"
    if code <= 1:
        return "☀️", "ensolarado"
    if code == 2:
        return "⛅", "parcialmente nublado"
    if code == 3:
        return "☁️", "nublado"
    if code in (45, 48):
        return "🌫️", "neblina"
    if 51 <= code <= 57:
        return "🌦️", "garoa"
    if code in (80, 81):
        return "🌦️", "pancadas de chuva"
    if 61 <= code <= 67 or code == 82:
        return "🌧️", "chuva"
    if 71 <= code <= 77 or code in (85, 86):
        return "❄️", "neve"
    if code >= 95:
        return "⛈️", "tempestade"
    return "🌤️", "tempo variável"


# ---------------------------------------------------------------- API
def _number(value) -> float | None:
    return float(value) if isinstance(value, (int, float)) else None


def parse_forecast(payload: dict) -> dict:
    """Normaliza a resposta multi-modelo do Open-Meteo. Modelos/variaveis ausentes viram None."""
    daily = payload.get("daily") or {}
    dates = [str(day) for day in (daily.get("time") or [])][:FORECAST_DAYS]
    if not dates:
        raise ValueError("Resposta do Open-Meteo sem dias de previsao")

    def series(variable: str, model: str) -> list[float | None]:
        raw = list(daily.get(f"{variable}_{model}") or [])
        return [_number(raw[i]) if i < len(raw) else None for i in range(len(dates))]

    data = {model: {var: series(var, model) for var in DAILY_VARIABLES} for model, _ in MODELS}
    labels = dict(MODELS)

    days = []
    for index, iso in enumerate(dates):
        day = {"date": iso}
        try:
            parsed = date.fromisoformat(iso)
            day["weekday"] = WEEKDAYS[parsed.weekday()]
            day["label"] = parsed.strftime("%d/%m")
        except ValueError:
            day["weekday"], day["label"] = "", iso
        sources = {}
        for key, variable in (("tmax", "temperature_2m_max"), ("tmin", "temperature_2m_min"),
                              ("precip", "precipitation_sum"), ("prob", "precipitation_probability_max"),
                              ("code", "weather_code")):
            day[key] = None
            for model, _ in MODELS:
                value = data[model][variable][index]
                if value is not None:
                    day[key] = round(value, 1) if key not in ("prob", "code") else round(value)
                    if model != PRIMARY_MODEL and key != "code":
                        sources[key] = labels[model]
                    break
        day["fallback"] = sources
        days.append(day)

    models = []
    for model, label in MODELS:
        precip = data[model]["precipitation_sum"]
        available = any(value is not None for value in precip)
        models.append({
            "code": model,
            "label": label,
            "primary": model == PRIMARY_MODEL,
            "available": available,
            "precip": [round(value, 1) if value is not None else None for value in precip],
            "prob": [round(value) if value is not None else None for value in data[model]["precipitation_probability_max"]],
            "total": round(sum(value for value in precip if value is not None), 1) if available else None,
        })
    if not any(model["available"] for model in models) and all(day["tmax"] is None for day in days):
        raise ValueError("Nenhum modelo trouxe dados")
    primary_ok = models[0]["available"]
    total = sum(day["precip"] for day in days if day["precip"] is not None)
    return {
        "days": days,
        "models": models,
        "primary_available": primary_ok,
        "total_precip": round(total, 1),
    }


def _fetch(lat: float, lng: float, client: httpx.Client | None = None) -> dict:
    params = {
        "latitude": f"{lat:.4f}",
        "longitude": f"{lng:.4f}",
        "daily": ",".join(DAILY_VARIABLES),
        "models": ",".join(model for model, _ in MODELS),
        "timezone": "auto",
        "forecast_days": FORECAST_DAYS,
    }
    owns = client is None
    client = client or httpx.Client(timeout=10.0, headers={"User-Agent": "SiSFarm/1.0"})
    try:
        response = client.get(API_URL, params=params)
        if response.status_code >= 400:
            try:
                reason = response.json().get("reason")
            except ValueError:
                reason = None
            raise ValueError(f"Open-Meteo HTTP {response.status_code}: {reason or response.text[:120]}")
        return parse_forecast(response.json())
    finally:
        if owns:
            client.close()


def get_farm_forecast(lat: float, lng: float, *, now: float | None = None, fetcher=None) -> dict:
    """Previsao com cache de 3 h. Sempre devolve dict com 'status' (ok | stale | error)."""
    now = time.time() if now is None else now
    key = (round(lat, 2), round(lng, 2))  # ~1 km: mesma grade dos modelos
    with _lock:
        entry = _cache.get(key)
    if entry and entry.get("data") and now - entry["fetched_at"] < CACHE_TTL_SECONDS:
        return {"status": "ok", **entry["data"], "fetched_at": entry["fetched_at"]}
    if entry and entry.get("failed_at") and now - entry["failed_at"] < ERROR_RETRY_SECONDS:
        return _fallback(entry)
    try:
        data = (fetcher or _fetch)(lat, lng)
    except Exception as exc:  # rede, HTTP, JSON ou formato: o Dashboard segue sem previsao
        logger.warning("Previsao do tempo indisponivel (%s, %s): %s", key[0], key[1], exc)
        with _lock:
            entry = dict(_cache.get(key) or {})
            entry["failed_at"] = now
            _cache[key] = entry
        return _fallback(entry)
    with _lock:
        _cache[key] = {"data": data, "fetched_at": now}
    return {"status": "ok", **data, "fetched_at": now}


def _fallback(entry: dict) -> dict:
    if entry.get("data"):
        return {"status": "stale", **entry["data"], "fetched_at": entry["fetched_at"]}
    return {"status": "error"}


def fetched_at_datetime(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def clear_cache() -> None:
    with _lock:
        _cache.clear()
