"""Gera imagem estática (satélite + contorno) para prévia de fazenda no servidor."""

from __future__ import annotations

import json
import logging
import math
import tempfile
from io import BytesIO
from pathlib import Path

import httpx
from PIL import Image, ImageDraw

logger = logging.getLogger(__name__)

TILE_SIZE = 256
MAX_TILES = 40
OUTPUT_WIDTH = 960
OUTPUT_HEIGHT = 540
ESRI_IMAGERY_URL = (
    "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
)
USER_AGENT = "FazendaBelaVista/1.0 (+https://github.com/renan087/fazenda_bela_vista)"

# Diretório servido em /static/generated/farm_previews/
_APP_DIR = Path(__file__).resolve().parents[1]
PREVIEW_DIR = _APP_DIR / "static" / "generated" / "farm_previews"


def farm_preview_relative_path(farm_id: int) -> str:
    return f"generated/farm_previews/{farm_id}.png"


def farm_preview_fs_path(farm_id: int) -> Path:
    return PREVIEW_DIR / f"{farm_id}.png"


def remove_farm_preview_image(farm_id: int) -> None:
    path = farm_preview_fs_path(farm_id)
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        logger.warning("Nao foi possivel remover preview da fazenda %s: %s", farm_id, exc)


def _lonlat_to_world_px(lon: float, lat: float, z: int) -> tuple[float, float]:
    sin_y = math.sin(math.radians(lat))
    sin_y = min(max(sin_y, -0.9999), 0.9999)
    scale = TILE_SIZE * (2**z)
    x = (lon + 180.0) / 360.0 * scale
    y = (0.5 - math.log((1 + sin_y) / (1 - sin_y)) / (4 * math.pi)) * scale
    return x, y


def _bbox_world_rect(lon_min: float, lat_min: float, lon_max: float, lat_max: float, z: int) -> tuple[float, float, float, float]:
    xs: list[float] = []
    ys: list[float] = []
    for lon in (lon_min, lon_max):
        for lat in (lat_min, lat_max):
            x, y = _lonlat_to_world_px(lon, lat, z)
            xs.append(x)
            ys.append(y)
    return min(xs), min(ys), max(xs), max(ys)


def _exterior_rings(geometry: dict) -> list[list[tuple[float, float]]]:
    gtype = geometry.get("type")
    coords = geometry.get("coordinates") or []
    rings: list[list[tuple[float, float]]] = []
    if gtype == "Polygon":
        if coords and isinstance(coords[0], list):
            ring = _ring_coords(coords[0])
            if len(ring) >= 3:
                rings.append(ring)
    elif gtype == "MultiPolygon":
        for poly in coords:
            if poly and isinstance(poly[0], list):
                ring = _ring_coords(poly[0])
                if len(ring) >= 3:
                    rings.append(ring)
    return rings


def _ring_coords(raw_ring) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    for pt in raw_ring:
        if isinstance(pt, (list, tuple)) and len(pt) >= 2:
            try:
                out.append((float(pt[0]), float(pt[1])))
            except (TypeError, ValueError):
                continue
    return out


def _parse_geometry(geojson_text: str) -> dict | None:
    try:
        data = json.loads(geojson_text)
    except json.JSONDecodeError:
        return None
    if isinstance(data, dict) and data.get("type") == "Feature":
        geom = data.get("geometry")
        return geom if isinstance(geom, dict) else None
    if isinstance(data, dict) and data.get("type") == "FeatureCollection":
        feats = data.get("features") or []
        if feats and isinstance(feats[0], dict):
            geom = feats[0].get("geometry")
            return geom if isinstance(geom, dict) else None
        return None
    if isinstance(data, dict) and "coordinates" in data:
        return data
    return None


def _rings_bbox(rings: list[list[tuple[float, float]]]) -> tuple[float, float, float, float] | None:
    if not rings:
        return None
    lons: list[float] = []
    lats: list[float] = []
    for ring in rings:
        for lon, lat in ring:
            lons.append(lon)
            lats.append(lat)
    if not lons:
        return None
    lon_min, lon_max = min(lons), max(lons)
    lat_min, lat_max = min(lats), max(lats)
    lon_pad = (lon_max - lon_min) * 0.08 + 1e-6
    lat_pad = (lat_max - lat_min) * 0.08 + 1e-6
    return lon_min - lon_pad, lat_min - lat_pad, lon_max + lon_pad, lat_max + lat_pad


def _pick_zoom(lon_min: float, lat_min: float, lon_max: float, lat_max: float) -> int | None:
    margin = 48.0
    for z in range(18, 5, -1):
        wx0, wy0, wx1, wy1 = _bbox_world_rect(lon_min, lat_min, lon_max, lat_max, z)
        w = wx1 - wx0
        h = wy1 - wy0
        if w <= 1 or h <= 1:
            continue
        w_view = w * 1.12
        h_view = h * 1.12
        if w_view > OUTPUT_WIDTH - margin or h_view > OUTPUT_HEIGHT - margin:
            continue
        pad_w = w * 0.06
        pad_h = h * 0.06
        ax0, ay0, ax1, ay1 = wx0 - pad_w, wy0 - pad_h, wx1 + pad_w, wy1 + pad_h
        tx0 = int(ax0 // TILE_SIZE)
        tx1 = int(ax1 // TILE_SIZE)
        ty0 = int(ay0 // TILE_SIZE)
        ty1 = int(ay1 // TILE_SIZE)
        tiles = (tx1 - tx0 + 1) * (ty1 - ty0 + 1)
        if tiles <= MAX_TILES:
            return z
    return None


def _fetch_tile(client: httpx.Client, z: int, x: int, y: int) -> Image.Image:
    url = ESRI_IMAGERY_URL.format(z=z, y=y, x=x)
    response = client.get(url)
    response.raise_for_status()
    return Image.open(BytesIO(response.content)).convert("RGB")


def generate_farm_preview_image(farm_id: int, boundary_geojson: str | None) -> bool:
    """
    Gera PNG em disco. Retorna True se salvou com sucesso.
    """
    if not boundary_geojson or not boundary_geojson.strip():
        remove_farm_preview_image(farm_id)
        return False

    geometry = _parse_geometry(boundary_geojson)
    if not geometry:
        logger.warning("GeoJSON invalido para preview da fazenda %s", farm_id)
        return False

    rings = _exterior_rings(geometry)
    bbox = _rings_bbox(rings)
    if not bbox:
        return False

    lon_min, lat_min, lon_max, lat_max = bbox
    z = _pick_zoom(lon_min, lat_min, lon_max, lat_max)
    if z is None:
        logger.warning("Nao foi possivel escolher zoom para preview da fazenda %s", farm_id)
        return False

    wx0, wy0, wx1, wy1 = _bbox_world_rect(lon_min, lat_min, lon_max, lat_max, z)
    pad_w = (wx1 - wx0) * 0.06
    pad_h = (wy1 - wy0) * 0.06
    wx0 -= pad_w
    wx1 += pad_w
    wy0 -= pad_h
    wy1 += pad_h

    tx0 = int(wx0 // TILE_SIZE)
    tx1 = int(wx1 // TILE_SIZE)
    ty0 = int(wy0 // TILE_SIZE)
    ty1 = int(wy1 // TILE_SIZE)

    mosaic_w = (tx1 - tx0 + 1) * TILE_SIZE
    mosaic_h = (ty1 - ty0 + 1) * TILE_SIZE
    mosaic = Image.new("RGB", (mosaic_w, mosaic_h), (30, 40, 55))

    try:
        with httpx.Client(
            timeout=httpx.Timeout(20.0),
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            for ty in range(ty0, ty1 + 1):
                for tx in range(tx0, tx1 + 1):
                    try:
                        tile = _fetch_tile(client, z, tx, ty)
                    except Exception as exc:
                        logger.warning("Tile satelite z=%s %s,%s falhou: %s", z, tx, ty, exc)
                        continue
                    ox = (tx - tx0) * TILE_SIZE
                    oy = (ty - ty0) * TILE_SIZE
                    mosaic.paste(tile, (ox, oy))
    except Exception as exc:
        logger.exception("Falha ao montar mosaic de satelite para fazenda %s: %s", farm_id, exc)
        return False

    crop_x0 = int(wx0 - tx0 * TILE_SIZE)
    crop_y0 = int(wy0 - ty0 * TILE_SIZE)
    crop_x1 = int(wx1 - tx0 * TILE_SIZE)
    crop_y1 = int(wy1 - ty0 * TILE_SIZE)
    crop_x0 = max(0, min(crop_x0, mosaic_w - 1))
    crop_y0 = max(0, min(crop_y0, mosaic_h - 1))
    crop_x1 = max(crop_x0 + 1, min(crop_x1, mosaic_w))
    crop_y1 = max(crop_y0 + 1, min(crop_y1, mosaic_h))

    cropped = mosaic.crop((crop_x0, crop_y0, crop_x1, crop_y1))
    try:
        cropped = cropped.resize((OUTPUT_WIDTH, OUTPUT_HEIGHT), Image.Resampling.LANCZOS)
    except AttributeError:
        cropped = cropped.resize((OUTPUT_WIDTH, OUTPUT_HEIGHT), Image.LANCZOS)

    crop_w = crop_x1 - crop_x0
    crop_h = crop_y1 - crop_y0

    overlay = Image.new("RGBA", (OUTPUT_WIDTH, OUTPUT_HEIGHT), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    stroke = 3

    for ring in rings:
        flat: list[tuple[float, float]] = []
        for lon, lat in ring:
            wx, wy = _lonlat_to_world_px(lon, lat, z)
            px = (wx - wx0) / crop_w * OUTPUT_WIDTH
            py = (wy - wy0) / crop_h * OUTPUT_HEIGHT
            flat.append((px, py))
        if len(flat) < 3:
            continue
        draw.polygon(
            flat,
            fill=(91, 179, 74, 95),
            outline=(65, 132, 54, 255),
            width=stroke,
        )

    base = cropped.convert("RGBA")
    composed = Image.alpha_composite(base, overlay).convert("RGB")

    PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    final_path = farm_preview_fs_path(farm_id)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False, dir=PREVIEW_DIR) as tmp:
            tmp_path = Path(tmp.name)
        composed.save(tmp_path, format="PNG", optimize=True)
        tmp_path.replace(final_path)
    except OSError as exc:
        logger.exception("Falha ao salvar preview da fazenda %s: %s", farm_id, exc)
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        return False

    return True
