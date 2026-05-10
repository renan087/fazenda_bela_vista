"""Gera imagem estática consolidada para o mapa do dashboard."""

from __future__ import annotations

import hashlib
import json
import logging
import tempfile
from pathlib import Path

import httpx
from PIL import Image, ImageDraw

from app.services.farm_preview_image import (
    OUTPUT_HEIGHT,
    OUTPUT_WIDTH,
    TILE_SIZE,
    USER_AGENT,
    _bbox_world_rect,
    _exterior_rings,
    _fetch_tile,
    _lonlat_to_world_px,
    _pick_zoom,
    _rings_bbox,
)

logger = logging.getLogger(__name__)

_APP_DIR = Path(__file__).resolve().parents[1]
DASHBOARD_MAP_DIR = _APP_DIR / "static" / "generated" / "dashboard_maps"

FARM_STYLE = {
    "fill": (183, 228, 199, 85),
    "outline": (47, 107, 56, 240),
    "width": 3,
}
PLOT_PALETTE = [
    ((147, 197, 253, 92), (37, 99, 235, 255)),
    ((252, 211, 77, 92), (217, 119, 6, 255)),
    ((196, 181, 253, 92), (124, 58, 237, 255)),
    ((103, 232, 249, 92), (8, 145, 178, 255)),
    ((253, 164, 175, 92), (190, 18, 60, 255)),
    ((190, 242, 100, 92), (77, 124, 15, 255)),
    ((253, 186, 116, 92), (194, 65, 12, 255)),
    ((94, 234, 212, 92), (15, 118, 110, 255)),
    ((240, 171, 252, 92), (162, 28, 175, 255)),
    ((165, 180, 252, 92), (67, 56, 202, 255)),
    ((134, 239, 172, 92), (21, 128, 61, 255)),
    ((253, 230, 138, 92), (180, 83, 9, 255)),
]


def dashboard_map_fingerprint(map_geojson: str) -> str:
    return hashlib.sha256((map_geojson or "").strip().encode("utf-8")).hexdigest()[:18]


def dashboard_map_preview_relative_path(fingerprint: str) -> str:
    return f"generated/dashboard_maps/{fingerprint}.png"


def dashboard_map_preview_fs_path(fingerprint: str) -> Path:
    return DASHBOARD_MAP_DIR / f"{fingerprint}.png"


def _feature_rings(map_geojson: str) -> list[dict]:
    try:
        data = json.loads(map_geojson or "{}")
    except json.JSONDecodeError:
        return []
    features = data.get("features") if isinstance(data, dict) else None
    if not isinstance(features, list):
        return []
    out: list[dict] = []
    for feature in features:
        if not isinstance(feature, dict):
            continue
        geometry = feature.get("geometry")
        props = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
        if not isinstance(geometry, dict):
            continue
        rings = _exterior_rings(geometry)
        if not rings:
            continue
        out.append(
            {
                "feature_type": (props.get("feature_type") or "").strip().lower(),
                "rings": rings,
            }
        )
    return out


def _expand_world_rect_to_output_ratio(
    wx0: float,
    wy0: float,
    wx1: float,
    wy1: float,
) -> tuple[float, float, float, float]:
    """Mantém escala X/Y igual ao redimensionar para o tamanho final da imagem."""
    width = max(wx1 - wx0, 1.0)
    height = max(wy1 - wy0, 1.0)
    target_ratio = OUTPUT_WIDTH / OUTPUT_HEIGHT
    current_ratio = width / height
    cx = (wx0 + wx1) / 2
    cy = (wy0 + wy1) / 2
    if current_ratio > target_ratio:
        height = width / target_ratio
    else:
        width = height * target_ratio
    return cx - width / 2, cy - height / 2, cx + width / 2, cy + height / 2


def generate_dashboard_map_preview(map_geojson: str, fingerprint: str | None = None) -> bool:
    """Gera PNG local do mapa do dashboard usando mosaico satélite e desenho local."""
    fingerprint = (fingerprint or dashboard_map_fingerprint(map_geojson)).strip()
    if not fingerprint:
        return False
    final_path = dashboard_map_preview_fs_path(fingerprint)
    if final_path.is_file() and final_path.stat().st_size > 0:
        return True

    features = _feature_rings(map_geojson)
    if not features:
        return False
    all_rings = [ring for feature in features for ring in feature["rings"]]
    bbox = _rings_bbox(all_rings)
    if not bbox:
        return False
    lon_min, lat_min, lon_max, lat_max = bbox
    z = _pick_zoom(lon_min, lat_min, lon_max, lat_max)
    if z is None:
        logger.warning("Nao foi possivel escolher zoom para imagem estatica do dashboard")
        return False

    wx0, wy0, wx1, wy1 = _bbox_world_rect(lon_min, lat_min, lon_max, lat_max, z)
    pad_w = (wx1 - wx0) * 0.06
    pad_h = (wy1 - wy0) * 0.06
    wx0 -= pad_w
    wx1 += pad_w
    wy0 -= pad_h
    wy1 += pad_h
    wx0, wy0, wx1, wy1 = _expand_world_rect_to_output_ratio(wx0, wy0, wx1, wy1)

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
                        logger.warning("Tile Esri z=%s %s,%s falhou no dashboard: %s", z, tx, ty, exc)
                        continue
                    mosaic.paste(tile, ((tx - tx0) * TILE_SIZE, (ty - ty0) * TILE_SIZE))
    except Exception as exc:
        logger.exception("Falha ao montar mosaico do mapa do dashboard: %s", exc)
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
        resample = Image.Resampling.LANCZOS
    except AttributeError:
        resample = Image.LANCZOS
    cropped = cropped.resize((OUTPUT_WIDTH, OUTPUT_HEIGHT), resample)
    crop_w = crop_x1 - crop_x0
    crop_h = crop_y1 - crop_y0

    def project_ring(ring: list[tuple[float, float]]) -> list[tuple[float, float]]:
        flat: list[tuple[float, float]] = []
        for lon, lat in ring:
            wx, wy = _lonlat_to_world_px(lon, lat, z)
            flat.append(((wx - wx0) / crop_w * OUTPUT_WIDTH, (wy - wy0) / crop_h * OUTPUT_HEIGHT))
        return flat

    overlay = Image.new("RGBA", (OUTPUT_WIDTH, OUTPUT_HEIGHT), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    for feature in features:
        if feature["feature_type"] != "farm":
            continue
        for ring in feature["rings"]:
            flat = project_ring(ring)
            if len(flat) >= 3:
                draw.polygon(flat, fill=FARM_STYLE["fill"], outline=FARM_STYLE["outline"], width=FARM_STYLE["width"])

    plot_index = 0
    for feature in features:
        if feature["feature_type"] == "farm":
            continue
        fill, outline = PLOT_PALETTE[plot_index % len(PLOT_PALETTE)]
        plot_index += 1
        for ring in feature["rings"]:
            flat = project_ring(ring)
            if len(flat) >= 3:
                draw.polygon(flat, fill=fill, outline=outline, width=2)

    final_img = Image.alpha_composite(cropped.convert("RGBA"), overlay).convert("RGB")
    DASHBOARD_MAP_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False, dir=DASHBOARD_MAP_DIR) as tmp:
            tmp_path = Path(tmp.name)
        final_img.save(tmp_path, format="PNG", optimize=True)
        tmp_path.replace(final_path)
        return True
    except OSError as exc:
        logger.exception("Falha ao salvar imagem estatica do mapa do dashboard: %s", exc)
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        return False
