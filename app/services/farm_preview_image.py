"""Gera imagem estática (satélite + contorno) para prévia de fazenda no servidor."""

from __future__ import annotations

import json
import logging
import math
import tempfile
from io import BytesIO
from pathlib import Path
from urllib.parse import urlencode

import httpx
from PIL import Image, ImageDraw

from app.core.config import get_settings

logger = logging.getLogger(__name__)

TILE_SIZE = 256
MAX_TILES = 40
OUTPUT_WIDTH = 960
OUTPUT_HEIGHT = 540
# Miniatura para cards (≈96×54 lógico × retina; JPEG leve)
THUMB_MAX_WIDTH = 240
THUMB_MAX_HEIGHT = 136
THUMB_JPEG_QUALITY = 82
ESRI_IMAGERY_URL = (
    "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
)
GOOGLE_STATIC_MAP_URL = "https://maps.googleapis.com/maps/api/staticmap"
USER_AGENT = "FazendaBelaVista/1.0 (+https://github.com/renan087/fazenda_bela_vista)"
STATIC_MAP_MAX_URL_LEN = 7600

# Diretório servido em /static/generated/farm_previews/
_APP_DIR = Path(__file__).resolve().parents[1]
PREVIEW_DIR = _APP_DIR / "static" / "generated" / "farm_previews"


def farm_preview_relative_path(farm_id: int) -> str:
    return f"generated/farm_previews/{farm_id}.png"


def farm_preview_thumb_relative_path(farm_id: int) -> str:
    return f"generated/farm_previews/{farm_id}_thumb.jpg"


def farm_preview_fs_path(farm_id: int) -> Path:
    return PREVIEW_DIR / f"{farm_id}.png"


def farm_preview_thumb_fs_path(farm_id: int) -> Path:
    return PREVIEW_DIR / f"{farm_id}_thumb.jpg"


def remove_farm_preview_image(farm_id: int) -> None:
    for path in (farm_preview_fs_path(farm_id), farm_preview_thumb_fs_path(farm_id)):
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            logger.warning("Nao foi possivel remover preview da fazenda %s (%s): %s", farm_id, path.name, exc)


def _resample_lanczos():
    try:
        return Image.Resampling.LANCZOS
    except AttributeError:
        return Image.LANCZOS


def save_preview_thumbnail_at(full_rgb: Image.Image, thumb_path: Path) -> bool:
    """Redimensiona e grava JPEG para uso nos cards (arquivo pequeno)."""
    thumb = full_rgb.copy()
    thumb.thumbnail((THUMB_MAX_WIDTH, THUMB_MAX_HEIGHT), _resample_lanczos())
    tmp_path: Path | None = None
    try:
        thumb_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False, dir=thumb_path.parent) as tmp:
            tmp_path = Path(tmp.name)
        thumb.save(
            tmp_path,
            format="JPEG",
            quality=THUMB_JPEG_QUALITY,
            optimize=True,
            progressive=True,
        )
        tmp_path.replace(thumb_path)
        return True
    except OSError as exc:
        logger.exception("Falha ao salvar miniatura %s: %s", thumb_path, exc)
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        return False


def ensure_farm_preview_thumb(farm_id: int) -> bool:
    """Gera só a miniatura a partir do PNG já existente (migração / reparo)."""
    png_path = farm_preview_fs_path(farm_id)
    if not png_path.is_file() or png_path.stat().st_size <= 0:
        return False
    try:
        with Image.open(png_path) as im:
            rgb = im.convert("RGB")
            return save_preview_thumbnail_at(rgb, farm_preview_thumb_fs_path(farm_id))
    except OSError as exc:
        logger.warning("Nao foi possivel abrir PNG do preview da fazenda %s: %s", farm_id, exc)
        return False


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


def _subsample_ring(ring: list[tuple[float, float]], max_points: int) -> list[tuple[float, float]]:
    if len(ring) < 3 or max_points < 3:
        return ring
    pts = list(ring)
    if len(pts) > 1 and pts[0] == pts[-1]:
        pts = pts[:-1]
    if len(pts) < 3:
        return ring
    if len(pts) <= max_points:
        return ring
    step = (len(pts) - 1) / (max_points - 1)
    return [pts[min(int(round(i * step)), len(pts) - 1)] for i in range(max_points)]


def _google_static_query_pairs(
    rings: list[list[tuple[float, float]]],
    bbox: tuple[float, float, float, float],
    api_key: str,
    max_points_per_ring: int,
) -> list[tuple[str, str]]:
    lon_min, lat_min, lon_max, lat_max = bbox
    visible = "|".join(
        [
            f"{lat_min:.7f},{lon_min:.7f}",
            f"{lat_min:.7f},{lon_max:.7f}",
            f"{lat_max:.7f},{lon_max:.7f}",
            f"{lat_max:.7f},{lon_min:.7f}",
        ]
    )
    pairs: list[tuple[str, str]] = [
        ("size", "640x360"),
        ("scale", "2"),
        ("maptype", "satellite"),
        ("visible", visible),
        ("key", api_key),
    ]
    for ring in rings:
        sampled = _subsample_ring(ring, max_points_per_ring)
        parts = ["fillcolor:0x5BB34AB3", "color:0x418436", "weight:2"]
        for lon, lat in sampled:
            parts.append(f"{lat:.7f},{lon:.7f}")
        pairs.append(("path", "|".join(parts)))
    return pairs


def _try_google_static_preview(
    rings: list[list[tuple[float, float]]],
    bbox: tuple[float, float, float, float],
    api_key: str,
) -> Image.Image | None:
    max_pts = 72
    pairs: list[tuple[str, str]] = []
    for _ in range(6):
        pairs = _google_static_query_pairs(rings, bbox, api_key.strip(), max_pts)
        if len(urlencode(pairs)) <= STATIC_MAP_MAX_URL_LEN:
            break
        max_pts = max(12, max_pts // 2)
    else:
        pairs = _google_static_query_pairs(rings, bbox, api_key.strip(), 12)

    url = f"{GOOGLE_STATIC_MAP_URL}?{urlencode(pairs)}"
    try:
        with httpx.Client(timeout=httpx.Timeout(30.0), follow_redirects=True) as client:
            response = client.get(url)
            if response.status_code != 200:
                logger.warning(
                    "Google Static Maps HTTP %s ao gerar preview",
                    response.status_code,
                )
                return None
            img = Image.open(BytesIO(response.content)).convert("RGB")
            if img.width < 64 or img.height < 64:
                logger.warning("Google Static Maps retornou imagem muito pequena (%sx%s)", img.width, img.height)
                return None
            if img.size != (OUTPUT_WIDTH, OUTPUT_HEIGHT):
                try:
                    resample = Image.Resampling.LANCZOS
                except AttributeError:
                    resample = Image.LANCZOS
                img = img.resize((OUTPUT_WIDTH, OUTPUT_HEIGHT), resample)
            return img
    except Exception as exc:
        logger.warning("Falha na requisicao Google Static Maps: %s", exc)
        return None


def _try_esri_tile_preview(
    log_entity_id: int,
    rings: list[list[tuple[float, float]]],
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
) -> Image.Image | None:
    z = _pick_zoom(lon_min, lat_min, lon_max, lat_max)
    if z is None:
        logger.warning("Nao foi possivel escolher zoom (Esri) para preview geografico id=%s", log_entity_id)
        return None

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
                        logger.warning("Tile Esri z=%s %s,%s falhou: %s", z, tx, ty, exc)
                        continue
                    ox = (tx - tx0) * TILE_SIZE
                    oy = (ty - ty0) * TILE_SIZE
                    mosaic.paste(tile, (ox, oy))
    except Exception as exc:
        logger.exception("Falha ao montar mosaic Esri para preview id=%s: %s", log_entity_id, exc)
        return None

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
    return Image.alpha_composite(base, overlay).convert("RGB")


def build_satellite_preview_from_geojson(
    boundary_geojson: str,
    *,
    log_entity_id: int = 0,
) -> Image.Image | None:
    """
    Monta imagem RGB (satélite + polígono verde) a partir de GeoJSON de perímetro.
    Não grava em disco. Usado por previews de fazenda e de setor.
    """
    text = (boundary_geojson or "").strip()
    if not text:
        return None
    geometry = _parse_geometry(text)
    if not geometry:
        logger.warning("GeoJSON invalido para preview geografico id=%s", log_entity_id)
        return None
    rings = _exterior_rings(geometry)
    bbox = _rings_bbox(rings)
    if not bbox:
        return None
    lon_min, lat_min, lon_max, lat_max = bbox
    final_img: Image.Image | None = None
    api_key = (get_settings().google_maps_api_key or "").strip()
    if api_key:
        final_img = _try_google_static_preview(rings, bbox, api_key)
        if final_img is not None and log_entity_id:
            logger.info("Preview id=%s gerado via Google Static Maps", log_entity_id)
    if final_img is None:
        final_img = _try_esri_tile_preview(log_entity_id, rings, lon_min, lat_min, lon_max, lat_max)
        if final_img is not None and api_key and log_entity_id:
            logger.info("Preview id=%s gerado via fallback Esri", log_entity_id)
        elif final_img is not None and log_entity_id:
            logger.info("Preview id=%s gerado via Esri", log_entity_id)
    return final_img


def generate_farm_preview_image(farm_id: int, boundary_geojson: str | None) -> bool:
    """
    Gera PNG em disco. Prefere Google Maps Static API (satélite + path) se
    GOOGLE_MAPS_API_KEY estiver definida; caso contrário usa tiles Esri + desenho local.
    """
    if not boundary_geojson or not boundary_geojson.strip():
        remove_farm_preview_image(farm_id)
        return False

    final_img = build_satellite_preview_from_geojson(boundary_geojson.strip(), log_entity_id=farm_id)
    if final_img is None:
        return False

    PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    final_path = farm_preview_fs_path(farm_id)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False, dir=PREVIEW_DIR) as tmp:
            tmp_path = Path(tmp.name)
        final_img.save(tmp_path, format="PNG", optimize=True)
        tmp_path.replace(final_path)
    except OSError as exc:
        logger.exception("Falha ao salvar preview da fazenda %s: %s", farm_id, exc)
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        return False

    if not save_preview_thumbnail_at(final_img.copy(), farm_preview_thumb_fs_path(farm_id)):
        logger.warning("Preview PNG salvo mas miniatura JPEG falhou para fazenda %s", farm_id)

    return True
