"""Gera imagem estática (satélite + contorno) para prévia de fazenda no servidor."""

from __future__ import annotations

import gc
import hashlib
import json
import logging
import math
import tempfile
import threading
from io import BytesIO
from pathlib import Path
from urllib.parse import urlencode

import httpx
from PIL import Image, ImageDraw

from app.core.config import get_settings

logger = logging.getLogger(__name__)

# Controle estrito de concorrência: no máximo 1 geração de preview Pillow por vez na instância
# (evita picos simultâneos de RAM no container de 512 MB do Render).
_PREVIEW_SEM = threading.Semaphore(1)
_IN_FLIGHT_LOCK = threading.Lock()
_IN_FLIGHT_KEYS: set[tuple[int, str]] = set()

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


def farm_geojson_hash(boundary_geojson: str | None) -> str:
    text = (boundary_geojson or "").strip()
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:14]


def _delete_preview_cache_db(farm_id: int) -> None:
    try:
        from app.db.session import SessionLocal
        from app.models.farm_preview_cache import FarmPreviewCache

        with SessionLocal() as db:
            db.query(FarmPreviewCache).filter(FarmPreviewCache.farm_id == farm_id).delete()
            db.commit()
    except Exception:
        logger.debug("Nao foi possivel limpar farm_preview_cache da fazenda %s", farm_id, exc_info=True)


def _save_preview_cache_db(farm_id: int, geojson_hash: str, png_bytes: bytes | None, jpg_bytes: bytes | None) -> None:
    if not geojson_hash or (not png_bytes and not jpg_bytes):
        return
    try:
        from app.db.session import SessionLocal
        from app.models.farm_preview_cache import FarmPreviewCache

        with SessionLocal() as db:
            row = db.query(FarmPreviewCache).filter(FarmPreviewCache.farm_id == farm_id).first()
            if not row:
                row = FarmPreviewCache(farm_id=farm_id, geojson_hash=geojson_hash)
                db.add(row)
            row.geojson_hash = geojson_hash
            if png_bytes is not None:
                row.preview_png = png_bytes
            if jpg_bytes is not None:
                row.thumb_jpg = jpg_bytes
            db.commit()
    except Exception:
        logger.warning("Nao foi possivel persistir preview da fazenda %s no banco", farm_id, exc_info=True)


def cached_preview_hashes(db, farm_ids: list[int]) -> dict[int, str]:
    """Consulta apenas (farm_id, geojson_hash) sem carregar nenhum byte binário."""
    if not farm_ids:
        return {}
    from app.models.farm_preview_cache import FarmPreviewCache

    rows = (
        db.query(FarmPreviewCache.farm_id, FarmPreviewCache.geojson_hash)
        .filter(FarmPreviewCache.farm_id.in_(farm_ids))
        .all()
    )
    return {int(fid): str(h or "") for fid, h in rows}


def read_or_restore_farm_preview(
    db,
    farm_id: int,
    *,
    kind: str = "thumb",
    expected_hash: str | None = None,
) -> bytes | None:
    """Lê preview do disco local; se ausente (ex.: após deploy no Render), restaura do PostgreSQL."""
    disk_path = farm_preview_thumb_fs_path(farm_id) if kind == "thumb" else farm_preview_fs_path(farm_id)
    if disk_path.is_file() and disk_path.stat().st_size > 0:
        try:
            return disk_path.read_bytes()
        except OSError:
            pass

    from app.models.farm_preview_cache import FarmPreviewCache

    col = FarmPreviewCache.thumb_jpg if kind == "thumb" else FarmPreviewCache.preview_png
    query = db.query(FarmPreviewCache.geojson_hash, col).filter(FarmPreviewCache.farm_id == farm_id)
    row = query.first()
    if not row:
        return None
    stored_hash, payload = row
    if expected_hash and stored_hash != expected_hash:
        return None
    if not payload:
        return None
    data = bytes(payload)
    # Reidrata o cache em disco de forma atômica para próximas requisições
    tmp_path: Path | None = None
    try:
        disk_path.parent.mkdir(parents=True, exist_ok=True)
        suffix = ".jpg" if kind == "thumb" else ".png"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False, dir=disk_path.parent) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write(data)
        tmp_path.replace(disk_path)
    except OSError:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
    return data


def remove_farm_preview_image(farm_id: int) -> None:
    for path in (farm_preview_fs_path(farm_id), farm_preview_thumb_fs_path(farm_id)):
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            logger.warning("Nao foi possivel remover preview da fazenda %s (%s): %s", farm_id, path.name, exc)
    _delete_preview_cache_db(farm_id)


def _resample_lanczos():
    try:
        return Image.Resampling.LANCZOS
    except AttributeError:
        return Image.LANCZOS


def save_preview_thumbnail_at(full_rgb: Image.Image, thumb_path: Path) -> bool:
    """Redimensiona e grava JPEG para uso nos cards (arquivo pequeno), fechando o buffer."""
    thumb = full_rgb.copy()
    tmp_path: Path | None = None
    try:
        thumb.thumbnail((THUMB_MAX_WIDTH, THUMB_MAX_HEIGHT), _resample_lanczos())
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
    finally:
        try:
            thumb.close()
        except Exception:
            pass


def ensure_farm_preview_thumb(farm_id: int) -> bool:
    """Gera só a miniatura a partir do PNG já existente (migração / reparo)."""
    png_path = farm_preview_fs_path(farm_id)
    if not png_path.is_file() or png_path.stat().st_size <= 0:
        return False
    rgb: Image.Image | None = None
    try:
        with Image.open(png_path) as im:
            rgb = im.convert("RGB")
        return save_preview_thumbnail_at(rgb, farm_preview_thumb_fs_path(farm_id))
    except OSError as exc:
        logger.warning("Nao foi possivel abrir PNG do preview da fazenda %s: %s", farm_id, exc)
        return False
    finally:
        if rgb is not None:
            try:
                rgb.close()
            except Exception:
                pass


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


def _google_static_build_pairs(
    bbox: tuple[float, float, float, float],
    api_key: str,
    rings_plot: list[list[tuple[float, float]]],
    rings_reference: list[list[tuple[float, float]]],
    max_points_per_ring: int,
) -> list[tuple[str, str]]:
    """Paths da fazenda (verde) primeiro; setor por cima (laranja se houver referência)."""
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
    for ring in rings_reference:
        if len(ring) < 3:
            continue
        sampled = _subsample_ring(ring, max_points_per_ring)
        parts = ["fillcolor:0x5BB34A77", "color:0x418436", "weight:2"]
        for lon, lat in sampled:
            parts.append(f"{lat:.7f},{lon:.7f}")
        pairs.append(("path", "|".join(parts)))
    for ring in rings_plot:
        if len(ring) < 3:
            continue
        sampled = _subsample_ring(ring, max_points_per_ring)
        if rings_reference:
            parts = ["fillcolor:0xF97316B3", "color:0xEA580C", "weight:3"]
        else:
            parts = ["fillcolor:0x5BB34AB3", "color:0x418436", "weight:2"]
        for lon, lat in sampled:
            parts.append(f"{lat:.7f},{lon:.7f}")
        pairs.append(("path", "|".join(parts)))
    return pairs


def _try_google_static_preview(
    rings_plot: list[list[tuple[float, float]]],
    bbox: tuple[float, float, float, float],
    api_key: str,
    rings_reference: list[list[tuple[float, float]]] | None = None,
) -> Image.Image | None:
    rings_reference = rings_reference or []
    max_pts = 72
    pairs: list[tuple[str, str]] = []
    for _ in range(6):
        pairs = _google_static_build_pairs(bbox, api_key.strip(), rings_plot, rings_reference, max_pts)
        if len(urlencode(pairs)) <= STATIC_MAP_MAX_URL_LEN:
            break
        max_pts = max(12, max_pts // 2)
    else:
        pairs = _google_static_build_pairs(bbox, api_key.strip(), rings_plot, rings_reference, 12)

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
    rings_plot: list[list[tuple[float, float]]],
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
    rings_reference: list[list[tuple[float, float]]] | None = None,
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
                    try:
                        mosaic.paste(tile, (ox, oy))
                    finally:
                        try:
                            tile.close()
                        except Exception:
                            pass
    except Exception as exc:
        logger.exception("Falha ao montar mosaic Esri para preview id=%s: %s", log_entity_id, exc)
        mosaic.close()
        return None

    crop_x0 = int(wx0 - tx0 * TILE_SIZE)
    crop_y0 = int(wy0 - ty0 * TILE_SIZE)
    crop_x1 = int(wx1 - tx0 * TILE_SIZE)
    crop_y1 = int(wy1 - ty0 * TILE_SIZE)
    crop_x0 = max(0, min(crop_x0, mosaic_w - 1))
    crop_y0 = max(0, min(crop_y0, mosaic_h - 1))
    crop_x1 = max(crop_x0 + 1, min(crop_x1, mosaic_w))
    crop_y1 = max(crop_y0 + 1, min(crop_y1, mosaic_h))

    raw_crop: Image.Image | None = None
    cropped: Image.Image | None = None
    overlay: Image.Image | None = None
    base: Image.Image | None = None
    composed: Image.Image | None = None
    try:
        raw_crop = mosaic.crop((crop_x0, crop_y0, crop_x1, crop_y1))
        mosaic.close()
        try:
            resample = Image.Resampling.LANCZOS
        except AttributeError:
            resample = Image.LANCZOS
        cropped = raw_crop.resize((OUTPUT_WIDTH, OUTPUT_HEIGHT), resample)
        raw_crop.close()
        raw_crop = None

        crop_w = crop_x1 - crop_x0
        crop_h = crop_y1 - crop_y0

        overlay = Image.new("RGBA", (OUTPUT_WIDTH, OUTPUT_HEIGHT), (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        rings_reference = rings_reference or []

        def _project_ring(ring: list[tuple[float, float]]) -> list[tuple[float, float]]:
            flat: list[tuple[float, float]] = []
            for lon, lat in ring:
                wx, wy = _lonlat_to_world_px(lon, lat, z)
                px = (wx - wx0) / crop_w * OUTPUT_WIDTH
                py = (wy - wy0) / crop_h * OUTPUT_HEIGHT
                flat.append((px, py))
            return flat

        for ring in rings_reference:
            flat = _project_ring(ring)
            if len(flat) < 3:
                continue
            draw.polygon(
                flat,
                fill=(91, 179, 74, 55),
                outline=(65, 132, 54, 220),
                width=2,
            )

        for ring in rings_plot:
            flat = _project_ring(ring)
            if len(flat) < 3:
                continue
            if rings_reference:
                draw.polygon(
                    flat,
                    fill=(249, 115, 22, 120),
                    outline=(234, 88, 12, 255),
                    width=3,
                )
            else:
                draw.polygon(
                    flat,
                    fill=(91, 179, 74, 95),
                    outline=(65, 132, 54, 255),
                    width=3,
                )

        base = cropped.convert("RGBA")
        composed = Image.alpha_composite(base, overlay)
        return composed.convert("RGB")
    finally:
        for img in (raw_crop, cropped, overlay, base, composed):
            if img is not None:
                try:
                    img.close()
                except Exception:
                    pass


def build_satellite_preview_from_geojson(
    boundary_geojson: str,
    *,
    log_entity_id: int = 0,
    reference_boundary_geojson: str | None = None,
) -> Image.Image | None:
    """
    Monta imagem RGB (satélite + polígonos) a partir de GeoJSON.
    `boundary_geojson` é o perímetro principal (setor ou fazenda).
    `reference_boundary_geojson` opcional: perímetro da fazenda (verde), desenhado por baixo do principal.
    Não grava em disco. Usado por previews de fazenda e de setor.
    """
    text = (boundary_geojson or "").strip()
    if not text:
        return None
    geometry = _parse_geometry(text)
    if not geometry:
        logger.warning("GeoJSON invalido para preview geografico id=%s", log_entity_id)
        return None
    rings_plot = _exterior_rings(geometry)
    if not rings_plot:
        return None

    rings_reference: list[list[tuple[float, float]]] = []
    ref_raw = (reference_boundary_geojson or "").strip()
    if ref_raw:
        gref = _parse_geometry(ref_raw)
        if gref:
            rings_reference = _exterior_rings(gref)

    all_rings = rings_reference + rings_plot
    bbox = _rings_bbox(all_rings)
    if not bbox:
        return None
    lon_min, lat_min, lon_max, lat_max = bbox
    final_img: Image.Image | None = None
    api_key = (get_settings().google_maps_api_key or "").strip()
    if api_key:
        final_img = _try_google_static_preview(rings_plot, bbox, api_key, rings_reference)
        if final_img is not None and log_entity_id:
            logger.info("Preview id=%s gerado via Google Static Maps", log_entity_id)
    if final_img is None:
        final_img = _try_esri_tile_preview(
            log_entity_id, rings_plot, lon_min, lat_min, lon_max, lat_max, rings_reference
        )
        if final_img is not None and api_key and log_entity_id:
            logger.info("Preview id=%s gerado via fallback Esri", log_entity_id)
        elif final_img is not None and log_entity_id:
            logger.info("Preview id=%s gerado via Esri", log_entity_id)
    return final_img


def generate_farm_preview_image(farm_id: int, boundary_geojson: str | None) -> bool:
    """
    Gera PNG e miniatura JPEG em disco e persiste no banco (farm_preview_cache).
    Executa sob semáforo global (1 geração por vez) e fecha todos os buffers Pillow.
    """
    text = (boundary_geojson or "").strip()
    if not text:
        remove_farm_preview_image(farm_id)
        return False

    geo_hash = farm_geojson_hash(text)
    with _PREVIEW_SEM:
        final_img = build_satellite_preview_from_geojson(text, log_entity_id=farm_id)
        if final_img is None:
            return False

        PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
        final_path = farm_preview_fs_path(farm_id)
        thumb_path = farm_preview_thumb_fs_path(farm_id)
        tmp_path: Path | None = None
        png_bytes: bytes | None = None
        jpg_bytes: bytes | None = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False, dir=PREVIEW_DIR) as tmp:
                tmp_path = Path(tmp.name)
            final_img.save(tmp_path, format="PNG", optimize=True)
            tmp_path.replace(final_path)
            png_bytes = final_path.read_bytes()

            if save_preview_thumbnail_at(final_img, thumb_path) and thumb_path.is_file():
                jpg_bytes = thumb_path.read_bytes()
            else:
                logger.warning("Preview PNG salvo mas miniatura JPEG falhou para fazenda %s", farm_id)
        except OSError as exc:
            logger.exception("Falha ao salvar preview da fazenda %s: %s", farm_id, exc)
            if tmp_path is not None:
                try:
                    tmp_path.unlink(missing_ok=True)
                except OSError:
                    pass
            return False
        finally:
            try:
                final_img.close()
            except Exception:
                pass
            del final_img
            gc.collect()

        _save_preview_cache_db(farm_id, geo_hash, png_bytes, jpg_bytes)
        del png_bytes, jpg_bytes
        return True


def schedule_farm_preview_generation(farm_id: int, boundary_geojson: str | None) -> bool:
    """
    Entrada segura para BackgroundTasks (roda no threadpool fora do event loop):
    deduplica tarefas em andamento por (farm_id, hash) e limita concorrência via _PREVIEW_SEM.
    """
    text = (boundary_geojson or "").strip()
    if not text:
        remove_farm_preview_image(farm_id)
        return False
    key = (int(farm_id), farm_geojson_hash(text))
    with _IN_FLIGHT_LOCK:
        if key in _IN_FLIGHT_KEYS:
            return False
        _IN_FLIGHT_KEYS.add(key)
    try:
        return generate_farm_preview_image(farm_id, text)
    finally:
        with _IN_FLIGHT_LOCK:
            _IN_FLIGHT_KEYS.discard(key)
