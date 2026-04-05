"""Gera imagem estática (satélite + contorno) para prévia de setor no servidor."""

from __future__ import annotations

import hashlib
import logging
import secrets
import tempfile
from pathlib import Path

from PIL import Image

from app.services.farm_preview_image import (
    build_satellite_preview_from_geojson,
    save_preview_thumbnail_at,
)

logger = logging.getLogger(__name__)

_APP_DIR = Path(__file__).resolve().parents[1]
PLOT_PREVIEW_DIR = _APP_DIR / "static" / "generated" / "plot_previews"


def plot_preview_fs_path(plot_id: int) -> Path:
    return PLOT_PREVIEW_DIR / f"{plot_id}.png"


def plot_preview_thumb_fs_path(plot_id: int) -> Path:
    return PLOT_PREVIEW_DIR / f"{plot_id}_thumb.jpg"


def plot_preview_draft_fs_path(plot_id: int) -> Path:
    return PLOT_PREVIEW_DIR / f"{plot_id}_draft.png"


def remove_plot_preview_draft(plot_id: int) -> None:
    try:
        plot_preview_draft_fs_path(plot_id).unlink(missing_ok=True)
    except OSError as exc:
        logger.warning("Nao foi possivel remover preview-rascunho do setor %s: %s", plot_id, exc)


def remove_plot_preview_image(plot_id: int) -> None:
    remove_plot_preview_draft(plot_id)
    for path in (plot_preview_fs_path(plot_id), plot_preview_thumb_fs_path(plot_id)):
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            logger.warning("Nao foi possivel remover preview do setor %s (%s): %s", plot_id, path.name, exc)


def ensure_plot_preview_thumb(plot_id: int) -> bool:
    png_path = plot_preview_fs_path(plot_id)
    if not png_path.is_file() or png_path.stat().st_size <= 0:
        return False
    try:
        with Image.open(png_path) as im:
            rgb = im.convert("RGB")
            return save_preview_thumbnail_at(rgb, plot_preview_thumb_fs_path(plot_id))
    except OSError as exc:
        logger.warning("Nao foi possivel abrir PNG do preview do setor %s: %s", plot_id, exc)
        return False


def generate_plot_preview_image(
    plot_id: int,
    boundary_geojson: str | None,
    farm_boundary_geojson: str | None = None,
) -> bool:
    if not boundary_geojson or not boundary_geojson.strip():
        remove_plot_preview_image(plot_id)
        return False
    farm_ref = (farm_boundary_geojson or "").strip() or None
    final_img = build_satellite_preview_from_geojson(
        boundary_geojson.strip(),
        log_entity_id=plot_id,
        reference_boundary_geojson=farm_ref,
    )
    if final_img is None:
        return False
    PLOT_PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    final_path = plot_preview_fs_path(plot_id)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False, dir=PLOT_PREVIEW_DIR) as tmp:
            tmp_path = Path(tmp.name)
        final_img.save(tmp_path, format="PNG", optimize=True)
        tmp_path.replace(final_path)
    except OSError as exc:
        logger.exception("Falha ao salvar preview do setor %s: %s", plot_id, exc)
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        return False
    if not save_preview_thumbnail_at(final_img.copy(), plot_preview_thumb_fs_path(plot_id)):
        logger.warning("Preview PNG do setor %s salvo mas miniatura JPEG falhou", plot_id)
    return True


def generate_plot_preview_draft(
    plot_id: int,
    boundary_geojson: str | None,
    farm_boundary_geojson: str | None = None,
) -> bool:
    """Grava PNG de prévia temporário ({id}_draft.png), sem alterar a miniatura oficial da lista."""
    if not boundary_geojson or not boundary_geojson.strip():
        remove_plot_preview_draft(plot_id)
        return False
    farm_ref = (farm_boundary_geojson or "").strip() or None
    final_img = build_satellite_preview_from_geojson(
        boundary_geojson.strip(),
        log_entity_id=plot_id,
        reference_boundary_geojson=farm_ref,
    )
    if final_img is None:
        return False
    PLOT_PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    draft_path = plot_preview_draft_fs_path(plot_id)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False, dir=PLOT_PREVIEW_DIR) as tmp:
            tmp_path = Path(tmp.name)
        final_img.save(tmp_path, format="PNG", optimize=True)
        tmp_path.replace(draft_path)
    except OSError as exc:
        logger.exception("Falha ao salvar preview-rascunho do setor %s: %s", plot_id, exc)
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        return False
    return True


def generate_plot_geometry_session_preview(
    boundary_geojson: str,
    farm_boundary_geojson: str | None,
) -> tuple[str | None, str | None]:
    """
    PNG temporário para pré-visualizar geometria antes de existir setor (cadastro novo).
    Retorna (URL path a partir de /static/..., revision) ou (None, None).
    """
    raw = (boundary_geojson or "").strip()
    if not raw:
        return None, None
    farm_ref = (farm_boundary_geojson or "").strip() or None
    final_img = build_satellite_preview_from_geojson(
        raw,
        log_entity_id=0,
        reference_boundary_geojson=farm_ref,
    )
    if final_img is None:
        return None, None
    token = secrets.token_hex(16)
    PLOT_PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    rel_name = f"_sess_{token}.png"
    final_path = PLOT_PREVIEW_DIR / rel_name
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False, dir=PLOT_PREVIEW_DIR) as tmp:
            tmp_path = Path(tmp.name)
        final_img.save(tmp_path, format="PNG", optimize=True)
        tmp_path.replace(final_path)
    except OSError as exc:
        logger.exception("Falha ao salvar preview de sessao de geometria: %s", exc)
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        return None, None
    revision = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:14]
    return f"/static/generated/plot_previews/{rel_name}", revision
