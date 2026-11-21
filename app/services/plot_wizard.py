"""Regras do assistente de setor: cor no mapa, grupo, tipo de solo e limite pelo perímetro da fazenda."""

from __future__ import annotations

import json
import re

# Mesmas cores do mapa de fazendas (contorno, preenchimento) + 6 para fazendas com muitos setores.
PLOT_COLORS: list[tuple[str, str, str]] = [
    ("Azul", "#0369a1", "#38bdf8"),
    ("Roxo", "#6d28d9", "#a78bfa"),
    ("Âmbar", "#b45309", "#fbbf24"),
    ("Verde-água", "#0f766e", "#2dd4bf"),
    ("Anil", "#1d4ed8", "#60a5fa"),
    ("Magenta", "#a21caf", "#e879f9"),
    ("Rosa", "#be123c", "#fb7185"),
    ("Laranja", "#c2410c", "#fb923c"),
    ("Lima", "#4d7c0f", "#a3e635"),
    ("Ciano", "#0e7490", "#22d3ee"),
    ("Terra", "#7c2d12", "#d6a16b"),
    ("Ardósia", "#334155", "#94a3b8"),
]
SOIL_TEXTURES = {"argiloso": "Argiloso", "medio": "Médio", "arenoso": "Arenoso", "nao_sei": "Não sei"}
_HEX = re.compile(r"^#[0-9a-fA-F]{6}$")
# ~3 m em graus: margem para pontos colocados exatamente na divisa.
_EDGE_TOLERANCE = 3e-5


def normalize_color(value: str | None) -> str | None:
    value = (value or "").strip()
    return value.lower() if _HEX.match(value) else None


def normalize_group(value: str | None) -> str | None:
    value = " ".join((value or "").split())[:80]
    return value or None


def normalize_soil(value: str | None) -> str | None:
    return value if value in SOIL_TEXTURES else None


def _points(geometry) -> list[tuple[float, float]]:
    """Primeiro anel do polígono como (lng, lat)."""
    if isinstance(geometry, str):
        try:
            geometry = json.loads(geometry)
        except (TypeError, ValueError):
            return []
    if not isinstance(geometry, dict):
        return []
    if geometry.get("type") == "Feature":
        return _points(geometry.get("geometry"))
    if geometry.get("type") == "FeatureCollection":
        features = geometry.get("features") or []
        return _points(features[0]) if features else []
    coords = geometry.get("coordinates") or []
    if geometry.get("type") == "MultiPolygon" and coords:
        coords = coords[0]
    if geometry.get("type") in {"Polygon", "MultiPolygon"} and coords and coords[0]:
        return [(float(x), float(y)) for x, y, *_ in coords[0]]
    return []


def _inside(pt, ring) -> bool:
    x, y = pt
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / ((yj - yi) or 1e-12) + xi:
            inside = not inside
        j = i
    return inside


def _edge_distance(pt, ring) -> float:
    x, y = pt
    best = float("inf")
    for i in range(len(ring)):
        (ax, ay), (bx, by) = ring[i], ring[(i + 1) % len(ring)]
        dx, dy = bx - ax, by - ay
        length = dx * dx + dy * dy
        t = max(0.0, min(1.0, ((x - ax) * dx + (y - ay) * dy) / length)) if length else 0.0
        best = min(best, ((ax + t * dx - x) ** 2 + (ay + t * dy - y) ** 2) ** 0.5)
    return best


def geometry_outside_farm(plot_geometry, farm_geometry) -> bool:
    """True quando algum canto do setor passa do perímetro da fazenda. Sem perímetro da fazenda, nunca bloqueia."""
    ring = _points(farm_geometry)
    plot_pts = _points(plot_geometry)
    if len(ring) < 3 or not plot_pts:
        return False
    return any(not _inside(pt, ring) and _edge_distance(pt, ring) > _EDGE_TOLERANCE for pt in plot_pts)


def _bbox(points):
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return min(xs), min(ys), max(xs), max(ys)


def pick_auto_color(other_plots, geometry=None) -> str:
    """Cor de preenchimento que não repete a dos setores vizinhos; sem vizinhos, a menos usada na fazenda."""
    fills = [fill for _, _, fill in PLOT_COLORS]
    used: dict[str, int] = {fill: 0 for fill in fills}
    neighbor_colors: set[str] = set()
    own = _points(geometry)
    own_box = _bbox(own) if own else None
    for index, plot in enumerate(other_plots):
        color = (getattr(plot, "map_color", None) or "").lower() or fills[index % len(fills)]
        if color in used:
            used[color] += 1
        pts = _points(getattr(plot, "boundary_geojson", None))
        if own_box and pts:
            x1, y1, x2, y2 = _bbox(pts)
            pad = 0.002  # ~200 m
            if x1 - pad <= own_box[2] and own_box[0] <= x2 + pad and y1 - pad <= own_box[3] and own_box[1] <= y2 + pad:
                neighbor_colors.add(color)
    candidates = [fill for fill in fills if fill not in neighbor_colors] or fills
    return min(candidates, key=lambda fill: (used[fill], fills.index(fill)))


def stroke_for(fill: str | None) -> str | None:
    for _, stroke, color in PLOT_COLORS:
        if color == fill:
            return stroke
    return fill


def wizard_color_context(plots) -> list[dict]:
    """Cores e caixas dos setores existentes, para o navegador escolher a cor automática como o servidor."""
    fills = [fill for _, _, fill in PLOT_COLORS]
    rows = []
    for index, plot in enumerate(plots):
        pts = _points(getattr(plot, "boundary_geojson", None))
        rows.append({
            "id": plot.id,
            "name": plot.name,
            "farm_id": plot.farm_id,
            "fill": (plot.map_color or "").lower() or fills[index % len(fills)],
            "box": list(_bbox(pts)) if pts else None,
        })
    return rows
