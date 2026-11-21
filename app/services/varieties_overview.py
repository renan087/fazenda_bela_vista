"""Números da página Variedades: onde cada variedade está plantada e como produziu."""

from __future__ import annotations

import unicodedata
from datetime import date

from app.models.crop_season import CropSeason
from app.models.harvest import HarvestRecord

MONTHS = ["Abr", "Mai", "Jun", "Jul", "Ago", "Set"]
CYCLES = [("Precoce", "pre"), ("Médio", "med"), ("Médio-tardio", "mtar"), ("Tardio", "tar")]
WINDOWS = {"pre": (0, 1), "med": (1, 2), "mtar": (2, 3), "tar": (3, 4)}
HEIGHTS = ["Baixo", "Alto"]
RUSTS = [("Resistente", "res"), ("Moderada", "mod"), ("Suscetível", "sus")]
SPECIES = ["Arábica", "Canéfora"]
FLAVOR_TAGS = ["Doce", "Chocolate", "Caramelo", "Frutado", "Cítrico", "Floral", "Nozes", "Encorpado"]
PALETTE = [
    ("#B4462F", "Terracota"), ("#2F6F8F", "Azul petróleo"), ("#C99A1E", "Dourado"), ("#7A5BA6", "Roxo"),
    ("#5E8C3A", "Verde-folha"), ("#1F8A7A", "Verde-água"), ("#C2477F", "Rosa"), ("#3B4FA3", "Anil"), ("#8A6F5A", "Castanho"),
]
# Características gerais das cultivares mais plantadas; o produtor confere e ajusta antes de salvar.
KNOWN_CULTIVARS = [
    {"name": "Catuaí Vermelho IAC 144", "species": "Arábica", "cycle": "Médio-tardio", "height": "Baixo", "rust": "Suscetível", "tags": ["Doce", "Chocolate"]},
    {"name": "Catuaí Amarelo IAC 62", "species": "Arábica", "cycle": "Médio", "height": "Baixo", "rust": "Suscetível", "tags": ["Doce", "Caramelo"]},
    {"name": "Mundo Novo IAC 379-19", "species": "Arábica", "cycle": "Médio-tardio", "height": "Alto", "rust": "Suscetível", "tags": ["Nozes"]},
    {"name": "Catucaí Amarelo 2SL", "species": "Arábica", "cycle": "Médio", "height": "Baixo", "rust": "Moderada", "tags": ["Doce", "Caramelo"]},
    {"name": "Arara", "species": "Arábica", "cycle": "Tardio", "height": "Baixo", "rust": "Resistente", "tags": ["Frutado", "Cítrico"]},
    {"name": "Paraíso MG H 419-1", "species": "Arábica", "cycle": "Médio", "height": "Baixo", "rust": "Resistente", "tags": ["Doce", "Frutado"]},
    {"name": "Obatã IAC 1669-20", "species": "Arábica", "cycle": "Tardio", "height": "Baixo", "rust": "Resistente", "tags": ["Encorpado", "Chocolate"]},
    {"name": "Bourbon Amarelo", "species": "Arábica", "cycle": "Precoce", "height": "Alto", "rust": "Suscetível", "tags": ["Doce", "Floral"]},
    {"name": "Conilon BRS Ouro Preto", "species": "Canéfora", "cycle": "Médio-tardio", "height": "Alto", "rust": "Resistente", "tags": ["Encorpado"]},
]


def _plain(value) -> str:
    text = unicodedata.normalize("NFD", str(value or "")).encode("ascii", "ignore").decode()
    return text.lower().strip()


def cycle_key(value) -> str:
    text = _plain(value)
    if "precoce" in text:
        return "pre"
    if "tard" in text:
        return "mtar" if ("medi" in text or "media" in text) else "tar"
    return "med"


def rust_key(value) -> str | None:
    text = _plain(value)
    if not text:
        return None
    if text.startswith("resist"):
        return "res"
    if text.startswith("moder"):
        return "mod"
    return "sus"


def plot_variety_id(plot) -> int | None:
    planting = getattr(plot, "active_planting", None)
    return (planting.variety_id if planting and planting.variety_id else None) or getattr(plot, "variety_id", None)


NO_VARIETY_COLOR = "#9AA59E"


def variety_colors(varieties) -> dict[int, str]:
    """Cor de cada variedade: a salva no cadastro ou, sem ela, uma livre da paleta (mesma regra em Variedades e no Mapa)."""
    used = {v.map_color for v in varieties if v.map_color}
    free = [c for c, _ in PALETTE if c not in used] or [c for c, _ in PALETTE]
    return {v.id: v.map_color or free[index % len(free)] for index, v in enumerate(varieties)}


def _planting_value(plot, attr):
    planting = getattr(plot, "active_planting", None)
    value = getattr(planting, attr, None) if planting else None
    return value if value not in (None, 0) else getattr(plot, attr, None)


def _num(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def br_number(value: float, digits: int = 1) -> str:
    text = f"{value:,.{digits}f}"
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def br_plants(value: int) -> str:
    if value >= 10000:
        return f"{br_number(value / 1000)} mil"
    return br_number(value, 0)


def _spacing(plot) -> str | None:
    row, plant = _planting_value(plot, "spacing_row_meters"), _planting_value(plot, "spacing_plant_meters")
    if row and plant:
        return f"{br_number(_num(row))} × {br_number(_num(plant))} m"
    return None


def _season_label(season) -> str:
    start, end = getattr(season, "start_date", None), getattr(season, "end_date", None)
    if start and end and start.year != end.year:
        return f"{start.year % 100:02d}/{end.year % 100:02d}"
    return season.name


def build_varieties_overview(db, varieties, plots, active_season=None, today: date | None = None) -> dict:
    today = today or date.today()
    by_plot = {plot.id: plot for plot in plots}
    season_sacks: dict[int, dict[int, float]] = {}
    if by_plot:
        rows = (
            db.query(HarvestRecord.plot_id, HarvestRecord.season_id, HarvestRecord.sacks_produced)
            .filter(HarvestRecord.plot_id.in_(list(by_plot)), HarvestRecord.season_id.isnot(None))
            .all()
        )
        for plot_id, season_id, sacks in rows:
            season_sacks.setdefault(season_id, {}).setdefault(plot_id, 0.0)
            season_sacks[season_id][plot_id] += _num(sacks)
    seasons = []
    if season_sacks:
        seasons = (
            db.query(CropSeason).filter(CropSeason.id.in_(list(season_sacks))).order_by(CropSeason.start_date.asc()).all()
        )[-3:]
    total_area = sum(_num(p.area_hectares) for p in plots)
    farm_yield = {}
    for season in seasons:
        sacks = sum(season_sacks[season.id].values())
        farm_yield[season.id] = round(sacks / total_area, 1) if total_area else None
    last_season = seasons[-1] if seasons else None
    active_plot_ids = {p.id for p in getattr(active_season, "plots", None) or []}

    colors = variety_colors(varieties)
    items = []
    for variety in varieties:
        color = colors[variety.id]
        vplots = sorted((p for p in plots if plot_variety_id(p) == variety.id), key=lambda p: p.name or "")
        area = sum(_num(p.area_hectares) for p in vplots)
        plants = int(sum(_num(_planting_value(p, "plant_count")) for p in vplots))
        ages = [
            (today - d).days / 365.25 for p in vplots if (d := _planting_value(p, "planting_date")) and d <= today
        ]
        history = []
        for season in seasons:
            sacks = sum(season_sacks[season.id].get(p.id, 0.0) for p in vplots)
            value = round(sacks / area, 1) if area and sacks else None
            history.append({"label": _season_label(season), "value": value, "farm": farm_yield.get(season.id), "sacks": sacks})
        last = history[-1] if history else None
        last_yield = last["value"] if last else None
        farm_last = last["farm"] if last else None
        key = cycle_key(variety.maturation_cycle)
        start, end = WINDOWS[key]
        estimate = sum(_num(_planting_value(p, "estimated_yield_sacks")) for p in vplots)
        items.append({
            "variety": variety,
            "id": variety.id,
            "color": color,
            "cycle_key": key,
            "cycle_label": variety.maturation_cycle or "Médio",
            "window": f"{MONTHS[start].lower()}–{MONTHS[end].lower()}",
            "cells": [start <= i <= end for i in range(len(MONTHS))],
            "rust_key": rust_key(variety.rust_resistance),
            "tags": [t.strip() for t in (variety.flavor_profile or "").split(",") if t.strip()],
            "used": bool(vplots),
            "plots": [
                {
                    "plot": p,
                    "area": br_number(_num(p.area_hectares)),
                    "plants": br_number(_num(_planting_value(p, "plant_count")), 0),
                    "year": (d.year if (d := _planting_value(p, "planting_date")) else None),
                    "spacing": _spacing(p),
                    "farm_name": getattr(p.farm, "name", None),
                }
                for p in vplots
            ],
            "area": area,
            "area_label": br_number(area),
            "plants": plants,
            "plants_label": br_plants(plants),
            "age_label": (f"{round(sum(ages) / len(ages))} anos" if ages else "—"),
            "history": history,
            "last_label": last["label"] if last else None,
            "last_yield": last_yield,
            "last_sacks": br_number(last["sacks"], 0) if last else None,
            "delta": (round(last_yield - farm_last, 1) if last_yield is not None and farm_last is not None else None),
            "estimate": br_number(estimate, 0) if estimate else None,
            "in_active_season": sum(1 for p in vplots if p.id in active_plot_ids),
            "search": " ".join(filter(None, [variety.name, variety.species, variety.maturation_cycle, variety.flavor_profile, variety.notes])),
        })

    used = [i for i in items if i["used"]]
    max_yield = max([h["value"] or 0 for i in used for h in i["history"]] + [h["farm"] or 0 for i in used for h in i["history"]] + [1])
    for item in items:
        for h in item["history"]:
            h["bar_pct"] = round((h["value"] or 0) / max_yield * 100)
            h["farm_pct"] = round((h["farm"] or 0) / max_yield * 100)
    ranked = [i for i in used if i["last_yield"] is not None]
    best = max(ranked, key=lambda i: i["last_yield"]) if ranked else None
    plants_total = sum(i["plants"] for i in items)
    return {
        "items": items,
        "months": MONTHS,
        "total": len(items),
        "used_count": len(used),
        "unused_count": len(items) - len(used),
        "area_label": br_number(sum(i["area"] for i in used)),
        "plot_count": sum(len(i["plots"]) for i in used),
        "plants_label": br_plants(plants_total),
        "plants_per_ha": br_number(plants_total / total_area, 0) if total_area and plants_total else None,
        "best": best,
        "farm_last": (farm_yield.get(last_season.id) if last_season else None),
        "last_label": _season_label(last_season) if last_season else None,
        "segments": [
            {"item": i, "pct": round(i["area"] / sum(u["area"] for u in used) * 100) if sum(u["area"] for u in used) else 0}
            for i in sorted(used, key=lambda i: -i["area"])
            if i["area"] > 0
        ],
        "calendar": sorted(used, key=lambda i: WINDOWS[i["cycle_key"]]),
    }


def color_options(items, current_id=None) -> list[dict]:
    """Cores do mapa; as que já aparecem para outra variedade (salvas ou automáticas) vêm marcadas."""
    owners = {}
    for item in items:
        if item["id"] != current_id:
            owners.setdefault(item["color"], item["variety"].name)
    return [{"value": c, "label": label, "taken_by": owners.get(c)} for c, label in PALETTE]
