"""Metas da safra (produção esperada, orçamento e preço) e regras do assistente de safras."""

from __future__ import annotations

import json
from datetime import date

from app.models.harvest import HarvestRecord

BUDGET_CATEGORIES: list[tuple[str, str, str]] = [
    ("adubacao", "Adubação e corretivos", "Programa de adubação"),
    ("defensivos", "Defensivos", "Pulverizações"),
    ("mao_obra", "Mão de obra", "Equipe e diárias"),
    ("colheita", "Colheita e pós-colheita", "Colheita, secagem, beneficiamento"),
    ("outros", "Outros", "Energia, frete, manutenção"),
]
_CATEGORY_KEYS = {key for key, _, _ in BUDGET_CATEGORIES}
FORMATION_YEARS = 2.5
_MAX_YIELD = 500.0
_MAX_MONEY = 10_000_000.0


def _num(value, limit: float) -> float | None:
    try:
        number = float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None
    if number != number or number < 0:  # NaN ou negativo
        return None
    return round(min(number, limit), 2)


def parse_goals(raw: str | None, plot_ids: set[int]) -> dict | None:
    """Valida o JSON vindo do assistente. Ignora setores fora da safra e categorias desconhecidas."""
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    yields: dict[str, float] = {}
    for key, value in (data.get("yields") or {}).items():
        plot_id = int(key) if str(key).isdigit() else None
        number = _num(value, _MAX_YIELD)
        if plot_id in plot_ids and number is not None:
            yields[str(plot_id)] = number
    budget: dict[str, float] = {}
    for key, value in (data.get("budget") or {}).items():
        number = _num(value, _MAX_MONEY)
        if key in _CATEGORY_KEYS and number is not None:
            budget[key] = number
    price = _num(data.get("price"), _MAX_MONEY)
    if not yields and not budget and not price:
        return None
    return {"yields": yields, "budget": budget, "price": price or None}


def load_goals(season) -> dict:
    try:
        data = json.loads(season.goals_json) if season and season.goals_json else {}
    except (TypeError, ValueError):
        data = {}
    return data if isinstance(data, dict) else {}


def goals_summary(season) -> dict | None:
    """Números das metas para mostrar no painel da safra."""
    goals = load_goals(season)
    if not goals:
        return None
    plots = {plot.id: plot for plot in season.plots}
    area = sum(float(plot.area_hectares or 0) for plot in plots.values())
    sacks = sum(float(plots[int(pid)].area_hectares or 0) * float(value) for pid, value in (goals.get("yields") or {}).items() if int(pid) in plots)
    productive_area = sum(float(plots[int(pid)].area_hectares or 0) for pid, value in (goals.get("yields") or {}).items() if int(pid) in plots and float(value) > 0)
    budget_per_ha = sum(float(value) for value in (goals.get("budget") or {}).values())
    budget_total = budget_per_ha * area
    price = float(goals.get("price") or 0)
    return {
        "sacks": round(sacks, 1),
        "budget_per_ha": round(budget_per_ha, 2),
        "budget_total": round(budget_total, 2),
        "price": price,
        "revenue": round(sacks * price, 2) if price else None,
        "margin": round(sacks * price - budget_total, 2) if price else None,
        "breakeven": round(budget_total / price / productive_area, 1) if price and productive_area else None,
    }


def status_from_dates(start: date, end: date, today: date, current: str | None = None) -> str:
    """Situação calculada pelas datas. 'Encerrada' só sai pelo encerramento, nunca é desfeita aqui."""
    if current == "encerrada":
        return "encerrada"
    if today < start:
        return "planejada"
    if today > end:
        return "encerrada"
    return "ativa"


def coffee_year_options(today: date) -> list[dict]:
    """Ano-safra do café (julho a junho): atual e próximo."""
    first = today.year if today.month >= 7 else today.year - 1
    options = []
    for offset, tag in ((0, "Ano-safra atual"), (1, "Próximo ano-safra")):
        year = first + offset
        options.append({
            "key": "atual" if offset == 0 else "proxima",
            "label": f"{year}/{str(year + 1)[-2:]}",
            "tag": tag,
            "start": date(year, 7, 1).isoformat(),
            "end": date(year + 1, 6, 30).isoformat(),
            "range": f"jul/{year} a jun/{year + 1}",
        })
    return options


def plot_stage(plot, today: date) -> str:
    """'prod' (em produção), 'form' (em formação) ou 'sem' (sem plantio informado)."""
    planting = getattr(plot, "active_planting", None)
    planting_date = getattr(planting, "planting_date", None) or getattr(plot, "planting_date", None)
    if not planting_date:
        return "sem"
    years = (today - planting_date).days / 365.25
    return "form" if years < FORMATION_YEARS else "prod"


def plot_yield_history(db, plots) -> dict[int, float]:
    """Média de sacas por hectare das últimas 3 safras com colheita de cada setor."""
    by_id = {plot.id: plot for plot in plots}
    if not by_id:
        return {}
    rows = (
        db.query(HarvestRecord.plot_id, HarvestRecord.season_id, HarvestRecord.sacks_produced)
        .filter(HarvestRecord.plot_id.in_(list(by_id)), HarvestRecord.season_id.isnot(None))
        .all()
    )
    per_plot: dict[int, dict[int, float]] = {}
    for plot_id, season_id, sacks in rows:
        per_plot.setdefault(plot_id, {}).setdefault(season_id, 0.0)
        per_plot[plot_id][season_id] += float(sacks or 0)
    result: dict[int, float] = {}
    for plot_id, seasons in per_plot.items():
        area = float(by_id[plot_id].area_hectares or 0)
        recent = [value for _, value in sorted(seasons.items(), reverse=True)[:3] if value > 0]
        if area and recent:
            result[plot_id] = round(sum(recent) / len(recent) / area, 1)
    return result


def common_variety_id(plots) -> int | None:
    """Variedade da safra: só quando todos os setores têm a mesma (senão fica em branco)."""
    ids = set()
    for plot in plots:
        planting = getattr(plot, "active_planting", None)
        ids.add(getattr(planting, "variety_id", None) or plot.variety_id)
    ids.discard(None)
    return ids.pop() if len(ids) == 1 else None
