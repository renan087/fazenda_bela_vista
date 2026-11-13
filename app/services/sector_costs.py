"""Consolidacao de custos operacionais com vinculo rastreavel ao setor."""

from __future__ import annotations

from datetime import date
import unicodedata


def _number(value) -> float:
    return float(value or 0)


def _normalized(value: str | None) -> str:
    return (
        unicodedata.normalize("NFD", str(value or ""))
        .encode("ascii", "ignore")
        .decode("ascii")
        .strip()
        .lower()
    )


def _within_period(value: date | None, start_date: date | None, end_date: date | None) -> bool:
    if not value:
        return False
    return (not start_date or value >= start_date) and (not end_date or value <= end_date)


def build_sector_cost_context(
    *,
    plots: list,
    stock_outputs: list,
    fertilizations: list,
    season_id: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict:
    """Calcula somente custos efetivamente vinculados a um setor.

    Saidas de estoque representam os insumos/combustiveis consumidos. O campo
    ``FertilizationRecord.cost`` ja e a soma dessas mesmas saidas; por isso os
    registros de fertilizacao entram somente como indicador operacional e nao
    como uma segunda fonte financeira.
    """
    plot_ids = {plot.id for plot in plots}
    rows: dict[int, dict] = {}
    for plot in plots:
        rows[plot.id] = {
            "plot": plot,
            "area_hectares": _number(plot.area_hectares),
            "inputs_cost": 0.0,
            "fuel_cost": 0.0,
            "stock_movements": 0,
            "applications": 0,
        }

    unallocated_cost = 0.0
    unallocated_movements = 0
    for output in stock_outputs:
        if season_id is not None and output.season_id != season_id:
            continue
        if not _within_period(output.movement_date, start_date, end_date):
            continue
        if "ajuste" in _normalized(output.origin):
            continue
        amount = _number(output.total_cost)
        if amount <= 0:
            continue
        row = rows.get(output.plot_id)
        if row is None:
            unallocated_cost += amount
            unallocated_movements += 1
            continue
        item_type = _normalized(getattr(output.input_catalog, "item_type", None))
        if item_type == "combustivel":
            row["fuel_cost"] += amount
        else:
            row["inputs_cost"] += amount
        row["stock_movements"] += 1

    for record in fertilizations:
        if record.plot_id not in plot_ids:
            continue
        if season_id is not None and record.season_id != season_id:
            continue
        if not _within_period(record.application_date, start_date, end_date):
            continue
        row = rows[record.plot_id]
        row["applications"] += 1

    sector_rows = []
    for row in rows.values():
        total = round(row["inputs_cost"] + row["fuel_cost"], 2)
        area = row["area_hectares"]
        sector_rows.append(
            {
                **row,
                "inputs_cost": round(row["inputs_cost"], 2),
                "fuel_cost": round(row["fuel_cost"], 2),
                "total_cost": total,
                "cost_per_hectare": round(total / area, 2) if area else 0.0,
            }
        )

    sector_rows.sort(key=lambda row: (-row["total_cost"], (row["plot"].name or "").lower()))
    allocated_total = round(sum(row["total_cost"] for row in sector_rows), 2)
    total_area = round(sum(row["area_hectares"] for row in sector_rows), 2)
    total_cost = round(allocated_total + unallocated_cost, 2)
    return {
        "rows": sector_rows,
        "allocated_total": allocated_total,
        "unallocated_total": round(unallocated_cost, 2),
        "unallocated_movements": unallocated_movements,
        "total_cost": total_cost,
        "total_area": total_area,
        "cost_per_hectare": round(allocated_total / total_area, 2) if total_area else 0.0,
        "inputs_total": round(sum(row["inputs_cost"] for row in sector_rows), 2),
        "fuel_total": round(sum(row["fuel_cost"] for row in sector_rows), 2),
        "application_total": 0.0,
    }
