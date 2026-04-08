"""Resumo financeiro para a página de gestão (valores derivados dos módulos existentes)."""

from __future__ import annotations

from datetime import date

from app.models import CropSeason, PurchasedInput
from app.repositories.farm import FarmRepository

EXTRACT_MAX_ROWS = 500


def _f(value: object) -> float:
    return float(value or 0)


def _entry_inventory_value(entry: PurchasedInput) -> float:
    avail = _f(entry.available_quantity)
    if avail <= 0:
        return 0.0
    tot_q = _f(entry.total_quantity) or 1.0
    tot_c = _f(entry.total_cost)
    return avail * (tot_c / tot_q)


def _filter_entries_by_farm(entries: list[PurchasedInput], farm_id: int | None) -> list[PurchasedInput]:
    if not farm_id:
        return entries
    return [e for e in entries if e.farm_id in (None, farm_id)]


def _item_type(entry: PurchasedInput) -> str:
    if entry.input_catalog and entry.input_catalog.item_type:
        return str(entry.input_catalog.item_type)
    return "insumo"


def _season_bounds(season: CropSeason | None) -> tuple:
    if not season:
        return None, None
    return season.start_date, season.end_date


def _in_date_range(d, start, end) -> bool:
    if d is None:
        return False
    if start and d < start:
        return False
    if end and d > end:
        return False
    return True


def build_finance_overview_context(
    repo: FarmRepository,
    *,
    farm_id: int | None,
    active_season: CropSeason | None,
) -> dict:
    """Agrega totais ligados a compras, estoque, saídas, fertilização e patrimônio."""
    scope_ready = bool(farm_id and active_season)
    plots = (
        repo.list_plots(
            farm_ids=[farm_id] if farm_id else None,
            variety_ids=[active_season.variety_id] if active_season and active_season.variety_id else None,
        )
        if farm_id
        else []
    )
    plot_ids = {p.id for p in plots}
    season_start, season_end = _season_bounds(active_season)

    all_entries = repo.list_purchased_inputs()
    entries = _filter_entries_by_farm(all_entries, farm_id) if farm_id else []

    inv_insumo = 0.0
    inv_suprimento = 0.0
    cost_purchased_insumo = 0.0
    cost_purchased_suprimento = 0.0

    for entry in entries:
        it = _item_type(entry)
        val = _entry_inventory_value(entry)
        tc = _f(entry.total_cost)
        if it == "suprimento":
            inv_suprimento += val
            cost_purchased_suprimento += tc
        else:
            inv_insumo += val
            cost_purchased_insumo += tc

    stock_out_cost_season = 0.0
    if scope_ready and farm_id and active_season:
        sid = active_season.id
        for output in repo.list_stock_outputs(farm_id=farm_id):
            if output.plot_id:
                if output.plot_id not in plot_ids:
                    continue
            elif output.season_id and output.season_id != sid:
                continue
            if not _in_date_range(output.movement_date, season_start, season_end):
                continue
            stock_out_cost_season += _f(output.total_cost)

    fertilization_cost_season = 0.0
    if scope_ready and plot_ids:
        for rec in repo.list_fertilizations():
            if rec.plot_id not in plot_ids:
                continue
            if not _in_date_range(rec.application_date, season_start, season_end):
                continue
            fertilization_cost_season += _f(rec.cost)

    assets_total = 0.0
    if farm_id:
        for asset in repo.list_equipment_assets(farm_id=farm_id):
            assets_total += _f(asset.acquisition_value)

    inventory_total = round(inv_insumo + inv_suprimento, 2)

    return {
        "finance_scope_ready": scope_ready,
        "farm_id": farm_id,
        "plot_count": len(plot_ids),
        "inventory_value_insumo": round(inv_insumo, 2),
        "inventory_value_suprimento": round(inv_suprimento, 2),
        "inventory_value_total": inventory_total,
        "historical_purchase_cost_insumo": round(cost_purchased_insumo, 2),
        "historical_purchase_cost_suprimento": round(cost_purchased_suprimento, 2),
        "historical_purchase_cost_total": round(cost_purchased_insumo + cost_purchased_suprimento, 2),
        "stock_output_cost_season": round(stock_out_cost_season, 2),
        "fertilization_cost_season": round(fertilization_cost_season, 2),
        "assets_acquisition_total": round(assets_total, 2),
        "operational_cost_season": round(stock_out_cost_season + fertilization_cost_season, 2),
    }


def build_finance_extract_rows(
    repo: FarmRepository,
    *,
    farm_id: int | None,
    active_season: CropSeason | None,
    limit: int = EXTRACT_MAX_ROWS,
) -> tuple[list[dict], bool]:
    """Linhas tipo extrato: compras, saídas de estoque, fertilizações e patrimônio (custos / investimento)."""
    if not farm_id:
        return [], False

    scope_ready = bool(farm_id and active_season)
    plots = (
        repo.list_plots(
            farm_ids=[farm_id],
            variety_ids=[active_season.variety_id] if active_season and active_season.variety_id else None,
        )
        if scope_ready
        else repo.list_plots(farm_ids=[farm_id])
    )
    plot_ids = {p.id for p in plots}
    season_start, season_end = _season_bounds(active_season)

    def _date_ok(d: date | None) -> bool:
        if not active_season:
            return True
        if d is None:
            return False
        return _in_date_range(d, season_start, season_end)

    raw: list[dict] = []

    for entry in _filter_entries_by_farm(repo.list_purchased_inputs(), farm_id):
        pd = entry.purchase_date
        if not _date_ok(pd):
            continue
        it = _item_type(entry)
        raw.append(
            {
                "date": pd,
                "sort_group": 1,
                "ref_id": entry.id,
                "module": "Compras",
                "description": f"Compra — {entry.name}",
                "detail": "Suprimentos" if it == "suprimento" else "Insumos",
                "debit": _f(entry.total_cost),
                "credit": None,
            }
        )

    sid = active_season.id if active_season else None
    for output in repo.list_stock_outputs(farm_id=farm_id):
        tc = _f(output.total_cost)
        if tc <= 0:
            continue
        md = output.movement_date
        if not _date_ok(md):
            continue
        if active_season and sid is not None:
            if output.plot_id:
                if output.plot_id not in plot_ids:
                    continue
            elif output.season_id and output.season_id != sid:
                continue
        elif output.plot_id and output.plot_id not in plot_ids:
            continue
        name = output.input_catalog.name if output.input_catalog else "Insumo"
        plot_label = output.plot.name if output.plot else ""
        detail = f"{name}" + (f" · {plot_label}" if plot_label else "")
        raw.append(
            {
                "date": md,
                "sort_group": 4,
                "ref_id": output.id,
                "module": "Estoque",
                "description": "Saída de estoque (custo)",
                "detail": detail,
                "debit": tc,
                "credit": None,
            }
        )

    for rec in repo.list_fertilizations():
        if rec.plot_id not in plot_ids:
            continue
        ad = rec.application_date
        if not _date_ok(ad):
            continue
        plot_name = rec.plot.name if rec.plot else ""
        raw.append(
            {
                "date": ad,
                "sort_group": 3,
                "ref_id": rec.id,
                "module": "Fertilização",
                "description": rec.product or "Aplicação",
                "detail": plot_name,
                "debit": _f(rec.cost),
                "credit": None,
            }
        )

    for asset in repo.list_equipment_assets(farm_id=farm_id):
        ad = asset.acquisition_date
        if not _date_ok(ad):
            continue
        av = _f(asset.acquisition_value)
        if av <= 0:
            continue
        raw.append(
            {
                "date": ad,
                "sort_group": 2,
                "ref_id": asset.id,
                "module": "Patrimônio",
                "description": f"Aquisição — {asset.name}",
                "detail": asset.category or "",
                "debit": av,
                "credit": None,
            }
        )

    raw.sort(
        key=lambda r: (
            r["date"],
            r["sort_group"],
            r["ref_id"],
        )
    )

    truncated = len(raw) > limit
    raw = raw[:limit]

    balance = 0.0
    out: list[dict] = []
    for r in raw:
        deb = float(r["debit"] or 0)
        cre = float(r["credit"] or 0)
        balance += deb - cre
        out.append(
            {
                **r,
                "balance": round(balance, 2),
            }
        )

    return out, truncated
