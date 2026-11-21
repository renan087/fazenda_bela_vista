"""Agregações e resumo operacional para a página inicial do módulo Setores."""

from __future__ import annotations

from datetime import date

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.fertilization import FertilizationRecord
from app.models.fertilization_schedule import FertilizationSchedule
from app.models.finance_transaction import FinanceTransaction
from app.models.plot_attachment import PlotAttachment
from app.models.soil_analysis import SoilAnalysis
from app.models.stock_output import StockOutput
from app.services.forms import PLANTING_END_REASONS
from app.services.plot_wizard import PLOT_COLORS, SOIL_TEXTURES, stroke_for
from app.services.season_goals import plot_stage, plot_yield_history

VARIETY_PALETTE: tuple[str, ...] = (
    "#446A36",
    "#2F6F8E",
    "#C98A1B",
    "#7D5BA6",
    "#1D8A6E",
    "#B45309",
    "#334155",
    "#0E7490",
)

IRRIGATION_LABELS: dict[str, str] = {
    "gotejo": "Gotejo",
    "aspersor": "Aspersor",
    "none": "Sequeiro",
}


def _age_label(planting_date: date | None, today: date) -> str:
    if not planting_date:
        return ""
    months = (today.year - planting_date.year) * 12 + (today.month - planting_date.month)
    if months < 0:
        return "plantio futuro"
    years = months // 12
    if years >= 1:
        return f"{years} {'ano' if years == 1 else 'anos'}"
    months = max(1, months)
    return f"{months} {'mês' if months == 1 else 'meses'}"


def _default_plot_color(index: int, saved_color: str | None) -> tuple[str, str]:
    fills = [fill for _, _, fill in PLOT_COLORS]
    fill = (saved_color or "").strip().lower() or fills[index % len(fills)]
    stroke = stroke_for(fill) or fill
    return fill, stroke


def build_plots_overview_context(
    db: Session,
    *,
    plots: list,
    scoped_farms: list,
    active_season=None,
    plot_preview_ready: dict[int, bool] | None = None,
    plot_preview_fingerprint: dict[int, str] | None = None,
    today: date | None = None,
) -> dict:
    """Consolida KPIs, pendências, composição por variedade e metadados por setor."""
    today = today or date.today()
    plot_preview_ready = plot_preview_ready or {}
    plot_preview_fingerprint = plot_preview_fingerprint or {}
    plot_ids = [plot.id for plot in plots]

    # Setores vinculados à safra ativa (se existir)
    active_season_plot_ids: set[int] = set()
    active_season_name: str | None = None
    active_season_id: int | None = None
    if active_season is not None:
        active_season_name = active_season.name
        active_season_id = active_season.id
        active_season_plot_ids = {item.id for item in (getattr(active_season, "plots", None) or [])}

    # Consultas em lote para enriquecer cartões e a Ficha Rápida sem N+1
    pending_fert_by_plot: dict[int, int] = {}
    last_fert_by_plot: dict[int, str] = {}
    last_soil_by_plot: dict[int, str] = {}
    attachments_by_plot: dict[int, int] = {}
    season_cost_by_plot: dict[int, float] = {}
    yield_history_by_plot: dict[int, float] = {}

    if plot_ids:
        # 1. Adubações programadas pendentes (atrasadas ou para hoje)
        sched_rows = (
            db.query(FertilizationSchedule.plot_id, func.count(FertilizationSchedule.id))
            .filter(
                FertilizationSchedule.plot_id.in_(plot_ids),
                FertilizationSchedule.status == "scheduled",
                FertilizationSchedule.scheduled_date <= today,
            )
            .group_by(FertilizationSchedule.plot_id)
            .all()
        )
        pending_fert_by_plot = {int(pid): int(cnt) for pid, cnt in sched_rows}

        # 2. Última adubação registrada por setor
        fert_rows = (
            db.query(FertilizationRecord.plot_id, FertilizationRecord.application_date, FertilizationRecord.product)
            .filter(FertilizationRecord.plot_id.in_(plot_ids))
            .order_by(FertilizationRecord.application_date.desc(), FertilizationRecord.id.desc())
            .all()
        )
        for pid, app_date, product in fert_rows:
            if pid not in last_fert_by_plot:
                date_txt = app_date.strftime("%d/%m/%Y") if app_date else ""
                prod_txt = (product or "").strip()
                last_fert_by_plot[pid] = f"{date_txt} · {prod_txt}" if date_txt and prod_txt else (prod_txt or date_txt)

        # 3. Última análise de solo por setor
        soil_rows = (
            db.query(SoilAnalysis.plot_id, SoilAnalysis.analysis_date, SoilAnalysis.base_saturation, SoilAnalysis.ph)
            .filter(SoilAnalysis.plot_id.in_(plot_ids))
            .order_by(SoilAnalysis.analysis_date.desc(), SoilAnalysis.id.desc())
            .all()
        )
        for pid, a_date, v_pct, ph in soil_rows:
            if pid not in last_soil_by_plot:
                parts = []
                if a_date:
                    parts.append(a_date.strftime("%m/%Y"))
                if v_pct is not None:
                    parts.append(f"V% {float(v_pct):.0f}%")
                elif ph is not None:
                    parts.append(f"pH {float(ph):.1f}".replace(".", ","))
                last_soil_by_plot[pid] = " · ".join(parts)

        # 4. Contagem de anexos por setor
        att_rows = (
            db.query(PlotAttachment.plot_id, func.count(PlotAttachment.id))
            .filter(PlotAttachment.plot_id.in_(plot_ids))
            .group_by(PlotAttachment.plot_id)
            .all()
        )
        attachments_by_plot = {int(pid): int(cnt) for pid, cnt in att_rows}

        # 5. Custos diretos acumulados (saídas de estoque + despesas operacionais/contas vinculadas)
        out_query = db.query(StockOutput.plot_id, func.coalesce(func.sum(StockOutput.total_cost), 0)).filter(
            StockOutput.plot_id.in_(plot_ids),
            StockOutput.total_cost.isnot(None),
        )
        if active_season_id is not None:
            out_query = out_query.filter(StockOutput.season_id == active_season_id)
        for pid, total_val in out_query.group_by(StockOutput.plot_id).all():
            season_cost_by_plot[int(pid)] = season_cost_by_plot.get(int(pid), 0.0) + float(total_val or 0)

        tx_query = db.query(FinanceTransaction.plot_id, func.coalesce(func.sum(FinanceTransaction.amount), 0)).filter(
            FinanceTransaction.plot_id.in_(plot_ids),
            FinanceTransaction.is_reversed.is_(False),
            func.lower(FinanceTransaction.operation_type) == "despesa",
            func.lower(func.coalesce(FinanceTransaction.source, "")).in_(["contas", "operacoes agricolas", "operações agrícolas"]),
        )
        if active_season_id is not None:
            tx_query = tx_query.filter(FinanceTransaction.season_id == active_season_id)
        for pid, total_val in tx_query.group_by(FinanceTransaction.plot_id).all():
            season_cost_by_plot[int(pid)] = season_cost_by_plot.get(int(pid), 0.0) + float(total_val or 0)

        # 6. Média histórica das últimas 3 safras com colheita (sc/ha)
        yield_history_by_plot = plot_yield_history(db, plots)

    # Monta itens enriquecidos e totais
    items: list[dict] = []
    variety_totals: dict[str, dict] = {}
    attention_items: list[dict] = []

    total_area = 0.0
    total_plants = 0
    total_sacks = 0.0
    geo_count = 0
    in_season_count = 0
    irrigated_count = 0
    irrigated_area = 0.0
    prod_count = 0
    prod_area = 0.0
    form_count = 0

    for idx, plot in enumerate(plots):
        area = float(plot.area_hectares or 0)
        plants = int(plot.plant_count or 0)
        sacks = float(plot.estimated_yield_sacks or 0)
        has_map = bool(plot.boundary_geojson)
        irrig_type = plot.irrigation_type or "none"
        irrig_label = IRRIGATION_LABELS.get(irrig_type, "Sequeiro")
        is_irrigated = irrig_type in {"gotejo", "aspersor"}

        active_planting = getattr(plot, "active_planting", None)
        planting_date = getattr(active_planting, "planting_date", None) or plot.planting_date
        stage = plot_stage(plot, today)
        variety_obj = getattr(active_planting, "variety", None) or plot.variety
        variety_name = variety_obj.name if variety_obj else "Sem variedade"
        variety_id = variety_obj.id if variety_obj else None
        culture = (getattr(active_planting, "culture", None) or "Café").strip() or "Café"

        in_active_season = (plot.id in active_season_plot_ids) if active_season_id is not None else False
        pending_fert = pending_fert_by_plot.get(plot.id, 0)

        reasons: list[str] = []
        if not has_map:
            reasons.append("sem perímetro no mapa")
        if pending_fert > 0:
            reasons.append(f"{pending_fert} adubação pendente" if pending_fert == 1 else f"{pending_fert} adubações pendentes")
        if active_season_id is not None and active_season_plot_ids and not in_active_season and stage == "prod":
            reasons.append("fora da safra ativa")
        if stage == "prod" and sacks <= 0:
            reasons.append("sem estimativa")

        fill_color, stroke_color = _default_plot_color(idx, getattr(plot, "map_color", None))
        density = round(plants / area) if area > 0 and plants > 0 else 0
        sc_per_ha = round(sacks / area, 1) if area > 0 and sacks > 0 else None
        season_cost = round(season_cost_by_plot.get(plot.id, 0.0), 2)
        cost_per_ha = round(season_cost / area, 2) if area > 0 and season_cost > 0 else 0.0

        fingerprint = plot_preview_fingerprint.get(plot.id, "")
        preview_ready = bool(has_map and plot_preview_ready.get(plot.id))
        thumb_url = f"/static/generated/plot_previews/{plot.id}_thumb.jpg?v={fingerprint}" if preview_ready else ""
        full_url = f"/static/generated/plot_previews/{plot.id}.png?v={fingerprint}" if preview_ready else ""

        past_plantings_data = []
        for pp in getattr(plot, "past_plantings", None) or []:
            start_txt = pp.planting_date.strftime("%Y") if pp.planting_date else "?"
            end_txt = pp.ended_on.strftime("%Y") if pp.ended_on else "?"
            past_plantings_data.append(
                {
                    "culture": pp.culture or "Café",
                    "variety": pp.variety.name if getattr(pp, "variety", None) else "Sem variedade",
                    "period": f"{start_txt} – {end_txt}",
                    "reason": PLANTING_END_REASONS.get(pp.end_reason or "", pp.end_reason or "Encerrado"),
                }
            )

        item = {
            "id": plot.id,
            "name": plot.name,
            "farm_id": plot.farm_id,
            "farm_name": plot.farm.name if plot.farm else "Sem fazenda",
            "group_name": (getattr(plot, "group_name", None) or "").strip(),
            "variety_id": variety_id,
            "variety_name": variety_name,
            "culture": culture,
            "area": round(area, 2),
            "plant_count": plants,
            "density": density,
            "spacing_row": float(plot.spacing_row_meters) if plot.spacing_row_meters is not None else None,
            "spacing_plant_cm": round(float(plot.spacing_plant_meters) * 100) if plot.spacing_plant_meters is not None else None,
            "estimated_sacks": round(sacks, 2),
            "sc_per_ha": sc_per_ha,
            "last_harvest_avg": yield_history_by_plot.get(plot.id),
            "planting_date_iso": planting_date.isoformat() if planting_date else "",
            "planting_month_year": planting_date.strftime("%m/%Y") if planting_date else "—",
            "age_label": _age_label(planting_date, today),
            "stage": stage,
            "irrigation_type": irrig_type,
            "irrigation_label": irrig_label,
            "is_irrigated": is_irrigated,
            "irrigation_line_count": plot.irrigation_line_count,
            "irrigation_line_length_meters": float(plot.irrigation_line_length_meters) if plot.irrigation_line_length_meters is not None else None,
            "drip_spacing_meters": float(plot.drip_spacing_meters) if plot.drip_spacing_meters is not None else None,
            "drip_liters_per_hour": float(plot.drip_liters_per_hour) if plot.drip_liters_per_hour is not None else None,
            "sprinkler_count": plot.sprinkler_count,
            "sprinkler_liters_per_hour": float(plot.sprinkler_liters_per_hour) if plot.sprinkler_liters_per_hour is not None else None,
            "soil_texture": (getattr(plot, "soil_texture", None) or "").strip(),
            "soil_texture_label": SOIL_TEXTURES.get(getattr(plot, "soil_texture", None) or "", ""),
            "field_reference": (getattr(plot, "field_reference", None) or "").strip(),
            "notes": (plot.notes or "").strip(),
            "map_color": fill_color,
            "map_stroke": stroke_color,
            "has_map": has_map,
            "preview_ready": preview_ready,
            "thumb_url": thumb_url,
            "full_url": full_url,
            "in_active_season": in_active_season,
            "pending_fert_count": pending_fert,
            "last_fert_summary": last_fert_by_plot.get(plot.id, ""),
            "last_soil_summary": last_soil_by_plot.get(plot.id, ""),
            "season_cost": season_cost,
            "cost_per_ha": cost_per_ha,
            "attachments_count": attachments_by_plot.get(plot.id, 0),
            "past_plantings": past_plantings_data,
            "attention_reasons": reasons,
            "needs_attention": bool(reasons),
        }
        items.append(item)

        # Acumuladores de resumo
        total_area += area
        total_plants += plants
        total_sacks += sacks
        if has_map:
            geo_count += 1
        if in_active_season:
            in_season_count += 1
        if is_irrigated:
            irrigated_count += 1
            irrigated_area += area
        if stage == "form":
            form_count += 1
        else:
            prod_count += 1
            prod_area += area
        if reasons:
            attention_items.append({"id": plot.id, "name": plot.name, "reasons": reasons})

        v_entry = variety_totals.setdefault(variety_name, {"name": variety_name, "area": 0.0, "count": 0})
        v_entry["area"] += area
        v_entry["count"] += 1

    # Composição por variedade ordenada por área
    variety_breakdown = []
    sorted_varieties = sorted(variety_totals.values(), key=lambda entry: (-entry["area"], entry["name"].lower()))
    for idx, entry in enumerate(sorted_varieties):
        pct = round((entry["area"] / total_area) * 100, 1) if total_area > 0 else 0.0
        variety_breakdown.append(
            {
                "name": entry["name"],
                "area": round(entry["area"], 1),
                "pct": pct,
                "pct_int": round(pct),
                "count": entry["count"],
                "color": VARIETY_PALETTE[idx % len(VARIETY_PALETTE)],
            }
        )

    farm_total_area = sum(float(getattr(farm, "total_area", 0) or 0) for farm in scoped_farms)
    occupation_pct = min(100, round((total_area / farm_total_area) * 100)) if farm_total_area > 0 else None
    free_area = max(0.0, round(farm_total_area - total_area, 1)) if farm_total_area > 0 else None
    geo_pct = round((geo_count / len(plots)) * 100) if plots else 0
    density_per_ha = round(total_plants / total_area) if total_area > 0 and total_plants > 0 else 0
    avg_sc_ha = round(total_sacks / prod_area, 1) if prod_area > 0 and total_sacks > 0 else 0.0

    groups = sorted({item["group_name"] for item in items if item["group_name"]}, key=str.lower)
    varieties_in_list = sorted({item["variety_name"] for item in items if item["variety_name"] != "Sem variedade"}, key=str.lower)

    summary = {
        "count": len(plots),
        "geo_count": geo_count,
        "geo_pct": geo_pct,
        "missing_geo": len(plots) - geo_count,
        "in_season_count": in_season_count,
        "active_season_name": active_season_name,
        "has_active_season": active_season_id is not None,
        "total_area": round(total_area, 1),
        "farm_total_area": round(farm_total_area, 1),
        "occupation_pct": occupation_pct,
        "free_area": free_area,
        "total_plants": total_plants,
        "density_per_ha": density_per_ha,
        "irrigated_count": irrigated_count,
        "irrigated_area": round(irrigated_area, 1),
        "total_sacks": round(total_sacks, 0),
        "prod_count": prod_count,
        "form_count": form_count,
        "avg_sc_ha": avg_sc_ha,
        "attention_count": len(attention_items),
    }

    return {
        "summary": summary,
        "attention_items": attention_items,
        "variety_breakdown": variety_breakdown,
        "rows": items,
        "by_id": {item["id"]: item for item in items},
        "groups": groups,
        "varieties_in_list": varieties_in_list,
    }
