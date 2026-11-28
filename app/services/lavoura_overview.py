"""Orquestração de dados para a Visão Geral da Lavoura (/lavoura)."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from app.services.climate_water import build_climate_water_context
from app.services.lavoura_board import build_lavoura_board
from app.services.fertilization_views import build_input_stock_metrics, compute_schedule_demand
from app.services.sector_costs import build_sector_cost_context
from app.services.soil_schedule_bridge import build_soil_page_context


def build_lavoura_overview_context(
    *,
    repo,
    farm,
    season,
    plots: list,
    today: date,
    rain_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Consolida KPIs, clima/janela, pendências acionáveis e o Radar dos Talhões da safra ativa."""
    farm_id = farm.id if farm else None
    season_id = season.id if season else None
    plot_ids = {p.id for p in plots}
    start_date = season.start_date if season else None
    end_date = season.end_date if season else None

    def _in_season(d: date | None) -> bool:
        if not d:
            return False
        if start_date and d < start_date:
            return False
        if end_date and d > end_date:
            return False
        return True

    # Dados base
    rainfalls = repo.list_rainfalls(farm_id=farm_id) if farm_id else []
    irrigations = [i for i in repo.list_irrigations() if i.plot_id in plot_ids] if plot_ids else []
    climate_ctx = build_climate_water_context(
        farm=farm,
        plots=plots,
        rainfalls=rainfalls,
        irrigations=irrigations,
        today=today,
        rain_settings=rain_settings,
    )

    fertilizations = [
        f for f in repo.list_fertilizations()
        if f.plot_id in plot_ids and ((season_id and f.season_id == season_id) or (not season_id and _in_season(f.application_date)))
    ] if plot_ids else []
    schedules = repo.list_fertilization_schedules_for_scope(plot_ids, start_date, end_date) if plot_ids else []
    active_schedules = [s for s in schedules if (s.status or "").lower() != "completed"]
    late_schedules = [s for s in active_schedules if s.scheduled_date and s.scheduled_date < today]
    today_schedules = [s for s in active_schedules if s.scheduled_date == today]
    next7_schedules = [s for s in active_schedules if s.scheduled_date and today <= s.scheduled_date <= today + timedelta(days=7)]

    operations = [
        op for op in (repo.list_agricultural_operations(plot_ids) if plot_ids else [])
        if (season_id and op.season_id == season_id) or (not season_id and _in_season(op.operation_date))
    ]
    incidents = [
        inc for inc in repo.list_pest_incidents()
        if inc.plot_id in plot_ids and _in_season(inc.occurrence_date)
    ] if plot_ids else []
    active_incidents = [
        inc for inc in incidents
        if (getattr(inc, "status", None) or "ativa").strip().lower() not in {"controlada", "resolvida"}
    ]
    critical_incidents = [inc for inc in active_incidents if int(inc.severity or 1) >= 4]

    soil_analyses = [
        a for a in (repo.list_soil_analyses(farm_id=farm_id) if farm_id else [])
        if a.plot_id in plot_ids
    ] if plot_ids else []
    soil_ctx = build_soil_page_context(analyses=soil_analyses, plots=plots, today=today)

    # Demanda de insumos para agenda
    catalog_inputs = repo.list_input_catalog(
        item_type="insumo_agricola",
        organization_id=farm.organization_id if farm else None,
    )
    purchased_inputs = repo.list_purchased_inputs(include_stock_adjustments=True)
    input_metrics = build_input_stock_metrics(catalog_inputs, purchased_inputs, farm_id=farm_id)
    demand = compute_schedule_demand(active_schedules, input_metrics)

    # Custos por setor na safra
    stock_outputs = repo.list_stock_outputs(farm_id=farm_id) if farm_id else []
    finance_txs = repo.list_finance_transactions(farm_id=farm_id) if farm_id else []
    cost_ctx = build_sector_cost_context(
        plots=plots,
        stock_outputs=stock_outputs,
        fertilizations=fertilizations,
        finance_transactions=finance_txs,
        season_id=season_id,
        start_date=start_date,
        end_date=end_date,
    )
    cost_by_plot = {r["plot"].id: r for r in cost_ctx["rows"]}

    # Precisa de atenção agora
    attention: list[dict[str, str]] = []
    if late_schedules or today_schedules:
        attention.append(
            {
                "tone": "err" if late_schedules else "warn",
                "title": f"{len(late_schedules)} atrasada(s) e {len(today_schedules)} para hoje na Agenda de adubação",
                "detail": "Conclua ou reagende em campo sem lançar duas vezes.",
                "href": "/fertilizacao/agendamentos",
                "action": "Abrir agenda",
            }
        )
    if active_incidents:
        attention.append(
            {
                "tone": "err" if critical_incidents else "warn",
                "title": f"{len(active_incidents)} foco(s) ativo(s) de pragas/doenças" + (f" ({len(critical_incidents)} crítico)" if critical_incidents else ""),
                "detail": "Acompanhe severidade, fotos e planeje pulverização corretiva.",
                "href": "/pragas",
                "action": "Ver ocorrências",
            }
        )
    if climate_ctx["kpi"]["dry_alert_count"]:
        attention.append(
            {
                "tone": "warn",
                "title": f"{climate_ctx['kpi']['dry_alert_count']} talhão(ões) irrigado(s) com atraso de água",
                "detail": "Confira o balanço hídrico (chuva + irrigação em mm) antes de acionar o turno.",
                "href": "/clima-agua",
                "action": "Ver balanço",
            }
        )
    if demand["total_shortage_items"]:
        attention.append(
            {
                "tone": "warn",
                "title": f"{demand['total_shortage_items']} insumo(s) em falta para a agenda programada",
                "detail": "Veja a lista de compras estimada para evitar atraso nas aplicações.",
                "href": "/fertilizacao/agendamentos",
                "action": "Ver faltantes",
            }
        )
    if soil_ctx["kpi"]["missing_or_stale"]:
        attention.append(
            {
                "tone": "info",
                "title": f"{soil_ctx['kpi']['missing_or_stale']} talhão(ões) sem laudo de solo recente",
                "detail": "Cadastre a análise para gerar calagem, gessagem e cronograma nutricional.",
                "href": "/analise-solo",
                "action": "Análise de solo",
            }
        )

    # Radar por talhão
    last_fert_by_plot: dict[int, Any] = {}
    for f in sorted(fertilizations, key=lambda x: (x.application_date or date.min, x.id), reverse=True):
        if f.plot_id not in last_fert_by_plot:
            last_fert_by_plot[f.plot_id] = f
    last_op_by_plot: dict[int, Any] = {}
    for op in sorted(operations, key=lambda x: (x.operation_date or date.min, x.id), reverse=True):
        if op.plot_id not in last_op_by_plot:
            last_op_by_plot[op.plot_id] = op
    incidents_by_plot: dict[int, list] = defaultdict(list)
    for inc in active_incidents:
        incidents_by_plot[inc.plot_id].append(inc)
    water_by_plot = {r["plot_id"]: r for r in climate_ctx["plot_water_rows"]}
    soil_by_plot = {r["plot_id"]: r for r in soil_ctx["plot_coverage"]}

    plot_radar: list[dict[str, Any]] = []
    for p in sorted(plots, key=lambda x: (x.name or "").lower()):
        lf = last_fert_by_plot.get(p.id)
        lo = last_op_by_plot.get(p.id)
        p_incs = incidents_by_plot.get(p.id, [])
        max_sev = max((int(i.severity or 1) for i in p_incs), default=0)
        w = water_by_plot.get(p.id, {})
        s = soil_by_plot.get(p.id, {})
        c = cost_by_plot.get(p.id, {})
        plot_radar.append(
            {
                "plot_id": p.id,
                "plot_name": p.name,
                "area": round(float(p.area_hectares or 0), 1),
                "variety": p.variety.name if p.variety else "",
                "last_fert_br": lf.application_date.strftime("%d/%m/%Y") if lf and lf.application_date else "Sem adubação",
                "last_fert_product": (lf.recipe_name or lf.product or "") if lf else "",
                "last_op_label": f"{lo.operation_type} ({lo.operation_date.strftime('%d/%m')})" if lo and lo.operation_date else "—",
                "water_15d_mm": w.get("total_15d_mm", 0.0),
                "water_status": w.get("status", "muted"),
                "water_label": w.get("status_label", "—"),
                "active_pests": len(p_incs),
                "max_severity": max_sev,
                "pest_tone": "err" if max_sev >= 4 else ("warn" if p_incs else "ok"),
                "pest_label": f"{len(p_incs)} foco(s) · Nv.{max_sev}" if p_incs else "Sadio",
                "v_pct": s.get("v_pct"),
                "v_tone": s.get("v_tone", "muted"),
                "cost_total": c.get("total_cost", 0.0),
                "cost_per_ha": c.get("cost_per_hectare", 0.0),
            }
        )

    total_area = sum(float(p.area_hectares or 0) for p in plots)
    invested_total = cost_ctx["allocated_total"]
    board = build_lavoura_board(
        plots=plots,
        today=today,
        fertilizations=fertilizations,
        active_schedules=active_schedules,
        operations=operations,
        active_incidents=active_incidents,
        irrigations=irrigations,
        soil_analyses=soil_analyses,
        water_rows=climate_ctx["plot_water_rows"],
        soil_rows=soil_ctx["plot_coverage"],
        cost_by_plot={pid: {"cost_per_hectare": r.get("cost_per_hectare", 0.0)} for pid, r in cost_by_plot.items()},
    )
    return {
        "climate": climate_ctx,
        "kpi": {
            "invested_total": invested_total,
            "cost_per_ha": round(invested_total / total_area, 2) if total_area > 0 else 0.0,
            "week_schedules": len(next7_schedules),
            "late_schedules": len(late_schedules),
            "active_incidents": len(active_incidents),
            "critical_incidents": len(critical_incidents),
            "fertilized_plots": len(last_fert_by_plot),
            "soil_covered_plots": soil_ctx["kpi"]["covered_plots"],
            "total_plots": len(plots),
            "operations_count": len(operations),
        },
        "attention": attention,
        "board": board,
        "total_area": total_area,
        "next7_count": len(next7_schedules),
        "plot_radar": plot_radar,
    }
