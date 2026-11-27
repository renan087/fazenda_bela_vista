"""Serviços e agregações para Operações e Pulverizações (/operacoes) e Pragas e Doenças (/pragas)."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.services.climate_water import get_forecast_days_safe

COFFEE_PEST_CATALOG: tuple[dict[str, str], ...] = (
    {"name": "Ferrugem-alaranjada", "category": "Doença"},
    {"name": "Bicho-mineiro", "category": "Praga"},
    {"name": "Broca-do-café", "category": "Praga"},
    {"name": "Cercosporiose (olho-pardo)", "category": "Doença"},
    {"name": "Phoma / Ascochyta", "category": "Doença"},
    {"name": "Ácaro-vermelho", "category": "Praga"},
    {"name": "Cochonilha", "category": "Praga"},
    {"name": "Cigarras", "category": "Praga"},
    {"name": "Nematoides", "category": "Praga"},
    {"name": "Mancha-aureolada", "category": "Doença"},
    {"name": "Lagartas", "category": "Praga"},
    {"name": "Ácaro-da-mancha-anular", "category": "Praga"},
)

SEVERITY_META: dict[int, dict[str, str]] = {
    1: {"label": "Nível 1 · Foco isolado", "short": "Nv. 1 · Isolado", "tone": "muted"},
    2: {"label": "Nível 2 · Baixa incidência", "short": "Nv. 2 · Baixa", "tone": "info"},
    3: {"label": "Nível 3 · Moderada (atenção)", "short": "Nv. 3 · Moderada", "tone": "warn"},
    4: {"label": "Nível 4 · Alta (ação recomendada)", "short": "Nv. 4 · Alta", "tone": "err"},
    5: {"label": "Nível 5 · Crítica (dano econômico)", "short": "Nv. 5 · Crítica", "tone": "err"},
}

PEST_STATUS_META: dict[str, dict[str, str]] = {
    "ativa": {"label": "Ativa", "tone": "err"},
    "em_tratamento": {"label": "Em tratamento", "tone": "warn"},
    "monitoramento": {"label": "Monitoramento", "tone": "info"},
    "controlada": {"label": "Controlada", "tone": "ok"},
}


def active_withdrawals_by_plot(operations: list, today: date) -> dict[int, dict[str, Any]]:
    """Retorna talhões com período de carência de defensivo ativo na data de referência."""
    active: dict[int, dict[str, Any]] = {}
    for op in operations:
        if (getattr(op, "status", "realizada") or "realizada") != "realizada":
            continue
        wd = getattr(op, "withdrawal_days", None)
        if not wd or int(wd) <= 0 or not op.operation_date:
            continue
        safe_date = op.operation_date + timedelta(days=int(wd))
        if safe_date <= today:
            continue
        prev = active.get(op.plot_id)
        if not prev or safe_date > prev["safe_date"]:
            active[op.plot_id] = {
                "operation_id": op.id,
                "plot_name": op.plot.name if op.plot else f"Talhão #{op.plot_id}",
                "operation_type": op.operation_type,
                "operation_date": op.operation_date,
                "withdrawal_days": int(wd),
                "safe_date": safe_date,
                "days_left": (safe_date - today).days,
                "target_pest": op.target_pest or "",
            }
    return active


def build_operations_page_context(
    *,
    operations: list,
    stock_outputs: list,
    today: date,
) -> dict[str, Any]:
    """KPIs, insumos vinculados por operação e status de carência para /operacoes."""
    outputs_by_op: dict[int, list] = {}
    for out in stock_outputs:
        if out.reference_type == "agricultural_operation" and out.reference_id:
            outputs_by_op.setdefault(out.reference_id, []).append(out)

    realized = [op for op in operations if (getattr(op, "status", None) or "realizada") != "planejada"]
    planned = [op for op in operations if (getattr(op, "status", None) or "realizada") == "planejada"]

    total_area = 0.0
    total_service_cost = 0.0
    total_inputs_cost = 0.0
    total_machine_hours = 0.0
    active_withdrawal_count = 0
    enriched: dict[int, dict[str, Any]] = {}

    for op in operations:
        is_planned = (getattr(op, "status", None) or "realizada") == "planejada"
        area = float(op.plot.area_hectares or 0) if op.plot else 0.0
        tx = op.finance_transaction
        service_cost = float(tx.amount or 0) if (tx and not tx.is_reversed) else 0.0
        op_outputs = outputs_by_op.get(op.id, [])
        in_cost = sum(float(o.total_cost or 0) for o in op_outputs) or float(getattr(op, "inputs_cost", 0) or 0)
        wd = getattr(op, "withdrawal_days", None)
        safe_date = (op.operation_date + timedelta(days=int(wd))) if (wd and int(wd) > 0 and op.operation_date) else None
        wd_active = bool(safe_date and safe_date > today and not is_planned)
        if not is_planned:
            total_area += area
            total_service_cost += service_cost
            total_inputs_cost += in_cost
            total_machine_hours += float(getattr(op, "equipment_hours", 0) or 0)
            if wd_active:
                active_withdrawal_count += 1
        enriched[op.id] = {
            "is_planned": is_planned,
            "service_cost": round(service_cost, 2),
            "inputs_cost": round(in_cost, 2),
            "total_cost": round(service_cost + in_cost, 2),
            "outputs": op_outputs,
            "safe_date": safe_date,
            "safe_date_br": safe_date.strftime("%d/%m/%Y") if safe_date else None,
            "withdrawal_active": wd_active,
            "withdrawal_days_left": (safe_date - today).days if wd_active and safe_date else 0,
        }

    total_cost = round(total_service_cost + total_inputs_cost, 2)
    return {
        "kpi": {
            "total_realized": len(realized),
            "total_planned": len(planned),
            "total_area": round(total_area, 1),
            "total_cost": total_cost,
            "service_cost": round(total_service_cost, 2),
            "inputs_cost": round(total_inputs_cost, 2),
            "machine_hours": round(total_machine_hours, 1),
            "active_withdrawals": active_withdrawal_count,
        },
        "enriched": enriched,
    }


def build_pests_page_context(
    *,
    incidents: list,
    farm,
    today: date,
) -> dict[str, Any]:
    """KPIs, metadados de status/severidade e alerta climático preventivo para /pragas."""
    del today
    active_count = 0
    treating_count = 0
    controlled_count = 0
    critical_count = 0
    affected_plot_ids: set[int] = set()
    meta_by_id: dict[int, dict[str, Any]] = {}

    for inc in incidents:
        raw_st = (getattr(inc, "status", None) or "ativa").strip().lower()
        if raw_st not in PEST_STATUS_META:
            raw_st = "ativa"
        sev = max(1, min(5, int(inc.severity or 1)))
        if raw_st == "controlada":
            controlled_count += 1
        else:
            active_count += 1
            affected_plot_ids.add(inc.plot_id)
            if raw_st == "em_tratamento":
                treating_count += 1
            if sev >= 4:
                critical_count += 1
        cat_clean = "Doença" if (inc.category or "").strip().lower() in {"doenca", "doença"} else (inc.category or "Praga")
        meta_by_id[inc.id] = {
            "status_code": raw_st,
            "status_label": PEST_STATUS_META[raw_st]["label"],
            "status_tone": PEST_STATUS_META[raw_st]["tone"],
            "severity_label": SEVERITY_META[sev]["short"],
            "severity_full": SEVERITY_META[sev]["label"],
            "severity_tone": SEVERITY_META[sev]["tone"],
            "category_label": cat_clean,
            "has_photo": bool(getattr(inc, "photo_data", None)),
        }

    # Alerta climático preventivo (chuva/umidade nos próximos 5 dias favorece ferrugem/phoma)
    forecast = get_forecast_days_safe(farm)[:5]
    rainy_days = sum(1 for d in forecast if d["precip_mm"] >= 5 or d["prob"] >= 60)
    climate_alert = None
    if rainy_days >= 3:
        climate_alert = {
            "tone": "warn",
            "title": "Atenção fitossanitária: sequência de dias úmidos prevista",
            "detail": f"Previsão de chuva em {rainy_days} dos próximos 5 dias — ambiente favorável para ferrugem e phoma. Reforce o monitoramento nos talhões mais adensados.",
        }

    return {
        "kpi": {
            "total": len(incidents),
            "active": active_count,
            "critical": critical_count,
            "treating": treating_count,
            "controlled": controlled_count,
            "affected_plots": len(affected_plot_ids),
        },
        "meta_by_id": meta_by_id,
        "climate_alert": climate_alert,
        "catalog": COFFEE_PEST_CATALOG,
    }
