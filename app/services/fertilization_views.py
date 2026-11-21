"""Orquestração de dados para as páginas remodeladas de Agendamentos e Fertilização.

Centraliza cálculos de demanda acumulada de estoque, custo médio FIFO, alertas climáticos,
cobertura nutricional por talhão e payloads dos wizards multi-etapas.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta
from typing import Any

from app.models import (
    FertilizationRecord,
    FertilizationSchedule,
    InputCatalog,
    IrrigationRecord,
    Plot,
    PurchasedInput,
)
from app.services.forms import (
    calculate_irrigation_volume,
    fertilization_application_method_label,
)


METHOD_META: dict[str, dict[str, str]] = {
    "fertirrigacao": {
        "code": "fertirrigacao",
        "label": "Fertirrigação",
        "short": "Fertirrigação",
        "tone": "info",
        "dot": "#1F5FA8",
        "desc": "Via sistema de irrigação (lança água automaticamente no gotejo)",
    },
    "adubacao_solida": {
        "code": "adubacao_solida",
        "label": "Adubação sólida",
        "short": "Sólida",
        "tone": "muted",
        "dot": "#5B6660",
        "desc": "Granulado no solo (lanço ou projeção de copa)",
    },
    "fertilizacao_foliar": {
        "code": "fertilizacao_foliar",
        "label": "Fertilização foliar",
        "short": "Foliar",
        "tone": "brand",
        "dot": "#446A36",
        "desc": "Pulverização nas folhas (sensível à chuva)",
    },
}

IRRIGATION_LABELS: dict[str, str] = {
    "gotejo": "Gotejo",
    "aspersor": "Aspersor",
    "sequeiro": "Sequeiro",
    "none": "Sequeiro",
}

WEEKDAYS_FULL = ("Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo")
WEEKDAYS_SHORT = ("Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom")
MONTHS_SHORT = ("", "jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez")


def format_day_header(target_date: date | None) -> dict[str, str]:
    if not target_date:
        return {"main": "Sem data", "sub": "", "short": "—", "iso": ""}
    return {
        "main": f"{target_date.day} {MONTHS_SHORT[target_date.month]}",
        "sub": WEEKDAYS_FULL[target_date.weekday()],
        "short": f"{WEEKDAYS_SHORT[target_date.weekday()]}, {target_date.strftime('%d/%m/%Y')}",
        "iso": target_date.isoformat(),
    }


def _infer_catalog_category(item: InputCatalog) -> tuple[str, str]:
    raw_cat = (item.category or "Geral").strip()
    hay = f"{raw_cat} {item.name or ''}".lower()
    if any(k in hay for k in ("foliar", "boro", "bórico", "borico", "zinco", "amino", "alga", "cobre", "mangan", "fosfito", "micro")):
        return "foliar", raw_cat if raw_cat.lower() != "geral" else "Foliar · Micro"
    if any(k in hay for k in ("calcário", "calcario", "gesso", "silicato", "corretivo", "enxofre")):
        return "corretivo", raw_cat if raw_cat.lower() != "geral" else "Corretivo · Solo"
    if any(k in hay for k in ("solúvel", "soluvel", "ureia", "uréia", "kcl", "cloreto", "map", "mkp", "nitrato", "fertir")):
        return "soluvel", raw_cat if raw_cat.lower() != "geral" else "Solúvel · Fertirrigação"
    if any(k in hay for k in ("npk", "granul", "superfosfato", "sulfato de am")):
        return "solido", raw_cat if raw_cat.lower() != "geral" else "Granulado · NPK"
    return "outros", raw_cat or "Insumo agrícola"


def _entry_unit_cost(entry: PurchasedInput) -> float:
    total_qty = float(entry.total_quantity or 0)
    total_cost = float(entry.total_cost or 0)
    if total_qty > 0 and total_cost > 0:
        return total_cost / total_qty
    return float(entry.unit_price or 0)


def _convert_qty(value: float, from_unit: str | None, to_unit: str | None) -> float | None:
    src = (from_unit or "").strip().lower()
    dst = (to_unit or "").strip().lower()
    if not src or not dst or src == dst:
        return value
    factors = {
        ("kg", "g"): 1000.0,
        ("g", "kg"): 0.001,
        ("l", "ml"): 1000.0,
        ("ml", "l"): 0.001,
    }
    factor = factors.get((src, dst))
    return value * factor if factor is not None else None


def build_input_stock_metrics(
    inputs_catalog: list[InputCatalog],
    purchased_inputs: list[PurchasedInput],
    farm_id: int | None = None,
) -> dict[int, dict[str, Any]]:
    """Calcula saldo disponível e custo médio FIFO ponderado por insumo do catálogo."""
    entries_by_input: dict[int, list[PurchasedInput]] = defaultdict(list)
    for entry in purchased_inputs:
        if not entry.input_id:
            continue
        if farm_id is not None and entry.farm_id is not None and entry.farm_id != farm_id:
            continue
        entries_by_input[entry.input_id].append(entry)

    metrics: dict[int, dict[str, Any]] = {}
    for item in inputs_catalog:
        unit = item.default_unit or "kg"
        total_avail = 0.0
        weighted_cost_sum = 0.0
        fallback_cost = 0.0
        entries = sorted(
            entries_by_input.get(item.id, []),
            key=lambda e: (e.purchase_date or date.min, e.id),
        )
        for entry in entries:
            u_cost = _entry_unit_cost(entry)
            conv_unit_factor = _convert_qty(1.0, entry.package_unit, unit)
            if conv_unit_factor is None:
                continue
            avail_conv = float(entry.available_quantity or 0) * conv_unit_factor
            cost_per_target_unit = u_cost / conv_unit_factor if conv_unit_factor else u_cost
            if cost_per_target_unit > 0:
                fallback_cost = cost_per_target_unit
            if avail_conv > 0:
                total_avail += avail_conv
                weighted_cost_sum += avail_conv * cost_per_target_unit

        avg_cost = (weighted_cost_sum / total_avail) if total_avail > 0 else fallback_cost
        metrics[item.id] = {
            "available": round(total_avail, 2),
            "unit": unit,
            "avg_cost": round(avg_cost, 4),
        }
    return metrics


def build_catalog_payload(
    inputs_catalog: list[InputCatalog],
    purchased_inputs: list[PurchasedInput],
    fertilizations: list[FertilizationRecord],
    schedules: list[FertilizationSchedule],
    farm_id: int | None = None,
) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]]]:
    metrics = build_input_stock_metrics(inputs_catalog, purchased_inputs, farm_id=farm_id)
    usage = Counter()
    for rec in fertilizations:
        for it in rec.items:
            if it.input_id:
                usage[it.input_id] += 2
    for sch in schedules:
        for it in sch.items:
            if it.input_id:
                usage[it.input_id] += 1

    payload: list[dict[str, Any]] = []
    for item in inputs_catalog:
        m = metrics.get(item.id, {"available": 0.0, "unit": item.default_unit or "kg", "avg_cost": 0.0})
        cat_code, cat_label = _infer_catalog_category(item)
        default_dose = 1.5 if cat_code == "foliar" else (150.0 if cat_code == "solido" else 15.0)
        step = 0.5 if cat_code == "foliar" else (10.0 if cat_code == "solido" else 2.0)
        payload.append(
            {
                "id": item.id,
                "name": item.name,
                "category": cat_code,
                "categoryLabel": cat_label,
                "unit": m["unit"],
                "available": m["available"],
                "avgCost": round(float(m["avg_cost"]), 4),
                "usedCount": int(usage.get(item.id, 0)),
                "defaultDose": default_dose,
                "step": step,
                "lowThreshold": float(item.low_stock_threshold) if item.low_stock_threshold is not None else None,
            }
        )
    return payload, metrics


def build_weather_map_for_farm(active_farm) -> dict[str, dict[str, Any]]:
    """Lê previsão em cache da fazenda sem disparar chamadas lentas de rede."""
    if not active_farm:
        return {}
    try:
        from app.services.weather_forecast import _cache, _lock, farm_coordinates

        coords = farm_coordinates(active_farm)
        if not coords:
            return {}
        key = (round(coords[0], 2), round(coords[1], 2))
        with _lock:
            entry = _cache.get(key)
        if not entry or not isinstance(entry, dict):
            return {}
        days = entry.get("days") or []
        out: dict[str, dict[str, Any]] = {}
        for day in days:
            iso = day.get("date")
            if not iso:
                continue
            rain_mm = float(day.get("precip_mm") or 0)
            prob = int(day.get("precip_prob") or 0)
            risk = "high" if (rain_mm >= 15 or (rain_mm >= 8 and prob >= 70)) else ("medium" if rain_mm >= 4 else "low")
            out[iso] = {
                "rainMm": round(rain_mm, 1),
                "prob": prob,
                "label": day.get("label") or ("Chuva prevista" if risk == "high" else "Tempo firme"),
                "emoji": day.get("emoji") or ("🌧️" if risk == "high" else "☀️"),
                "risk": risk,
            }
        return out
    except Exception:
        return {}


def compute_schedule_demand(
    active_schedules: list[FertilizationSchedule],
    input_metrics: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    """Calcula demanda acumulada por insumo e validação cronológica acumulada por agendamento."""
    demand_by_input: dict[int, dict[str, Any]] = {}
    for sch in active_schedules:
        for item in sch.items:
            if not item.input_id:
                continue
            metric = input_metrics.get(item.input_id, {"available": 0.0, "unit": item.unit or "kg", "avg_cost": 0.0})
            target_unit = metric["unit"]
            qty_raw = float(item.quantity or 0)
            qty_conv = _convert_qty(qty_raw, item.unit, target_unit)
            qty_eff = qty_conv if qty_conv is not None else qty_raw
            bucket = demand_by_input.setdefault(
                item.input_id,
                {
                    "input_id": item.input_id,
                    "name": item.input_catalog.name if item.input_catalog else item.name,
                    "unit": target_unit,
                    "available": float(metric["available"]),
                    "avg_cost": float(metric["avg_cost"]),
                    "required": 0.0,
                    "schedule_ids": set(),
                },
            )
            bucket["required"] += qty_eff
            bucket["schedule_ids"].add(sch.id)

    demand_rows: list[dict[str, Any]] = []
    for bucket in demand_by_input.values():
        required = round(bucket["required"], 2)
        available = round(bucket["available"], 2)
        projected = round(available - required, 2)
        missing = round(max(0.0, required - available), 2)
        pct = min(100, round((required / available) * 100)) if available > 0 else (100 if required > 0 else 0)
        demand_rows.append(
            {
                "input_id": bucket["input_id"],
                "name": bucket["name"],
                "unit": bucket["unit"],
                "available": available,
                "required": required,
                "projected": projected,
                "missing": missing,
                "est_buy_cost": round(missing * bucket["avg_cost"], 2),
                "schedule_count": len(bucket["schedule_ids"]),
                "pct": pct,
            }
        )
    demand_rows.sort(key=lambda r: (-r["missing"], -r["required"], r["name"].lower()))

    # Validação cronológica acumulada
    running_avail = {iid: float(m["available"]) for iid, m in input_metrics.items()}
    sorted_active = sorted(active_schedules, key=lambda s: (s.scheduled_date or date.max, s.id))
    by_schedule: dict[int, dict[str, Any]] = {}
    for sch in sorted_active:
        direct_shortages = []
        cumulative_shortages = []
        est_cost = 0.0
        for item in sch.items:
            qty_raw = float(item.quantity or 0)
            if not item.input_id:
                continue
            metric = input_metrics.get(item.input_id, {"available": 0.0, "unit": item.unit or "kg", "avg_cost": 0.0})
            target_unit = metric["unit"]
            qty_conv = _convert_qty(qty_raw, item.unit, target_unit)
            qty_eff = qty_conv if qty_conv is not None else qty_raw
            est_cost += qty_eff * float(metric["avg_cost"])

            current_avail = float(metric["available"])
            if qty_eff > current_avail + 1e-6:
                direct_shortages.append(
                    {
                        "name": item.input_catalog.name if item.input_catalog else item.name,
                        "required": round(qty_eff, 2),
                        "available": round(current_avail, 2),
                        "missing": round(qty_eff - current_avail, 2),
                        "unit": target_unit,
                    }
                )
            rem = running_avail.get(item.input_id, 0.0)
            if qty_eff > rem + 1e-6:
                cumulative_shortages.append(
                    {
                        "name": item.input_catalog.name if item.input_catalog else item.name,
                        "required": round(qty_eff, 2),
                        "available": round(max(0.0, rem), 2),
                        "missing": round(qty_eff - max(0.0, rem), 2),
                        "unit": target_unit,
                    }
                )
            running_avail[item.input_id] = round(rem - qty_eff, 2)

        by_schedule[sch.id] = {
            "direct_ok": len(direct_shortages) == 0,
            "cumulative_ok": len(cumulative_shortages) == 0,
            "direct_shortages": direct_shortages,
            "cumulative_shortages": cumulative_shortages,
            "est_cost": round(est_cost, 2),
        }

    missing_rows = [r for r in demand_rows if r["missing"] > 0]
    return {
        "rows": demand_rows,
        "missing_rows": missing_rows,
        "total_shortage_items": len(missing_rows),
        "est_buy_total": round(sum(r["est_buy_cost"] for r in missing_rows), 2),
        "by_schedule": by_schedule,
    }


def plot_liters_per_minute(plot: Plot | None) -> float:
    if not plot:
        return 0.0
    vol_hour = calculate_irrigation_volume(plot, 60)
    if not vol_hour:
        return 0.0
    return round(float(vol_hour) / 60.0, 2)


def _short_input_label(name: str) -> str:
    clean = (name or "").split("(")[0].strip()
    words = clean.split()
    if len(words) <= 2:
        return clean
    return " ".join(words[:2])


def serialize_schedule_for_client(
    sch: FertilizationSchedule,
    val_info: dict[str, Any],
    input_metrics: dict[int, dict[str, Any]],
    today: date,
) -> dict[str, Any]:
    plot = sch.plot
    area = float(plot.area_hectares or 0) if plot else 0.0
    method_code = sch.application_method or "fertirrigacao"
    method = METHOD_META.get(method_code, METHOD_META["fertirrigacao"])
    lpm = plot_liters_per_minute(plot)
    diff = (sch.scheduled_date - today).days if sch.scheduled_date else 0

    items_list = []
    short_names = []
    for it in sch.items:
        name = it.input_catalog.name if it.input_catalog else it.name
        qty = float(it.quantity or 0)
        metric = input_metrics.get(it.input_id or 0, {"available": 0.0, "unit": it.unit, "avg_cost": 0.0})
        dose_ha = round(qty / area, 2) if area > 0 else qty
        items_list.append(
            {
                "input_id": it.input_id,
                "name": name,
                "unit": it.unit or metric["unit"],
                "quantity": round(qty, 2),
                "dose_per_ha": dose_ha,
                "available": metric["available"],
                "avg_cost": metric["avg_cost"],
            }
        )
        short_names.append(_short_input_label(name))

    summary_inputs = (
        ", ".join(short_names[:3]) + (f" +{len(short_names) - 3}" if len(short_names) > 3 else "")
        if short_names
        else "Sem insumos"
    )
    return {
        "id": sch.id,
        "plot_id": sch.plot_id,
        "plot_name": plot.name if plot else "Setor removido",
        "plot_area": round(area, 2),
        "plot_variety": plot.variety.name if (plot and plot.variety) else "",
        "plot_irrigation": (plot.irrigation_type or "none") if plot else "none",
        "plot_irrigation_label": IRRIGATION_LABELS.get((plot.irrigation_type or "none") if plot else "none", "Sequeiro"),
        "plot_lpm": lpm,
        "scheduled_date": sch.scheduled_date.isoformat() if sch.scheduled_date else "",
        "scheduled_date_br": sch.scheduled_date.strftime("%d/%m/%Y") if sch.scheduled_date else "—",
        "diff_days": diff,
        "status": sch.status or "scheduled",
        "method": method_code,
        "method_label": method["label"],
        "method_short": method["short"],
        "method_tone": method["tone"],
        "method_dot": method["dot"],
        "duration_minutes": sch.duration_minutes,
        "responsible_name": sch.responsible_name or "",
        "recipe_name": sch.recipe_name or "",
        "notes": sch.notes or "",
        "fertilization_record_id": sch.fertilization_record_id,
        "est_cost": val_info.get("est_cost", 0.0),
        "direct_ok": val_info.get("direct_ok", True),
        "cumulative_ok": val_info.get("cumulative_ok", True),
        "direct_shortages": val_info.get("direct_shortages", []),
        "cumulative_shortages": val_info.get("cumulative_shortages", []),
        "items": items_list,
        "inputs_summary": summary_inputs,
        "completed_date_br": (
            sch.fertilization_record.application_date.strftime("%d/%m/%Y")
            if sch.fertilization_record and sch.fertilization_record.application_date
            else None
        ),
        "completed_cost": float(sch.fertilization_record.cost or 0) if sch.fertilization_record else None,
    }


def build_schedules_page_context(
    *,
    active_schedules: list[FertilizationSchedule],
    completed_schedules: list[FertilizationSchedule],
    inputs_catalog: list[InputCatalog],
    purchased_inputs: list[PurchasedInput],
    active_farm,
    today: date,
    farm_id: int | None = None,
) -> dict[str, Any]:
    input_metrics = build_input_stock_metrics(inputs_catalog, purchased_inputs, farm_id=farm_id)
    demand = compute_schedule_demand(active_schedules, input_metrics)
    weather_map = build_weather_map_for_farm(active_farm)

    late_count = sum(1 for s in active_schedules if s.scheduled_date and s.scheduled_date < today)
    today_count = sum(1 for s in active_schedules if s.scheduled_date and s.scheduled_date == today)
    next7_end = today + timedelta(days=7)
    next7_items = [s for s in active_schedules if s.scheduled_date and today <= s.scheduled_date <= next7_end]
    next7_area = round(sum(float(s.plot.area_hectares or 0) for s in next7_items if s.plot), 1)
    total_active_area = round(sum(float(s.plot.area_hectares or 0) for s in active_schedules if s.plot), 1)
    total_planned_cost = round(sum(demand["by_schedule"].get(s.id, {}).get("est_cost", 0.0) for s in active_schedules), 2)
    shortage_schedule_count = sum(
        1 for s in active_schedules if not demand["by_schedule"].get(s.id, {}).get("cumulative_ok", True)
    )

    serialized_active = [
        serialize_schedule_for_client(s, demand["by_schedule"].get(s.id, {}), input_metrics, today)
        for s in sorted(active_schedules, key=lambda x: (x.scheduled_date or date.max, x.id))
    ]
    serialized_completed = [
        serialize_schedule_for_client(s, {"direct_ok": True, "cumulative_ok": True, "est_cost": 0.0}, input_metrics, today)
        for s in sorted(completed_schedules, key=lambda x: (x.scheduled_date or date.min, x.id), reverse=True)
    ]

    # Agrupamento por data exata
    groups_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in serialized_active:
        groups_map[item["scheduled_date"]].append(item)

    day_groups: list[dict[str, Any]] = []
    for iso in sorted(groups_map.keys()):
        items = groups_map[iso]
        dt_obj = date.fromisoformat(iso) if iso else None
        hdr = format_day_header(dt_obj)
        diff = (dt_obj - today).days if dt_obj else 0
        weather = weather_map.get(iso)
        has_rain_sensitive = any(i["method"] in ("fertilizacao_foliar", "adubacao_solida") for i in items)
        rain_alert = bool(weather and weather.get("risk") == "high" and has_rain_sensitive)
        methods_summary = ", ".join(dict.fromkeys(i["method_short"] for i in items))
        day_groups.append(
            {
                "date": iso,
                "header_main": hdr["main"],
                "header_sub": hdr["sub"],
                "diff": diff,
                "is_late": diff < 0,
                "is_today": diff == 0,
                "is_tomorrow": diff == 1,
                "total_area": round(sum(i["plot_area"] for i in items), 1),
                "total_cost": round(sum(i["est_cost"] for i in items), 2),
                "methods_summary": methods_summary,
                "weather": weather,
                "rain_alert": rain_alert,
                "suggest_dry_date": _find_next_dry_date(dt_obj, weather_map) if (rain_alert and dt_obj) else None,
                "items": items,
                "item_ids_csv": ",".join(str(i["id"]) for i in items),
            }
        )

    return {
        "kpi": {
            "attention_total": late_count + today_count,
            "late": late_count,
            "today": today_count,
            "next7_count": len(next7_items),
            "next7_area": next7_area,
            "shortage_inputs": demand["total_shortage_items"],
            "shortage_schedules": shortage_schedule_count,
            "active_count": len(active_schedules),
            "completed_count": len(completed_schedules),
            "planned_cost": total_planned_cost,
            "planned_cost_per_ha": round(total_planned_cost / total_active_area, 2) if total_active_area > 0 else 0.0,
        },
        "demand": demand,
        "day_groups": day_groups,
        "active_items": serialized_active,
        "completed_items": serialized_completed,
        "weather_map": weather_map,
        "today_iso": today.isoformat(),
    }


def _find_next_dry_date(start_date: date, weather_map: dict[str, dict[str, Any]]) -> dict[str, str] | None:
    for offset in range(1, 7):
        cand = start_date + timedelta(days=offset)
        w = weather_map.get(cand.isoformat())
        if not w or w.get("risk") != "high":
            return {"iso": cand.isoformat(), "short": cand.strftime("%d/%m")}
    cand = start_date + timedelta(days=2)
    return {"iso": cand.isoformat(), "short": cand.strftime("%d/%m")}


def build_fertilization_page_context(
    *,
    repo,
    plots: list[Plot],
    fertilizations: list[FertilizationRecord],
    schedules: list[FertilizationSchedule],
    today: date,
) -> dict[str, Any]:
    """Monta KPIs da safra, cobertura nutricional por talhão e histórico com rastreabilidade FIFO."""
    fert_ids = [f.id for f in fertilizations]
    irr_by_fert: dict[int, float] = {}
    if fert_ids:
        irr_rows = (
            repo.db.query(IrrigationRecord)
            .filter(
                IrrigationRecord.reference_type == "fertilization_record",
                IrrigationRecord.reference_id.in_(fert_ids),
                IrrigationRecord.origin == "fertilizacao",
            )
            .all()
        )
        for irr in irr_rows:
            if irr.reference_id:
                irr_by_fert[irr.reference_id] = round(
                    irr_by_fert.get(irr.reference_id, 0.0) + float(irr.volume_liters or 0), 1
                )

    total_cost = round(sum(float(f.cost or 0) for f in fertilizations), 2)
    total_area = round(sum(float(p.area_hectares or 0) for p in plots), 2)
    covered_plot_ids = {f.plot_id for f in fertilizations if f.plot_id}
    from_schedule_count = sum(1 for f in fertilizations if f.schedule is not None)
    total_irr_liters = round(sum(irr_by_fert.values()), 0)

    active_pending = [
        s for s in schedules if (s.status or "").lower() != "completed" and s.scheduled_date and s.scheduled_date <= today
    ]

    # Situação nutricional por talhão
    ferts_by_plot: dict[int, list[FertilizationRecord]] = defaultdict(list)
    for f in fertilizations:
        ferts_by_plot[f.plot_id].append(f)

    next_sched_by_plot: dict[int, FertilizationSchedule] = {}
    for s in sorted(
        [x for x in schedules if (x.status or "").lower() != "completed" and x.scheduled_date],
        key=lambda x: (x.scheduled_date, x.id),
    ):
        next_sched_by_plot.setdefault(s.plot_id, s)

    plot_health: list[dict[str, Any]] = []
    for p in sorted(plots, key=lambda x: (x.name or "").lower()):
        p_apps = sorted(ferts_by_plot.get(p.id, []), key=lambda x: (x.application_date or date.min, x.id), reverse=True)
        p_cost = round(sum(float(a.cost or 0) for a in p_apps), 2)
        p_area = float(p.area_hectares or 0)
        last_date = p_apps[0].application_date if p_apps else None
        days_since = (today - last_date).days if last_date else None
        next_sch = next_sched_by_plot.get(p.id)
        plot_health.append(
            {
                "plot_id": p.id,
                "name": p.name,
                "area": round(p_area, 1),
                "variety": p.variety.name if p.variety else "",
                "irrigation_label": IRRIGATION_LABELS.get(p.irrigation_type or "none", "Sequeiro"),
                "count": len(p_apps),
                "last_date_br": last_date.strftime("%d/%m") if last_date else None,
                "days_since": days_since,
                "cost": p_cost,
                "cost_per_ha": round(p_cost / p_area, 2) if p_area > 0 else 0.0,
                "next_schedule_br": next_sch.scheduled_date.strftime("%d/%m") if next_sch else None,
                "never_done": len(p_apps) == 0,
            }
        )

    # Distribuição de custo por método
    method_breakdown = []
    for code in ("fertirrigacao", "adubacao_solida", "fertilizacao_foliar"):
        m_cost = round(sum(float(f.cost or 0) for f in fertilizations if (f.application_method or "fertirrigacao") == code), 2)
        pct = round((m_cost / total_cost) * 100) if total_cost > 0 else 0
        method_breakdown.append(
            {
                "code": code,
                "label": METHOD_META[code]["label"],
                "dot": METHOD_META[code]["dot"],
                "cost": m_cost,
                "pct": pct,
            }
        )

    # Histórico serializado para cards + gaveta FIFO
    history_rows: list[dict[str, Any]] = []
    for f in sorted(fertilizations, key=lambda x: (x.application_date or date.min, x.id), reverse=True):
        plot = f.plot
        p_area = float(plot.area_hectares or 0) if plot else 0.0
        m_code = f.application_method or "fertirrigacao"
        m = METHOD_META.get(m_code, METHOD_META["fertirrigacao"])
        irr_liters = irr_by_fert.get(f.id)
        items_data = []
        for it in f.items:
            qty = float(it.total_quantity or 0)
            dose_ha = round(qty / p_area, 2) if p_area > 0 else qty
            lots = []
            for alloc in it.stock_allocations:
                pi = alloc.purchased_input
                code_lbl = (
                    (pi.batch_number if pi and pi.batch_number else None)
                    or (f"Compra #{pi.id}" if pi else "Estoque")
                )
                supplier = pi.supplier_name if (pi and pi.supplier_name) else ""
                lots.append(
                    {
                        "code": code_lbl,
                        "supplier": supplier,
                        "qty": round(float(alloc.quantity_used or 0), 2),
                        "unit_cost": round(float(alloc.unit_cost or 0), 2),
                        "total_cost": round(float(alloc.total_cost or 0), 2),
                    }
                )
            items_data.append(
                {
                    "input_id": it.input_id,
                    "name": it.name,
                    "unit": it.unit,
                    "total_qty": round(qty, 2),
                    "dose_per_ha": dose_ha,
                    "unit_cost": round(float(it.unit_cost or 0), 2),
                    "total_cost": round(float(it.total_cost or 0), 2),
                    "lots": lots,
                }
            )
        history_rows.append(
            {
                "id": f.id,
                "plot_id": f.plot_id,
                "plot_name": plot.name if plot else "Setor removido",
                "plot_area": round(p_area, 2),
                "application_date": f.application_date.isoformat() if f.application_date else "",
                "application_date_br": f.application_date.strftime("%d/%m/%Y") if f.application_date else "—",
                "method": m_code,
                "method_label": m["label"],
                "method_short": m["short"],
                "method_tone": m["tone"],
                "method_dot": m["dot"],
                "duration_minutes": f.duration_minutes,
                "irrigation_liters": irr_liters,
                "schedule_id": f.schedule.id if f.schedule else None,
                "responsible_name": f.responsible_name or "",
                "recipe_name": f.recipe_name or "",
                "product": f.product or "",
                "dose": f.dose or "",
                "notes": f.notes or "",
                "total_cost": round(float(f.cost or 0), 2),
                "cost_per_ha": round(float(f.cost or 0) / p_area, 2) if p_area > 0 else 0.0,
                "items": items_data,
            }
        )

    unfertilized_plots = [r for r in plot_health if r["never_done"]]
    return {
        "kpi": {
            "total_cost": total_cost,
            "cost_per_ha": round(total_cost / total_area, 2) if total_area > 0 else 0.0,
            "total_count": len(fertilizations),
            "from_schedule_count": from_schedule_count,
            "manual_count": len(fertilizations) - from_schedule_count,
            "covered_plots": len(covered_plot_ids),
            "total_plots": len(plots),
            "coverage_pct": round((len(covered_plot_ids) / len(plots)) * 100) if plots else 0,
            "irrigation_liters": total_irr_liters,
            "method_counts": {
                "fertirrigacao": sum(1 for f in fertilizations if (f.application_method or "fertirrigacao") == "fertirrigacao"),
                "adubacao_solida": sum(1 for f in fertilizations if f.application_method == "adubacao_solida"),
                "fertilizacao_foliar": sum(1 for f in fertilizations if f.application_method == "fertilizacao_foliar"),
            },
        },
        "plot_health": plot_health,
        "unfertilized_plots": unfertilized_plots,
        "pending_schedules_count": len(active_pending),
        "method_breakdown": method_breakdown,
        "history_rows": history_rows,
    }


def build_wizard_payload(
    *,
    mode: str,
    launch_plots: list[Plot],
    inputs_catalog: list[InputCatalog],
    purchased_inputs: list[PurchasedInput],
    recommendations: list,
    fertilizations: list[FertilizationRecord],
    schedules: list[FertilizationSchedule],
    active_farm,
    today: date,
    farm_id: int | None = None,
    edit_schedule: FertilizationSchedule | None = None,
    edit_fertilization: FertilizationRecord | None = None,
    preselect_plot_id: int | None = None,
    preselect_date: str | None = None,
    copy_from_fertilization: FertilizationRecord | None = None,
) -> dict[str, Any]:
    catalog_payload, _ = build_catalog_payload(
        inputs_catalog, purchased_inputs, fertilizations, schedules, farm_id=farm_id
    )
    valid_input_ids = {c["id"] for c in catalog_payload}
    weather_map = build_weather_map_for_farm(active_farm)

    plots_payload = [
        {
            "id": p.id,
            "name": p.name,
            "area": round(float(p.area_hectares or 0), 2),
            "plants": int(p.plant_count or 0),
            "variety": p.variety.name if p.variety else "",
            "irrigation": p.irrigation_type or "none",
            "irrigationLabel": IRRIGATION_LABELS.get(p.irrigation_type or "none", "Sequeiro"),
            "litersPerMin": plot_liters_per_minute(p),
        }
        for p in sorted(launch_plots, key=lambda x: (x.name or "").lower())
    ]

    recs_payload = []
    for rec in recommendations:
        rec_items = []
        for it in rec.items:
            if it.input_id and it.input_id in valid_input_ids:
                rec_items.append(
                    {
                        "inputId": it.input_id,
                        "unit": it.unit,
                        "dosePerHa": round(float(it.quantity or 0), 2),
                    }
                )
        if not rec_items:
            continue
        recs_payload.append(
            {
                "id": rec.id,
                "name": rec.application_name,
                "source": (f"{rec.plot.name}" if rec.plot else "Recomendação cadastrada"),
                "items": rec_items,
            }
        )

    # Estado inicial do formulário
    is_sched = "schedule" in mode
    initial: dict[str, Any] = {
        "plotIds": [preselect_plot_id] if preselect_plot_id else ([plots_payload[0]["id"]] if plots_payload else []),
        "date": preselect_date or ((today + timedelta(days=1)).isoformat() if is_sched else today.isoformat()),
        "method": "fertirrigacao",
        "durationMinutes": 45,
        "responsible": "",
        "recipeName": "",
        "notes": "",
        "doseMode": "per_ha",
        "items": [],
    }

    if edit_schedule:
        area = float(edit_schedule.plot.area_hectares or 0) if edit_schedule.plot else 0.0
        initial.update(
            {
                "plotIds": [edit_schedule.plot_id],
                "date": edit_schedule.scheduled_date.isoformat() if edit_schedule.scheduled_date else today.isoformat(),
                "method": edit_schedule.application_method or "fertirrigacao",
                "durationMinutes": edit_schedule.duration_minutes or 45,
                "responsible": edit_schedule.responsible_name or "",
                "recipeName": edit_schedule.recipe_name or "",
                "notes": edit_schedule.notes or "",
                "doseMode": "per_ha" if area > 0 else "fixed_plot",
                "items": [
                    {
                        "inputId": it.input_id,
                        "value": round(float(it.quantity or 0) / area, 2) if area > 0 else round(float(it.quantity or 0), 2),
                    }
                    for it in edit_schedule.items
                    if it.input_id in valid_input_ids
                ],
            }
        )
    elif edit_fertilization:
        area = float(edit_fertilization.plot.area_hectares or 0) if edit_fertilization.plot else 0.0
        initial.update(
            {
                "plotIds": [edit_fertilization.plot_id],
                "date": edit_fertilization.application_date.isoformat() if edit_fertilization.application_date else today.isoformat(),
                "method": edit_fertilization.application_method or "fertirrigacao",
                "durationMinutes": edit_fertilization.duration_minutes or 45,
                "responsible": edit_fertilization.responsible_name or "",
                "recipeName": edit_fertilization.recipe_name or "",
                "notes": edit_fertilization.notes or "",
                "doseMode": "per_ha" if area > 0 else "fixed_plot",
                "items": [
                    {
                        "inputId": it.input_id,
                        "value": round(float(it.total_quantity or 0) / area, 2) if area > 0 else round(float(it.total_quantity or 0), 2),
                    }
                    for it in edit_fertilization.items
                    if it.input_id in valid_input_ids
                ],
            }
        )
    elif copy_from_fertilization:
        area = float(copy_from_fertilization.plot.area_hectares or 0) if copy_from_fertilization.plot else 0.0
        initial.update(
            {
                "plotIds": [copy_from_fertilization.plot_id] if copy_from_fertilization.plot_id else initial["plotIds"],
                "method": copy_from_fertilization.application_method or "fertirrigacao",
                "durationMinutes": copy_from_fertilization.duration_minutes or 45,
                "responsible": copy_from_fertilization.responsible_name or "",
                "recipeName": copy_from_fertilization.recipe_name or "",
                "doseMode": "per_ha" if area > 0 else "fixed_plot",
                "items": [
                    {
                        "inputId": it.input_id,
                        "value": round(float(it.total_quantity or 0) / area, 2) if area > 0 else round(float(it.total_quantity or 0), 2),
                    }
                    for it in copy_from_fertilization.items
                    if it.input_id in valid_input_ids
                ],
            }
        )

    return {
        "mode": mode,
        "today": today.isoformat(),
        "plots": plots_payload,
        "inputs": catalog_payload,
        "recommendations": recs_payload,
        "weather": weather_map,
        "initial": initial,
    }
