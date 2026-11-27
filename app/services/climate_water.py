"""Serviço consolidado do módulo Clima e Água: previsão, balanço hídrico em mm, irrigação e pluviometria."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from app.services.fertilization_views import IRRIGATION_LABELS
from app.services.weather_forecast import farm_coordinates, get_farm_forecast

MONTHS_SHORT = ("", "jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez")
MONTHS_LONG = (
    "", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
)


def irrigation_mm(volume_liters: float | None, area_hectares: float | None) -> float:
    """Converte volume irrigado (litros) em lâmina de água (mm): 1 mm em 1 ha = 10.000 L."""
    vol = float(volume_liters or 0)
    area = float(area_hectares or 0)
    if vol <= 0 or area <= 0:
        return 0.0
    return round(vol / (area * 10000.0), 1)


def get_forecast_days_safe(active_farm) -> list[dict[str, Any]]:
    """Retorna os dias de previsão (Open-Meteo em cache/consulta segura) sem levantar exceção."""
    if not active_farm:
        return []
    try:
        coords = farm_coordinates(active_farm)
        if not coords:
            return []
        data = get_farm_forecast(coords[0], coords[1])
        if not data or data.get("status") not in {"ok", "stale"}:
            return []
        days = data.get("days") or []
        out: list[dict[str, Any]] = []
        for d in days:
            precip = float(d.get("precip_mm") if d.get("precip_mm") is not None else (d.get("precip") or 0))
            prob = int(d.get("precip_prob") if d.get("precip_prob") is not None else (d.get("prob") or 0))
            risk = "high" if (precip >= 15 or (precip >= 8 and prob >= 70)) else ("medium" if precip >= 4 or prob >= 55 else "low")
            out.append(
                {
                    "date": d.get("date") or "",
                    "weekday": d.get("weekday") or "",
                    "day_label": d.get("day_label") or (d.get("date")[8:10] + "/" + d.get("date")[5:7] if d.get("date") and len(d.get("date")) >= 10 else ""),
                    "emoji": d.get("emoji") or ("🌧️" if risk == "high" else ("🌦️" if risk == "medium" else "☀️")),
                    "label": d.get("label") or "",
                    "tmax": d.get("tmax"),
                    "tmin": d.get("tmin"),
                    "precip_mm": round(precip, 1),
                    "prob": prob,
                    "risk": risk,
                }
            )
        return out
    except Exception:
        return []


def build_spray_window_hint(forecast_days: list[dict[str, Any]]) -> dict[str, str]:
    """Resumo prático de janela de campo (pulverização/adubação) para hoje e amanhã."""
    if not forecast_days:
        return {
            "tone": "muted",
            "title": "Previsão indisponível",
            "detail": "Cadastre o perímetro da fazenda ou dos talhões para ativar a previsão meteorológica de 7 dias.",
        }
    today_d = forecast_days[0]
    tomorrow_d = forecast_days[1] if len(forecast_days) > 1 else None
    if today_d["risk"] == "high":
        return {
            "tone": "warn",
            "title": "Chuva forte prevista hoje",
            "detail": f"Previsão de {today_d['precip_mm']:.1f} mm ({today_d['prob']}%). Evite pulverizações foliares e adubações a lanço hoje.",
        }
    if today_d["risk"] == "medium":
        return {
            "tone": "info",
            "title": "Possibilidade de chuva hoje",
            "detail": f"Previsão de {today_d['precip_mm']:.1f} mm ({today_d['prob']}%). Priorize aplicações pela manhã e acompanhe a umidade.",
        }
    if tomorrow_d and tomorrow_d["risk"] == "high":
        return {
            "tone": "ok",
            "title": "Boa janela hoje · chuva amanhã",
            "detail": f"Hoje tempo firme; amanhã previsão de {tomorrow_d['precip_mm']:.1f} mm. Aproveite hoje para pulverizar ou adubar.",
        }
    return {
        "tone": "ok",
        "title": "Janela favorável para campo",
        "detail": "Sem risco relevante de chuva hoje e amanhã — bom momento para pulverizações, foliares e manejo.",
    }


def _rain_for_plot(rainfalls: list, plot_id: int, start_date: date, end_date: date) -> tuple[float, date | None]:
    """Soma chuva (mm) recebida pelo talhão no período (registros específicos do talhão ou gerais da fazenda)."""
    total = 0.0
    last_day: date | None = None
    for r in rainfalls:
        if not r.rainfall_date or r.rainfall_date > end_date:
            continue
        if r.plot_id is not None and r.plot_id != plot_id:
            continue
        mm = float(r.millimeters or 0)
        if mm > 0 and (last_day is None or r.rainfall_date > last_day):
            last_day = r.rainfall_date
        if r.rainfall_date >= start_date:
            total += mm
    return round(total, 1), last_day


def build_climate_water_context(
    *,
    farm,
    plots: list,
    rainfalls: list,
    irrigations: list,
    today: date,
    rain_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Monta a visão integrada de Clima e Água: previsão 7d, balanço hídrico por talhão e gráfico combinado."""
    rain_settings = rain_settings or {}
    rainy_day_mm = float(rain_settings.get("rainy_day_mm") or 1.0)
    since_year = rain_settings.get("average_since_year")
    forecast_days = get_forecast_days_safe(farm)
    spray_window = build_spray_window_hint(forecast_days)

    start_7 = today - timedelta(days=6)
    start_15 = today - timedelta(days=14)
    start_30 = today - timedelta(days=29)

    # Chuva consolidada (mensal, média histórica, 7d, 15d, 30d)
    rain_by_ym: dict[tuple[int, int], float] = defaultdict(float)
    rain_7d = 0.0
    rain_15d = 0.0
    rain_30d = 0.0
    latest_rain = None
    for r in rainfalls:
        if not r.rainfall_date:
            continue
        mm = float(r.millimeters or 0)
        rain_by_ym[(r.rainfall_date.year, r.rainfall_date.month)] += mm
        if start_7 <= r.rainfall_date <= today:
            rain_7d += mm
        if start_15 <= r.rainfall_date <= today:
            rain_15d += mm
        if start_30 <= r.rainfall_date <= today:
            rain_30d += mm
        if mm >= rainy_day_mm and (latest_rain is None or r.rainfall_date > latest_rain.rainfall_date):
            latest_rain = r

    month_rain = round(rain_by_ym.get((today.year, today.month), 0.0), 1)
    prev_years = [
        val for (yr, mo), val in rain_by_ym.items()
        if mo == today.month and yr < today.year and (not since_year or yr >= since_year)
    ]
    month_avg = round(sum(prev_years) / len(prev_years), 1) if prev_years else None
    forecast_3d_mm = round(sum(d["precip_mm"] for d in forecast_days[:3]), 1)
    forecast_7d_mm = round(sum(d["precip_mm"] for d in forecast_days[:7]), 1)

    # Irrigação por talhão
    irr_by_plot: dict[int, list] = defaultdict(list)
    irr_by_ym_mm: dict[tuple[int, int], list[float]] = defaultdict(list)
    total_irr_liters_30d = 0.0
    plot_by_id = {p.id: p for p in plots}
    for irr in irrigations:
        if not irr.irrigation_date or irr.plot_id not in plot_by_id:
            continue
        irr_by_plot[irr.plot_id].append(irr)
        p = plot_by_id[irr.plot_id]
        mm_val = irrigation_mm(irr.volume_liters, p.area_hectares)
        if mm_val > 0:
            irr_by_ym_mm[(irr.irrigation_date.year, irr.irrigation_date.month)].append(mm_val)
        if start_30 <= irr.irrigation_date <= today:
            total_irr_liters_30d += float(irr.volume_liters or 0)

    # Balanço hídrico por talhão (últimos 15 dias)
    plot_water_rows: list[dict[str, Any]] = []
    dry_alert_count = 0
    for p in sorted(plots, key=lambda x: (x.name or "").lower()):
        p_area = float(p.area_hectares or 0)
        p_rain_15, last_rain_day = _rain_for_plot(rainfalls, p.id, start_15, today)
        p_irrs = irr_by_plot.get(p.id, [])
        p_irr_15_liters = sum(
            float(i.volume_liters or 0) for i in p_irrs if i.irrigation_date and start_15 <= i.irrigation_date <= today
        )
        p_irr_15_mm = irrigation_mm(p_irr_15_liters, p_area)
        last_irr_day = max((i.irrigation_date for i in p_irrs if i.irrigation_date), default=None)

        last_water_day = max(filter(None, (last_rain_day, last_irr_day)), default=None)
        days_without_water = (today - last_water_day).days if last_water_day else None
        is_irrigated = (p.irrigation_type or "none") in {"gotejo", "aspersor"}
        if days_without_water is None:
            status = "warn" if is_irrigated else "muted"
            status_label = "Sem registro"
        elif is_irrigated and days_without_water >= 8:
            status = "err"
            status_label = f"{days_without_water}d sem água"
            dry_alert_count += 1
        elif is_irrigated and days_without_water >= 5:
            status = "warn"
            status_label = f"{days_without_water}d sem água"
            dry_alert_count += 1
        else:
            status = "ok"
            status_label = "Hoje" if days_without_water == 0 else f"Há {days_without_water}d"

        plot_water_rows.append(
            {
                "plot_id": p.id,
                "plot_name": p.name,
                "area": round(p_area, 1),
                "irrigation_type": p.irrigation_type or "none",
                "irrigation_label": IRRIGATION_LABELS.get(p.irrigation_type or "none", "Sequeiro"),
                "is_irrigated": is_irrigated,
                "rain_15d_mm": p_rain_15,
                "irr_15d_mm": p_irr_15_mm,
                "irr_15d_liters": round(p_irr_15_liters, 0),
                "total_15d_mm": round(p_rain_15 + p_irr_15_mm, 1),
                "last_water_br": last_water_day.strftime("%d/%m/%Y") if last_water_day else "—",
                "days_without_water": days_without_water,
                "status": status,
                "status_label": status_label,
            }
        )

    # Gráfico combinado últimos 6 meses (Chuva mm vs Irrigação média mm)
    chart_labels: list[str] = []
    chart_rain: list[float] = []
    chart_irr: list[float] = []
    cur_y, cur_m = today.year, today.month
    months_seq: list[tuple[int, int]] = []
    for _ in range(6):
        months_seq.append((cur_y, cur_m))
        cur_m -= 1
        if cur_m == 0:
            cur_m = 12
            cur_y -= 1
    months_seq.reverse()
    for yr, mo in months_seq:
        chart_labels.append(f"{MONTHS_SHORT[mo]}/{str(yr)[2:]}")
        chart_rain.append(round(rain_by_ym.get((yr, mo), 0.0), 1))
        vals = irr_by_ym_mm.get((yr, mo), [])
        chart_irr.append(round(sum(vals) / max(len(plots), 1), 1) if vals else 0.0)

    return {
        "forecast_days": forecast_days,
        "spray_window": spray_window,
        "kpi": {
            "month_label": MONTHS_LONG[today.month],
            "month_rain_mm": month_rain,
            "month_avg_mm": month_avg,
            "rain_7d_mm": round(rain_7d, 1),
            "rain_15d_mm": round(rain_15d, 1),
            "rain_30d_mm": round(rain_30d, 1),
            "forecast_3d_mm": forecast_3d_mm,
            "forecast_7d_mm": forecast_7d_mm,
            "latest_rain_days": (today - latest_rain.rainfall_date).days if latest_rain else None,
            "latest_rain_mm": round(float(latest_rain.millimeters or 0), 1) if latest_rain else None,
            "irr_30d_m3": round(total_irr_liters_30d / 1000.0, 1),
            "dry_alert_count": dry_alert_count,
            "irrigated_plots_count": sum(1 for r in plot_water_rows if r["is_irrigated"]),
        },
        "plot_water_rows": plot_water_rows,
        "chart": {
            "labels": chart_labels,
            "rain": chart_rain,
            "irrigation": chart_irr,
        },
    }


def build_irrigation_page_context(
    *,
    irrigations: list,
    plots: list,
    rainfalls: list,
    farm,
    today: date,
) -> dict[str, Any]:
    """KPIs e enriquecimento (mm, L/planta, contexto de chuva) para a página de Irrigação."""
    plot_by_id = {p.id: p for p in plots}
    total_liters = 0.0
    total_minutes = 0
    from_fert_count = 0
    mm_by_id: dict[int, float] = {}
    l_per_plant_by_id: dict[int, float | None] = {}
    covered_plots: set[int] = set()

    for item in irrigations:
        vol = float(item.volume_liters or 0)
        total_liters += vol
        total_minutes += int(item.duration_minutes or 0)
        if item.origin == "fertilizacao":
            from_fert_count += 1
        p = item.plot or plot_by_id.get(item.plot_id)
        if p:
            covered_plots.add(p.id)
            mm_by_id[item.id] = irrigation_mm(vol, p.area_hectares)
            l_per_plant_by_id[item.id] = round(vol / p.plant_count, 2) if p.plant_count else None
        else:
            mm_by_id[item.id] = 0.0
            l_per_plant_by_id[item.id] = None

    total_area_covered = sum(float(plot_by_id[pid].area_hectares or 0) for pid in covered_plots if pid in plot_by_id)
    avg_mm = round(total_liters / (total_area_covered * 10000.0), 1) if total_area_covered > 0 else 0.0

    start_7 = today - timedelta(days=6)
    rain_7d = round(
        sum(float(r.millimeters or 0) for r in rainfalls if r.rainfall_date and start_7 <= r.rainfall_date <= today),
        1,
    )
    forecast_days = get_forecast_days_safe(farm)
    forecast_3d = round(sum(d["precip_mm"] for d in forecast_days[:3]), 1)

    return {
        "kpi": {
            "total_liters": round(total_liters, 0),
            "total_m3": round(total_liters / 1000.0, 1),
            "avg_mm": avg_mm,
            "total_hours": round(total_minutes / 60.0, 1),
            "total_count": len(irrigations),
            "from_fert_count": from_fert_count,
            "manual_count": len(irrigations) - from_fert_count,
            "covered_plots": len(covered_plots),
            "rain_7d_mm": rain_7d,
            "forecast_3d_mm": forecast_3d,
        },
        "mm_by_id": mm_by_id,
        "l_per_plant_by_id": l_per_plant_by_id,
    }


def build_rainfall_page_context(
    *,
    rainfalls_all: list,
    farm,
    today: date,
    rain_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """KPIs, série histórica de 12 meses e previsão resumida para a página de Chuva e Alertas."""
    rain_settings = rain_settings or {}
    rainy_day_mm = float(rain_settings.get("rainy_day_mm") or 1.0)
    heavy_rain_mm = float(rain_settings.get("heavy_rain_mm") or 30.0)
    since_year = rain_settings.get("average_since_year")

    by_ym: dict[tuple[int, int], float] = defaultdict(float)
    rainy_days_month = 0
    heavy_events_12m = 0
    total_12m = 0.0
    cutoff_12m = today - timedelta(days=365)
    latest = None

    for r in rainfalls_all:
        if not r.rainfall_date:
            continue
        mm = float(r.millimeters or 0)
        by_ym[(r.rainfall_date.year, r.rainfall_date.month)] += mm
        if r.rainfall_date.year == today.year and r.rainfall_date.month == today.month and mm >= rainy_day_mm:
            rainy_days_month += 1
        if r.rainfall_date >= cutoff_12m:
            total_12m += mm
            if mm >= heavy_rain_mm:
                heavy_events_12m += 1
        if mm >= rainy_day_mm and (latest is None or r.rainfall_date > latest.rainfall_date):
            latest = r

    month_total = round(by_ym.get((today.year, today.month), 0.0), 1)
    prev_years = [
        val for (yr, mo), val in by_ym.items()
        if mo == today.month and yr < today.year and (not since_year or yr >= since_year)
    ]
    month_avg = round(sum(prev_years) / len(prev_years), 1) if prev_years else None

    # Série 12 meses
    labels: list[str] = []
    values: list[float] = []
    cur_y, cur_m = today.year, today.month
    seq: list[tuple[int, int]] = []
    for _ in range(12):
        seq.append((cur_y, cur_m))
        cur_m -= 1
        if cur_m == 0:
            cur_m = 12
            cur_y -= 1
    seq.reverse()
    for yr, mo in seq:
        labels.append(f"{MONTHS_SHORT[mo]}/{str(yr)[2:]}")
        values.append(round(by_ym.get((yr, mo), 0.0), 1))

    return {
        "kpi": {
            "month_label": MONTHS_LONG[today.month],
            "month_total_mm": month_total,
            "month_avg_mm": month_avg,
            "rainy_days_month": rainy_days_month,
            "total_12m_mm": round(total_12m, 1),
            "heavy_events_12m": heavy_events_12m,
            "latest_date_br": latest.rainfall_date.strftime("%d/%m/%Y") if latest else "—",
            "latest_mm": round(float(latest.millimeters or 0), 1) if latest else None,
            "latest_days": (today - latest.rainfall_date).days if latest else None,
        },
        "chart": {"labels": labels, "values": values},
        "forecast_days": get_forecast_days_safe(farm)[:5],
    }
