import json
from collections import defaultdict
from datetime import date

from app.repositories.farm import FarmRepository


def _float(value) -> float:
    return float(value or 0)


def _paginate(items: list, page: int, per_page: int = 4) -> dict:
    total = len(items)
    pages = max(1, (total + per_page - 1) // per_page)
    current_page = min(max(page, 1), pages)
    start = (current_page - 1) * per_page
    end = start + per_page
    return {
        "items": items[start:end],
        "page": current_page,
        "pages": pages,
        "has_prev": current_page > 1,
        "has_next": current_page < pages,
        "prev_page": current_page - 1,
        "next_page": current_page + 1,
        "total": total,
    }


def _page_series(items: list[dict], page: int) -> list[dict]:
    meta = _paginate(items, page)
    return [{"page": index, "active": index == meta["page"]} for index in range(1, meta["pages"] + 1)]


def calculate_forecast(repository: FarmRepository) -> dict:
    harvests = repository.list_harvests()
    grouped: dict[int, list[float]] = defaultdict(list)
    plots = {plot.id: plot for plot in repository.list_plots()}

    for harvest in harvests:
        productivity = _float(harvest.productivity_per_hectare)
        if not productivity and harvest.plot:
            productivity = _float(harvest.sacks_produced) / max(_float(harvest.plot.area_hectares), 1)
        grouped[harvest.plot_id].append(productivity)

    projection_total = 0.0
    plot_forecasts = []
    for plot_id, plot in plots.items():
        history = [item for item in grouped.get(plot_id, []) if item > 0]
        average_productivity = (sum(history) / len(history)) if history else (_float(plot.estimated_yield_sacks) / max(_float(plot.area_hectares), 1) if _float(plot.estimated_yield_sacks) else 0)
        projected_sacks = average_productivity * max(_float(plot.area_hectares), 0)
        projection_total += projected_sacks
        plot_forecasts.append(
            {
                "plot_id": plot.id,
                "plot": plot.name,
                "projected_sacks": round(projected_sacks, 2),
                "productivity": round(average_productivity, 2),
            }
        )

    return {
        "total_projection": round(projection_total, 2),
        "plots": plot_forecasts,
    }


def build_dashboard_context(
    repository: FarmRepository,
    rain_start_date: date | None = None,
    rain_end_date: date | None = None,
    pages: dict | None = None,
) -> dict:
    pages = pages or {}
    plots = repository.list_plots()
    farms = repository.list_farms()
    harvests = repository.list_harvests()
    irrigations = repository.list_irrigations()
    fertilizations = repository.list_fertilizations()
    incidents = repository.list_pest_incidents()
    soil_analyses = repository.list_soil_analyses()[:6]
    today = date.today()
    month_start = today.replace(day=1)
    rainfalls = repository.list_rainfalls(
        start_date=rain_start_date,
        end_date=rain_end_date,
    )
    month_rainfalls = repository.list_rainfalls(
        start_date=month_start,
        end_date=today,
    )
    forecast = calculate_forecast(repository)

    total_area = sum(_float(plot.area_hectares) for plot in plots)
    total_production = sum(_float(item.sacks_produced) for item in harvests)
    productivity_per_hectare = total_production / total_area if total_area else 0
    estimated_production = sum(_float(plot.estimated_yield_sacks) for plot in plots)
    total_cost = sum(_float(item.cost) for item in fertilizations)
    cost_per_hectare = total_cost / total_area if total_area else 0
    monthly_rainfall = sum(_float(item.millimeters) for item in month_rainfalls)
    rainfall_period_total = sum(_float(item.millimeters) for item in rainfalls)

    production_by_plot = defaultdict(float)
    productivity_by_plot = {}
    harvest_timeline = defaultdict(float)
    rainfall_timeline = []
    for harvest in harvests:
        plot_name = harvest.plot.name if harvest.plot else f"Setor {harvest.plot_id}"
        production_by_plot[plot_name] += _float(harvest.sacks_produced)
        harvest_timeline[str(harvest.harvest_date)] += _float(harvest.sacks_produced)
        if harvest.plot and harvest.productivity_per_hectare is not None:
            productivity_by_plot[plot_name] = _float(harvest.productivity_per_hectare)

    for plot in plots:
        plot_name = plot.name
        if plot_name not in productivity_by_plot:
            estimated = _float(plot.estimated_yield_sacks)
            productivity_by_plot[plot_name] = round((estimated / _float(plot.area_hectares)) if estimated and _float(plot.area_hectares) else 0, 2)

    irrigation_chart = [
        {"label": item.irrigation_date.isoformat(), "value": _float(item.volume_liters)}
        for item in reversed(irrigations)
    ]
    for index, rainfall in enumerate(sorted(rainfalls, key=lambda item: (item.rainfall_date, item.id))):
        label = rainfall.rainfall_date.isoformat()
        if sum(1 for item in rainfalls if item.rainfall_date == rainfall.rainfall_date) > 1:
            label = f"{label} #{index + 1}"
        rainfall_timeline.append(
            {
                "label": label,
                "value": round(_float(rainfall.millimeters), 2),
            }
        )

    map_features = []
    for farm in farms:
        if farm.boundary_geojson:
            try:
                geometry = json.loads(farm.boundary_geojson)
            except json.JSONDecodeError:
                geometry = None
        else:
            geometry = None
        if geometry:
            map_features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "feature_type": "farm",
                        "name": farm.name,
                        "location": farm.location,
                        "area": _float(farm.total_area),
                    },
                    "geometry": geometry,
                }
            )

    for plot in plots:
        if plot.boundary_geojson:
            try:
                geometry = json.loads(plot.boundary_geojson)
            except json.JSONDecodeError:
                geometry = None
        else:
            geometry = None
        if geometry:
            map_features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "feature_type": "plot",
                        "name": plot.name,
                        "farm": plot.farm.name if plot.farm else "Sem fazenda",
                        "variety": plot.variety.name if plot.variety else "Sem variedade",
                        "area": _float(plot.area_hectares),
                        "estimated": _float(plot.estimated_yield_sacks),
                    },
                    "geometry": geometry,
                }
            )

    activity_timeline = []
    for irrigation in irrigations:
        activity_timeline.append(
            {
                "date": irrigation.irrigation_date.isoformat(),
                "title": "Irrigacao registrada",
                "subtitle": irrigation.plot.name if irrigation.plot else "Setor removido",
                "detail": f"{_float(irrigation.volume_liters):.2f} L em {irrigation.duration_minutes} min",
                "link": f"/irrigacao?edit_id={irrigation.id}",
                "kind": "irrigacao",
            }
        )
    for fertilization in fertilizations:
        activity_timeline.append(
            {
                "date": fertilization.application_date.isoformat(),
                "title": "Fertilizacao registrada",
                "subtitle": fertilization.plot.name if fertilization.plot else "Setor removido",
                "detail": f"{len(fertilization.items) or 1} insumo(s) • R$ {_float(fertilization.cost):.2f}",
                "link": f"/fertilizacao?edit_id={fertilization.id}",
                "kind": "fertilizacao",
            }
        )
    for harvest in harvests:
        activity_timeline.append(
            {
                "date": harvest.harvest_date.isoformat(),
                "title": "Colheita registrada",
                "subtitle": harvest.plot.name if harvest.plot else "Setor removido",
                "detail": f"{_float(harvest.sacks_produced):.2f} sacas • {_float(harvest.productivity_per_hectare):.2f} sc/ha",
                "link": f"/producao?edit_id={harvest.id}",
                "kind": "producao",
            }
        )
    for incident in incidents:
        activity_timeline.append(
            {
                "date": incident.occurrence_date.isoformat(),
                "title": f"{incident.category} registrada",
                "subtitle": incident.plot.name if incident.plot else "Setor removido",
                "detail": incident.name,
                "link": f"/pragas?edit_id={incident.id}",
                "kind": "sanidade",
            }
        )
    activity_timeline.sort(key=lambda item: item["date"], reverse=True)

    irrigations_page = _paginate(irrigations, pages.get("irrigations", 1))
    rainfalls_page = _paginate(rainfalls, pages.get("rainfalls", 1))
    fertilizations_page = _paginate(fertilizations, pages.get("fertilizations", 1))
    incidents_page = _paginate(incidents, pages.get("incidents", 1))
    harvests_page = _paginate(harvests, pages.get("harvests", 1))
    forecast_page = _paginate(forecast["plots"], pages.get("forecast", 1))
    timeline_page = _paginate(activity_timeline, pages.get("timeline", 1))

    return {
        "kpis": {
            "area_total": round(total_area, 2),
            "plot_count": len(plots),
            "estimated_production": round(estimated_production or forecast["total_projection"], 2),
            "total_production": round(total_production, 2),
            "productivity_per_hectare": round(productivity_per_hectare, 2),
            "forecast_production": forecast["total_projection"],
            "cost_per_hectare": round(cost_per_hectare, 2),
            "monthly_rainfall": round(monthly_rainfall, 2),
            "rainfall_period_total": round(rainfall_period_total, 2),
        },
        "recent_irrigations": irrigations_page["items"],
        "recent_rainfalls": rainfalls_page["items"],
        "recent_fertilizations": fertilizations_page["items"],
        "recent_incidents": incidents_page["items"],
        "recent_harvests": harvests_page["items"],
        "recent_soil_analyses": soil_analyses,
        "forecast_plots": forecast_page["items"],
        "production_chart": json.dumps(
            {
                "labels": list(production_by_plot.keys()),
                "values": [round(value, 2) for value in production_by_plot.values()],
            }
        ),
        "timeline_chart": json.dumps(
            {
                "labels": list(harvest_timeline.keys()),
                "values": [round(value, 2) for value in harvest_timeline.values()],
            }
        ),
        "productivity_chart": json.dumps(
            {
                "labels": list(productivity_by_plot.keys()),
                "values": [round(value, 2) for value in productivity_by_plot.values()],
            }
        ),
        "irrigation_chart": json.dumps(
            {
                "labels": [item["label"] for item in irrigation_chart],
                "values": [item["value"] for item in irrigation_chart],
            }
        ),
        "rainfall_chart": json.dumps(
            {
                "labels": [item["label"] for item in rainfall_timeline],
                "values": [item["value"] for item in rainfall_timeline],
            }
        ),
        "map_geojson": json.dumps({"type": "FeatureCollection", "features": map_features}),
        "farms": farms,
        "activity_timeline": timeline_page["items"],
        "dashboard_pages": {
            "irrigations": irrigations_page,
            "rainfalls": rainfalls_page,
            "fertilizations": fertilizations_page,
            "incidents": incidents_page,
            "harvests": harvests_page,
            "forecast": forecast_page,
            "timeline": timeline_page,
        },
        "dashboard_page_series": {
            "irrigations": _page_series(irrigations, pages.get("irrigations", 1)),
            "rainfalls": _page_series(rainfalls, pages.get("rainfalls", 1)),
            "fertilizations": _page_series(fertilizations, pages.get("fertilizations", 1)),
            "incidents": _page_series(incidents, pages.get("incidents", 1)),
            "harvests": _page_series(harvests, pages.get("harvests", 1)),
            "forecast": _page_series(forecast["plots"], pages.get("forecast", 1)),
            "timeline": _page_series(activity_timeline, pages.get("timeline", 1)),
        },
    }
