import json
from collections import defaultdict

from app.repositories.farm import FarmRepository


def _float(value) -> float:
    return float(value or 0)


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


def build_dashboard_context(repository: FarmRepository) -> dict:
    plots = repository.list_plots()
    farms = repository.list_farms()
    harvests = repository.list_harvests()
    irrigations = repository.list_irrigations(limit=6)
    fertilizations = repository.list_fertilizations(limit=6)
    incidents = repository.list_pest_incidents(limit=6)
    forecast = calculate_forecast(repository)

    total_area = sum(_float(plot.area_hectares) for plot in plots)
    total_production = sum(_float(item.sacks_produced) for item in harvests)
    productivity_per_hectare = total_production / total_area if total_area else 0
    estimated_production = sum(_float(plot.estimated_yield_sacks) for plot in plots)

    production_by_plot = defaultdict(float)
    harvest_timeline = defaultdict(float)
    for harvest in harvests:
        plot_name = harvest.plot.name if harvest.plot else f"Setor {harvest.plot_id}"
        production_by_plot[plot_name] += _float(harvest.sacks_produced)
        harvest_timeline[str(harvest.harvest_date)] += _float(harvest.sacks_produced)

    irrigation_chart = [
        {"label": item.irrigation_date.isoformat(), "value": _float(item.volume_liters)}
        for item in reversed(irrigations)
    ]

    map_features = []
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
                        "name": plot.name,
                        "variety": plot.variety.name if plot.variety else "Sem variedade",
                        "area": _float(plot.area_hectares),
                        "estimated": _float(plot.estimated_yield_sacks),
                    },
                    "geometry": geometry,
                }
            )

    return {
        "kpis": {
            "area_total": round(total_area, 2),
            "plot_count": len(plots),
            "estimated_production": round(estimated_production or forecast["total_projection"], 2),
            "total_production": round(total_production, 2),
            "productivity_per_hectare": round(productivity_per_hectare, 2),
            "forecast_production": forecast["total_projection"],
        },
        "recent_irrigations": irrigations,
        "recent_fertilizations": fertilizations,
        "recent_incidents": incidents,
        "recent_harvests": harvests[:8],
        "forecast_plots": forecast["plots"],
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
        "irrigation_chart": json.dumps(
            {
                "labels": [item["label"] for item in irrigation_chart],
                "values": [item["value"] for item in irrigation_chart],
            }
        ),
        "map_geojson": json.dumps({"type": "FeatureCollection", "features": map_features}),
        "farms": farms,
    }
