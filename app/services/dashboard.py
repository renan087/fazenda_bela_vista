import json
import unicodedata
from collections import defaultdict
from datetime import date, timedelta

from app.core.timezone import today_in_app_timezone
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


def _in_season(record_date, season) -> bool:
    if not season or not record_date:
        return True
    return season.start_date <= record_date <= season.end_date


def _normalize_payment_method(value: str | None) -> str:
    return (
        unicodedata.normalize("NFD", str(value or ""))
        .encode("ascii", "ignore")
        .decode("ascii")
        .strip()
        .lower()
    )


def _finance_transaction_uses_future_settlement(transaction) -> bool:
    payment_condition = (transaction.payment_condition or "").strip().lower()
    if payment_condition == "a_prazo":
        return True
    if transaction.credit_card_id and _normalize_payment_method(transaction.payment_method) == "cartao de credito":
        return True
    return False


def _account_initial_balance(account) -> float:
    return round(_float(account.initial_balance), 2)


def _finance_source_label(transaction) -> str:
    source = (transaction.source or "").strip().lower()
    if source == "insumos":
        return "Compra de Insumos"
    if source == "suprimentos":
        return "Suprimentos"
    if source in {"patrimônio", "patrimonio"}:
        return "Patrimônio"
    if source in {"comercialização", "comercializacao"}:
        return "Comercialização"
    return "Contas"


def _build_dashboard_finance_flow(repository: FarmRepository, farm_id: int | None, today: date) -> dict:
    if not farm_id:
        return {
            "balance": 0,
            "payable_open_total": 0,
            "receivable_open_total": 0,
            "overdue_total": 0,
            "overdue_count": 0,
            "due_today_total": 0,
            "due_today_count": 0,
            "realized_credit_month": 0,
            "realized_debit_month": 0,
            "realized_balance_month": 0,
            "upcoming_7_days_total": 0,
            "action_items": [],
        }

    month_start = today.replace(day=1)
    upcoming_limit = today + timedelta(days=7)
    accounts = repository.list_finance_accounts(farm_id=farm_id)
    account_balances = {account.id: _account_initial_balance(account) for account in accounts}

    totals = {
        "payable_open_total": 0.0,
        "receivable_open_total": 0.0,
        "overdue_total": 0.0,
        "overdue_count": 0,
        "due_today_total": 0.0,
        "due_today_count": 0,
        "realized_credit_month": 0.0,
        "realized_debit_month": 0.0,
        "upcoming_7_days_total": 0.0,
    }
    action_items: list[dict] = []

    for transaction in repository.list_finance_transactions(farm_id=farm_id):
        operation_type = (transaction.operation_type or "").strip().lower()
        is_revenue = operation_type == "receita"
        amount = abs(_float(transaction.amount))
        account_id = transaction.finance_account_id
        source_label = _finance_source_label(transaction)

        if _finance_transaction_uses_future_settlement(transaction):
            installments = sorted(
                transaction.installments or [],
                key=lambda item: (item.due_date or date.max, item.installment_number or 0, item.id),
            )
            total_parts = int(transaction.installment_count or 0) or len(installments) or 1
            for installment in installments:
                installment_amount = abs(_float(installment.amount))
                if installment_amount <= 0:
                    continue
                status = (installment.status or "pendente").strip().lower()
                due_date = installment.due_date
                paid_at = installment.paid_at
                if status == "pago":
                    if account_id in account_balances and paid_at and paid_at <= today:
                        if is_revenue:
                            account_balances[account_id] = round(account_balances[account_id] + installment_amount, 2)
                        else:
                            account_balances[account_id] = round(account_balances[account_id] - installment_amount, 2)
                    if paid_at and month_start <= paid_at <= today:
                        if is_revenue:
                            totals["realized_credit_month"] += installment_amount
                        else:
                            totals["realized_debit_month"] += installment_amount
                    continue

                if is_revenue:
                    totals["receivable_open_total"] += installment_amount
                else:
                    totals["payable_open_total"] += installment_amount

                item_status = "open"
                item_label = "Em aberto"
                if due_date and due_date < today:
                    totals["overdue_total"] += installment_amount
                    totals["overdue_count"] += 1
                    item_status = "overdue"
                    item_label = "Atrasado"
                elif due_date and due_date == today:
                    totals["due_today_total"] += installment_amount
                    totals["due_today_count"] += 1
                    item_status = "today"
                    item_label = "Vence hoje"
                elif due_date and today < due_date <= upcoming_limit:
                    totals["upcoming_7_days_total"] += installment_amount
                    item_status = "upcoming"
                    item_label = "Próx. 7 dias"

                if item_status in {"overdue", "today", "upcoming"}:
                    action_items.append(
                        {
                            "date": due_date,
                            "status": item_status,
                            "status_label": item_label,
                            "type": "receber" if is_revenue else "pagar",
                            "source": source_label,
                            "title": transaction.product_service,
                            "installment": f"{installment.installment_number}/{total_parts}",
                            "amount": round(installment_amount, 2),
                            "link": f"/gestao-financeira/contas?finance_tab={'receivables' if is_revenue else 'payables'}",
                        }
                    )
            continue

        launch_date = transaction.launch_date
        if amount > 0 and account_id in account_balances and launch_date and launch_date <= today:
            if is_revenue:
                account_balances[account_id] = round(account_balances[account_id] + amount, 2)
            else:
                account_balances[account_id] = round(account_balances[account_id] - amount, 2)
        if launch_date and month_start <= launch_date <= today:
            if is_revenue:
                totals["realized_credit_month"] += amount
            else:
                totals["realized_debit_month"] += amount

    action_items.sort(
        key=lambda item: (
            {"overdue": 0, "today": 1, "upcoming": 2}.get(item["status"], 3),
            item["date"] or date.max,
            item["title"] or "",
        )
    )
    realized_balance = totals["realized_credit_month"] - totals["realized_debit_month"]
    balance = round(sum(account_balances.values()), 2)
    payable_open = round(totals["payable_open_total"], 2)
    receivable_open = round(totals["receivable_open_total"], 2)
    projected_balance = round(balance + receivable_open - payable_open, 2)
    chart_max = max(abs(balance), abs(receivable_open), abs(payable_open), abs(projected_balance), 1)
    projected_chart = [
        {
            "label": "Saldo atual",
            "value": balance,
            "width": round((abs(balance) / chart_max) * 100, 2),
            "kind": "neutral",
        },
        {
            "label": "A receber",
            "value": receivable_open,
            "width": round((abs(receivable_open) / chart_max) * 100, 2),
            "kind": "positive",
        },
        {
            "label": "A pagar",
            "value": -payable_open,
            "width": round((abs(payable_open) / chart_max) * 100, 2),
            "kind": "negative",
        },
        {
            "label": "Projetado",
            "value": projected_balance,
            "width": round((abs(projected_balance) / chart_max) * 100, 2),
            "kind": "positive" if projected_balance >= 0 else "negative",
        },
    ]
    return {
        "balance": balance,
        "payable_open_total": payable_open,
        "receivable_open_total": receivable_open,
        "projected_balance": projected_balance,
        "projected_chart": projected_chart,
        "overdue_total": round(totals["overdue_total"], 2),
        "overdue_count": int(totals["overdue_count"]),
        "due_today_total": round(totals["due_today_total"], 2),
        "due_today_count": int(totals["due_today_count"]),
        "realized_credit_month": round(totals["realized_credit_month"], 2),
        "realized_debit_month": round(totals["realized_debit_month"], 2),
        "realized_balance_month": round(realized_balance, 2),
        "upcoming_7_days_total": round(totals["upcoming_7_days_total"], 2),
        "current_month_start": month_start.isoformat(),
        "current_month_end": today.isoformat(),
        "action_items": action_items[:6],
    }


def calculate_forecast(
    repository: FarmRepository,
    plots: list | None = None,
    harvests: list | None = None,
) -> dict:
    harvests = harvests if harvests is not None else repository.list_harvests()
    grouped: dict[int, list[float]] = defaultdict(list)
    plots = {plot.id: plot for plot in (plots if plots is not None else repository.list_plots())}

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
    farm_id: int | None = None,
    season=None,
) -> dict:
    pages = pages or {}
    plots = repository.list_plots(
        farm_ids=[farm_id] if farm_id else None,
        variety_ids=[season.variety_id] if season and season.variety_id else None,
    )
    farms = [farm for farm in repository.list_farms() if not farm_id or farm.id == farm_id]
    plot_ids = {plot.id for plot in plots}
    harvests = [
        harvest
        for harvest in repository.list_harvests()
        if harvest.plot_id in plot_ids and _in_season(harvest.harvest_date, season)
    ]
    irrigations = [
        irrigation
        for irrigation in repository.list_irrigations()
        if irrigation.plot_id in plot_ids and _in_season(irrigation.irrigation_date, season)
    ]
    fertilizations = [
        fertilization
        for fertilization in repository.list_fertilizations()
        if fertilization.plot_id in plot_ids and _in_season(fertilization.application_date, season)
    ]
    schedules = [
        schedule
        for schedule in repository.list_fertilization_schedules()
        if schedule.plot_id in plot_ids and _in_season(schedule.scheduled_date, season)
    ]
    purchased_inputs = [
        entry
        for entry in repository.list_purchased_inputs()
        if not farm_id or entry.farm_id in (None, farm_id)
    ]
    catalog_inputs = repository.list_input_catalog()
    incidents = [
        incident
        for incident in repository.list_pest_incidents()
        if incident.plot_id in plot_ids and _in_season(incident.occurrence_date, season)
    ]
    soil_analyses = [
        analysis
        for analysis in repository.list_soil_analyses(farm_id=farm_id)
        if analysis.plot_id in plot_ids and _in_season(analysis.analysis_date, season)
    ][:6]
    today = today_in_app_timezone()
    month_start = today.replace(day=1)
    rainfalls = repository.list_rainfalls(
        farm_id=farm_id,
        start_date=rain_start_date,
        end_date=rain_end_date,
    )
    month_rainfalls = repository.list_rainfalls(
        farm_id=farm_id,
        start_date=month_start,
        end_date=today,
    )
    finance_flow = _build_dashboard_finance_flow(repository, farm_id, today)
    forecast = calculate_forecast(repository, plots=plots, harvests=harvests)

    total_area = sum(_float(plot.area_hectares) for plot in plots)
    total_production = sum(_float(item.sacks_produced) for item in harvests)
    productivity_per_hectare = total_production / total_area if total_area else 0
    estimated_production = sum(_float(plot.estimated_yield_sacks) for plot in plots)
    total_cost = sum(_float(item.cost) for item in fertilizations)
    cost_per_hectare = total_cost / total_area if total_area else 0
    monthly_rainfall = sum(_float(item.millimeters) for item in month_rainfalls)
    rainfall_period_total = sum(_float(item.millimeters) for item in rainfalls)
    stock_by_input: dict[int, dict] = {}
    for catalog in catalog_inputs:
        related_entries = [entry for entry in purchased_inputs if entry.input_id == catalog.id]
        available_quantity = sum(_float(entry.available_quantity) for entry in related_entries)
        total_quantity = sum(_float(entry.total_quantity) for entry in related_entries)
        total_value = sum(
            (_float(entry.available_quantity) * (_float(entry.total_cost) / max(_float(entry.total_quantity), 1)))
            for entry in related_entries
            if _float(entry.available_quantity) > 0
        )
        latest_entry = max(
            related_entries,
            key=lambda entry: ((entry.purchase_date or today), entry.id),
            default=None,
        )
        stock_by_input[catalog.id] = {
            "id": catalog.id,
            "name": catalog.name,
            "unit": catalog.default_unit,
            "available_quantity": round(available_quantity, 2),
            "total_quantity": round(total_quantity, 2),
            "low_stock_threshold": round(_float(catalog.low_stock_threshold), 2),
            "farm": latest_entry.farm if latest_entry else None,
            "last_purchase_date": latest_entry.purchase_date if latest_entry else None,
            "average_cost": round(total_value / available_quantity, 4) if available_quantity else 0,
        }

    low_stock_items = [
        item
        for item in stock_by_input.values()
        if item["low_stock_threshold"] > 0 and item["available_quantity"] <= item["low_stock_threshold"]
    ]
    low_stock_items.sort(key=lambda item: (item["available_quantity"], item["name"]))
    schedule_alerts = []
    for schedule in sorted(schedules, key=lambda schedule: (schedule.scheduled_date, schedule.id)):
        status = schedule.status
        if status == "completed":
            continue
        validation = {
            "ok": True,
            "shortages": [],
        }
        for item in schedule.items:
            available = stock_by_input.get(item.input_id or 0, {}).get("available_quantity", 0)
            required = _float(item.quantity)
            if required > available:
                validation["ok"] = False
                validation["shortages"].append(
                    {
                        "name": item.name,
                        "missing": round(required - available, 2),
                        "unit": item.unit or stock_by_input.get(item.input_id or 0, {}).get("unit", ""),
                    }
                )
        schedule_alerts.append(
            {
                "id": schedule.id,
                "plot": schedule.plot.name if schedule.plot else "Setor removido",
                "date": schedule.scheduled_date.isoformat(),
                "status": "late" if schedule.scheduled_date < today else "scheduled",
                "stock_ok": validation["ok"],
                "shortages": validation["shortages"],
                "link": f"/fertilizacao/agendamentos?edit_id={schedule.id}",
            }
        )
    cost_by_plot = defaultdict(float)
    for fertilization in fertilizations:
        plot_name = fertilization.plot.name if fertilization.plot else f"Setor {fertilization.plot_id}"
        cost_by_plot[plot_name] += _float(fertilization.cost)

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
            "low_stock_count": len(low_stock_items),
            "late_schedule_count": sum(1 for item in schedule_alerts if item["status"] == "late"),
            "finance_balance": finance_flow["balance"],
            "finance_payable_open": finance_flow["payable_open_total"],
            "finance_receivable_open": finance_flow["receivable_open_total"],
            "finance_realized_month": finance_flow["realized_balance_month"],
            "finance_overdue_count": finance_flow["overdue_count"],
        },
        "finance_flow": finance_flow,
        "recent_irrigations": irrigations,
        "recent_rainfalls": rainfalls,
        "recent_fertilizations": fertilizations,
        "recent_incidents": incidents,
        "recent_harvests": harvests,
        "recent_soil_analyses": soil_analyses,
        "low_stock_items": low_stock_items,
        "schedule_alerts": schedule_alerts,
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
        "cost_by_plot_chart": json.dumps(
            {
                "labels": list(cost_by_plot.keys()),
                "values": [round(value, 2) for value in cost_by_plot.values()],
            }
        ),
        "map_geojson": json.dumps({"type": "FeatureCollection", "features": map_features}),
        "farms": farms,
        "activity_timeline": activity_timeline,
    }
