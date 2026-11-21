import json
import unicodedata
from collections import defaultdict
from datetime import date, timedelta

from app.core.timezone import today_in_app_timezone
from app.repositories.farm import FarmRepository
from app.services.coffee_quotes import latest_coffee_quote_context
from app.services.finance_overview import build_finance_overview_context
from app.services.forms import fertilization_application_method_label
from app.services.sector_costs import build_sector_cost_context
from app.services.harvest_scope import harvest_matches_season


def _float(value) -> float:
    return float(value or 0)


def filter_dashboard_attention(overview: dict, permissions) -> None:
    items = [item for item in overview.get("inbox_all", []) if item.get("permission") in permissions]
    overview["inbox_all"] = items
    overview["inbox"] = items[:6]
    upcoming = overview.get("finance_upcoming", []) if "page.finance" in permissions else []
    overview["attention"] = build_attention(items, upcoming, overview.get("today") or date.today())


_SHORT_MONTHS = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]


def _day_label(day) -> str:
    return f"{day.day} {_SHORT_MONTHS[day.month - 1]}" if day else ""


def build_attention(inbox: list[dict], upcoming: list[dict], today: date) -> dict:
    """Agrupa as pendencias para o cartao e o modal: parcelas da mesma conta viram uma linha."""
    def group_bills(items: list[dict], section: str) -> list[dict]:
        groups: dict = {}
        for item in items:
            key = (item.get("name") or item.get("title"), item.get("type"))
            groups.setdefault(key, []).append(item)
        rows = []
        for (name, kind), parts in groups.items():
            receive = kind == "receber"
            total = round(sum(_float(p.get("amount")) for p in parts), 2)
            source = parts[0].get("source") or ""
            first = parts[0].get("date")
            if section == "late":
                meta = f"{len(parts)} parcelas vencidas" if len(parts) > 1 else f"Venceu em {_day_label(first)}"
            elif section == "today":
                meta = "Vence hoje"
            else:
                meta = f"Vence em {_day_label(first)}"
            if receive:
                meta += " · a receber"
            rows.append({
                "kind": "bill", "section": section, "title": name or "Conta", "meta": f"{meta} · {source}" if source else meta,
                "amount": total, "receive": receive, "action": "Receber" if receive else "Pagar", "link": parts[0].get("link"),
                "kids": [{"n": p.get("installment"), "date": _day_label(p.get("date")), "amount": _float(p.get("amount"))} for p in parts],
            })
        return rows

    bills = [i for i in inbox if i.get("kind") == "bill"]
    fields = [i for i in inbox if i.get("kind") != "bill"]
    late_rows = group_bills([i for i in bills if i.get("status") == "overdue"], "late")
    late_rows += [{"kind": "field", "section": "late", "title": i.get("title"), "meta": i.get("text"), "amount": None,
                   "action": i.get("action"), "link": i.get("link"), "tone": i.get("tone"), "kids": []} for i in fields]
    today_rows = group_bills([i for i in bills if i.get("status") == "today"], "today")
    next_rows = group_bills(upcoming, "next")
    payables = [i for i in bills if i.get("type") != "receber"] + [i for i in upcoming if i.get("type") != "receber"]
    days = []
    for offset in range(-7, 7):
        day = today + timedelta(days=offset)
        value = round(sum(_float(i.get("amount")) for i in payables if i.get("date") == day), 2)
        days.append({"value": value, "offset": offset, "label": "Hoje" if offset == 0 else _day_label(day)})
    peak = max([d["value"] for d in days] + [0])
    for d in days:
        d["pct"] = round(d["value"] / peak * 100, 1) if peak else 0
    return {
        "count": len(inbox),
        "bill_count": len(bills),
        "field_count": len(fields),
        "overdue_total": round(sum(_float(i.get("amount")) for i in bills if i.get("status") == "overdue" and i.get("type") != "receber"), 2),
        "next_total": round(sum(_float(i.get("amount")) for i in upcoming if i.get("type") != "receber"), 2),
        "days": days,
        "card_rows": (today_rows + late_rows)[:4],
        "sections": [
            {"key": "late", "title": "Atrasadas", "rows": late_rows},
            {"key": "today", "title": "Hoje", "rows": today_rows},
            {"key": "next", "title": "Próximos 7 dias", "rows": next_rows},
        ],
    }


def _compact_currency_label(value) -> str:
    numeric = _float(value)
    sign = "-" if numeric < 0 else ""
    amount = abs(int(numeric))
    if amount >= 1_000_000_000:
        return f"{sign}{amount // 1_000_000_000}bi"
    if amount >= 1_000_000:
        return f"{sign}{amount // 1_000_000}M"
    if amount >= 1_000:
        return f"{sign}{amount // 1_000}k"
    return f"{sign}{amount}"


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


def _month_end(day: date) -> date:
    return date(day.year + (1 if day.month == 12 else 0), 1 if day.month == 12 else day.month + 1, 1) - timedelta(days=1)


def _month_label(day: date | None) -> str:
    if not day:
        return "Sem mês em aberto"
    names = [
        "Janeiro",
        "Fevereiro",
        "Março",
        "Abril",
        "Maio",
        "Junho",
        "Julho",
        "Agosto",
        "Setembro",
        "Outubro",
        "Novembro",
        "Dezembro",
    ]
    return f"{names[day.month - 1]}/{day.year}"


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


def _coffee_quote_type_for_harvest(harvest) -> str:
    variety = getattr(getattr(harvest, "plot", None), "variety", None)
    species = (
        unicodedata.normalize("NFD", str(getattr(variety, "species", "") or ""))
        .encode("ascii", "ignore")
        .decode("ascii")
        .strip()
        .lower()
    )
    if any(token in species for token in ("robusta", "conilon", "canephora")):
        return "robusta"
    return "arabica"


def _build_coffee_market_context(repository: FarmRepository, harvests: list, farm_id: int | None, scoped_farm_id_set: set[int] | None) -> dict:
    quote_context = latest_coffee_quote_context(repository)
    quotes = quote_context["quotes"]
    commercializations = repository.list_coffee_commercializations(farm_id=farm_id)
    if scoped_farm_id_set is not None:
        commercializations = [item for item in commercializations if item.farm_id in scoped_farm_id_set]
    sold_by_harvest: dict[int, float] = defaultdict(float)
    for item in commercializations:
        if item.harvest_id:
            sold_by_harvest[item.harvest_id] += _float(item.equivalent_sacks)

    available_sacks_by_type = {"arabica": 0.0, "robusta": 0.0}
    estimated_value_by_type = {"arabica": 0.0, "robusta": 0.0}
    available_lots = 0
    for harvest in harvests:
        available = max(_float(harvest.sacks_produced) - sold_by_harvest.get(harvest.id, 0.0), 0.0)
        if available <= 0:
            continue
        quote_type = _coffee_quote_type_for_harvest(harvest)
        available_lots += 1
        available_sacks_by_type[quote_type] += available
        quote = quotes.get(quote_type)
        if quote:
            estimated_value_by_type[quote_type] += available * _float(quote.price_brl)

    estimated_total = round(sum(estimated_value_by_type.values()), 2)
    available_total = round(sum(available_sacks_by_type.values()), 2)
    return {
        "quotes": quotes,
        "history": quote_context["history"],
        "source": quote_context["source"],
        "source_url": quote_context["source_url"],
        "available_sacks": {
            "arabica": round(available_sacks_by_type["arabica"], 2),
            "robusta": round(available_sacks_by_type["robusta"], 2),
            "total": available_total,
        },
        "estimated_value": {
            "arabica": round(estimated_value_by_type["arabica"], 2),
            "robusta": round(estimated_value_by_type["robusta"], 2),
            "total": estimated_total,
        },
        "available_lots": available_lots,
    }


def _build_dashboard_finance_flow(repository: FarmRepository, farm_id: int | None, today: date) -> dict:
    month_start = today.replace(day=1)
    empty_projection_chart = {
        "labels": ["Saldo atual", "Após receber", "Após pagar", "Projetado"],
        "values": [0.0, 0.0, 0.0, 0.0],
    }
    if not farm_id:
        return {
            "balance": 0,
            "payable_open_total": 0,
            "payable_next_month_total": 0,
            "payable_next_month_label": "Sem mês em aberto",
            "payable_next_month_start": "",
            "payable_next_month_end": "",
            "receivable_open_total": 0,
            "projected_balance": 0,
            "projected_chart": empty_projection_chart,
            "overdue_total": 0,
            "overdue_count": 0,
            "due_today_total": 0,
            "due_today_count": 0,
            "realized_credit_month": 0,
            "realized_debit_month": 0,
            "realized_balance_month": 0,
            "upcoming_7_days_total": 0,
            "current_month_start": month_start.isoformat(),
            "current_month_end": today.isoformat(),
            "action_items": [],
            "all_action_items": [],
        }

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
    payable_month_totals: dict[tuple[int, int], float] = defaultdict(float)

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
                    if due_date and due_date >= month_start:
                        payable_month_totals[(due_date.year, due_date.month)] += installment_amount

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
    next_payable_month = min(payable_month_totals.keys(), default=None)
    next_payable_month_start = date(next_payable_month[0], next_payable_month[1], 1) if next_payable_month else None
    next_payable_month_end = _month_end(next_payable_month_start) if next_payable_month_start else None
    payable_next_month_total = round(payable_month_totals.get(next_payable_month, 0.0), 2) if next_payable_month else 0
    receivable_open = round(totals["receivable_open_total"], 2)
    projected_balance = round(balance + receivable_open - payable_open, 2)
    projected_chart = {
        "labels": ["Saldo atual", "Após receber", "Após pagar", "Projetado"],
        "values": [
            balance,
            round(balance + receivable_open, 2),
            projected_balance,
            projected_balance,
        ],
    }
    return {
        "balance": balance,
        "payable_open_total": payable_open,
        "payable_next_month_total": payable_next_month_total,
        "payable_next_month_label": _month_label(next_payable_month_start),
        "payable_next_month_start": next_payable_month_start.isoformat() if next_payable_month_start else "",
        "payable_next_month_end": next_payable_month_end.isoformat() if next_payable_month_end else "",
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
        "all_action_items": action_items,
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
        plot = plots.get(harvest.plot_id) or harvest.plot
        current = plot.active_planting if plot is not None else None
        # Colheitas de um plantio anterior (antes da renovacao) nao entram na previsao da lavoura atual.
        if current and current.planting_date and harvest.harvest_date and harvest.harvest_date < current.planting_date:
            continue
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
    organization_id: int | None = None,
    context_farm_ids: list[int] | None = None,
    rain_settings: dict | None = None,
) -> dict:
    pages = pages or {}
    scoped_farm_ids = [farm_id] if farm_id else context_farm_ids
    scoped_farm_id_set = set(scoped_farm_ids or []) if scoped_farm_ids is not None else None
    plots = repository.list_plots(
        farm_ids=scoped_farm_ids,
        variety_ids=[season.variety_id] if season and season.variety_id else None,
    )
    linked_plots = [plot for plot in season.plots if scoped_farm_id_set is None or plot.farm_id in scoped_farm_id_set] if season and season.plots else []
    if season and season.plots:
        plots = linked_plots
    farms = repository.list_farms(organization_id=organization_id)
    if scoped_farm_id_set is not None:
        farms = [farm for farm in farms if farm.id in scoped_farm_id_set]
    plot_ids = {plot.id for plot in plots}
    harvests = [
        harvest
        for harvest in repository.list_harvests()
        if harvest.plot_id in plot_ids and harvest_matches_season(harvest, season)
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
        for entry in repository.list_purchased_inputs(include_stock_adjustments=True)
        if (scoped_farm_id_set is None or entry.farm_id in scoped_farm_id_set)
        and entry.input_catalog
        and entry.input_catalog.item_type in {"insumo_agricola", "combustivel"}
    ]
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
    coffee_market = _build_coffee_market_context(repository, harvests, farm_id, scoped_farm_id_set)

    finance_overview = build_finance_overview_context(repository, farm_id=farm_id, active_season=season)
    sector_cost_per_ha: dict[int, float] = {}
    operational_cost_season = _float(finance_overview.get("operational_cost_season"))
    if season and season.plots and farm_id:
        sector_costs = build_sector_cost_context(
            plots=plots,
            stock_outputs=repository.list_stock_outputs(farm_id=farm_id),
            fertilizations=fertilizations,
            finance_transactions=repository.list_finance_transactions(farm_id=farm_id),
            season_id=season.id,
            start_date=season.start_date,
            end_date=season.end_date,
        )
        operational_cost_season = sector_costs["allocated_total"]
        sector_cost_per_ha = {
            row["plot"].id: _float(row.get("cost_per_hectare")) for row in sector_costs.get("rows", []) if row.get("plot") is not None
        }

    total_area = sum(_float(plot.area_hectares) for plot in plots)
    total_production = sum(_float(item.sacks_produced) for item in harvests)
    productivity_per_hectare = total_production / total_area if total_area else 0
    season_plantings = season.planting_by_plot if season is not None and hasattr(season, "planting_by_plot") else {}
    estimated_production = sum(_float((season_plantings.get(plot.id) or plot).estimated_yield_sacks) for plot in plots)
    total_cost = sum(_float(item.cost) for item in fertilizations)
    cost_per_hectare = total_cost / total_area if total_area else 0
    operational_cost_per_hectare = operational_cost_season / total_area if total_area else 0
    operational_cost_per_sack = operational_cost_season / total_production if total_production else 0
    monthly_rainfall = sum(_float(item.millimeters) for item in month_rainfalls)
    rainfall_period_total = sum(_float(item.millimeters) for item in rainfalls)
    stock_by_input: dict[int, dict] = {}
    catalog_ids = {
        entry.input_id
        for entry in purchased_inputs
        if entry.input_id and entry.input_catalog
    }
    for catalog_id in catalog_ids:
        related_entries = [entry for entry in purchased_inputs if entry.input_id == catalog_id]
        latest_entry = max(
            related_entries,
            key=lambda entry: ((entry.purchase_date or today), entry.id),
            default=None,
        )
        catalog = latest_entry.input_catalog if latest_entry else None
        if not catalog:
            continue
        available_quantity = sum(_float(entry.available_quantity) for entry in related_entries)
        total_quantity = sum(_float(entry.total_quantity) for entry in related_entries)
        total_value = sum(
            (_float(entry.available_quantity) * (_float(entry.total_cost) / max(_float(entry.total_quantity), 1)))
            for entry in related_entries
            if _float(entry.available_quantity) > 0
        )
        stock_by_input[catalog.id] = {
            "id": catalog.id,
            "name": catalog.name,
            "unit": latest_entry.package_unit or catalog.default_unit,
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
    for item in low_stock_items:
        item["stock_level"] = (
            "critical"
            if item["available_quantity"] <= item["low_stock_threshold"] * 0.4
            else "low"
        )
        item["stock_level_label"] = "Crítico" if item["stock_level"] == "critical" else "Baixo"
    low_stock_items.sort(
        key=lambda item: (
            0 if item["stock_level"] == "critical" else 1,
            item["available_quantity"],
            item["name"],
        )
    )
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
                "application_method": schedule.application_method or "fertirrigacao",
                "application_method_label": fertilization_application_method_label(schedule.application_method),
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

    finance_scope_ready = bool(finance_overview.get("finance_scope_ready"))
    operational_cost_ha_available = bool(finance_scope_ready and total_area > 0)
    operational_cost_sack_available = bool(finance_scope_ready and total_production > 0)
    cost_per_hectare_value = round(cost_per_hectare, 2)
    operational_cost_per_hectare_value = round(operational_cost_per_hectare, 2)
    operational_cost_per_sack_value = round(operational_cost_per_sack, 2)

    overview = build_dashboard_overview(
        repository,
        farm_id=farm_id,
        season=season,
        today=today,
        plots=plots,
        total_area=total_area,
        estimated_production=round(estimated_production or forecast["total_projection"], 2),
        operational_cost_season=operational_cost_season,
        coffee_market=coffee_market,
        finance_flow=finance_flow,
        low_stock_items=low_stock_items,
        schedule_alerts=schedule_alerts,
        incidents=incidents,
        forecast_plots=forecast["plots"],
        cost_by_plot=cost_by_plot,
        sector_cost_per_ha=sector_cost_per_ha,
        activity_timeline=activity_timeline,
        rain_settings=rain_settings,
    )

    return {
        "overview": overview,
        "kpis": {
            "area_total": round(total_area, 2),
            "plot_count": len(plots),
            "estimated_production": round(estimated_production or forecast["total_projection"], 2),
            "total_production": round(total_production, 2),
            "productivity_per_hectare": round(productivity_per_hectare, 2),
            "forecast_production": forecast["total_projection"],
            "cost_per_hectare": cost_per_hectare_value,
            "cost_per_hectare_compact": _compact_currency_label(cost_per_hectare_value),
            "operational_cost_per_hectare": operational_cost_per_hectare_value,
            "operational_cost_per_hectare_compact": _compact_currency_label(operational_cost_per_hectare_value),
            "operational_cost_per_sack": operational_cost_per_sack_value,
            "operational_cost_per_sack_compact": _compact_currency_label(operational_cost_per_sack_value),
            "operational_cost_ha_available": operational_cost_ha_available,
            "operational_cost_sack_available": operational_cost_sack_available,
            "finance_scope_ready": finance_scope_ready,
            "monthly_rainfall": round(monthly_rainfall, 2),
            "rainfall_period_total": round(rainfall_period_total, 2),
            "low_stock_count": len(low_stock_items),
            "late_schedule_count": sum(1 for item in schedule_alerts if item["status"] == "late"),
            "finance_balance": finance_flow["balance"],
            "finance_balance_compact": _compact_currency_label(finance_flow["balance"]),
            "finance_payable_open": finance_flow["payable_open_total"],
            "finance_payable_open_compact": _compact_currency_label(finance_flow["payable_open_total"]),
            "finance_receivable_open": finance_flow["receivable_open_total"],
            "finance_receivable_open_compact": _compact_currency_label(finance_flow["receivable_open_total"]),
            "finance_realized_month": finance_flow["realized_balance_month"],
            "finance_realized_month_compact": _compact_currency_label(finance_flow["realized_balance_month"]),
            "finance_overdue_count": finance_flow["overdue_count"],
        },
        "finance_flow": finance_flow,
        "coffee_market": coffee_market,
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


# ---------------------------------------------------------------------------
# Painel de comando da safra (Dashboard novo)
# ---------------------------------------------------------------------------
HEAVY_RAIN_MM = 30.0
_MONTHS_SHORT = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
_MONTHS_LONG = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]


def _spark_path(values: list, width: float = 168, height: float = 56) -> dict | None:
    """Linha e area do mini grafico de cotacao (SVG); None quando ha menos de 2 pontos."""
    values = [float(v) for v in values][-12:]
    if len(values) < 2:
        return None
    low, high = min(values), max(values)
    span = (high - low) or 1
    step = (width - 8) / (len(values) - 1)
    points = [(4 + i * step, height - 6 - (v - low) / span * (height - 14)) for i, v in enumerate(values)]
    line = " ".join(f"{'M' if i == 0 else 'L'}{x:.1f} {y:.1f}" for i, (x, y) in enumerate(points))
    return {
        "line": line,
        "area": f"{line} L{points[-1][0]:.1f} {height} L4 {height} Z",
        "x": round(points[-1][0], 1), "y": round(points[-1][1], 1),
        "change": round((values[-1] / values[0] - 1) * 100, 1) if values[0] else 0,
    }


def _brl(value: float, decimals: int = 0) -> str:
    text = f"{abs(value):,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return ("− " if value < 0 else "") + "R$ " + text


def _brl_compact(value: float) -> str:
    sign = "− " if value < 0 else ""
    amount = abs(value)
    if amount >= 1_000_000:
        return f"{sign}R$ " + f"{amount / 1_000_000:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + " mi"
    if amount >= 1_000:
        return f"{sign}R$ " + f"{amount / 1_000:,.0f}".replace(",", ".") + " mil"
    return f"{sign}R$ " + f"{amount:,.0f}".replace(",", ".")


def _dashboard_cash_weeks(repository: FarmRepository, farm_id: int | None, today: date, weeks: int = 5) -> list[dict]:
    """Entradas e saidas previstas (parcelas em aberto) por semana, a partir de hoje."""
    buckets = []
    start = today
    for index in range(weeks):
        end = start + timedelta(days=6)
        buckets.append({"start": start, "end": end, "in": 0.0, "out": 0.0})
        start = end + timedelta(days=1)
    if not farm_id:
        return buckets
    horizon = buckets[-1]["end"]
    for transaction in repository.list_finance_transactions(farm_id=farm_id):
        if not _finance_transaction_uses_future_settlement(transaction):
            continue
        is_revenue = (transaction.operation_type or "").strip().lower() == "receita"
        for installment in transaction.installments or []:
            if (installment.status or "pendente").strip().lower() == "pago" or not installment.due_date:
                continue
            due = max(installment.due_date, today)  # atrasadas entram na semana atual
            if due > horizon:
                continue
            for bucket in buckets:
                if bucket["start"] <= due <= bucket["end"]:
                    bucket["in" if is_revenue else "out"] += abs(_float(installment.amount))
                    break
    return buckets


def _dashboard_rain(repository: FarmRepository, farm_id: int | None, today: date, since_year: int | None = None) -> dict:
    records = repository.list_rainfalls(farm_id=farm_id) if farm_id else []
    by_month: dict[tuple[int, int], float] = defaultdict(float)
    for record in records:
        if record.rainfall_date:
            by_month[(record.rainfall_date.year, record.rainfall_date.month)] += _float(record.millimeters)
    month_total = by_month.get((today.year, today.month), 0.0)
    previous_years = [
        value for (year, month), value in by_month.items()
        if month == today.month and year < today.year and (not since_year or year >= since_year)
    ]
    average = sum(previous_years) / len(previous_years) if previous_years else None
    latest = max((r for r in records if r.rainfall_date), key=lambda r: (r.rainfall_date, r.id), default=None)
    return {
        "month_label": _MONTHS_LONG[today.month - 1],
        "month_total": round(month_total, 1),
        "average": round(average, 1) if average is not None else None,
        "pct": min(round(month_total / average * 100), 100) if average else (100 if month_total else 0),
        "years": len(previous_years),
        "latest": latest,
        "latest_days": (today - latest.rainfall_date).days if latest else None,
        "has_records": bool(records),
    }


def _dashboard_calendar(repository: FarmRepository, plot_ids: set[int], today: date) -> list[dict]:
    window_start = today - timedelta(days=62)
    window_end = today + timedelta(days=93)
    items = []
    for schedule in repository.list_fertilization_schedules():
        if schedule.plot_id not in plot_ids or not schedule.scheduled_date:
            continue
        if not (window_start <= schedule.scheduled_date <= window_end):
            continue
        done = (schedule.status or "").strip().lower() == "completed"
        status = "done" if done else ("late" if schedule.scheduled_date < today else "plan")
        products = ", ".join(item.name for item in (schedule.items or []) if item.name) or "Sem insumos informados"
        items.append(
            {
                "id": schedule.id,
                "date": schedule.scheduled_date.isoformat(),
                "status": status,
                "plot": schedule.plot.name if schedule.plot else "Setor removido",
                "method": fertilization_application_method_label(schedule.application_method),
                "products": products,
                "link": f"/fertilizacao/agendamentos?edit_id={schedule.id}",
            }
        )
    items.sort(key=lambda item: (item["date"], item["id"]))
    return items


def _dashboard_agenda(repository: FarmRepository, farm_id: int | None, plot_ids: set[int], today: date) -> list[dict]:
    """Todas as atividades com data: aplicacoes, parcelas a pagar/receber e manutencoes."""
    start, end = today - timedelta(days=62), today + timedelta(days=184)
    events: list[dict] = []
    for item in _dashboard_calendar(repository, plot_ids, today):
        if start.isoformat() <= item["date"] <= end.isoformat():
            events.append({
                "id": f"app-{item['id']}", "schedule_id": item["id"], "date": item["date"], "type": "app",
                "title": f"{item['method']} — {item['plot']}", "meta": item["products"], "status": item["status"],
                "link": item["link"],
            })
    if farm_id:
        for transaction in repository.list_finance_transactions(farm_id=farm_id):
            if not _finance_transaction_uses_future_settlement(transaction):
                continue
            is_revenue = (transaction.operation_type or "").strip().lower() == "receita"
            parts = int(transaction.installment_count or 0) or len(transaction.installments or []) or 1
            for installment in transaction.installments or []:
                due = installment.due_date
                if not due or not (start <= due <= end):
                    continue
                paid = (installment.status or "pendente").strip().lower() == "pago"
                events.append({
                    "id": f"fin-{installment.id}", "date": due.isoformat(), "type": "rec" if is_revenue else "pay",
                    "title": transaction.product_service or transaction.category or ("Recebimento" if is_revenue else "Pagamento"),
                    "meta": f"{_brl(_float(installment.amount), 2)} · parcela {installment.installment_number}/{parts}"
                            + (f" · {transaction.finance_account.account_name}" if getattr(transaction, "finance_account", None) else ""),
                    "status": "done" if paid else ("late" if due < today else "plan"),
                    "link": f"/gestao-financeira/contas?finance_tab={'receivables' if is_revenue else 'payables'}",
                })
        for maintenance in repository.list_equipment_maintenances(farm_id=farm_id):
            day = maintenance.maintenance_date
            if not day or not (start <= day <= end) or (maintenance.status or "") == "cancelada":
                continue
            done = (maintenance.status or "") == "concluida"
            asset = getattr(maintenance, "asset", None)
            events.append({
                "id": f"mnt-{maintenance.id}", "date": day.isoformat(), "type": "maint",
                "title": f"{(maintenance.maintenance_type or 'Manutenção').capitalize()} — {asset.name if asset else 'equipamento'}",
                "meta": maintenance.description or maintenance.responsible_name or "",
                "status": "done" if done else ("late" if day < today else "plan"),
                "link": f"/insumos/patrimonio/manutencoes?edit_id={maintenance.id}",
            })
        for plan in repository.list_equipment_maintenance_plans(farm_id=farm_id):
            day = plan.next_due_date
            if not plan.is_active or not day or not (start <= day <= end):
                continue
            asset = getattr(plan, "asset", None)
            events.append({
                "id": f"plan-{plan.id}", "date": day.isoformat(), "type": "maint",
                "title": f"Revisão prevista — {asset.name if asset else 'equipamento'}",
                "meta": plan.description or "Plano de manutenção",
                "status": "late" if day < today else "plan",
                "link": "/insumos/patrimonio/manutencoes",
            })
    events.sort(key=lambda e: (e["date"], e["type"], e["title"]))
    return events


def build_dashboard_overview(
    repository: FarmRepository,
    *,
    farm_id: int | None,
    season,
    today: date,
    plots: list,
    total_area: float,
    estimated_production: float,
    operational_cost_season: float,
    coffee_market: dict,
    finance_flow: dict,
    low_stock_items: list,
    schedule_alerts: list,
    incidents: list,
    forecast_plots: list,
    cost_by_plot: dict,
    sector_cost_per_ha: dict,
    activity_timeline: list,
    rain_settings: dict | None = None,
) -> dict:
    rain_settings = rain_settings or {}
    heavy_rain_mm = float(rain_settings.get("heavy_rain_mm") or HEAVY_RAIN_MM)
    quotes = coffee_market.get("quotes") or {}
    arabica = quotes.get("arabica")
    price = _float(arabica.price_brl) if arabica else 0.0

    # Resultado previsto da safra
    cycle_pct, days_left = None, None
    if season and season.start_date and season.end_date and season.end_date > season.start_date:
        total_days = (season.end_date - season.start_date).days
        elapsed = min(max((today - season.start_date).days, 0), total_days)
        cycle_pct = round(elapsed / total_days * 100)
        days_left = max((season.end_date - today).days, 0)
    revenue = estimated_production * price
    margin = revenue - operational_cost_season
    hero = {
        "season_name": season.name if season else None,
        "cycle_pct": cycle_pct,
        "days_left": days_left,
        "area": round(total_area, 2),
        "plot_count": len(plots),
        "sacks": estimated_production,
        "price": price,
        "price_date": arabica.quote_date if arabica else None,
        "cost": round(operational_cost_season, 2),
        "cost_per_ha": round(operational_cost_season / total_area, 2) if total_area else 0,
        "cost_per_sack": round(operational_cost_season / estimated_production, 2) if estimated_production else 0,
        "revenue": round(revenue, 2),
        "margin": round(margin, 2),
        "margin_pct": round(margin / revenue * 100) if revenue else None,
        "ready": bool(season and estimated_production and price),
    }
    # Rodape do hero: linha do ciclo (aplicacoes) e composicao do custo
    calendar_items = _dashboard_calendar(repository, {p.id for p in plots}, today)
    if cycle_pct is not None:
        total_days = (season.end_date - season.start_date).days
        marks = []
        for item in calendar_items:
            day = date.fromisoformat(item["date"])
            if season.start_date <= day <= season.end_date:
                marks.append({"pos": round((day - season.start_date).days / total_days * 100, 2), "status": item["status"],
                              "title": f"{day.strftime('%d/%m')} · {item['method']} — {item['plot']}", "link": item.get("link")})
        hero["timeline"] = {
            "start": season.start_date.strftime("%d/%m/%Y"), "end": season.end_date.strftime("%d/%m/%Y"),
            "marks": marks, "done": sum(1 for m in marks if m["status"] == "done"),
            "late": sum(1 for m in marks if m["status"] == "late"), "total": len(marks),
        }
    hero["cost_groups"] = []
    if season is not None and farm_id and plots and getattr(season, "plots", None):
        from app.services.season_costs import build_cost_tree, cost_group_summary
        try:
            hero["cost_groups"] = cost_group_summary(build_cost_tree(repository, farm_id=farm_id, season=season, plots=plots, today=today, with_previous=False))
        except Exception:  # o rodape e complementar; nunca derruba o Dashboard
            hero["cost_groups"] = []
    hero["revenue_fmt"] = _brl_compact(hero["revenue"])
    hero["cost_fmt"] = _brl_compact(hero["cost"])
    hero["margin_fmt"] = _brl_compact(hero["margin"])

    # Precisa de voce hoje
    inbox: list[dict] = []
    for item in finance_flow.get("all_action_items", finance_flow.get("action_items", [])):
        if item["status"] not in {"overdue", "today"}:
            continue
        verb = "Receber" if item["type"] == "receber" else "Pagar"
        when = "venceu" if item["status"] == "overdue" else "vence hoje"
        inbox.append({
            "permission": "page.finance",
            "tone": "danger" if item["status"] == "overdue" else "warning",
            "title": f"{item['title']} — {when}",
            "text": f"{_brl(item['amount'], 2)} · parcela {item['installment']} · {item['source']}",
            "action": verb, "link": item["link"],
            "kind": "bill", "name": item["title"], "type": item["type"], "status": item["status"], "date": item.get("date"),
            "amount": item["amount"], "installment": item["installment"], "source": item["source"],
        })
    finance_upcoming = [item for item in finance_flow.get("all_action_items", finance_flow.get("action_items", [])) if item["status"] == "upcoming"]
    late = [item for item in schedule_alerts if item["status"] == "late"]
    for item in late:
        inbox.append({
            "permission": "page.agenda",
            "tone": "danger",
            "title": f"Aplicação atrasada — {item['plot']}",
            "text": f"{item['application_method_label']} programada para {date.fromisoformat(item['date']).strftime('%d/%m')}",
            "action": "Ver", "link": item["link"],
        })
    for item in low_stock_items:
        inbox.append({
            "permission": "page.inputs",
            "tone": "danger" if item["stock_level"] == "critical" else "warning",
            "title": f"{item['name']}: estoque {item['stock_level_label'].lower()}",
            "text": f"Restam {item['available_quantity']:g} {item['unit'] or ''} · mínimo {item['low_stock_threshold']:g}".strip(),
            "action": "Comprar", "link": "/insumos/comprados",
        })
    recent_pests = [i for i in incidents if i.occurrence_date and (today - i.occurrence_date).days <= 15 and (i.severity or 0) >= 3]
    for incident in sorted(recent_pests, key=lambda i: (-(i.severity or 0), i.occurrence_date)):
        inbox.append({
            "permission": "page.pests",
            "tone": "danger" if (incident.severity or 0) >= 4 else "warning",
            "title": f"{incident.name} — {incident.plot.name if incident.plot else 'setor'}",
            "text": f"{incident.category} nível {incident.severity} registrada em {incident.occurrence_date.strftime('%d/%m')}",
            "action": "Ver", "link": f"/pragas?edit_id={incident.id}",
        })
    rain = _dashboard_rain(repository, farm_id, today, since_year=rain_settings.get("average_since_year"))
    latest = rain["latest"]
    rain_alert_on = rain_settings.get("alert_registered", True) and rain_settings.get("notify_dashboard", True)
    if rain_alert_on and latest and rain["latest_days"] is not None and rain["latest_days"] <= 2 and _float(latest.millimeters) >= heavy_rain_mm:
        inbox.append({
            "permission": "page.rainfall",
            "tone": "info",
            "title": f"Chuva forte registrada: {_float(latest.millimeters):g} mm",
            "text": f"Em {latest.rainfall_date.strftime('%d/%m')} · confira se alguma aplicação recente precisa ser refeita",
            "action": "Ver", "link": "/pluviometria",
        })

    # Mapa vivo dos setores
    forecast_by_plot = {item["plot_id"]: item for item in forecast_plots}
    tiles = []
    for plot in plots:
        area = _float(plot.area_hectares)
        productivity = _float(forecast_by_plot.get(plot.id, {}).get("productivity"))
        if plot.id in sector_cost_per_ha:
            cost_ha = sector_cost_per_ha[plot.id]
        else:
            cost_ha = (cost_by_plot.get(plot.name, 0.0) / area) if area else 0.0
        plot_incidents = [i for i in incidents if i.plot_id == plot.id and i.occurrence_date and (today - i.occurrence_date).days <= 45]
        worst = max((i.severity or 0 for i in plot_incidents), default=0)
        health = "critico" if worst >= 4 else "atencao" if worst >= 2 else "ok"
        last = max(plot_incidents, key=lambda i: i.occurrence_date, default=None)
        planting = plot.active_planting if hasattr(plot, "active_planting") else None
        variety = (planting.variety if planting and getattr(planting, "variety", None) else plot.variety)
        tiles.append({
            "id": plot.id, "name": plot.name, "area": round(area, 2),
            "variety": variety.name if variety else "Sem variedade",
            "prod": round(productivity, 1), "cost": round(cost_ha, 2), "health": health,
            "health_label": {"ok": "Sem ocorrências", "atencao": "Atenção", "critico": "Crítico"}[health],
            "health_detail": f"{last.name} (nível {last.severity})" if last else "Sem ocorrências nos últimos 45 dias",
            "link": f"/setores?view_id={plot.id}",
        })
    tiles.sort(key=lambda t: -t["area"])

    # Mercado do cafe (12 meses)
    history_12m = (coffee_market.get("history") or {}).get("12m") or {"labels": [], "arabica": [], "robusta": []}
    available = coffee_market.get("available_sacks") or {}

    # Caixa
    weeks = _dashboard_cash_weeks(repository, farm_id, today)
    balance = _float(finance_flow.get("balance"))
    running = balance
    cash = []
    for index, bucket in enumerate(weeks):
        running += bucket["in"] - bucket["out"]
        label = "Esta semana" if index == 0 else f"{bucket['start'].strftime('%d/%m')}–{bucket['end'].strftime('%d/%m')}"
        cash.append({"label": label, "in": round(bucket["in"], 2), "out": round(bucket["out"], 2), "net": round(bucket["in"] - bucket["out"], 2)})
    cash_max = max([c["in"] for c in cash] + [c["out"] for c in cash] + [1])

    hero["spark"] = _spark_path([v for v in (history_12m.get("arabica") or []) if v])
    sacks_whole = int(round(_float(hero["sacks"])))
    hero["sack_icons"] = list(range(min(sacks_whole, 10)))
    hero["sack_more"] = sacks_whole > 10
    return {
        "today": today,
        "weekday": ["Segunda-feira", "Terça-feira", "Quarta-feira", "Quinta-feira", "Sexta-feira", "Sábado", "Domingo"][today.weekday()],
        "date_label": f"{today.day} de {_MONTHS_LONG[today.month - 1]}",
        "hero": hero,
        "inbox": inbox[:6],
        "inbox_all": inbox,
        "finance_upcoming": finance_upcoming,
        "attention": build_attention(inbox, finance_upcoming, today),
        "tiles": tiles,
        "market": {
            "history": history_12m,
            "available_sacks": available,
            "estimated_value": coffee_market.get("estimated_value") or {},
            "source": coffee_market.get("source"),
        },
        "cash": cash,
        "cash_max": cash_max,
        "cash_balance": round(balance, 2),
        "cash_projected": round(running, 2),
        "rain": rain,
        "calendar": calendar_items,
        "agenda": _dashboard_agenda(repository, farm_id, {p.id for p in plots}, today),
        "activity": activity_timeline[:6],
    }
