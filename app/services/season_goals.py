"""Metas da safra (produção esperada, orçamento e preço) e regras do assistente de safras."""

from __future__ import annotations

import json
from datetime import date

from app.models.harvest import HarvestRecord

BUDGET_CATEGORIES: list[tuple[str, str, str]] = [
    ("adubacao", "Adubação e corretivos", "Programa de adubação"),
    ("defensivos", "Defensivos", "Pulverizações"),
    ("mao_obra", "Mão de obra", "Equipe e diárias"),
    ("colheita", "Colheita e pós-colheita", "Colheita, secagem, beneficiamento"),
    ("outros", "Outros", "Energia, frete, manutenção"),
]
_CATEGORY_KEYS = {key for key, _, _ in BUDGET_CATEGORIES}
FORMATION_YEARS = 2.5
_MAX_YIELD = 500.0
_MAX_MONEY = 10_000_000.0


def _num(value, limit: float) -> float | None:
    try:
        number = float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None
    if number != number or number < 0:  # NaN ou negativo
        return None
    return round(min(number, limit), 2)


def parse_goals(raw: str | None, plot_ids: set[int]) -> dict | None:
    """Valida o JSON vindo do assistente. Ignora setores fora da safra e categorias desconhecidas."""
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    yields: dict[str, float] = {}
    for key, value in (data.get("yields") or {}).items():
        plot_id = int(key) if str(key).isdigit() else None
        number = _num(value, _MAX_YIELD)
        if plot_id in plot_ids and number is not None:
            yields[str(plot_id)] = number
    budget: dict[str, float] = {}
    for key, value in (data.get("budget") or {}).items():
        number = _num(value, _MAX_MONEY)
        if key in _CATEGORY_KEYS and number is not None:
            budget[key] = number
    price = _num(data.get("price"), _MAX_MONEY)
    if not yields and not budget and not price:
        return None
    return {"yields": yields, "budget": budget, "price": price or None}


def load_goals(season) -> dict:
    try:
        data = json.loads(season.goals_json) if season and season.goals_json else {}
    except (TypeError, ValueError):
        data = {}
    return data if isinstance(data, dict) else {}


def goals_summary(season) -> dict | None:
    """Números das metas para mostrar no painel da safra."""
    goals = load_goals(season)
    if not goals:
        return None
    plots = {plot.id: plot for plot in season.plots}
    area = sum(float(plot.area_hectares or 0) for plot in plots.values())
    sacks = sum(float(plots[int(pid)].area_hectares or 0) * float(value) for pid, value in (goals.get("yields") or {}).items() if int(pid) in plots)
    productive_area = sum(float(plots[int(pid)].area_hectares or 0) for pid, value in (goals.get("yields") or {}).items() if int(pid) in plots and float(value) > 0)
    budget_per_ha = sum(float(value) for value in (goals.get("budget") or {}).values())
    budget_total = budget_per_ha * area
    price = float(goals.get("price") or 0)
    return {
        "sacks": round(sacks, 1),
        "budget_per_ha": round(budget_per_ha, 2),
        "budget_total": round(budget_total, 2),
        "price": price,
        "revenue": round(sacks * price, 2) if price else None,
        "margin": round(sacks * price - budget_total, 2) if price else None,
        "breakeven": round(budget_total / price / productive_area, 1) if price and productive_area else None,
    }


def status_from_dates(start: date, end: date, today: date, current: str | None = None) -> str:
    """Situação calculada pelas datas. 'Encerrada' só sai pelo encerramento, nunca é desfeita aqui."""
    if current == "encerrada":
        return "encerrada"
    if today < start:
        return "planejada"
    if today > end:
        return "encerrada"
    return "ativa"


def coffee_year_options(today: date) -> list[dict]:
    """Ano-safra do café (julho a junho): atual e próximo."""
    first = today.year if today.month >= 7 else today.year - 1
    options = []
    for offset, tag in ((0, "Ano-safra atual"), (1, "Próximo ano-safra")):
        year = first + offset
        options.append({
            "key": "atual" if offset == 0 else "proxima",
            "label": f"{year}/{str(year + 1)[-2:]}",
            "tag": tag,
            "start": date(year, 7, 1).isoformat(),
            "end": date(year + 1, 6, 30).isoformat(),
            "range": f"jul/{year} a jun/{year + 1}",
        })
    return options


def plot_stage(plot, today: date) -> str:
    """'prod' (em produção), 'form' (em formação) ou 'sem' (sem plantio informado)."""
    planting = getattr(plot, "active_planting", None)
    planting_date = getattr(planting, "planting_date", None) or getattr(plot, "planting_date", None)
    if not planting_date:
        return "sem"
    years = (today - planting_date).days / 365.25
    return "form" if years < FORMATION_YEARS else "prod"


def plot_yield_history(db, plots) -> dict[int, float]:
    """Média de sacas por hectare das últimas 3 safras com colheita de cada setor."""
    by_id = {plot.id: plot for plot in plots}
    if not by_id:
        return {}
    rows = (
        db.query(HarvestRecord.plot_id, HarvestRecord.season_id, HarvestRecord.sacks_produced)
        .filter(HarvestRecord.plot_id.in_(list(by_id)), HarvestRecord.season_id.isnot(None))
        .all()
    )
    per_plot: dict[int, dict[int, float]] = {}
    for plot_id, season_id, sacks in rows:
        per_plot.setdefault(plot_id, {}).setdefault(season_id, 0.0)
        per_plot[plot_id][season_id] += float(sacks or 0)
    result: dict[int, float] = {}
    for plot_id, seasons in per_plot.items():
        area = float(by_id[plot_id].area_hectares or 0)
        recent = [value for _, value in sorted(seasons.items(), reverse=True)[:3] if value > 0]
        if area and recent:
            result[plot_id] = round(sum(recent) / len(recent) / area, 1)
    return result


def common_variety_id(plots) -> int | None:
    """Variedade da safra: só quando todos os setores têm a mesma (senão fica em branco)."""
    ids = set()
    for plot in plots:
        planting = getattr(plot, "active_planting", None)
        ids.add(getattr(planting, "variety_id", None) or plot.variety_id)
    ids.discard(None)
    return ids.pop() if len(ids) == 1 else None


# ---------------------------------------------------------------------------
# Integrações do assistente (passo 4) e encerramento da safra
# ---------------------------------------------------------------------------
INTEGRATION_KEYS = ("adub", "fin", "colh", "est", "lemb")
MONTHS_PT = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
MONTHS_FULL = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]


def parse_integrations(raw: str | None) -> dict:
    """{"fin": true, "colh": true, ...} vindo do assistente; ignora chaves desconhecidas."""
    try:
        data = json.loads(raw) if raw else {}
    except (TypeError, ValueError):
        data = {}
    if not isinstance(data, dict):
        return {}
    return {key: bool(data.get(key)) for key in INTEGRATION_KEYS}


def harvest_window(season) -> dict | None:
    """Janela de colheita do café: maio a agosto do ano em que a safra termina."""
    if not season or not season.end_date:
        return None
    year = season.end_date.year
    return {"start": f"{year}-05-01", "end": f"{year}-08-31", "label": f"maio a agosto/{year}"}


def integrations_of(season) -> dict:
    return (load_goals(season).get("integrations") or {}) if season else {}


def season_cost_total(repo, season) -> float:
    """Custo operacional da safra: insumos consumidos (estoque) + aplicações — o mesmo da lista de safras."""
    outputs = [output for output in repo.list_stock_outputs(farm_id=season.farm_id) if output.season_id == season.id]
    fertilizations = [record for record in repo.list_fertilizations(farm_ids=[season.farm_id]) if record.season_id == season.id]
    return round(sum(float(output.total_cost or 0) for output in outputs) + sum(float(record.cost or 0) for record in fertilizations), 2)


def season_finance_expenses(repo, season) -> float:
    """Despesas lançadas no Financeiro com esta safra (sem estornos)."""
    total = 0.0
    for tx in repo.list_finance_transactions(farm_id=season.farm_id):
        if tx.season_id == season.id and not tx.is_reversed and (tx.operation_type or "").lower() in {"despesa", "saida", "expense", "pagar"}:
            total += float(tx.amount or 0)
    return round(total, 2)


def copy_fertilization_program(repo, source_season, target_season) -> int:
    """Copia as aplicações programadas da safra anterior para a nova, como rascunho (programadas) a revisar.
    Datas avançam a diferença entre os inícios das safras; só setores que estão nas duas safras."""
    from app.models.fertilization_schedule import FertilizationSchedule
    from app.models.fertilization_schedule_item import FertilizationScheduleItem

    if not source_season or not target_season:
        return 0
    target_plot_ids = {plot.id for plot in target_season.plots}
    shift = target_season.start_date - source_season.start_date
    copied = 0
    for schedule in list(source_season.fertilization_schedules):
        if schedule.plot_id not in target_plot_ids or not schedule.scheduled_date:
            continue
        clone = FertilizationSchedule(
            plot_id=schedule.plot_id,
            season_id=target_season.id,
            scheduled_date=schedule.scheduled_date + shift,
            status="scheduled",
            duration_minutes=schedule.duration_minutes,
            application_method=schedule.application_method,
            notes=f"Rascunho copiado da {source_season.name}. Revise data e doses." + (f"\n{schedule.notes}" if schedule.notes else ""),
        )
        for item in schedule.items:
            clone.items.append(FertilizationScheduleItem(
                input_id=item.input_id, purchased_input_id=None, name=item.name, unit=item.unit, quantity=item.quantity,
            ))
        repo.db.add(clone)
        copied += 1
    if copied:
        repo.db.commit()
    return copied


def planned_inputs_vs_stock(repo, season) -> list[dict]:
    """Insumos das aplicações programadas (ainda não feitas) da safra x saldo em estoque da fazenda."""
    planned: dict[tuple, dict] = {}
    for schedule in season.fertilization_schedules:
        if schedule.status == "completed":
            continue
        for item in schedule.items:
            key = (item.input_id, item.unit)
            row = planned.setdefault(key, {"input_id": item.input_id, "name": item.name, "unit": item.unit, "planned": 0.0})
            row["planned"] += float(item.quantity or 0)
    if not planned:
        return []
    available: dict[int, float] = {}
    for lot in repo.list_purchased_inputs():
        if lot.farm_id == season.farm_id and lot.input_id:
            available[lot.input_id] = available.get(lot.input_id, 0.0) + float(lot.available_quantity or 0)
    rows = []
    for row in planned.values():
        stock = round(available.get(row["input_id"], 0.0), 2)
        rows.append({**row, "planned": round(row["planned"], 2), "stock": stock, "missing": round(max(0.0, row["planned"] - stock), 2)})
    return sorted(rows, key=lambda row: (-row["missing"], row["name"].lower()))


def season_closing_data(repo, season) -> dict:
    """Pendências e previsto x realizado para a tela Encerrar safra."""
    from app.models.coffee_classification import CoffeeClassification

    db = repo.db
    harvests = [harvest for harvest in repo.list_harvests(farm_ids=[season.farm_id]) if harvest.season_id == season.id]
    classified = {row[0] for row in db.query(CoffeeClassification.harvest_id).filter(
        CoffeeClassification.harvest_id.in_([harvest.id for harvest in harvests] or [0])).all()}
    unclassified = sorted((harvest for harvest in harvests if harvest.id not in classified), key=lambda harvest: harvest.harvest_date)
    no_plot_expenses = [
        tx for tx in repo.list_finance_transactions(farm_id=season.farm_id)
        if tx.season_id == season.id and not tx.plot_id and not tx.is_reversed and (tx.operation_type or "").lower() in {"despesa", "saida", "expense", "pagar"}
    ]
    open_schedules = [schedule for schedule in season.fertilization_schedules if schedule.status != "completed"]
    pending = []
    if unclassified:
        dates = ", ".join(harvest.harvest_date.strftime("%d/%m") for harvest in unclassified[:3]) + ("…" if len(unclassified) > 3 else "")
        pending.append({"key": "colh", "module": "Colheita", "href": "/producao/classificacao",
                        "title": f"{len(unclassified)} {'colheita' if len(unclassified) == 1 else 'colheitas'} sem classificação",
                        "desc": f"Lotes de {dates} sem tipo e peneira"})
    if no_plot_expenses:
        amount = sum(float(tx.amount or 0) for tx in no_plot_expenses)
        pending.append({"key": "fin", "module": "Financeiro", "href": "/gestao-financeira",
                        "title": f"{len(no_plot_expenses)} {'despesa' if len(no_plot_expenses) == 1 else 'despesas'} sem setor",
                        "desc": f"R$ {amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + " que não entram no custo por hectare"})
    if open_schedules:
        pending.append({"key": "adub", "module": "Adubação", "href": "/fertilizacao/agendamentos",
                        "title": f"{len(open_schedules)} {'aplicação ainda programada' if len(open_schedules) == 1 else 'aplicações ainda programadas'}",
                        "desc": "Marque como feita ou exclua as que não vão acontecer"})

    area = sum(float(plot.area_hectares or 0) for plot in season.plots)
    produced = round(sum(float(harvest.sacks_produced or 0) for harvest in harvests), 1)
    harvest_ids = {harvest.id for harvest in harvests}
    sales = [sale for sale in repo.list_coffee_commercializations(farm_id=season.farm_id) if sale.harvest_id in harvest_ids]
    sold_sacks = round(sum(float(sale.equivalent_sacks or 0) for sale in sales), 1)
    sold_value = round(sum(float(sale.total_value or 0) for sale in sales), 2)
    cost = season_cost_total(repo, season)
    summary = goals_summary(season) or {}
    planned_sacks = summary.get("sacks") or 0
    planned_cost = summary.get("budget_total") or 0
    return {
        "pending": pending,
        "area": area,
        "plots_count": len(season.plots),
        "produced": produced,
        "planned_sacks": planned_sacks,
        "planned_yield": round(planned_sacks / area, 1) if area and planned_sacks else None,
        "real_yield": round(produced / area, 1) if area else None,
        "cost": cost,
        "planned_cost": planned_cost,
        "cost_per_ha": round(cost / area, 2) if area else None,
        "planned_cost_per_ha": summary.get("budget_per_ha"),
        "cost_per_sack": round(cost / produced, 2) if produced else None,
        "planned_cost_per_sack": round(planned_cost / planned_sacks, 2) if planned_sacks and planned_cost else None,
        "sold_sacks": sold_sacks,
        "sold_value": sold_value,
        "avg_price": round(sold_value / sold_sacks, 2) if sold_sacks else None,
        "unsold": round(max(0.0, produced - sold_sacks), 1),
    }


def _fmt(value, kind: str) -> str:
    if value is None:
        return "—"
    if kind == "money":
        return "R$ " + f"{value:,.0f}".replace(",", ".")
    if kind == "sc":
        return f"{value:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".").removesuffix(",0") + " sc"
    return f"{value:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".").removesuffix(",0") + " sc/ha"


def closing_rows(data: dict) -> list[dict]:
    """Linhas da tabela "Previsto x realizado" com a diferença e o tom (bom, atenção, ruim)."""
    def diff(planned, real, *, higher_is_good: bool, kind: str):
        if not planned or real is None:
            return "—", "muted"
        delta = real - planned
        pct = delta / planned * 100
        good = delta >= 0 if higher_is_good else delta <= 0
        tone = "ok" if good else ("bad" if abs(pct) >= 10 else "warn")
        if kind == "pct":
            return f"{pct:+.0f}%".replace("+-", "−"), tone
        sign = "+" if delta >= 0 else "−"
        return sign + _fmt(abs(delta), kind).replace("R$ ", "R$ "), tone

    rows = []
    for label, planned, real, kind, good, mode, source in (
        ("Produção", data["planned_sacks"] or None, data["produced"], "sc", True, "pct", "Colheita"),
        ("Produtividade", data["planned_yield"], data["real_yield"], "ha", True, "abs", "Colheita"),
        ("Custo total", data["planned_cost"] or None, data["cost"], "money", False, "pct", "Insumos e aplicações"),
        ("Custo por hectare", data["planned_cost_per_ha"] or None, data["cost_per_ha"], "money", False, "abs", "Insumos e aplicações"),
        ("Custo por saca", data["planned_cost_per_sack"], data["cost_per_sack"], "money", False, "abs", "Insumos e colheita"),
    ):
        text, tone = diff(planned, real, higher_is_good=good, kind="pct" if mode == "pct" else kind)
        rows.append({"label": label, "planned": _fmt(planned, kind), "real": _fmt(real, kind), "diff": text, "tone": tone, "source": source})
    sold = f"{_fmt(data['sold_sacks'], 'sc')} · {_fmt(data['sold_value'], 'money')}" if data["sold_sacks"] else "Nada vendido ainda"
    rows.append({"label": "Vendido", "planned": "—", "real": sold, "diff": (_fmt(data["avg_price"], "money") + "/sc") if data["avg_price"] else "—",
                 "tone": "ok" if data["avg_price"] else "muted", "source": "Comercialização"})
    return rows
