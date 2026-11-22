"""Insumos e máquinas: saldo único, alertas, uso, abastecimento e situação das máquinas.

Reúne num lugar só o que antes ficava espalhado entre Estoque, Suprimentos,
Máquinas e Manutenções. Nada aqui grava sem que a rota tenha passado pela
conferência do usuário.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

import re

from app.core.timezone import today_in_app_timezone
from app.models.equipment_fueling import EquipmentFueling
from app.models.equipment_maintenance import EquipmentMaintenance
from app.services.forms import STOCK_ADJUSTMENT_OUTPUT_REFERENCE, create_manual_stock_output

STOCK_TYPES = ("insumo_agricola", "combustivel", "suprimento")
FUELING_REFERENCE = "equipment_fueling"
GROUPS = [
    ("def", "Defensivos"),
    ("fert", "Fertilizantes e corretivos"),
    ("comb", "Combustível e lubrificante"),
    ("sup", "Suprimentos e EPIs"),
    ("outros", "Outros"),
]
GROUP_LABELS = dict(GROUPS)
_DEFENSIVE = {"acaricida", "adjuvante", "fungicida", "herbicida", "inseticida", "outros defensivos"}
_FERTILIZER = {"fertilizantes", "corretivos de solo"}
_MACHINE_CATEGORIES = {"Máquina", "Equipamento", "Veículo"}
CONSUMPTION_WINDOW_DAYS = 30
EXPIRING_DAYS = 45
INVENTORY_STALE_DAYS = 60
DUE_SOON_DAYS = 15
DUE_SOON_HOURS = 25


def _f(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def parse_br_number(raw) -> float | None:
    """Aceita "1.268", "1268,5", "1268.5" e "4"; devolve None se vazio."""
    text = str(raw if raw is not None else "").strip().replace(" ", "")
    if not text:
        return None
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"\d{1,3}(\.\d{3})+", text):
        text = text.replace(".", "")
    return float(text)


def _unit_key(unit) -> str:
    return (unit or "").strip().lower().rstrip(".")


def br_number(value, places: int = 0) -> str:
    text = f"{_f(value):,.{places}f}"
    return text.replace(",", "§").replace(".", ",").replace("§", ".")


_PLURAL_UNITS = {"par": "pares", "saco": "sacos", "peça": "peças", "rolo": "rolos", "caixa": "caixas", "galão": "galões", "balde": "baldes"}


def br_qty(value, unit: str | None = None) -> str:
    number = _f(value)
    places = 0 if abs(number - round(number)) < 0.005 else (1 if abs(number * 10 - round(number * 10)) < 0.05 else 2)
    if unit and abs(number) > 1 and unit in _PLURAL_UNITS:
        unit = _PLURAL_UNITS[unit]
    return f"{br_number(number, places)} {unit}".strip() if unit else br_number(number, places)


def br_money(value) -> str:
    number = _f(value)
    return f"R$ {br_number(number, 0 if abs(number) >= 1000 else 2)}"


def item_group(item_type: str | None, category: str | None) -> str:
    if item_type == "combustivel":
        return "comb"
    if item_type == "suprimento":
        return "sup"
    key = (category or "").strip().lower()
    if key in _DEFENSIVE:
        return "def"
    if key in _FERTILIZER:
        return "fert"
    if key in {"combustíveis", "lubrificantes"}:
        return "comb"
    return "outros"


def _in_farm(record, farm_id: int | None) -> bool:
    return farm_id is None or getattr(record, "farm_id", None) in (None, farm_id)


def _is_consumption(output) -> bool:
    return (output.reference_type or "") != STOCK_ADJUSTMENT_OUTPUT_REFERENCE


# ---------------------------------------------------------------- estoque
def build_stock_items(repo, farm_id: int | None, today: date) -> list[dict]:
    """Saldo de cada item (insumos, combustível e suprimentos) com lotes, cobertura e movimentos."""
    catalog = [item for item in repo.list_input_catalog() if item.item_type in STOCK_TYPES]
    entries_by_item: dict[int, list] = defaultdict(list)
    for entry in repo.list_purchased_inputs(include_stock_adjustments=True):
        if entry.input_id and _in_farm(entry, farm_id):
            entries_by_item[entry.input_id].append(entry)
    outputs_by_item: dict[int, list] = defaultdict(list)
    for output in repo.list_stock_outputs():
        if _in_farm(output, farm_id):
            outputs_by_item[output.input_id].append(output)

    window_start = today - timedelta(days=CONSUMPTION_WINDOW_DAYS)
    rows = []
    for item in catalog:
        entries = entries_by_item.get(item.id) or []
        if not entries:
            continue
        units = sorted(
            [e for e in entries if e.package_unit],
            key=lambda e: (_f(e.available_quantity) > 0, e.purchase_date or date.min, e.id or 0),
            reverse=True,
        )
        unit = units[0].package_unit if units else item.default_unit
        available = round(sum(_f(e.available_quantity) for e in entries), 2)
        value = sum(_f(e.available_quantity) * _f(e.total_cost) / max(_f(e.total_quantity), 1) for e in entries)
        thresholds = [_f(item.low_stock_threshold)] + [_f(e.low_stock_threshold) for e in entries]
        minimum = max(thresholds) if any(thresholds) else 0.0
        outputs = outputs_by_item.get(item.id) or []
        used = sum(
            _f(o.quantity)
            for o in outputs
            if _is_consumption(o) and o.movement_date and o.movement_date >= window_start and _unit_key(o.unit) == _unit_key(unit)
        )
        daily = used / CONSUMPTION_WINDOW_DAYS
        days_left = int(available / daily) if daily > 0 else None
        lots = []
        for entry in sorted(entries, key=lambda e: (e.expiration_date or date.max, e.purchase_date or date.max, e.id or 0)):
            if _f(entry.available_quantity) <= 0:
                continue
            days_to_expire = (entry.expiration_date - today).days if entry.expiration_date else None
            lots.append({
                "label": entry.batch_number and f"Lote {entry.batch_number}" or (
                    f"Compra de {entry.purchase_date.strftime('%d/%m/%Y')}" if entry.purchase_date else "Entrada"
                ),
                "qty": br_qty(entry.available_quantity, unit),
                "expires": entry.expiration_date.strftime("%d/%m/%Y") if entry.expiration_date else None,
                "days_to_expire": days_to_expire,
                "tone": "red" if days_to_expire is not None and days_to_expire < 0 else (
                    "amber" if days_to_expire is not None and days_to_expire <= EXPIRING_DAYS else "muted"
                ),
            })
        expiring = [lot for lot in lots if lot["tone"] in {"amber", "red"}]
        if available <= 0:
            status = "out"
        elif minimum and available <= minimum:
            status = "low"
        elif days_left is not None and days_left <= 10:
            status = "short"
        else:
            status = "ok"
        moves = []
        for entry in entries:
            moves.append((entry.purchase_date or date.min, entry.id, {
                "label": "Ajuste de contagem" if entry.entry_origin == "stock_adjustment_entry" else (
                    f"Compra{' · ' + entry.supplier_name if entry.supplier_name else ''}"
                ),
                "qty": "+" + br_qty(entry.total_quantity, unit), "tone": "green",
                "date": entry.purchase_date.strftime("%d/%m") if entry.purchase_date else "",
            }))
        for output in outputs:
            where = output.plot.name if getattr(output, "plot", None) else (output.notes or "Sem setor")
            label = "Ajuste de contagem" if not _is_consumption(output) else f"Uso · {where}"
            moves.append((output.movement_date or date.min, output.id, {
                "label": label[:60], "qty": "−" + br_qty(output.quantity, output.unit or unit), "tone": "red",
                "date": output.movement_date.strftime("%d/%m") if output.movement_date else "",
            }))
        moves.sort(key=lambda m: (m[0], m[1] or 0), reverse=True)
        group = item_group(item.item_type, item.category)
        rows.append({
            "id": item.id,
            "name": item.name,
            "group": group,
            "group_label": GROUP_LABELS[group],
            "category": item.category,
            "item_type": item.item_type,
            "unit": unit,
            "available": available,
            "available_label": br_qty(available, unit),
            "minimum": minimum,
            "minimum_label": br_qty(minimum, unit) if minimum else None,
            "value": round(value, 2),
            "value_label": br_money(value),
            "daily": round(daily, 3),
            "days_left": days_left,
            "days_label": (f"~{days_left} dias" if days_left is not None else "—"),
            "level_pct": _level_pct(available, minimum),
            "status": status,
            "lots": lots,
            "expiring_count": len(expiring),
            "moves": [m[2] for m in moves[:5]],
            "search": f"{item.name} {item.category} {' '.join(l['label'] for l in lots)}".lower(),
        })
    order = {"out": 0, "low": 1, "short": 2, "ok": 3}
    rows.sort(key=lambda r: (order[r["status"]], -r["expiring_count"], r["name"].lower()))
    return rows


def _level_pct(available: float, minimum: float) -> int:
    if available <= 0:
        return 0
    if not minimum:
        return 60
    return max(4, min(100, round(available / (minimum * 2.5) * 100)))


def group_counts(items: list[dict]) -> list[dict]:
    counts = defaultdict(int)
    for item in items:
        counts[item["group"]] += 1
    chips = [{"key": "todos", "label": "Tudo", "count": len(items)}]
    chips += [{"key": key, "label": label, "count": counts[key]} for key, label in GROUPS if counts[key]]
    return chips


# ---------------------------------------------------------------- máquinas
def meter_unit_for(asset) -> str:
    return "km" if (asset.category or "") == "Veículo" else "h"


def is_machine(asset) -> bool:
    return (asset.category or "") in _MACHINE_CATEGORIES


def list_fuelings(db, asset_ids) -> list[EquipmentFueling]:
    if not asset_ids:
        return []
    return (
        db.query(EquipmentFueling)
        .filter(EquipmentFueling.asset_id.in_(list(asset_ids)))
        .order_by(EquipmentFueling.fueling_date.desc(), EquipmentFueling.id.desc())
        .all()
    )


def build_machines(db, assets, maintenances, plans, today: date, since: date | None) -> list[dict]:
    asset_ids = {a.id for a in assets}
    fuelings = list_fuelings(db, asset_ids)
    fuel_by_asset = defaultdict(list)
    for fueling in fuelings:
        fuel_by_asset[fueling.asset_id].append(fueling)
    maint_by_asset = defaultdict(list)
    for maintenance in maintenances:
        if maintenance.asset_id in asset_ids:
            maint_by_asset[maintenance.asset_id].append(maintenance)
    plans_by_asset = defaultdict(list)
    for plan in plans:
        if plan.asset_id in asset_ids and plan.is_active:
            plans_by_asset[plan.asset_id].append(plan)

    cards = []
    for asset in assets:
        fuels = fuel_by_asset.get(asset.id, [])
        maints = maint_by_asset.get(asset.id, [])
        meter = current_meter(fuels, maints)
        unit = meter_unit_for(asset)
        plan_rows = [_plan_status(plan, meter, today, unit) for plan in plans_by_asset.get(asset.id, [])]
        plan_rows.sort(key=lambda p: p["rank"])
        in_season = lambda d: d and (since is None or d >= since)  # noqa: E731
        fuel_cost = sum(_f(f.total_cost) for f in fuels if in_season(f.fueling_date))
        fuel_liters = sum(_f(f.liters) for f in fuels if in_season(f.fueling_date))
        maint_cost = sum(
            _f(m.service_cost) + _f(m.parts_cost) for m in maints if m.status != "cancelada" and in_season(m.maintenance_date)
        )
        status_key = {"ativo": "ok", "em_manutencao": "man", "em_construcao": "obra", "baixado": "par"}.get(asset.status, "ok")
        cards.append({
            "id": asset.id,
            "name": asset.name,
            "category": asset.category,
            "subtitle": " · ".join(str(p) for p in (asset.category, asset.subtype and asset.subtype.replace("_", " ").capitalize(), asset.manufacture_year) if p),
            "is_machine": is_machine(asset),
            "status_key": status_key,
            "status_label": {"ok": "Trabalhando" if is_machine(asset) else "Em uso", "man": "Em manutenção", "obra": "Em obra", "par": "Baixado"}[status_key],
            "meter": meter,
            "meter_unit": unit,
            "meter_label": f"{br_number(meter)} {unit}" if meter else "—",
            "plans": plan_rows,
            "next": plan_rows[0] if plan_rows else None,
            "fuel_cost": round(fuel_cost, 2),
            "fuel_liters": round(fuel_liters, 2),
            "maint_cost": round(maint_cost, 2),
            "season_cost": round(fuel_cost + maint_cost, 2),
            "season_cost_label": br_money(fuel_cost + maint_cost),
            "last_fueling": fuels[0] if fuels else None,
            "search": f"{asset.name} {asset.category} {asset.brand_model or ''} {asset.asset_code or ''}".lower(),
        })
    cards.sort(key=lambda c: (not c["is_machine"], c["next"]["rank"] if c["next"] else (9, 0), c["name"].lower()))
    return cards


def current_meter(fuelings, maintenances) -> float:
    readings = [_f(f.meter_reading) for f in fuelings if f.meter_reading is not None]
    readings += [_f(m.meter_reading_hours) for m in maintenances if m.meter_reading_hours is not None]
    return max(readings) if readings else 0.0


def _plan_status(plan, meter: float, today: date, unit: str) -> dict:
    days_left = (plan.next_due_date - today).days if plan.next_due_date else None
    hours_left = round(_f(plan.next_due_hours) - meter, 1) if plan.next_due_hours and meter else None
    overdue = (days_left is not None and days_left < 0) or (hours_left is not None and hours_left < 0)
    soon = not overdue and (
        (days_left is not None and days_left <= DUE_SOON_DAYS) or (hours_left is not None and hours_left <= DUE_SOON_HOURS)
    )
    if hours_left is not None and (days_left is None or hours_left < 0 or (days_left >= 0 and hours_left <= days_left * 8)):
        when = f"{br_number(abs(hours_left))} {unit} atrasada" if hours_left < 0 else f"em {br_number(hours_left)} {unit}"
        pct = 100 if hours_left < 0 else max(4, min(100, round(100 - hours_left / max(_f(plan.interval_hours) or 250, 1) * 100)))
    elif days_left is not None:
        when = f"{abs(days_left)} dias atrasada" if days_left < 0 else ("hoje" if days_left == 0 else f"em {days_left} dias")
        pct = 100 if days_left < 0 else max(4, min(100, round(100 - days_left / max(plan.interval_days or 180, 1) * 100)))
    else:
        when, pct = "sem data", 0
    rule = []
    if plan.interval_hours:
        rule.append(f"a cada {br_number(plan.interval_hours)} {unit}")
    if plan.interval_days:
        rule.append(f"a cada {plan.interval_days} dias")
    return {
        "id": plan.id,
        "description": plan.description,
        "type": plan.maintenance_type,
        "when": when,
        "rule": " ou ".join(rule) or "sem intervalo",
        "tone": "red" if overdue else ("amber" if soon else "green"),
        "pct": pct,
        "overdue": overdue,
        "soon": soon,
        "due_date": plan.next_due_date,
        "rank": (0 if overdue else 1 if soon else 2, days_left if days_left is not None else (hours_left or 0) / 8),
    }


# ---------------------------------------------------------------- painel
def build_alerts(stock_items, machines, last_inventory: date | None, today: date) -> list[dict]:
    alerts = []
    for card in machines:
        for plan in card["plans"]:
            if plan["overdue"] or plan["soon"]:
                alerts.append({
                    "tone": "red" if plan["overdue"] else "amber",
                    "title": f"{plan['description']} · {card['name']}",
                    "detail": f"Manutenção {plan['when']}.",
                    "action": "Abrir ficha", "href": f"/insumos/patrimonio/{card['id']}/ficha",
                    "rank": 0 if plan["overdue"] else 2,
                })
    for item in stock_items:
        if item["status"] in {"out", "low", "short"}:
            if item["status"] == "out":
                title, rank = f"{item['name']} acabou", 0
            elif item["days_left"] is not None and item["days_left"] <= 10:
                title, rank = f"{item['name']} acaba em ~{item['days_left']} dias", 1
            else:
                title, rank = f"{item['name']} abaixo do mínimo", 1
            detail = f"Tem {item['available_label']}"
            if item["minimum_label"]:
                detail += f" · mínimo {item['minimum_label']}"
            alerts.append({"tone": "red" if item["status"] == "out" else "amber", "title": title, "detail": detail + ".",
                           "action": "Lançar compra", "href": "/insumos/comprados/nova", "rank": rank})
        if item["expiring_count"]:
            lot = next(l for l in item["lots"] if l["tone"] in {"amber", "red"})
            expired = lot["days_to_expire"] < 0
            alerts.append({
                "tone": "red" if expired else "amber",
                "title": f"{item['name']}: {'lote vencido' if expired else 'lote vence em ' + str(lot['days_to_expire']) + ' dias'}",
                "detail": f"{lot['label']} · {lot['qty']}. {'Separe para devolução.' if expired else 'Use primeiro.'}",
                "action": "Ver lotes", "href": f"/insumos/estoque?item={item['id']}", "rank": 1 if expired else 3,
            })
    if stock_items and (last_inventory is None or (today - last_inventory).days > INVENTORY_STALE_DAYS):
        detail = "Nenhuma contagem registrada ainda." if last_inventory is None else f"Última em {last_inventory.strftime('%d/%m/%Y')}."
        alerts.append({"tone": "blue", "title": "Hora de contar o estoque do galpão", "detail": detail + " Conferir evita diferença no fim da safra.",
                       "action": "Começar contagem", "href": "/insumos/estoque?contar=1", "rank": 4})
    tone_rank = {"red": 0, "amber": 1, "blue": 2}
    alerts.sort(key=lambda a: (a["rank"], tone_rank[a["tone"]]))
    return alerts


def build_overview(repo, db, *, farm_id, farm_ids, season, today: date) -> dict:
    items = build_stock_items(repo, farm_id, today)
    since = getattr(season, "start_date", None)
    assets = repo.list_equipment_assets(farm_id=farm_id, farm_ids=None if farm_id else farm_ids)
    asset_ids = {a.id for a in assets}
    maintenances = [m for m in repo.list_equipment_maintenances() if m.asset_id in asset_ids]
    plans = [p for p in repo.list_equipment_maintenance_plans() if p.asset_id in asset_ids]
    machines = build_machines(db, assets, maintenances, plans, today, since)
    sessions = repo.list_stock_inventory_sessions(farm_id=farm_id) if farm_id else []
    last_inventory = max((s.inventory_date for s in sessions if s.inventory_date), default=None)
    alerts = build_alerts(items, machines, last_inventory, today)

    month_start = today.replace(day=1)
    previous_start = (month_start - timedelta(days=1)).replace(day=1)
    purchases = [e for e in repo.list_purchased_inputs() if _in_farm(e, farm_id) and e.purchase_date]
    bought_month = sum(_f(e.total_cost) for e in purchases if e.purchase_date >= month_start)
    bought_prev = sum(_f(e.total_cost) for e in purchases if previous_start <= e.purchase_date < month_start)
    machine_cards = [c for c in machines if c["is_machine"]]
    stopped = [c for c in machine_cards if c["status_key"] == "man"]
    fuel_liters = sum(c["fuel_liters"] for c in machine_cards)
    fuel_cost = sum(c["fuel_cost"] for c in machine_cards)
    if bought_prev:
        delta = round((bought_month - bought_prev) / bought_prev * 100)
        month_hint = f"{abs(delta)}% {'acima' if delta > 0 else 'abaixo'} do mês passado" if delta else "igual ao mês passado"
    else:
        month_hint = "primeiras compras registradas" if bought_month else "nenhuma compra neste mês"
    kpis = [
        {"label": "Valor em estoque", "value": br_money(sum(i["value"] for i in items)), "hint": f"{len(items)} itens com saldo", "tone": "muted"},
        {"label": "Comprado no mês", "value": br_money(bought_month), "hint": month_hint, "tone": "muted"},
        {"label": "Máquinas paradas", "value": f"{len(stopped)} de {len(machine_cards)}" if machine_cards else "—",
         "hint": (stopped[0]["name"] + " em manutenção") if stopped else "todas trabalhando", "tone": "amber" if stopped else "muted"},
        {"label": "Combustível na safra", "value": br_qty(fuel_liters, "L"), "hint": br_money(fuel_cost) if fuel_liters else "registre os abastecimentos", "tone": "muted"},
    ]

    sector_totals = defaultdict(float)
    feed = []
    for output in repo.list_stock_outputs():
        if not _in_farm(output, farm_id):
            continue
        if _is_consumption(output) and output.plot and (since is None or (output.movement_date and output.movement_date >= since)):
            sector_totals[output.plot.name] += _f(output.total_cost)
        if output.reference_type == FUELING_REFERENCE:
            continue
        feed.append((output.movement_date or date.min, output.created_at, {
            "kind": "Ajuste" if not _is_consumption(output) else "Saída", "tone": "red" if not _is_consumption(output) else "amber",
            "item": f"{br_qty(output.quantity, output.unit)} de {output.input_catalog.name if output.input_catalog else 'item'}",
            "where": output.plot.name if output.plot else (output.notes or "sem setor"), "date": output.movement_date,
        }))
    for entry in purchases:
        feed.append((entry.purchase_date, None, {
            "kind": "Entrada", "tone": "green", "item": f"{br_qty(entry.total_quantity, entry.package_unit)} de {entry.name}",
            "where": entry.supplier_name or "compra", "date": entry.purchase_date,
        }))
    names = {a.id: a.name for a in assets}
    for fueling in list_fuelings(db, asset_ids)[:20]:
        feed.append((fueling.fueling_date, fueling.created_at, {
            "kind": "Abasteceu", "tone": "blue", "item": f"{br_qty(fueling.liters, 'L')} de combustível",
            "where": names.get(fueling.asset_id, "máquina") + (f" · {br_number(fueling.meter_reading)} {fueling.meter_unit}" if fueling.meter_reading else ""),
            "date": fueling.fueling_date,
        }))
    for maintenance in maintenances:
        if maintenance.status == "concluida":
            feed.append((maintenance.maintenance_date, maintenance.created_at, {
                "kind": "Manutenção", "tone": "muted", "item": maintenance.description,
                "where": names.get(maintenance.asset_id, "máquina"), "date": maintenance.maintenance_date,
            }))
    feed.sort(key=lambda f: (f[0] or date.min, str(f[1] or "")), reverse=True)
    top = sorted(sector_totals.items(), key=lambda kv: kv[1], reverse=True)[:5]
    peak = top[0][1] if top else 0
    return {
        "items": items,
        "machines": machines,
        "alerts": alerts,
        "kpis": kpis,
        "sectors": [{"name": n, "value": br_money(v), "pct": max(4, round(v / peak * 100)) if peak else 0} for n, v in top],
        "feed": [f[2] for f in feed[:8]],
        "last_inventory": last_inventory,
    }


# ---------------------------------------------------------------- gravação
def register_uses(repo, lines: list[dict], *, farm_id, season_id, plots: dict) -> list:
    """Grava vários usos numa transação só. `plots` = setores permitidos {id: plot}."""
    if not lines:
        raise ValueError("Adicione pelo menos um item para registrar o uso.")
    outputs = []
    try:
        for number, line in enumerate(lines, start=1):
            prefix = f"Linha {number}: " if len(lines) > 1 else ""
            try:
                input_id = int(line.get("input_id") or 0)
                quantity = parse_br_number(line.get("quantity")) or 0.0
            except (TypeError, ValueError):
                raise ValueError(prefix + "confira o item e a quantidade.")
            if not input_id or quantity <= 0:
                raise ValueError(prefix + "escolha o item e informe uma quantidade maior que zero.")
            plot_id = line.get("plot_id")
            plot = plots.get(int(plot_id)) if str(plot_id or "").isdigit() else None
            if plot_id and not plot:
                raise ValueError(prefix + "o setor escolhido não está na safra ativa.")
            catalog = repo.get_input_catalog(input_id)
            unit = (line.get("unit") or "").strip() or _display_unit(catalog)
            if is_integer_unit(unit) and abs(quantity - round(quantity)) > 1e-9:
                raise ValueError(prefix + f"{unit} é contado em números inteiros. Use, por exemplo, {int(quantity) or 1} {unit}.")
            try:
                outputs.append(create_manual_stock_output(repo, {
                    "farm_id": farm_id, "plot_id": plot.id if plot else None, "input_id": input_id, "season_id": season_id,
                    "movement_date": line.get("date") or None, "quantity": quantity, "unit": unit,
                    "notes": (line.get("notes") or "").strip()[:240] or None,
                }, commit=False))
            except ValueError as exc:
                raise ValueError(prefix + str(exc))
        repo.db.commit()
    except Exception:
        repo.db.rollback()
        raise
    return outputs


def _display_unit(catalog) -> str:
    if not catalog:
        return "kg"
    entries = sorted(
        [e for e in (catalog.purchase_entries or []) if e.package_unit],
        key=lambda e: (_f(e.available_quantity) > 0, e.purchase_date or date.min, e.id or 0),
        reverse=True,
    )
    return entries[0].package_unit if entries else (catalog.default_unit or "kg")


def register_fuelings(repo, db, lines: list[dict], *, assets: dict, farm_id, season_id, user_id=None) -> list[EquipmentFueling]:
    """Grava abastecimentos: baixa o combustível do estoque e atualiza o horímetro."""
    if not lines:
        raise ValueError("Adicione pelo menos um abastecimento.")
    created = []
    last_meter: dict[int, float] = {}
    try:
        ordered = sorted(enumerate(lines, start=1), key=lambda pair: (pair[1].get("date") or "", pair[0]))
        for number, line in ordered:
            prefix = f"Linha {number}: " if len(lines) > 1 else ""
            asset = assets.get(int(line["asset_id"])) if str(line.get("asset_id") or "").isdigit() else None
            if not asset:
                raise ValueError(prefix + "escolha a máquina.")
            try:
                liters = parse_br_number(line.get("liters")) or 0.0
                meter = parse_br_number(line.get("meter"))
                fueling_date = date.fromisoformat(line["date"]) if line.get("date") else today_in_app_timezone()
            except (TypeError, ValueError):
                raise ValueError(prefix + "confira litros, horímetro e data.")
            if liters <= 0:
                raise ValueError(prefix + "informe quantos litros foram abastecidos.")
            if asset.id not in last_meter:
                maints = db.query(EquipmentMaintenance).filter(EquipmentMaintenance.asset_id == asset.id).all()
                last_meter[asset.id] = current_meter(list_fuelings(db, [asset.id]), maints)
            unit = meter_unit_for(asset)
            if meter is not None and last_meter[asset.id] and meter < last_meter[asset.id]:
                raise ValueError(
                    prefix + f"o {'hodômetro' if unit == 'km' else 'horímetro'} de {asset.name} não pode ser menor que o último "
                    f"registro ({br_number(last_meter[asset.id])} {unit})."
                )
            source = "posto" if line.get("source") == "posto" else "estoque"
            output = None
            cost = None
            input_id = None
            if source == "estoque":
                try:
                    input_id = int(line.get("input_id") or 0)
                except (TypeError, ValueError):
                    input_id = 0
                if not input_id:
                    raise ValueError(prefix + "escolha de qual combustível do estoque saiu.")
                try:
                    output = create_manual_stock_output(repo, {
                        "farm_id": farm_id, "plot_id": None, "input_id": input_id, "season_id": season_id,
                        "movement_date": fueling_date.isoformat(), "quantity": liters, "unit": "L",
                        "origin": "abastecimento", "reference_type": FUELING_REFERENCE,
                        "notes": f"Abastecimento · {asset.name}"[:240],
                    }, commit=False)
                except ValueError as exc:
                    raise ValueError(prefix + str(exc))
                cost = _f(output.total_cost)
            else:
                price = parse_br_number(line.get("price"))
                cost = round(price * liters, 2) if price else None
            fueling = EquipmentFueling(
                asset_id=asset.id, farm_id=asset.farm_id or farm_id, season_id=season_id, input_id=input_id or None,
                stock_output_id=output.id if output else None, fueling_date=fueling_date, liters=round(liters, 2),
                meter_reading=meter, meter_unit=unit, source=source, total_cost=cost,
                responsible_name=(line.get("responsible") or "").strip()[:120] or None,
                notes=(line.get("notes") or "").strip()[:240] or None, created_by=user_id,
            )
            db.add(fueling)
            if meter is not None:
                last_meter[asset.id] = max(last_meter[asset.id], meter)
            created.append((fueling, output))
        db.flush()
        for fueling, output in created:
            if output is not None:
                output.reference_id = fueling.id
        db.commit()
    except Exception:
        db.rollback()
        raise
    return [fueling for fueling, _ in created]


def fueling_rate(fueling, previous) -> dict | None:
    """Consumo entre dois abastecimentos: litros / horas (ou km / litro para veículos)."""
    if not previous or fueling.meter_reading is None or previous.meter_reading is None:
        return None
    worked = _f(fueling.meter_reading) - _f(previous.meter_reading)
    if worked <= 0:
        return None
    if fueling.meter_unit == "km":
        return {"value": worked / _f(fueling.liters), "label": f"{br_number(worked / _f(fueling.liters), 1)} km/L"}
    return {"value": _f(fueling.liters) / worked, "label": f"{br_number(_f(fueling.liters) / worked, 1)} L/h"}


# ---------------------------------------------------------------- movimentações
_WEEKDAYS = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]
_MONTHS = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
MOVE_KINDS = [("todos", "Tudo"), ("compra", "Entradas"), ("uso", "Usos"), ("abast", "Abastecimentos"), ("ajuste", "Ajustes")]
MANUAL_REFERENCE = "manual_stock_output"
ALLOCATION_REFERENCE = "manual_stock_output_allocation"
PERIODS = {"7": 7, "30": 30}
MAX_COUNT_QUANTITY = 9_999_999.99
INTEGER_UNITS = {"un", "un.", "unid", "unidade", "par", "pares", "peça", "peca", "kit", "cx", "caixa", "rolo"}


def is_integer_unit(unit: str | None) -> bool:
    return (unit or "").strip().lower() in INTEGER_UNITS
PAGE_SIZES = (15, 30, 50, 100)


def day_label(day: date, today: date) -> str:
    text = f"{_WEEKDAYS[day.weekday()]}, {day.day} de {_MONTHS[day.month - 1]}"
    if day == today:
        return f"Hoje · {text}"
    if day == today - timedelta(days=1):
        return f"Ontem · {text}"
    if day.year != today.year:
        text += f" de {day.year}"
    return text[0].upper() + text[1:]


def period_range(period: str | None, season, today: date, start: str | None = None, end: str | None = None) -> tuple[str, date | None, date | None, str]:
    """Devolve (chave, início, fim, rótulo) do período escolhido nas Movimentações."""
    if period == "personalizado":
        try:
            first = date.fromisoformat(start) if start else None
            last = date.fromisoformat(end) if end else None
        except ValueError:
            first = last = None
        if first or last:
            label = f"{first.strftime('%d/%m/%Y') if first else 'início'} a {last.strftime('%d/%m/%Y') if last else 'hoje'}"
            return "personalizado", first, last, label
    if period == "safra" and getattr(season, "start_date", None):
        return "safra", season.start_date, None, f"safra {season.name}"
    if period == "7":
        return "7", today - timedelta(days=6), None, "últimos 7 dias"
    return "30", today - timedelta(days=29), None, "últimos 30 dias"


def _origin_label(output) -> str | None:
    origin = (output.origin or "").lower()
    if origin.startswith("assistente"):
        return "pelo Pezinho"
    if origin.startswith("fertiliza"):
        return "pela Adubação"
    if origin == "whatsapp":
        return "pelo WhatsApp"
    return None


def restore_output(repo, output) -> None:
    """Devolve ao estoque o que uma saída manual tirou dos lotes (sem gravar ainda)."""
    db = repo.db
    allocations = (
        db.query(type(output))
        .filter(type(output).reference_type == ALLOCATION_REFERENCE, type(output).reference_id == output.id)
        .all()
    )
    if allocations:
        for allocation in allocations:
            if allocation.purchased_input:
                allocation.purchased_input.available_quantity = round(
                    _f(allocation.purchased_input.available_quantity) + _f(allocation.quantity), 2
                )
            db.delete(allocation)
    elif output.purchased_input:
        output.purchased_input.available_quantity = round(_f(output.purchased_input.available_quantity) + _f(output.quantity), 2)
    db.delete(output)


def undo_movement(repo, db, output, farm_id) -> str:
    """Desfaz um uso manual ou um abastecimento. Devolve o texto para o aviso."""
    if not output or (farm_id and output.farm_id not in (None, farm_id)):
        raise ValueError("Movimentação não encontrada nesta fazenda.")
    name = output.input_catalog.name if output.input_catalog else "item"
    qty = br_qty(output.quantity, output.unit)
    if output.reference_type == FUELING_REFERENCE:
        fueling = db.query(EquipmentFueling).filter(EquipmentFueling.stock_output_id == output.id).first()
        try:
            if fueling:
                db.delete(fueling)
            restore_output(repo, output)
            db.commit()
        except Exception:
            db.rollback()
            raise
        return f"Abastecimento desfeito: {qty} de {name} voltaram para o estoque."
    if output.reference_type != MANUAL_REFERENCE:
        raise ValueError("Este uso veio da Adubação. Corrija por lá para manter tudo ligado.")
    try:
        restore_output(repo, output)
        db.commit()
    except Exception:
        db.rollback()
        raise
    return f"Uso desfeito: {qty} de {name} voltaram para o estoque."


def build_movements(repo, db, farm_id, *, today: date, start: date | None, end: date | None, kind: str = "todos", search: str = "",
                    page: int = 1, per_page: int = 15, open_id: str | None = None) -> dict:
    entries = [e for e in repo.list_purchased_inputs(include_stock_adjustments=True) if _in_farm(e, farm_id) and e.input_id]
    outputs = [o for o in repo.list_stock_outputs() if _in_farm(o, farm_id)]
    fuel_ids = [o.id for o in outputs if o.reference_type == FUELING_REFERENCE]
    fuelings = {}
    if fuel_ids:
        for fueling in db.query(EquipmentFueling).filter(EquipmentFueling.stock_output_id.in_(fuel_ids)).all():
            fuelings[fueling.stock_output_id] = fueling

    # saldo depois de cada movimento, por item (na ordem em que aconteceram)
    events = [((e.purchase_date or date.min), 0, e.id, e.input_id, _f(e.total_quantity), ("e", e.id)) for e in entries]
    events += [((o.movement_date or date.min), 1, o.id, o.input_id, -_f(o.quantity), ("o", o.id)) for o in outputs]
    events.sort(key=lambda ev: (ev[0], ev[1], ev[2]))
    running: dict[int, float] = defaultdict(float)
    after: dict[tuple, float] = {}
    for _, _, _, input_id, delta, key in events:
        running[input_id] += delta
        after[key] = round(running[input_id], 2)

    moves = []
    groups: dict = defaultdict(list)
    for e in entries:
        if e.entry_origin == "stock_adjustment_entry":
            reason = e.notes or e.adjustment_reason or "Ajuste de saldo"
            moves.append({
                "kind": "ajuste", "date": e.purchase_date, "stamp": str(e.id), "id": f"ae-{e.id}",
                "title": e.name, "where": reason[:120], "qty": "+ " + br_qty(e.total_quantity, e.package_unit), "tone": "green",
                "after": br_qty(after.get(("e", e.id), 0), e.package_unit), "value": None, "value_num": 0.0,
                "rows": [("Item", e.name), ("Motivo", reason)], "undo": None, "fix": None,
            })
        else:
            groups[e.finance_transaction_id or f"e{e.id}"].append(e)
    for key, items in groups.items():
        items.sort(key=lambda i: i.id)
        first = items[0]
        total = sum(_f(i.total_cost) for i in items)
        marker = next((part.strip() for part in (first.notes or "").split("·") if part.strip().startswith("Nota fiscal")), None)
        supplier = first.supplier_name or "fornecedor não informado"
        if len(items) > 1:
            title = f"{marker.replace('Nota fiscal', 'Nota')} · {supplier}" if marker else f"Compra · {supplier}"
            where = ", ".join(i.name for i in items[:4]) + (f" e mais {len(items) - 4}" if len(items) > 4 else "")
            qty, item_after = f"+ {len(items)} itens", None
        else:
            title = f"{br_qty(first.total_quantity, first.package_unit)} de {first.name}"
            where = " · ".join(p for p in (supplier, marker.replace("Nota fiscal", "nota") if marker else None) if p)
            qty, item_after = "+ " + br_qty(first.total_quantity, first.package_unit), br_qty(after.get(("e", first.id), 0), first.package_unit)
        pay = {"a_prazo": f"{first.installment_count}x", "a_vista": "à vista"}.get(first.payment_condition or "", "")
        moves.append({
            "kind": "compra", "date": first.purchase_date, "stamp": str(first.id), "id": f"c-{first.id}",
            "title": title, "where": where + (f" · {pay}" if pay and first.finance_transaction_id else ""), "qty": qty, "tone": "green",
            "after": item_after, "value": br_money(total), "value_num": total,
            "rows": [("Fornecedor", supplier)] + ([("Nota", marker.replace("Nota fiscal ", ""))] if marker else [])
                    + [(i.name, f"{br_qty(i.total_quantity, i.package_unit)} · {br_money(i.total_cost)}" + (f" · lote {i.batch_number}" if i.batch_number else "")) for i in items],
            "undo": None, "fix": None, "link": None, "_entries": items,
        })
    for o in outputs:
        name = o.input_catalog.name if o.input_catalog else "item"
        base = {"date": o.movement_date, "stamp": f"{o.created_at or ''}{o.id}", "after": br_qty(after.get(("o", o.id), 0), o.unit), "item_id": o.input_id}
        if o.reference_type == STOCK_ADJUSTMENT_OUTPUT_REFERENCE:
            reason = o.notes or o.adjustment_reason or "Ajuste de saldo"
            moves.append({**base, "kind": "ajuste", "id": f"ao-{o.id}", "title": name, "where": reason[:120],
                          "qty": "− " + br_qty(o.quantity, o.unit), "tone": "red", "value": None, "value_num": 0.0,
                          "rows": [("Item", name), ("Motivo", reason)], "undo": None, "fix": None})
            continue
        if o.reference_type == FUELING_REFERENCE:
            f = fuelings.get(o.id)
            machine = f.asset.name if f and f.asset else "máquina"
            meter = f" · {'hodômetro' if f.meter_unit == 'km' else 'horímetro'} {br_number(f.meter_reading)} {f.meter_unit}" if f and f.meter_reading is not None else ""
            who = f" · {f.responsible_name}" if f and f.responsible_name else ""
            moves.append({**base, "kind": "abast", "id": f"o-{o.id}", "title": f"{br_qty(o.quantity, o.unit)} de {name}",
                          "where": f"{machine}{meter}{who}", "qty": "− " + br_qty(o.quantity, o.unit), "tone": "ink",
                          "value": br_money(o.total_cost), "value_num": _f(o.total_cost), "liters": _f(o.quantity),
                          "rows": [("Máquina", machine), ("Combustível", name)] + ([("Horímetro", f"{br_number(f.meter_reading)} {f.meter_unit}")] if f and f.meter_reading is not None else [])
                                  + ([("Quem abasteceu", f.responsible_name)] if f and f.responsible_name else []),
                          "undo": o.id, "fix": None, "link": f"/insumos/patrimonio/{f.asset_id}/ficha" if f else None})
            continue
        place = o.plot.name if o.plot else "Uso geral"
        bits = [place] + ([o.notes] if o.notes and o.reference_type == MANUAL_REFERENCE else []) + ([_origin_label(o)] if _origin_label(o) else [])
        manual = o.reference_type == MANUAL_REFERENCE
        moves.append({**base, "kind": "uso", "id": f"o-{o.id}", "title": f"{br_qty(o.quantity, o.unit)} de {name}",
                      "where": " · ".join(b for b in bits if b)[:160], "qty": "− " + br_qty(o.quantity, o.unit), "tone": "ink",
                      "value": br_money(o.total_cost) if o.total_cost else None, "value_num": _f(o.total_cost), "plot": o.plot.name if o.plot else None,
                      "rows": [("Onde", place), ("Safra", o.season.name if getattr(o, "season", None) else "—")] + ([("Para quê", o.notes)] if o.notes and manual else []),
                      "undo": o.id if manual else None,
                      "fix": {"output": o.id, "item": o.input_id, "plot": o.plot_id, "qty": br_number(o.quantity, 2).rstrip("0").rstrip(","), "date": o.movement_date.isoformat() if o.movement_date else "", "notes": o.notes or ""} if manual else None,
                      "link": None if manual else "/fertilizacao"})

    in_period = [m for m in moves if m["date"] and (start is None or m["date"] >= start) and (end is None or m["date"] <= end)]
    summary = {
        "compra": {"value": sum(m["value_num"] for m in in_period if m["kind"] == "compra"), "count": sum(1 for m in in_period if m["kind"] == "compra")},
        "uso": {"value": sum(m["value_num"] for m in in_period if m["kind"] == "uso"), "count": sum(1 for m in in_period if m["kind"] == "uso"),
                "plots": len({m.get("plot") for m in in_period if m["kind"] == "uso" and m.get("plot")})},
        "abast": {"value": sum(m["value_num"] for m in in_period if m["kind"] == "abast"), "count": sum(1 for m in in_period if m["kind"] == "abast"),
                  "liters": br_qty(sum(m.get("liters", 0) for m in in_period if m["kind"] == "abast"), "L")},
        "ajuste": {"count": sum(1 for m in in_period if m["kind"] == "ajuste")},
    }
    counts = {k: (len(in_period) if k == "todos" else sum(1 for m in in_period if m["kind"] == k)) for k, _ in MOVE_KINDS}
    q = (search or "").strip().lower()
    shown = [m for m in in_period if (kind in ("todos", "", None) or m["kind"] == kind) and (not q or q in f"{m['title']} {m['where']}".lower())]
    shown.sort(key=lambda m: (m["date"], m["stamp"]), reverse=True)
    per_page = per_page if per_page in PAGE_SIZES else PAGE_SIZES[0]
    total = len(shown)
    pages = max(1, -(-total // per_page))
    if open_id:
        index = next((i for i, m in enumerate(shown) if m["id"] == open_id), None)
        if index is not None:
            page = index // per_page + 1
    page = min(max(1, int(page or 1)), pages)
    chunk = shown[(page - 1) * per_page: page * per_page]
    for m in chunk:
        if m["kind"] == "compra":
            m["note"] = purchase_note_detail(db, m.pop("_entries"))
    days, by_day = [], defaultdict(list)
    for m in chunk:
        by_day[m["date"]].append(m)
    for day in sorted(by_day, reverse=True):
        rows = by_day[day]
        days.append({"label": day_label(day, today), "count": len(rows), "rows": rows})
    pagination = {
        "page": page, "pages": pages, "per_page": per_page, "total": total, "sizes": PAGE_SIZES,
        "first": (page - 1) * per_page + 1 if total else 0, "last": min(page * per_page, total),
    }
    return {"days": days, "summary": summary, "counts": counts, "total": total, "pagination": pagination}


# ---------------------------------------------------------------- contagem
def count_sheet(items: list[dict], counts: dict) -> dict:
    rows = []
    for item in sorted(items, key=lambda i: (i["group_label"], i["name"].lower())):
        raw = counts.get(str(item["id"]))
        try:
            counted = parse_br_number(raw) if raw not in (None, "") else None
        except ValueError:
            counted, raw = None, ""
        if counted is not None and (counted < 0 or counted > MAX_COUNT_QUANTITY):
            counted, raw = None, ""
        unit_cost = item["value"] / item["available"] if item["available"] else 0
        if counted is None:
            status, diff, diff_label = "todo", None, "falta contar"
        else:
            diff = round(counted - item["available"], 2)
            status = "ok" if abs(diff) < 0.005 else "diff"
            diff_label = "bateu" if status == "ok" else f"{'+' if diff > 0 else '−'} {br_qty(abs(diff), item['unit'])} · {'+' if diff > 0 else '−'} {br_money(abs(diff) * unit_cost)}"
        rows.append({**{k: item[k] for k in ("id", "name", "unit", "available_label", "group_label", "category")},
                     "available": item["available"], "unit_cost": round(unit_cost, 4),
                     "counted": raw or "", "status": status, "diff": diff, "diff_label": diff_label, "diff_value": (diff or 0) * unit_cost})
    done = [r for r in rows if r["status"] != "todo"]
    return {
        "rows": rows,
        "done": len(done),
        "total": len(rows),
        "pct": round(len(done) / len(rows) * 100) if rows else 0,
        "ok": sum(1 for r in rows if r["status"] == "ok"),
        "diff": sum(1 for r in rows if r["status"] == "diff"),
        "missing_value": br_money(sum(-r["diff_value"] for r in rows if r["status"] == "diff" and r["diff_value"] < 0)),
        "extra_value": br_money(sum(r["diff_value"] for r in rows if r["status"] == "diff" and r["diff_value"] > 0)),
    }


def purchase_note_detail(db, entries) -> dict:
    """Dados do painel de uma compra: itens, o que já foi usado, parcelas, anexos e se dá para excluir."""
    from app.services import purchase_note as pn

    first = entries[0]
    tx = first.finance_transaction if first.finance_transaction_id else None
    blockers = pn.delete_blockers(db, entries)
    items = []
    for e in entries:
        used = pn.consumed(e)
        lot = " · ".join(p for p in (f"lote {e.batch_number}" if e.batch_number else None,
                                     f"vence {e.expiration_date.strftime('%m/%Y')}" if e.expiration_date else None) if p)
        items.append({
            "name": e.name, "total": br_money(e.total_cost),
            "detail": " · ".join(p for p in (
                f"{br_qty(e.quantity_purchased)} × {br_qty(e.package_size, e.package_unit)}" if _f(e.package_size) != 1 else br_qty(e.total_quantity, e.package_unit),
                f"{br_money(e.unit_price)} cada", lot or None, f"já usados {br_qty(used, e.package_unit)}" if used else None) if p),
        })
    installments = []
    for i in sorted(tx.installments, key=lambda i: i.installment_number) if tx is not None else []:
        paid = (i.status or "").strip().lower() == "pago"
        installments.append({"n": f"{i.installment_number}ª", "due": i.due_date.strftime("%d/%m/%Y"), "amount": br_money(i.amount),
                             "paid": paid, "method": tx.payment_method or ""})
    seen, attachments = set(), []
    for e in entries:
        for a in sorted(e.attachments or [], key=lambda a: a.id):
            if a.filename not in seen:
                seen.add(a.filename)
                attachments.append({"id": a.id, "filename": a.filename})
    if installments:
        paid_count = sum(1 for i in installments if i["paid"])
        total_count = len(installments)
        if paid_count == 0:
            pay_label = f"{total_count} {'parcela' if total_count == 1 else 'parcelas'} a pagar"
        elif paid_count == total_count:
            pay_label = "tudo pago"
        else:
            pay_label = f"{paid_count} de {total_count} {'paga' if paid_count == 1 else 'pagas'}"
    elif tx is not None:
        pay_label = "à vista" if not tx.credit_card_id else "no cartão"
    else:
        pay_label = "fora do Financeiro"
    uses = [{**u, "label": f"{br_qty(u['quantity'], u['unit'])} de {u['name']}",
             "where": " · ".join(p for p in (u["plot"] or ("Ajuste de contagem" if u["adjustment"] else "Uso geral"),
                                             u["date"].strftime("%d/%m") if u["date"] else None) if p)} for u in blockers["uses"][:6]]
    return {
        "entry_id": first.id, "items": items, "installments": installments, "attachments": attachments,
        "pay_label": pay_label, "total": br_money(sum(_f(e.total_cost) for e in entries)),
        "blocked": blockers["blocked"], "uses": uses, "more_uses": max(0, len(blockers["uses"]) - 6),
        "linked": blockers["linked"], "paid": blockers["paid"], "count": len(entries),
        "finance": tx is not None,
    }
