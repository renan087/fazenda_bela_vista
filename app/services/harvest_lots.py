"""Lotes de colheita: medida da roça → sacas beneficiadas, etapas pós-colheita e linha do tempo do lote."""

from __future__ import annotations

from datetime import date

from app.models.harvest import HarvestLotEvent, HarvestRecord

# unidade: (rótulo, sigla, rendimento padrão em "unidades por saca de 60 kg", explicação do rendimento)
MEASURE_UNITS = {
    "litros": ("Litros", "L", 480.0, "litros por saca"),
    "sacos_roca": ("Sacos da roça", "sacos", 8.0, "sacos por saca"),
    "sc_60": ("Sacas de 60 kg", "sc", 1.0, ""),
}
MEASURE_HINTS = {"litros": "medida da roça", "sacos_roca": "sacos cheios no talhão", "sc_60": "já beneficiado"}
LOT_STAGES = [("secagem", "Secando"), ("tulha", "Na tulha"), ("beneficiado", "Beneficiado"), ("armazem", "Armazém de terceiros")]
LOT_STAGE_LABELS = dict(LOT_STAGES)
STAGE_HINTS = {
    "secagem": "Volume ainda estimado. Informe as sacas reais ao beneficiar.",
    "tulha": "Café seco guardado na fazenda.",
    "beneficiado": "Pronto para vender, com as sacas reais informadas.",
    "armazem": "Informe o armazém e o número do recibo de depósito.",
}
DESTINATIONS = [("Terreiro", "Terreiro"), ("Lavador", "Lavador"), ("Secador", "Secador"), ("Tulha", "Tulha")]
HARVEST_TYPES = [("Manual", "Manual"), ("Semimecanizada", "Semimec."), ("Mecanizada", "Mecanizada")]
COFFEE_STAGES = [("Cereja", "Cereja"), ("Verde", "Verde"), ("Boia", "Boia"), ("Mistura", "Mistura")]


def _num(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def estimate_sacks(unit: str | None, quantity: float, factor: float | None) -> float:
    if unit == "sc_60" or not unit:
        return round(quantity, 2)
    factor = factor or MEASURE_UNITS.get(unit, (None, None, 1.0))[2]
    return round(quantity / factor, 2) if factor else 0.0


def initial_stage(unit: str | None, destination: str | None) -> str:
    if unit == "sc_60":
        return "beneficiado"
    return "tulha" if (destination or "").strip().lower() == "tulha" else "secagem"


def farm_yield_average(db, plot_ids: list[int], unit: str) -> float | None:
    """Rendimento médio real (unidades por saca) dos lotes já beneficiados destes talhões."""
    if unit not in {"litros", "sacos_roca"} or not plot_ids:
        return None
    rows = (db.query(HarvestRecord.measured_quantity, HarvestRecord.sacks_produced)
            .filter(HarvestRecord.plot_id.in_(plot_ids), HarvestRecord.measure_unit == unit,
                    HarvestRecord.sacks_estimated.is_(False), HarvestRecord.measured_quantity.isnot(None))
            .all())
    qty = sum(_num(q) for q, s in rows if _num(s) > 0)
    sacks = sum(_num(s) for q, s in rows if _num(s) > 0)
    return round(qty / sacks, 1) if sacks else None


def stage_of(harvest: HarvestRecord, available: float | None = None) -> tuple[str, str]:
    """(código, rótulo) da etapa atual; 'vendido' quando não sobra saldo."""
    if available is not None and available <= 0.00005 and _num(harvest.sacks_produced) > 0:
        return "vendido", "Todo vendido"
    code = harvest.lot_stage or ("beneficiado" if not harvest.sacks_estimated else "secagem")
    label = LOT_STAGE_LABELS.get(code, "Na tulha")
    if code == "armazem" and harvest.storage_location:
        label = harvest.storage_location
    if code == "tulha" and harvest.sacks_estimated:
        label = "Na tulha · falta beneficiar"
    return code, label


def add_event(db, harvest: HarvestRecord, *, event_date: date, kind: str, title: str, detail: str | None = None,
              stage: str | None = None) -> HarvestLotEvent:
    event = HarvestLotEvent(harvest_id=harvest.id, event_date=event_date, kind=kind, stage=stage, title=title[:160],
                            detail=detail)
    db.add(event)
    return event


def _fmt(value: float, places: int = 0) -> str:
    text = f"{value:,.{places}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return text


def harvest_summary(harvest: HarvestRecord) -> str:
    parts = []
    if harvest.harvest_type:
        parts.append(harvest.harvest_type)
    if harvest.measure_unit in {"litros", "sacos_roca"} and harvest.measured_quantity:
        parts.append(f"{_fmt(_num(harvest.measured_quantity))} {MEASURE_UNITS[harvest.measure_unit][1]}")
    if harvest.maturation_percentage is not None:
        parts.append(f"{_fmt(_num(harvest.maturation_percentage))}% cereja")
    if harvest.initial_destination:
        parts.append(f"foi para {harvest.initial_destination.lower()}")
    if harvest.responsible_name:
        parts.append(f"responsável {harvest.responsible_name}")
    return " · ".join(parts)


def timeline(harvest: HarvestRecord, analyses, sale_groups, today: date) -> list[dict]:
    """Caminho do lote: colheita, etapas, classificação e vendas, em ordem de data."""
    items = [{"date": harvest.harvest_date, "order": 0, "kind": "colheita",
              "title": f"Colheita no {harvest.plot.name if harvest.plot else 'setor removido'}",
              "detail": harvest_summary(harvest) or None}]
    for event in harvest.events or []:
        items.append({"date": event.event_date, "order": 1, "kind": event.kind, "title": event.title, "detail": event.detail})
    for analysis in analyses:
        bits = [analysis.evaluator, f"tipo {analysis.coffee_type}" if analysis.coffee_type else None,
                f"peneira {analysis.sieve}" if analysis.sieve else None,
                f"bebida {analysis.beverage.lower()}" if analysis.beverage else None,
                f"{_fmt(_num(analysis.sensory_score), 1).replace(',0', '')} pts" if analysis.sensory_score is not None else None]
        items.append({"date": analysis.analysis_date, "order": 2, "kind": "classificacao", "title": "Classificado",
                      "detail": " · ".join(b for b in bits if b) or None, "analysis_id": analysis.id})
    for group in sale_groups:
        mine = sum(lot["sacks"] for lot in group["lots"] if lot["harvest_id"] == harvest.id)
        detail = f"{_fmt(mine, 2)} sc para {group['buyer_name']} · R$ {_fmt(group['unit_price'], 2)}/sc"
        items.append({"date": group["sale_date"], "order": 3, "kind": "venda", "title": f"Venda {group['code']} negociada",
                      "detail": detail, "sale_code": group["code"]})
        if group["delivered_on"]:
            items.append({"date": group["delivered_on"], "order": 4, "kind": "entrega", "title": f"Venda {group['code']} entregue",
                          "detail": f"{_fmt(mine, 2)} sc saíram do lote", "sale_code": group["code"]})
        if group["received_on"]:
            items.append({"date": group["received_on"], "order": 5, "kind": "recebimento", "title": f"Venda {group['code']} recebida",
                          "detail": None, "sale_code": group["code"]})
        if group["status"] == "negociado" and group["delivery_due_date"]:
            late = group["delivery_due_date"] < today
            items.append({"date": group["delivery_due_date"], "order": 6, "kind": "proximo", "pending": True,
                          "title": f"{'Entrega atrasada' if late else 'Próximo'}: entregar {_fmt(mine, 2)} sc",
                          "detail": f"Venda {group['code']} · {group['buyer_name']}", "sale_code": group["code"]})
    items.sort(key=lambda item: (item["date"] or date.min, item["order"]))
    return items
