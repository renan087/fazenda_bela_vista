"""Parcelamento, épocas de aplicação e verificação de incompatibilidades."""
from __future__ import annotations

from datetime import date, timedelta

from app.services.agronomy import references as R

MONTHS = ("jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez")
AMMONIACAL = ("ureia", "sulfato_amonio", "map")


def label(day: date) -> str:
    return f"{day.day:02d}/{MONTHS[day.month - 1]}/{day.year}"


def _cycle_dates(cycle_start: date, month_days: list[tuple[int, int]]) -> list[date]:
    return [date(cycle_start.year + (1 if month < 7 else 0), month, day) for month, day in month_days]


def split_plan(species: str, irrigation: str, n_total: float) -> tuple[list[tuple[int, int]], list[float], str]:
    """Datas-modelo (mês, dia) e frações das parcelas de N e K no ano agrícola."""
    if irrigation == "gotejo":
        days = [(9, 1), (10, 1), (11, 1), (12, 1), (1, 1), (2, 1), (3, 1), (4, 1)]
        return days, [1 / len(days)] * len(days), (
            "Fertirrigação mensal de setembro (florada) a abril: mais parcelas reduzem perdas e acompanham a demanda "
            "(Embrapa Pará, p. 270–271).")
    if species == "canephora":
        return [(10, 15), (12, 10), (2, 5)], list(R.SPLIT_CONILON), (
            "Três parcelas: 40 % no início das chuvas e 30 % + 30 % a cada 50–60 dias (Embrapa Pará, p. 270–271).")
    count = 4 if irrigation == "aspersor" or n_total > 250 else 3
    days = [(10, 15), (12, 1), (1, 15), (3, 1)] if count == 4 else [(10, 15), (12, 15), (2, 15)]
    return days, [1 / count] * count, (
        f"{count} parcelas iguais no período chuvoso, para reduzir perdas de N e K (Manual do Café, p. 47).")


def remaining_dates(cycle_start: date, today: date, month_days):
    """Datas ainda por vir neste ciclo; se o período chuvoso acabou, usa o ciclo seguinte."""
    dates = _cycle_dates(cycle_start, month_days)
    future = [day for day in dates if day >= today]
    if future and len(future) >= 2:
        return future, len(future) < len(dates), False
    if dates[-1] >= today or today.month < 5:  # ainda há chuva: duas parcelas a partir de agora
        start = today + timedelta(days=7)
        return [start, start + timedelta(days=45)], True, False
    next_start = cycle_start.replace(year=cycle_start.year + 1)
    return _cycle_dates(next_start, month_days), False, True


def dry_window(today: date) -> date:
    """Calagem e gessagem na seca (maio a agosto), com 60 dias antes da 1ª adubação de outubro."""
    if 5 <= today.month <= 8:
        return today + timedelta(days=7)
    year = today.year + 1 if today.month >= 9 else today.year
    return date(year, 5, 15)


def foliar_dates(cycle_start: date, today: date) -> list[date]:
    dates = _cycle_dates(cycle_start, [(9, 15), (11, 15), (1, 15)])
    future = [day for day in dates if day >= today]
    if future:
        return future
    return _cycle_dates(cycle_start.replace(year=cycle_start.year + 1), [(9, 15), (11, 15), (1, 15)])


def compatibility(events: list[dict], corrections: dict) -> list[dict]:
    """Regras de manejo entre aplicações do plano."""
    notes = []
    lime = corrections.get("calagem") or {}
    gyp = corrections.get("gessagem") or {}
    lime_dates = [event["data_obj"] for event in events if event.get("chave") == "calagem"]
    for event in events:
        keys = {item.get("fonte") for item in event.get("itens", [])}
        if lime_dates and keys & set(AMMONIACAL):
            gap = min(abs((event["data_obj"] - day).days) for day in lime_dates)
            if gap < R.LIMING_MIN_DAYS_BEFORE_FERTILIZATION:
                notes.append({"nivel": "atencao", "texto": f"{event['quando']}: adubação nitrogenada a {gap} dias da calagem. "
                              "Mantenha 60 dias entre calcário e adubação (Embrapa Pará, p. 268) para reduzir perdas de N."})
        if "nitrato_calcio" in keys and keys & {"ssp", "tsp", "map", "sulfato_amonio", "sulfato_potassio"}:
            notes.append({"nivel": "critico", "texto": f"{event['quando']}: nitrato de cálcio não pode ir no mesmo tanque que "
                          "sulfatos ou fosfatos (forma precipitado). Use tanques separados."})
        if "ureia" in keys and "ssp" in keys:
            notes.append({"nivel": "atencao", "texto": f"{event['quando']}: não misture ureia com superfosfato simples com "
                          "antecedência — a mistura absorve umidade e empedra. Aplique separados ou misture na hora."})
    if gyp.get("necessario") and not lime.get("necessario"):
        notes.append({"nivel": "atencao", "texto": "Gesso sem calcário pode levar magnésio e potássio para camadas profundas. "
                      "Toda gessagem deve ser acompanhada de calagem (Manual do Café, p. 32 e 42); confira o Mg antes."})
    if lime.get("necessario"):
        notes.append({"nivel": "info", "texto": "Não misture calcário com adubos fosfatados ou nitrogenados amoniacais na mesma "
                      "operação; o gesso pode ser aplicado junto ou logo após a calagem (Incaper, p. 429)."})
    return notes
