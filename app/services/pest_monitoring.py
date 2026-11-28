"""Regras e montagem da página Pragas (/pragas): focos, nível de controle, reinspeções e rondas.

A situação de cada foco é calculada a partir da contagem (amostradas × com sintoma)
comparada ao nível de controle da praga. Os códigos gravados no banco continuam os
mesmos usados pelo restante do sistema (Lavoura, Painel, Pezinho):

    ativa          → "Precisa de controle"
    em_tratamento  → "Em controle"
    monitoramento  → "Em observação"
    controlada     → "Controlado"
"""

from __future__ import annotations

import unicodedata
from datetime import date, timedelta
from typing import Any

# Níveis de controle iniciais (referência de manejo do café; o agrônomo pode ajustar por foco).
PEST_CATALOG: tuple[dict[str, Any], ...] = (
    {"key": "bicho_mineiro", "name": "Bicho-mineiro", "category": "Praga", "threshold": 30, "unit": "folhas minadas", "noun": "Folhas avaliadas", "days": 14, "aliases": ("bicho mineiro",)},
    {"key": "ferrugem", "name": "Ferrugem-alaranjada", "category": "Doença", "threshold": 5, "unit": "folhas com pústulas", "noun": "Folhas avaliadas", "days": 15, "aliases": ("ferrugem",)},
    {"key": "broca", "name": "Broca-do-café", "category": "Praga", "threshold": 3, "unit": "frutos broqueados", "noun": "Frutos avaliados", "days": 15, "aliases": ("broca",)},
    {"key": "cercosporiose", "name": "Cercosporiose", "category": "Doença", "threshold": 10, "unit": "folhas com lesão", "noun": "Folhas avaliadas", "days": 15, "aliases": ("cercosporiose (olho-pardo)", "olho-pardo", "cercospora")},
    {"key": "acaro_vermelho", "name": "Ácaro-vermelho", "category": "Praga", "threshold": 20, "unit": "folhas com ácaro", "noun": "Folhas avaliadas", "days": 10, "aliases": ("acaro vermelho",)},
    {"key": "phoma", "name": "Phoma", "category": "Doença", "threshold": 10, "unit": "ramos com sintoma", "noun": "Ramos avaliados", "days": 10, "aliases": ("phoma / ascochyta", "ascochyta")},
    {"key": "cochonilha", "name": "Cochonilha", "category": "Praga", "threshold": 10, "unit": "plantas com colônia", "noun": "Plantas avaliadas", "days": 15, "aliases": ()},
    {"key": "mancha_aureolada", "name": "Mancha-aureolada", "category": "Doença", "threshold": 5, "unit": "folhas com mancha", "noun": "Folhas avaliadas", "days": 10, "aliases": ()},
    {"key": "lagartas", "name": "Lagartas", "category": "Praga", "threshold": 10, "unit": "plantas atacadas", "noun": "Plantas avaliadas", "days": 7, "aliases": ("lagarta",)},
    {"key": "nematoides", "name": "Nematoides", "category": "Praga", "threshold": 10, "unit": "plantas com sintoma", "noun": "Plantas avaliadas", "days": 30, "aliases": ("nematoide",)},
    {"key": "cigarras", "name": "Cigarras", "category": "Praga", "threshold": 10, "unit": "plantas atacadas", "noun": "Plantas avaliadas", "days": 30, "aliases": ("cigarra",)},
    {"key": "acaro_mancha_anular", "name": "Ácaro-da-mancha-anular", "category": "Praga", "threshold": 10, "unit": "folhas com mancha", "noun": "Folhas avaliadas", "days": 15, "aliases": ()},
)
DEFAULT_COMMON_KEYS = ("bicho_mineiro", "ferrugem", "broca", "cercosporiose")
OTHER_DEFAULTS = {"unit": "plantas com sintoma", "noun": "Plantas avaliadas", "days": 10, "threshold": 10}
LOCATIONS = ("Bordadura", "Reboleira", "Talhão todo")
ROUND_LEVELS = ("Ausente", "Baixa", "Média", "Alta")

STATUS_UI: dict[str, dict[str, str]] = {
    "ativa": {"key": "precisa", "label": "Precisa de controle", "fg": "#B42318", "bg": "#FDECEA"},
    "em_tratamento": {"key": "controle", "label": "Em controle", "fg": "#1F5FA8", "bg": "#E8F1FB"},
    "monitoramento": {"key": "observacao", "label": "Em observação", "fg": "#8A5A00", "bg": "#FFF6E5"},
    "controlada": {"key": "controlado", "label": "Controlado", "fg": "#1F7A3A", "bg": "#E6F4E2"},
}
TONE_COLORS = {"err": "#B42318", "warn": "#D18A00", "info": "#1F5FA8", "ok": "#1F7A3A", "muted": "#7A857E"}
WEEKDAYS = ("seg", "ter", "qua", "qui", "sex", "sáb", "dom")


def _norm(text: str | None) -> str:
    raw = unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode("ascii")
    return " ".join(raw.lower().replace("-", " ").replace("/", " ").split())


def catalog_entry(key_or_name: str | None) -> dict[str, Any] | None:
    """Acha a praga do catálogo pela chave, nome ou apelido (aceita nomes antigos)."""
    needle = _norm(key_or_name)
    if not needle:
        return None
    for item in PEST_CATALOG:
        names = (item["key"], item["name"], *item["aliases"])
        if any(_norm(n) == needle for n in names):
            return item
    return None


def pest_spec(name: str | None, category: str | None = None, threshold: float | None = None) -> dict[str, Any]:
    """Regras da praga: nível de controle, unidade de contagem e prazo de reinspeção."""
    entry = catalog_entry(name)
    if entry:
        spec = dict(entry)
    else:
        spec = {"key": None, "name": (name or "").strip() or "Praga", "category": category or "Praga", **OTHER_DEFAULTS, "aliases": ()}
    if threshold is not None and float(threshold) > 0:
        spec["threshold"] = float(threshold)
    return spec


def compute_incidence(sampled: int | None, affected: int | None) -> float | None:
    if not sampled or sampled <= 0 or affected is None or affected < 0 or affected > sampled:
        return None
    return round(affected / sampled * 100, 2)


def classify(incidence: float | None, threshold: float, level: int | None = None) -> tuple[str, int]:
    """Situação (código do banco) e severidade 1–5 a partir da incidência ou do nível da ronda."""
    if incidence is None:
        lvl = int(level or 0)
        return ("ativa", 4) if lvl >= 3 else ("monitoramento", 2 if lvl <= 1 else 3)
    ratio = incidence / threshold if threshold else 0
    if ratio > 1:
        return "ativa", 5 if ratio >= 2 else 4
    return "monitoramento", 3 if ratio >= 0.7 else (2 if ratio >= 0.3 else 1)


def decision_for(incidence: float, threshold: float) -> str:
    """Sugestão após a reinspeção: encerrar, continuar observando ou novo controle."""
    if incidence < threshold * 0.4:
        return "encerrar"
    return "manter" if incidence <= threshold else "novo"


def fmt_pct(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{float(value):.1f}".replace(".", ",") + "%"


def fmt_num(value: float | None, decimals: int = 2) -> str:
    if value is None:
        return "—"
    txt = f"{float(value):,.{decimals}f}"
    return txt.replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_threshold(value: float | None) -> str:
    if value is None:
        return "—"
    v = float(value)
    return (f"{v:.0f}" if v == int(v) else f"{v:.1f}".replace(".", ",")) + "%"


def ddmm(value: date | None) -> str:
    return value.strftime("%d/%m") if value else ""


def common_pest_keys(profile_text: str | None) -> list[str]:
    """Pragas recorrentes do Perfil agronômico (texto livre) → chaves do catálogo."""
    text = _norm(profile_text)
    keys = [item["key"] for item in PEST_CATALOG if text and any(_norm(n) in text for n in (item["name"], *item["aliases"]))]
    return keys or list(DEFAULT_COMMON_KEYS)


def forecast_strip(forecast_days: list[dict[str, Any]], today: date) -> list[dict[str, Any]]:
    """Próximos 5 dias para escolher quando aplicar (ideal = sem chuva prevista)."""
    out = []
    for d in forecast_days[:5]:
        try:
            day = date.fromisoformat(str(d.get("date"))[:10])
        except ValueError:
            continue
        if day < today:
            continue
        precip = float(d.get("precip_mm") or 0)
        prob = int(d.get("prob") or 0)
        kind = "rain" if precip >= 1 or prob >= 60 else ("cloud" if prob >= 40 else "sun")
        ideal = kind == "sun"
        out.append({
            "iso": day.isoformat(),
            "weekday": WEEKDAYS[day.weekday()],
            "day": day.strftime("%d"),
            "label": ddmm(day),
            "kind": kind,
            "ideal": ideal,
            "value": "Ideal" if ideal else (f"{fmt_num(precip, 0)} mm" if precip >= 1 else f"{prob}% chuva"),
            "aria": f"{WEEKDAYS[day.weekday()]} {ddmm(day)}: " + (f"{fmt_num(precip, 1)} mm de chuva, {prob}% de chance" if precip else f"sem chuva prevista, {prob}% de chance"),
        })
    return out


def climate_alert(forecast_days: list[dict[str, Any]], strip: list[dict[str, Any]]) -> dict[str, str] | None:
    rainy = sum(1 for d in forecast_days[:5] if float(d.get("precip_mm") or 0) >= 5 or int(d.get("prob") or 0) >= 60)
    if rainy < 3:
        return None
    best = next((d for d in strip if d["ideal"]), None)
    detail = f"Chuva prevista em {rainy} dos próximos 5 dias."
    if best:
        detail += f" Melhor janela para aplicar: {best['weekday']} {best['label']}, sem chuva prevista."
    return {"title": "Risco alto de ferrugem e phoma nesta semana.", "detail": detail}


def _incident_spec(incident) -> dict[str, Any]:
    return pest_spec(incident.name, incident.category, float(incident.control_threshold_pct) if incident.control_threshold_pct is not None else None)


def _events_for(incident, spec, ops: list, today: date) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    inspections = list(getattr(incident, "inspections", None) or [])
    if not inspections:
        detail = fmt_pct(float(incident.incidence_pct)) if incident.incidence_pct is not None else "Sem contagem"
        events.append({"date": incident.occurrence_date, "title": "Foco registrado", "text": detail, "tone": "muted", "order": 0})
    for ins in inspections:
        pct = float(ins.incidence_pct) if ins.incidence_pct is not None else None
        if ins.kind == "registro":
            title = "Foco registrado"
        elif ins.kind == "ronda":
            title = "Ronda de monitoramento"
        elif ins.kind == "correcao":
            title = "Registro corrigido"
        else:
            title = "Reinspeção" + (" e encerramento" if ins.decision == "encerrar" else "")
        if pct is not None and ins.sampled_count:
            text = f"{ins.affected_count} de {ins.sampled_count} {spec['unit']} ({fmt_pct(pct)})"
        elif pct is not None:
            text = fmt_pct(pct)
        elif ins.level is not None:
            text = f"Nível {ROUND_LEVELS[max(0, min(3, int(ins.level)))].lower()} na ronda"
        else:
            text = ""
        if ins.note:
            text = (text + " · " if text else "") + ins.note
        tone = "muted"
        if pct is not None:
            tone = "err" if pct > spec["threshold"] else ("ok" if ins.kind == "reinspecao" else "warn")
        elif ins.level:
            tone = "err" if ins.level >= 3 else "warn"
        events.append({"date": ins.inspection_date, "title": title, "text": text, "tone": tone, "order": ins.id or 0})
    for op in ops:
        planned = (op.status or "realizada") == "planejada"
        bits = [op.notes.split("\n")[0]] if op.notes else []
        if op.withdrawal_days:
            bits.append(f"carência {op.withdrawal_days} dias")
        events.append({
            "date": op.operation_date,
            "title": "Pulverização planejada" if planned else "Pulverização realizada",
            "text": " · ".join(b for b in bits if b),
            "tone": "info",
            "order": 10_000 + (op.id or 0),
            "op_id": op.id,
        })
    if incident.status != "controlada" and incident.reinspection_date and incident.reinspection_date <= today:
        events.append({"date": incident.reinspection_date, "title": "Reinspeção prevista", "text": "Ainda não foi feita", "tone": "warn", "order": 20_000})
    if incident.status == "controlada" and incident.resolved_date and not any(i.decision == "encerrar" for i in inspections):
        events.append({"date": incident.resolved_date, "title": "Encerrado", "text": "Foco controlado", "tone": "ok", "order": 30_000})
    events.sort(key=lambda e: (e["date"], e["order"]), reverse=True)
    for e in events:
        e["label"] = ddmm(e["date"])
        e["color"] = TONE_COLORS.get(e["tone"], TONE_COLORS["muted"])
        e.pop("date", None)
        e.pop("order", None)
    return events


def _history(incident) -> list[dict[str, Any]]:
    points = [
        {"label": ddmm(ins.inspection_date), "value": float(ins.incidence_pct)}
        for ins in (getattr(incident, "inspections", None) or [])
        if ins.incidence_pct is not None
    ]
    if not points and incident.incidence_pct is not None:
        points = [{"label": ddmm(incident.occurrence_date), "value": float(incident.incidence_pct)}]
    return points[-6:]


def build_incident_row(incident, ops: list, today: date) -> dict[str, Any]:
    spec = _incident_spec(incident)
    status_db = incident.status if incident.status in STATUS_UI else "ativa"
    ui = STATUS_UI[status_db]
    inc = float(incident.incidence_pct) if incident.incidence_pct is not None else None
    threshold = float(spec["threshold"])
    plot = incident.plot
    variety = getattr(getattr(plot, "variety", None), "name", None) or ""
    last_level = next((ins.level for ins in reversed(list(incident.inspections or [])) if ins.level is not None), None)

    action, action_label, action_sub, late = None, "", "", False
    due = incident.reinspection_date
    if status_db == "ativa":
        action, action_label, action_sub = "controle", "Planejar controle", "Passou do nível de controle" if inc is not None else "Nível alto na ronda"
    elif status_db == "controlada":
        action_sub = f"Encerrado em {ddmm(incident.resolved_date)}" if incident.resolved_date else "Encerrado"
    else:
        action, action_label = "reinspecao", "Reinspecionar"
        if due:
            delta = (due - today).days
            late = delta < 0
            if delta < 0:
                action_sub = f"Atrasada há {-delta} dia" + ("s" if delta < -1 else "")
            elif delta == 0:
                action_sub = "Para hoje"
            else:
                action_sub = f"Em {delta} dia" + ("s" if delta > 1 else "") + f" · {ddmm(due)}"
        else:
            action_sub = "Sem data marcada"

    if inc is not None:
        incidence_txt = fmt_pct(inc)
        bar_w = min(100.0, inc / (threshold * 2) * 100) if threshold else 0
        above = inc > threshold
    else:
        incidence_txt = f"Nível {ROUND_LEVELS[int(last_level)].lower()}" if last_level else "Sem contagem"
        bar_w = {1: 22, 2: 50, 3: 78}.get(int(last_level or 0), 0)
        above = (last_level or 0) >= 3
    bar_c = "#1F7A3A" if status_db == "controlada" else ("#B42318" if above else "#D18A00")

    withdrawal_until = None
    for op in ops:
        if op.withdrawal_days and op.operation_date:
            end = op.operation_date + timedelta(days=int(op.withdrawal_days))
            if end >= today and (withdrawal_until is None or end > withdrawal_until):
                withdrawal_until = end

    hist = _history(incident)
    hmax = max([threshold * 1.6] + [h["value"] for h in hist]) if hist else threshold * 1.6
    bars = [
        {"label": h["label"], "value": fmt_pct(h["value"]), "height": max(4, round(h["value"] / hmax * 80)), "above": h["value"] > threshold}
        for h in hist
    ]
    prev = hist[-1] if hist else None

    next_box = {
        "ativa": {"kicker": "Próximo passo", "text": f"Passou do nível de controle em {ddmm(incident.occurrence_date)}. Planeje a pulverização para segurar o avanço.", "primary": "controle", "secondary": "reinspecao", "tone": "err"},
        "em_tratamento": {"kicker": "Reinspeção atrasada" if late else "Próximo passo", "text": f"Confira se a aplicação funcionou. {action_sub}.", "primary": "reinspecao", "secondary": "controle", "tone": "warn" if late else "info"},
        "monitoramento": {"kicker": "Próximo passo", "text": f"Abaixo do nível de controle — não precisa aplicar agora. Reinspeção: {action_sub.lower()}.", "primary": "reinspecao", "secondary": "controle", "tone": "warn"},
        "controlada": {"kicker": "Foco encerrado", "text": f"Controlado em {ddmm(incident.resolved_date) or ddmm(incident.occurrence_date)}. Se voltar a aparecer, reabra para manter o histórico junto.", "primary": "reabrir", "secondary": None, "tone": "ok"},
    }[status_db]

    return {
        "id": incident.id,
        "name": incident.name,
        "category": "Doença" if _norm(incident.category) == "doenca" else (incident.category or "Praga"),
        "is_disease": _norm(incident.category) == "doenca",
        "pest_key": spec.get("key") or _norm(incident.name),
        "plot_id": incident.plot_id,
        "plot_name": getattr(plot, "name", "") or "",
        "variety": variety,
        "status_db": status_db,
        "status": ui["key"],
        "status_label": ui["label"],
        "fg": ui["fg"],
        "bg": ui["bg"],
        "incidence": inc,
        "incidence_txt": incidence_txt,
        "threshold": threshold,
        "threshold_txt": fmt_threshold(threshold),
        "bar_w": round(bar_w),
        "bar_c": bar_c,
        "location": incident.location_in_plot or "—",
        "since": ddmm(incident.occurrence_date),
        "action": action,
        "action_label": action_label,
        "action_sub": action_sub,
        "late": late,
        "search": _norm(f"{incident.name} {getattr(plot, 'name', '')} {variety}"),
        "panel": {
            "id": incident.id,
            "name": incident.name,
            "category": "Doença" if _norm(incident.category) == "doenca" else (incident.category or "Praga"),
            "plot": getattr(plot, "name", "") or "",
            "variety": variety,
            "status_label": ui["label"],
            "fg": ui["fg"],
            "bg": ui["bg"],
            "incidence_txt": incidence_txt,
            "unit": spec["unit"],
            "noun": spec["noun"],
            "threshold": threshold,
            "threshold_txt": fmt_threshold(threshold),
            "threshold_bottom": round(threshold / hmax * 80) if hist else 0,
            "bars": bars,
            "events": _events_for(incident, spec, ops, today),
            "withdrawal_until": withdrawal_until.strftime("%d/%m/%Y") if withdrawal_until else "",
            "location": incident.location_in_plot or "—",
            "since": incident.occurrence_date.strftime("%d/%m/%Y"),
            "by": incident.created_by or "—",
            "has_photo": bool(incident.photo_data),
            "notes": incident.notes or "",
            "next": next_box,
            "prev_txt": fmt_pct(prev["value"]) if prev else "—",
            "prev_value": prev["value"] if prev else None,
            "prev_label": prev["label"] if prev else "",
            "plot_area": float(getattr(plot, "area_hectares", 0) or 0),
            "sampled": incident.sampled_count,
            "affected": incident.affected_count,
            "location_raw": incident.location_in_plot or "",
            "occurrence_iso": incident.occurrence_date.isoformat(),
            "status": ui["key"],
        },
    }


def build_plot_tiles(plots: list, rows: list[dict[str, Any]], last_round_by_plot: dict[int, date]) -> list[dict[str, Any]]:
    tiles = []
    for plot in plots:
        active = [r for r in rows if r["plot_id"] == plot.id and r["status"] != "controlado"]
        worst = next((r for r in active if r["status"] == "precisa"), None) or next((r for r in active if r["status"] == "controle"), None) or (active[0] if active else None)
        last = last_round_by_plot.get(plot.id)
        tiles.append({
            "id": plot.id,
            "name": plot.name,
            "variety": getattr(getattr(plot, "variety", None), "name", None) or "",
            "area": fmt_num(float(plot.area_hectares or 0), 1) + " ha",
            "label": (f"{len(active)} foco" + ("s" if len(active) > 1 else "")) if active else "Sadio",
            "fg": worst["fg"] if worst else "#1F7A3A",
            "bg": worst["bg"] if worst else "#E6F4E2",
            "detail": " · ".join(f"{r['name']} {r['incidence_txt']}" for r in active) if active else (f"Última ronda {ddmm(last)}" if last else "Sem ronda registrada"),
        })
    return tiles


def build_summary(rows: list[dict[str, Any]], plots: list) -> dict[str, Any]:
    count = lambda k: sum(1 for r in rows if r["status"] == k)  # noqa: E731
    late = sum(1 for r in rows if r["late"])
    active_plots = {r["plot_id"] for r in rows if r["status"] != "controlado"}
    return {
        "segments": [
            {"key": "precisa", "label": "Precisa de controle", "count": count("precisa"), "sub": "acima do nível de controle", "dot": "#B42318"},
            {"key": "controle", "label": "Em controle", "count": count("controle"), "sub": (f"{late} reinspeção atrasada" if late == 1 else f"{late} reinspeções atrasadas") if late else "aguardando reinspeção", "dot": "#1F5FA8"},
            {"key": "observacao", "label": "Em observação", "count": count("observacao"), "sub": "abaixo do nível, vigiando", "dot": "#D18A00"},
            {"key": "controlado", "label": "Controlados", "count": count("controlado"), "sub": "nesta safra", "dot": "#1F7A3A"},
        ],
        "active": sum(1 for r in rows if r["status"] != "controlado"),
        "healthy": len([p for p in plots if p.id not in active_plots]),
        "plots_total": len(plots),
    }


def round_row(rnd, plots_by_id: dict[int, Any]) -> dict[str, Any]:
    ids = [int(x) for x in (rnd.plot_ids or "").split(",") if x.strip().isdigit()]
    keys = [k for k in (rnd.pest_keys or "").split(",") if k.strip()]
    names = [plots_by_id[i].name for i in ids if i in plots_by_id]
    plots_txt = names[0] if len(names) == 1 else (f"{len(names)} talhões" if len(names) > 3 else ", ".join(names))
    if rnd.status != "concluida":
        found, fg, bg = "Planejada", "#5B6660", "#EEF1ED"
    elif rnd.new_count:
        found = f"{rnd.new_count} foco novo" if rnd.new_count == 1 else f"{rnd.new_count} focos novos"
        if rnd.worse_count:
            found += f" · {rnd.worse_count} piorou"
        fg, bg = "#B42318", "#FDECEA"
    elif rnd.worse_count:
        found, fg, bg = f"{rnd.worse_count} foco piorou", "#8A5A00", "#FFF6E5"
    else:
        found, fg, bg = "Sem novidades", "#1F7A3A", "#E6F4E2"
    return {
        "id": rnd.id,
        "date": f"{WEEKDAYS[rnd.round_date.weekday()]} {ddmm(rnd.round_date)}",
        "by": rnd.responsible_name or "—",
        "plots": plots_txt or "—",
        "pests": "1 praga observada" if len(keys) == 1 else f"{len(keys)} pragas observadas",
        "found": found,
        "fg": fg,
        "bg": bg,
        "open": rnd.status != "concluida",
    }
