"""Quadro da Visão geral da Lavoura: situação de cada talhão por pilar (nutrição, sanidade, água, solo)."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from app.services.climate_water import irrigation_mm

PILLARS = ("nutri", "san", "agua", "solo")
PILLAR_NAMES = {"nutri": "Nutrição", "san": "Sanidade", "agua": "Água", "solo": "Solo"}
PILLAR_MODULES = {
    "nutri": ("Adubação", "/fertilizacao"),
    "san": ("Pragas", "/pragas"),
    "agua": ("Clima e Água", "/clima-agua"),
    "solo": ("Análise de solo", "/analise-solo"),
}
RANK = {"err": 3, "warn": 2, "muted": 1, "ok": 0}
ATTENTION = {"err", "warn"}


def _br(d: date | None) -> str:
    return d.strftime("%d/%m") if d else ""


def _days(n: int) -> str:
    return "1 dia" if n == 1 else f"{n} dias"


def _register_links(plot_id: int) -> dict[str, str]:
    return {
        "adub": f"/fertilizacao/nova?plot_id={plot_id}",
        "pulv": f"/pragas/pulverizacoes?novo=1&plot_id={plot_id}",
        "irr": f"/irrigacao?novo=1&plot_id={plot_id}",
        "praga": f"/pragas?novo=1&plot_id={plot_id}",
        "solo": f"/analise-solo?plot_id={plot_id}",
        "trato": f"/pragas/tratos?novo=1&plot_id={plot_id}",
    }


def build_lavoura_board(
    *,
    plots: list,
    today: date,
    fertilizations: list,
    active_schedules: list,
    operations: list,
    active_incidents: list,
    irrigations: list,
    soil_analyses: list,
    water_rows: list[dict],
    soil_rows: list[dict],
    cost_by_plot: dict[int, dict],
) -> dict[str, Any]:
    fert_by_plot: dict[int, list] = defaultdict(list)
    for f in fertilizations:
        fert_by_plot[f.plot_id].append(f)
    sched_by_plot: dict[int, list] = defaultdict(list)
    for s in active_schedules:
        if s.scheduled_date:
            sched_by_plot[s.plot_id].append(s)
    inc_by_plot: dict[int, list] = defaultdict(list)
    for inc in active_incidents:
        inc_by_plot[inc.plot_id].append(inc)
    ops_by_plot: dict[int, list] = defaultdict(list)
    for op in operations:
        ops_by_plot[op.plot_id].append(op)
    irr_by_plot: dict[int, list] = defaultdict(list)
    for irr in irrigations:
        irr_by_plot[irr.plot_id].append(irr)
    soil_by_plot: dict[int, list] = defaultdict(list)
    for a in soil_analyses:
        soil_by_plot[a.plot_id].append(a)
    water = {r["plot_id"]: r for r in water_rows}
    soil = {r["plot_id"]: r for r in soil_rows}

    rows: list[dict[str, Any]] = []
    for p in plots:
        links = _register_links(p.id)
        area = float(p.area_hectares or 0)
        status: dict[str, dict[str, Any]] = {}

        # Nutrição: agenda atrasada > agenda próxima > última adubação
        scheds = sorted(sched_by_plot.get(p.id, []), key=lambda s: s.scheduled_date)
        late = [s for s in scheds if s.scheduled_date < today]
        soon = [s for s in scheds if today <= s.scheduled_date <= today + timedelta(days=7)]
        ferts = sorted(fert_by_plot.get(p.id, []), key=lambda f: (f.application_date or date.min, f.id), reverse=True)
        last_fert = ferts[0] if ferts else None
        if late:
            s0 = late[0]
            status["nutri"] = {
                "tone": "err", "label": f"Atrasada {_days((today - s0.scheduled_date).days)}",
                "why": f"Estava na Agenda para {_br(s0.scheduled_date)}" + (f" · {s0.recipe_name}" if s0.recipe_name else "") + ".",
                "action": "Abrir na Agenda", "href": "/fertilizacao/agendamentos",
            }
        elif soon:
            s0 = soon[0]
            status["nutri"] = {
                "tone": "warn", "label": "Adubar hoje" if s0.scheduled_date == today else f"Adubar até {_br(s0.scheduled_date)}",
                "why": "Programada na Agenda" + (f" · {s0.recipe_name}" if s0.recipe_name else "") + ".",
                "action": "Abrir na Agenda", "href": "/fertilizacao/agendamentos",
            }
        elif last_fert and last_fert.application_date:
            n = (today - last_fert.application_date).days
            status["nutri"] = {"tone": "ok", "label": "Adubado hoje" if n <= 0 else f"Adubado há {_days(n)}"}
        else:
            status["nutri"] = {"tone": "muted", "label": "Sem adubação"}

        # Sanidade: focos ativos
        incs = sorted(inc_by_plot.get(p.id, []), key=lambda i: (int(i.severity or 1), i.occurrence_date or date.min), reverse=True)
        if incs:
            top = incs[0]
            sev = int(top.severity or 1)
            extra = f" +{len(incs) - 1}" if len(incs) > 1 else ""
            status["san"] = {
                "tone": "err" if sev >= 4 else "warn", "label": f"{top.name} nv.{sev}{extra}",
                "why": f"Apontado em {_br(top.occurrence_date)}." + (" Nível alto: planeje a pulverização." if sev >= 4 else " Acompanhe a evolução."),
                "action": "Registrar pulverização" if sev >= 4 else "Ver ocorrência",
                "href": links["pulv"] if sev >= 4 else "/pragas",
            }
        else:
            status["san"] = {"tone": "ok", "label": "Sem focos"}

        # Água: balanço dos últimos dias (só talhões irrigados geram alerta)
        w = water.get(p.id, {})
        w_status = w.get("status", "muted")
        if w_status in ATTENTION:
            status["agua"] = {
                "tone": w_status, "label": w.get("status_label", "Sem registro"),
                "why": f"Última água em {w.get('last_water_br', '—')}." if w.get("last_water_br") not in (None, "—") else "Nenhuma chuva ou irrigação registrada.",
                "action": "Registrar irrigação", "href": links["irr"],
            }
        elif w and not w.get("is_irrigated"):
            status["agua"] = {"tone": "muted", "label": "Sequeiro"}
        else:
            status["agua"] = {"tone": "ok" if w_status == "ok" else "muted", "label": w.get("status_label", "—") if w_status == "ok" else "Sem registro"}
            if w_status == "ok":
                status["agua"]["label"] = "Água em dia"

        # Solo: laudo vigente
        s = soil.get(p.id, {})
        s_status = s.get("status", "missing")
        if s_status == "missing":
            status["solo"] = {
                "tone": "err", "label": "Sem análise",
                "why": "Sem laudo, a recomendação de adubação fica sem base.",
                "action": "Lançar análise", "href": links["solo"],
            }
        elif s_status == "stale":
            status["solo"] = {
                "tone": "warn", "label": f"Laudo de {s.get('latest_date_br', '')[-4:]}",
                "why": "O laudo tem mais de 2 anos. Colete uma amostra nova.",
                "action": "Lançar análise", "href": links["solo"],
            }
        else:
            v = s.get("v_pct")
            status["solo"] = {"tone": "ok", "label": f"V {v:.0f}%".replace(".", ",") if v is not None else "Laudo em dia"}

        # Linha do tempo: o que aconteceu no talhão, de todos os módulos
        events: list[tuple] = []
        for f in ferts[:4]:
            events.append((f.application_date, "nutri", f"Adubação · {f.recipe_name or f.product or 'aplicação'}", "Adubação"))
        for op in sorted(ops_by_plot.get(p.id, []), key=lambda o: (o.operation_date or date.min, o.id), reverse=True)[:4]:
            events.append((op.operation_date, "pulv" if "pulver" in (op.operation_type or "").lower() else "trato", op.operation_type, "Operações"))
        for inc in incs[:3]:
            events.append((inc.occurrence_date, "praga", f"{inc.name} nível {int(inc.severity or 1)} apontado", "Pragas"))
        for irr in sorted(irr_by_plot.get(p.id, []), key=lambda i: (i.irrigation_date or date.min, i.id), reverse=True)[:3]:
            mm = irrigation_mm(float(irr.volume_liters or 0), area)
            events.append((irr.irrigation_date, "agua", f"Irrigação {mm:.0f} mm".replace(".", ","), "Clima e Água"))
        for a in sorted(soil_by_plot.get(p.id, []), key=lambda a: (a.analysis_date or date.min, a.id), reverse=True)[:2]:
            v = f" · V {float(a.base_saturation):.0f}%" if a.base_saturation is not None else ""
            events.append((a.analysis_date, "solo", f"Análise de solo{v}", "Análise de solo"))
        events = [e for e in events if e[0]]
        events.sort(key=lambda e: e[0], reverse=True)

        worst_key = max(PILLARS, key=lambda k: RANK[status[k]["tone"]])
        need = status[worst_key] if status[worst_key]["tone"] in ATTENTION else None
        c = cost_by_plot.get(p.id, {})
        rows.append({
            "id": p.id,
            "name": p.name,
            "variety": p.variety.name if getattr(p, "variety", None) else "",
            "area": area,
            "cost_per_ha": float(c.get("cost_per_hectare", 0.0) or 0.0),
            "status": status,
            "need": ({**need, "pillar": PILLAR_NAMES[worst_key], "key": worst_key} if need else None),
            "score": sum(RANK[status[k]["tone"]] ** 2 for k in PILLARS),
            "attention": [k for k in PILLARS if status[k]["tone"] in ATTENTION],
            "links": links,
            "timeline": [
                {"date": _br(e[0]) if e[0].year == today.year else e[0].strftime("%d/%m/%Y"), "key": e[1], "title": e[2], "module": e[3]}
                for e in events[:4]
            ],
        })
    rows.sort(key=lambda r: (-r["score"], (r["name"] or "").lower()))

    pillars = []
    for k in PILLARS:
        bad = sorted((r for r in rows if r["status"][k]["tone"] in ATTENTION), key=lambda r: -RANK[r["status"][k]["tone"]])
        tone = bad[0]["status"][k]["tone"] if bad else "ok"
        module, href = PILLAR_MODULES[k]
        pillars.append({
            "key": k,
            "name": PILLAR_NAMES[k],
            "module": module,
            "href": href,
            "count": len(bad),
            "ok_count": sum(1 for r in rows if r["status"][k]["tone"] == "ok"),
            "tone": tone,
            "line": (f"{bad[0]['name']}: {bad[0]['status'][k]['label'].lower()}" + (f" · +{len(bad) - 1}" if len(bad) > 1 else "")) if bad else "Tudo em dia",
        })
    return {"rows": rows, "pillars": {p["key"]: p for p in pillars}, "pillar_list": pillars, "attention_count": sum(1 for r in rows if r["attention"])}
