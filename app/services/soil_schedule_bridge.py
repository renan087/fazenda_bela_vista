"""Ponte entre Análise de Solo (motor agronômico v3) e Agenda de Adubação, mais visão gerencial de fertilidade."""

from __future__ import annotations

import re
import unicodedata
from datetime import date, timedelta
from typing import Any

from app.models import InputCatalog
from app.services.fertilization_views import build_input_stock_metrics
from app.services.forms import create_fertilization_schedule
from app.services.openai_service import read_soil_recommendation
from app.services.soil_measurements import parse_metadata


def _norm(value: str | None) -> str:
    return (
        unicodedata.normalize("NFD", str(value or ""))
        .encode("ascii", "ignore")
        .decode("ascii")
        .strip()
        .lower()
    )


def _tokens(value: str | None) -> set[str]:
    return {t for t in re.split(r"[^a-z0-9]+", _norm(value)) if len(t) >= 2}


PRODUCT_SYNONYMS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("ureia", ("ureia", "urea")),
    ("kcl", ("cloreto de potassio", "kcl")),
    ("map", ("map", "fosfato monoamonico")),
    ("ssp", ("superfosfato simples", "ssp")),
    ("tsp", ("superfosfato triplo", "tsp")),
    ("calcario", ("calcario", "dolomitico", "calcitico")),
    ("gesso", ("gesso", "sulfato de calcio")),
    ("nitrato_calcio", ("nitrato de calcio",)),
    ("sulfato_amonio", ("sulfato de amonio",)),
    ("sulfato_potassio", ("sulfato de potassio",)),
    ("boro", ("acido borico", "boro", "borax")),
    ("zinco", ("sulfato de zinco", "zinco")),
    ("cobre", ("sulfato de cobre", "cobre", "oxicloreto")),
)


def match_catalog_input(product_name: str, inputs_catalog: list[InputCatalog]) -> InputCatalog | None:
    """Encontra o melhor insumo ativo do catálogo correspondente a um produto do plano agronômico."""
    target = _norm(product_name)
    if not target:
        return None
    # 1. Match exato ou substring direta
    for item in inputs_catalog:
        cname = _norm(item.name)
        if cname == target or target in cname or cname in target:
            return item
    # 2. Sinônimos agronômicos
    for _, synonyms in PRODUCT_SYNONYMS:
        if any(s in target for s in synonyms):
            for item in inputs_catalog:
                cname = _norm(item.name)
                if any(s in cname for s in synonyms):
                    return item
    # 3. Sobreposição de tokens relevantes
    t_tokens = _tokens(product_name)
    best_item = None
    best_score = 0
    for item in inputs_catalog:
        score = len(t_tokens & _tokens(item.name))
        if score > best_score and score >= 1:
            best_score = score
            best_item = item
    return best_item


def infer_application_method(event: dict[str, Any]) -> str:
    raw = _norm(f"{event.get('metodo', '')} {event.get('operacao', '')} {event.get('chave', '')}")
    if "fertirrig" in raw or "gotej" in raw:
        return "fertirrigacao"
    if "foliar" in raw or "pulveriz" in raw:
        return "fertilizacao_foliar"
    return "adubacao_solida"


def _ph_tone(val: float | None) -> tuple[str, str]:
    if val is None:
        return "muted", "—"
    if val < 5.2:
        return "err", "Ácido"
    if val < 5.5 or val > 6.8:
        return "warn", "Atenção"
    return "ok", "Ideal"


def _v_tone(val: float | None) -> tuple[str, str]:
    if val is None:
        return "muted", "—"
    if val < 50:
        return "err", "Baixo"
    if val < 60:
        return "warn", "Atenção"
    if val <= 75:
        return "ok", "Ideal"
    return "info", "Alto"


def _depth_label(metadata: dict[str, Any]) -> tuple[str, bool]:
    d_from = metadata.get("depth_from_cm")
    d_to = metadata.get("depth_to_cm")
    if d_from is not None and d_to is not None:
        is_sub = float(d_from) >= 15
        return f"{int(d_from) if float(d_from).is_integer() else d_from}–{int(d_to) if float(d_to).is_integer() else d_to} cm", is_sub
    return "0–20 cm", False


def build_soil_page_context(
    *,
    analyses: list,
    plots: list,
    today: date,
) -> dict[str, Any]:
    """Calcula KPIs, cobertura por talhão e semáforos rápidos de fertilidade."""
    stale_cutoff = today - timedelta(days=730)  # 24 meses
    latest_by_plot: dict[int, Any] = {}
    has_subsoil_by_plot: dict[int, bool] = {}
    analysis_meta: dict[int, dict[str, Any]] = {}
    liming_plot_ids: set[int] = set()

    for item in analyses:
        md = parse_metadata(item.laboratory_metadata)
        depth_lbl, is_sub = _depth_label(md)
        plan = read_soil_recommendation(item.ai_recommendation)
        events = (plan.get("cronograma") or []) if (plan and plan.get("versao") == 3) else []
        ph_val = float(item.ph) if item.ph is not None else None
        v_val = float(item.base_saturation) if item.base_saturation is not None else None
        ph_tone, ph_status = _ph_tone(ph_val)
        v_tone, v_status = _v_tone(v_val)
        needs_liming = bool(
            (item.liming_need_t_ha is not None and float(item.liming_need_t_ha) > 0)
            or (plan and any(c.get("tipo") == "calagem" and c.get("necessario") for c in (plan.get("corretivos") or [])))
            or (v_val is not None and v_val < 55)
        )
        analysis_meta[item.id] = {
            "depth_label": depth_lbl,
            "is_subsoil": is_sub,
            "ph_tone": ph_tone,
            "ph_status": ph_status,
            "v_tone": v_tone,
            "v_status": v_status,
            "has_plan": bool(plan and plan.get("versao") == 3),
            "can_schedule": bool(events),
            "events_count": len(events),
            "needs_liming": needs_liming,
        }
        if item.plot_id:
            if is_sub and item.analysis_date and item.analysis_date >= stale_cutoff:
                has_subsoil_by_plot[item.plot_id] = True
            if not is_sub and item.plot_id not in latest_by_plot:
                latest_by_plot[item.plot_id] = item
                if needs_liming:
                    liming_plot_ids.add(item.plot_id)
            elif item.plot_id not in latest_by_plot:
                latest_by_plot[item.plot_id] = item

    plot_coverage: list[dict[str, Any]] = []
    covered_recent = 0
    v_values: list[float] = []
    for plot in sorted(plots, key=lambda p: (p.name or "").lower()):
        latest = latest_by_plot.get(plot.id)
        if latest and latest.analysis_date:
            age_days = max(0, (today - latest.analysis_date).days)
            age_months = age_days // 30
            is_recent = latest.analysis_date >= stale_cutoff
            status = "ok" if is_recent else "stale"
            if is_recent:
                covered_recent += 1
            if latest.base_saturation is not None:
                v_values.append(float(latest.base_saturation))
            v_tone, v_lbl = _v_tone(float(latest.base_saturation) if latest.base_saturation is not None else None)
            ph_tone, ph_lbl = _ph_tone(float(latest.ph) if latest.ph is not None else None)
            plot_coverage.append(
                {
                    "plot_id": plot.id,
                    "plot_name": plot.name,
                    "area": round(float(plot.area_hectares or 0), 1),
                    "status": status,
                    "latest_id": latest.id,
                    "latest_date_br": latest.analysis_date.strftime("%d/%m/%Y"),
                    "age_months": age_months,
                    "ph": float(latest.ph) if latest.ph is not None else None,
                    "ph_tone": ph_tone,
                    "v_pct": float(latest.base_saturation) if latest.base_saturation is not None else None,
                    "v_tone": v_tone,
                    "v_label": v_lbl,
                    "has_subsoil": bool(has_subsoil_by_plot.get(plot.id)),
                    "needs_liming": plot.id in liming_plot_ids,
                }
            )
        else:
            plot_coverage.append(
                {
                    "plot_id": plot.id,
                    "plot_name": plot.name,
                    "area": round(float(plot.area_hectares or 0), 1),
                    "status": "missing",
                    "latest_id": None,
                    "latest_date_br": None,
                    "age_months": None,
                    "ph": None,
                    "ph_tone": "muted",
                    "v_pct": None,
                    "v_tone": "muted",
                    "v_label": "Sem laudo",
                    "has_subsoil": False,
                    "needs_liming": False,
                }
            )

    total_plots = len(plots)
    avg_v = round(sum(v_values) / len(v_values), 1) if v_values else None
    return {
        "kpi": {
            "total_analyses": len(analyses),
            "covered_plots": covered_recent,
            "total_plots": total_plots,
            "coverage_pct": round((covered_recent / total_plots) * 100) if total_plots else 0,
            "missing_or_stale": max(0, total_plots - covered_recent),
            "avg_v_pct": avg_v,
            "liming_needed": len(liming_plot_ids),
        },
        "plot_coverage": plot_coverage,
        "analysis_meta": analysis_meta,
    }


def build_soil_schedule_preview(
    *,
    analysis,
    inputs_catalog: list[InputCatalog],
    purchased_inputs: list,
    farm_id: int | None,
    today: date,
) -> dict[str, Any]:
    """Prepara o rascunho conferível de agendamentos a partir do cronograma do plano agronômico v3."""
    plan = read_soil_recommendation(analysis.ai_recommendation)
    if not plan or plan.get("versao") != 3:
        return {"ok": False, "error": "Gere primeiro a recomendação agronômica deste laudo.", "events": []}

    area_ha = float(analysis.plot.area_hectares or 0) if analysis.plot else 0.0
    metrics = build_input_stock_metrics(inputs_catalog, purchased_inputs, farm_id=farm_id)
    events_out: list[dict[str, Any]] = []

    for idx, event in enumerate(plan.get("cronograma") or []):
        raw_date = str(event.get("data") or "").strip()
        try:
            parsed = date.fromisoformat(raw_date)
        except ValueError:
            parsed = today + timedelta(days=7 + idx * 15)
        if parsed < today:
            parsed = today + timedelta(days=3 + idx * 7)
        method = infer_application_method(event)
        items_out: list[dict[str, Any]] = []
        for it in event.get("itens") or []:
            prod_name = str(it.get("produto") or "").strip()
            if not prod_name:
                continue
            total_qty = it.get("total_kg")
            unit = "kg"
            if total_qty is None:
                if it.get("kg_ha") is not None and area_ha > 0:
                    total_qty = float(it["kg_ha"]) * area_ha
                elif it.get("quantidade") is not None:
                    total_qty = float(it["quantidade"])
                    unit = str(it.get("unidade") or "kg")
                elif it.get("g_100l") is not None:
                    # Foliar em g/100L -> estimativa prática de 400 L calda/ha
                    calda_l = max(area_ha, 1.0) * 400.0
                    total_qty = (float(it["g_100l"]) * (calda_l / 100.0)) / 1000.0
                    unit = "kg"
            qty_val = round(float(total_qty or 0), 2)
            if qty_val <= 0:
                continue
            matched = match_catalog_input(prod_name, inputs_catalog)
            m = metrics.get(matched.id, {"available": 0.0, "unit": matched.default_unit or unit, "avg_cost": 0.0}) if matched else None
            items_out.append(
                {
                    "recommended_name": prod_name,
                    "input_id": matched.id if matched else None,
                    "input_name": matched.name if matched else prod_name,
                    "unit": (matched.default_unit if matched and matched.default_unit else unit),
                    "quantity": qty_val,
                    "available": round(float(m["available"]), 2) if m else 0.0,
                    "matched": matched is not None,
                }
            )
        if not items_out:
            continue
        events_out.append(
            {
                "index": idx,
                "key": str(event.get("chave") or "adubacao"),
                "operation": str(event.get("operacao") or "Aplicação recomendada"),
                "scheduled_date": parsed.isoformat(),
                "when_label": str(event.get("quando") or parsed.strftime("%d/%m/%Y")),
                "method": method,
                "purpose": str(event.get("finalidade") or ""),
                "items": items_out,
            }
        )

    catalog_options = [
        {
            "id": c.id,
            "name": c.name,
            "unit": c.default_unit or "kg",
            "available": round(float(metrics.get(c.id, {}).get("available") or 0), 2),
        }
        for c in inputs_catalog
    ]
    return {
        "ok": True,
        "analysis_id": analysis.id,
        "plot_id": analysis.plot_id,
        "plot_name": analysis.plot.name if analysis.plot else "Talhão",
        "area_ha": round(area_ha, 2),
        "events": events_out,
        "catalog": catalog_options,
    }


def create_schedules_from_soil_plan(
    *,
    repo,
    analysis,
    season_id: int | None,
    entries: list[dict[str, Any]],
    organization_id: int | None,
) -> int:
    """Confirma e grava os agendamentos de adubação a partir do rascunho revisado pelo produtor."""
    created = 0
    catalog_by_id = {
        c.id: c
        for c in repo.list_input_catalog(item_type="insumo_agricola", organization_id=organization_id)
    }
    for entry in entries:
        if not entry.get("selected", True):
            continue
        raw_date = str(entry.get("scheduled_date") or "").strip()
        if not raw_date:
            continue
        try:
            parsed_date = date.fromisoformat(raw_date)
        except ValueError:
            continue
        method = str(entry.get("method") or "adubacao_solida").strip()
        if method not in {"fertirrigacao", "adubacao_solida", "fertilizacao_foliar"}:
            method = "adubacao_solida"
        operation = str(entry.get("operation") or "Recomendação de solo").strip()
        schedule_items: list[dict[str, Any]] = []
        for it in entry.get("items") or []:
            try:
                qty = round(float(it.get("quantity") or 0), 2)
            except (TypeError, ValueError):
                qty = 0.0
            if qty <= 0:
                continue
            raw_input_id = it.get("input_id")
            input_id = int(raw_input_id) if raw_input_id not in (None, "", "auto") else None
            cat = catalog_by_id.get(input_id) if input_id else None
            if cat is None:
                auto_name = str(it.get("recommended_name") or it.get("input_name") or "").strip()
                if not auto_name:
                    continue
                unit = str(it.get("unit") or "kg").strip() or "kg"
                # Reaproveita se já existir por nome normalizado ou cria no catálogo
                norm_name = _norm(auto_name)
                existing = next((c for c in catalog_by_id.values() if _norm(c.name) == norm_name), None)
                if existing:
                    cat = existing
                else:
                    cat = InputCatalog(
                        organization_id=organization_id,
                        name=auto_name[:120],
                        normalized_name=norm_name[:120],
                        item_type="insumo_agricola",
                        category="Fertilizantes",
                        default_unit=unit[:20],
                        is_active=True,
                    )
                    repo.db.add(cat)
                    repo.db.flush()
                    catalog_by_id[cat.id] = cat
            schedule_items.append(
                {
                    "input_id": cat.id,
                    "unit": cat.default_unit or str(it.get("unit") or "kg"),
                    "quantity": qty,
                }
            )
        if not schedule_items:
            continue
        note_parts = [f"Origem: Análise de solo #{analysis.id}"]
        if entry.get("purpose"):
            note_parts.append(str(entry["purpose"]).strip())
        create_fertilization_schedule(
            repo,
            {
                "plot_id": analysis.plot_id,
                "scheduled_date": parsed_date.isoformat(),
                "season_id": season_id,
                "status": "scheduled",
                "duration_minutes": 45 if method == "fertirrigacao" else None,
                "application_method": method,
                "recipe_name": operation[:160],
                "notes": " · ".join(note_parts),
                "items": schedule_items,
            },
        )
        created += 1
    return created
