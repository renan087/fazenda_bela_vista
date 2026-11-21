"""Recomendação agronômica: cálculo determinístico + explicação por IA."""
from __future__ import annotations

import json
import logging
import time
from datetime import date

from sqlalchemy.orm import Session

from app.services.agronomy.context import gather
from app.services.agronomy.engine import ENGINE_NAME, VERSION, build_plan, deterministic_summary
from app.services.agronomy.narrative import narrate

logger = logging.getLogger(__name__)


def generate(db: Session, analysis, allowed_pages: set[str], today: date, settings) -> dict:
    started = time.perf_counter()
    try:
        inputs = gather(db, analysis, today, allowed_pages)
        plan = build_plan(inputs)
    finally:
        logging.getLogger("uvicorn.error").info("soil_ai stage=context duration_ms=%.1f", (time.perf_counter() - started) * 1000)
    text = narrate(plan, settings)
    plan["resumo"] = text.get("resumo") or deterministic_summary(plan)
    plan["orientacoes"] = text.get("orientacoes", [])
    for item in plan["corretivos"]:
        item["explicacao"] = text.get("justificativas", {}).get(item["tipo"])
    for item in plan["necessidades"] + plan["micronutrientes"]:
        item["explicacao"] = text.get("justificativas", {}).get(item["nutriente"])
    plan["ia"] = {"usada": bool(text), "modelo": text.get("modelo")}
    model = ENGINE_NAME + (f" + {text['modelo']}" if text else "")
    return {"plan": plan, "json": json.dumps(plan, ensure_ascii=False, allow_nan=False), "model": model}


def read(value: str | None) -> dict | None:
    if not value or not value.lstrip().startswith("{"):
        return None
    try:
        parsed = json.loads(value)
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) and parsed.get("versao") in (1, 2, VERSION) else None
