"""Ponto de entrada da recomendação agronômica usado pelas rotas e pelo assistente.

O cálculo fica em app.services.agronomy (determinístico e rastreável); a OpenAI só redige
o resumo e as justificativas sobre o plano calculado.
"""
import logging

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.timezone import today_in_app_timezone, utc_now
from app.models import SoilAnalysis
from app.services import agronomy

logger = logging.getLogger(__name__)


def read_soil_recommendation(value: str | None) -> dict | None:
    return agronomy.read(value)


def soil_recommendation_summary(value: str | None, allowed_pages: set[str]) -> str:
    parsed = read_soil_recommendation(value)
    if not parsed:
        return value or ""
    if not set(parsed.get("modulos_utilizados", [])) <= allowed_pages:
        return ""
    if parsed.get("versao") == agronomy.VERSION:
        priorities = [row["titulo"] for row in parsed.get("prioridades", [])]
        return " ".join([parsed.get("resumo", ""), *priorities]).strip()
    return " ".join(parsed.get("diagnostico", []))


def gerar_recomendacao_adubacao(db: Session, analysis: SoilAnalysis, allowed_pages: set[str]) -> dict:
    try:
        result = agronomy.generate(db, analysis, allowed_pages, today_in_app_timezone(), get_settings())
    except Exception as exc:
        logger.warning("Falha na recomendação agronômica %s (%s)", analysis.id, type(exc).__name__, exc_info=True)
        return {"status": "error", "error": "Não foi possível gerar a recomendação nesta tentativa. Confira os dados do laudo."}
    return {"status": "generated", "recommendation": result["json"], "model": result["model"],
            "generated_at": utc_now(), "error": None}
