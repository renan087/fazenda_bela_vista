import json
from datetime import datetime, timezone

from app.core.config import get_settings
from app.models import AgronomicProfile, Farm, Plot, SoilAnalysis


def gerar_recomendacao_adubacao(analise_solo: SoilAnalysis) -> dict:
    settings = get_settings()
    if not settings.openai_api_key:
        return {
            "status": "skipped",
            "recommendation": None,
            "model": None,
            "generated_at": None,
            "error": "OPENAI_API_KEY nao configurada.",
        }

    from openai import OpenAI

    client = OpenAI(api_key=settings.openai_api_key, timeout=settings.openai_timeout_seconds)
    prompt = _build_prompt(analise_solo)
    try:
        response = client.responses.create(
            model=settings.openai_recommendation_model,
            input=prompt,
        )
        recommendation = getattr(response, "output_text", None) or ""
        recommendation = recommendation.strip() or "A IA nao retornou recomendacao textual."
        return {
            "status": "generated",
            "recommendation": recommendation,
            "model": settings.openai_recommendation_model,
            "generated_at": datetime.now(timezone.utc),
            "error": None,
        }
    except Exception as exc:
        return {
            "status": "error",
            "recommendation": None,
            "model": settings.openai_recommendation_model,
            "generated_at": None,
            "error": str(exc),
        }


def _build_prompt(analise_solo: SoilAnalysis) -> str:
    farm: Farm | None = analise_solo.farm
    plot: Plot | None = analise_solo.plot
    profile: AgronomicProfile | None = farm.agronomic_profile if farm else None

    profile_data = {
        "fazenda": farm.name if farm else None,
        "cultura": profile.culture if profile else "Cafe arabica",
        "regiao": profile.region if profile else "Bahia - Brasil",
        "clima": profile.climate if profile else None,
        "tipo_solo": profile.soil_type if profile else None,
        "sistema_irrigacao": profile.irrigation_system if profile else None,
        "espacamento_plantas": profile.plant_spacing if profile else None,
        "espacamento_gotejo": profile.drip_spacing if profile else None,
        "fertilizantes_utilizados": _split_multiline(profile.fertilizers_used if profile else None),
        "fase_lavoura": _split_multiline(profile.crop_stage if profile else None),
        "pragas_comuns": _split_multiline(profile.common_pests if profile else None),
        "setor": plot.name if plot else None,
        "area_setor_ha": float(plot.area_hectares) if plot and plot.area_hectares is not None else None,
    }

    analysis_data = {
        "data_analise": analise_solo.analysis_date.isoformat(),
        "laboratorio": analise_solo.laboratory,
        "ph": _float(analise_solo.ph),
        "materia_organica": _float(analise_solo.organic_matter),
        "fosforo": _float(analise_solo.phosphorus),
        "potassio": _float(analise_solo.potassium),
        "calcio": _float(analise_solo.calcium),
        "magnesio": _float(analise_solo.magnesium),
        "aluminio": _float(analise_solo.aluminum),
        "h_al": _float(analise_solo.h_al),
        "ctc": _float(analise_solo.ctc),
        "saturacao_bases": _float(analise_solo.base_saturation),
        "observacoes": analise_solo.observations,
        "recomendacao_base_calculada": {
            "necessidade_calcario_t_ha": _float(analise_solo.liming_need_t_ha),
            "npk": analise_solo.npk_recommendation,
            "micronutrientes": analise_solo.micronutrient_recommendation,
        },
    }

    return (
        "Voce e um engenheiro agronomo especialista em cafe.\n"
        "Com base na analise de solo, gere recomendacao de adubacao.\n"
        "Considere o perfil agronomico da fazenda e devolva uma resposta objetiva em portugues, "
        "com secoes: Diagnostico, Calagem, Adubacao NPK, Micronutrientes, Manejo via fertirrigacao, Alertas.\n\n"
        f"Perfil agronomico da fazenda:\n{json.dumps(profile_data, ensure_ascii=True, indent=2)}\n\n"
        f"Analise de solo:\n{json.dumps(analysis_data, ensure_ascii=True, indent=2)}"
    )


def _split_multiline(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.replace(",", "\n").splitlines() if item.strip()]


def _float(value):
    return float(value) if value is not None else None
