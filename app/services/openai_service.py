import json
from datetime import datetime, timezone
import os

from app.core.config import get_settings
from app.models import AgronomicProfile, Farm, Plot, SoilAnalysis

def gerar_recomendacao_adubacao(analise_solo: SoilAnalysis) -> dict:
    settings = get_settings()
    
    # Puxa a chave e as configurações que você colocou no Render
    api_key = os.getenv("OPENAI_API_KEY") or settings.openai_api_key
    model = os.getenv("OPENAI_RECOMMENDATION_MODEL") or "gpt-4o-mini"
    # Aumentamos o tempo de espera para 60 segundos para evitar o "Timeout"
    timeout = 60 

    if not api_key:
        return {
            "status": "error",
            "recommendation": None,
            "model": None,
            "generated_at": None,
            "error": "OPENAI_API_KEY nao configurada no Render.",
        }

    from openai import OpenAI
    client = OpenAI(api_key=api_key)
    prompt = _build_prompt(analise_solo)

    try:
        # TROCA DO COMANDO: Usando o comando oficial e mais estável da OpenAI
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Você é um engenheiro agrônomo especialista em café."},
                {"role": "user", "content": prompt}
            ],
            timeout=timeout
        )
        
        # Pega o texto da resposta da IA
        recommendation = response.choices[0].message.content.strip()
        
        return {
            "status": "generated",
            "recommendation": recommendation,
            "model": model,
            "generated_at": datetime.now(timezone.utc),
            "error": None,
        }
    except Exception as exc:
        return {
            "status": "error",
            "recommendation": None,
            "model": model,
            "generated_at": None,
            "error": f"Erro na conexão: {str(exc)}",
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
        "recomendacao_base_calculada": {
            "necessidade_calcario_t_ha": _float(analise_solo.liming_need_t_ha),
            "npk": analise_solo.npk_recommendation,
            "micronutrientes": analise_solo.micronutrient_recommendation,
        },
    }

    return (
        "Com base na analise de solo abaixo, gere uma recomendação de adubação detalhada.\n"
        "Devolva a resposta em português, com as seções: Diagnóstico, Calagem, Adubação NPK, "
        "Micronutrientes, Manejo via fertirrigação e Alertas.\n\n"
        f"Perfil agronômico:\n{json.dumps(profile_data, indent=2)}\n\n"
        f"Análise de solo:\n{json.dumps(analysis_data, indent=2)}"
    )

def _split_multiline(value: str | None) -> list[str]:
    if not value: return []
    return [item.strip() for item in value.replace(",", "\n").splitlines() if item.strip()]

def _float(value):
    return float(value) if value is not None else None
