import json
import os

from app.core.config import get_settings
from app.core.timezone import utc_now
from app.models import AgronomicProfile, Farm, Plot, SoilAnalysis

def gerar_recomendacao_adubacao(analise_solo: SoilAnalysis) -> dict:
    settings = get_settings()
    api_key = os.getenv("OPENAI_API_KEY") or settings.openai_api_key
    model = os.getenv("OPENAI_RECOMMENDATION_MODEL") or "gpt-4o-mini"

    if not api_key:
        return {"status": "error", "error": "OPENAI_API_KEY nao configurada."}

    from openai import OpenAI
    client = OpenAI(api_key=api_key)
    prompt = _build_prompt(analise_solo)

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Você é um Engenheiro Agrônomo sênior, especialista em Cafeicultura de alta precisão. Sua missão é analisar dados químicos e o histórico de manejo para dar recomendações customizadas, evitando redundâncias com o que o produtor já aplicou."},
                {"role": "user", "content": prompt}
            ],
            timeout=60
        )
        return {
            "status": "generated",
            "recommendation": response.choices[0].message.content.strip(),
            "model": model,
            "generated_at": utc_now(),
            "error": None,
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}

def _build_prompt(analise_solo: SoilAnalysis) -> str:
    farm = analise_solo.farm
    plot = analise_solo.plot
    profile = farm.agronomic_profile if farm else None

    # Monta o contexto da fazenda (Onde a IA vai ler o que você já aplicou)
    profile_data = {
        "fazenda": farm.name if farm else None,
        "cultura": profile.culture if profile else "Cafe arabica",
        "cultivar_predominante": profile.cultivar if profile else None,
        "sistema_de_producao": profile.production_system if profile else None,
        "regiao": profile.region if profile else None,
        "clima": profile.climate if profile else None,
        "tipo_de_solo": profile.soil_type if profile else None,
        "fase_e_historico_de_manejo": profile.crop_stage if profile else "Não informado",
        "sistema_irrigacao": profile.irrigation_system if profile else None,
        "espacamento_plantas": profile.plant_spacing if profile else None,
        "espacamento_gotejo": profile.drip_spacing if profile else None,
        "altitude_metros": profile.altitude_meters if profile else None,
        "declividade": profile.terrain_slope if profile else None,
        "exposicao_solar": profile.sun_exposure if profile else None,
        "meta_produtividade_sc_ha": _float(profile.target_productivity_sacks_per_ha) if profile else None,
        "data_ultima_calagem": profile.last_liming_date.isoformat() if profile and profile.last_liming_date else None,
        "data_ultima_gessagem": profile.last_gypsum_date.isoformat() if profile and profile.last_gypsum_date else None,
        "data_ultima_materia_organica": profile.last_organic_matter_date.isoformat() if profile and profile.last_organic_matter_date else None,
        "fertilizantes_e_insumos_ja_utilizados": profile.fertilizers_used if profile else None,
        "historico_de_podas": profile.pruning_history if profile else None,
        "pragas_doencas_recorrentes": profile.common_pests if profile else None,
        "limitacoes_atuais": profile.current_limitations if profile else None,
        "area_setor_ha": float(plot.area_hectares) if plot and plot.area_hectares is not None else None,
    }

    # Monta os dados químicos puros (Sem a recomendação base calculada)
    analysis_data = {
        "ph_agua": _float(analise_solo.ph),
        "materia_organica_g_dm3": _float(analise_solo.organic_matter),
        "p_resina_mg_dm3": _float(analise_solo.phosphorus),
        "k_cmolc_dm3": _float(analise_solo.potassium),
        "ca_cmolc_dm3": _float(analise_solo.calcium),
        "mg_cmolc_dm3": _float(analise_solo.magnesium),
        "al_cmolc_dm3": _float(analise_solo.aluminum),
        "h_al_cmolc_dm3": _float(analise_solo.h_al),
        "v_porcentagem_saturacao_bases": _float(analise_solo.base_saturation),
    }

    return (
        "ATENÇÃO AGRÔNOMO: Analise os dados químicos abaixo e o HISTÓRICO DE MANEJO informado.\n"
        "IMPORTANTE: Se o produtor informou que já aplicou corretivos (como calcário) ou adubos recentemente, "
        "NÃO recomende aplicar novamente o que já foi feito. Ajuste sua recomendação apenas para o que falta.\n\n"
        f"--- HISTÓRICO E PERFIL DA LAVOURA ---\n{json.dumps(profile_data, indent=2, ensure_ascii=False)}\n\n"
        f"--- RESULTADOS DA ANÁLISE QUÍMICA ---\n{json.dumps(analysis_data, indent=2, ensure_ascii=False)}\n\n"
        "Gere a recomendação em português, com: Diagnóstico Realista, Ajustes Necessários (se houver), "
        "Plano de Adubação de Cobertura/Manutenção e Alertas de Manejo."
    )

def _float(value):
    return float(value) if value is not None else None
