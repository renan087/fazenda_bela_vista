"""Cálculos determinísticos: calagem, gessagem, NPK, enxofre e micronutrientes.

Cada função devolve um dicionário com o resultado e o rastro do cálculo (fórmula, entradas,
passos e referência). Nenhum valor é estimado: sem o dado indispensável, a função devolve a
pendência correspondente.
"""
from __future__ import annotations

from app.services.agronomy import references as R
from app.services.agronomy.context import FarmInputs
from app.services.agronomy.soil import (
    Texture, classify, gypsum_by_texture, texture_for, phosphorus_class_arabica, phosphorus_class_conilon, sulfur_class,
    micronutrient_extractor, y_factor,
)

# Limites de segurança: doses acima destes valores indicam erro de dado ou de unidade.
SAFETY_MAX_KG_HA = {"N": 620, "P2O5": 140, "K2O": 600, "S": 120, "B": 3, "Zn": 6, "Cu": 3, "Mn": 15}
SAFETY_MAX_LIME_T_HA = 6.0
SAFETY_MAX_GYPSUM_T_HA = 2.0


def fmt(value, digits=2):
    text = f"{value:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return text.rstrip("0").rstrip(",") if "," in text else text


def trace(formula, entradas, passos, ref, criterio):
    return {"formula": formula, "entradas": entradas, "passos": passos, "referencia": R.ref_info(ref, criterio)}


def pending(dado, motivo, impacto):
    return {"dado": dado, "motivo": motivo, "impacto": impacto}


def _row(rows: tuple, value: float) -> int:
    return next(index for index, limit in enumerate(rows) if value <= limit)


# ---------------------------------------------------------------- calagem

def liming(inputs: FarmInputs, texture: Texture, with_gypsum: bool) -> dict:
    r = inputs.reading
    result = {"tipo": "calagem", "titulo": "Calagem", "necessario": None, "pendencias": [], "alertas": []}
    if r.is_subsurface:
        result["pendencias"].append(pending("Análise de 0–20 cm", "Este laudo é da camada de 20–40 cm.",
                                            "A calagem é calculada pela camada superficial."))
        return result
    if inputs.last_liming and r.sample_date and inputs.last_liming > r.sample_date:
        result["necessario"] = False
        result["justificativa"] = (f"Houve calagem em {inputs.last_liming.strftime('%d/%m/%Y')}, depois da coleta. "
                                   "O laudo não reflete a correção feita.")
        result["pendencias"].append(pending("Nova análise de solo", "A calagem foi feita depois da coleta.",
                                            "Sem nova análise não é seguro calcular outra calagem."))
        return result
    T, V = r.get("ctc"), r.get("base_saturation")
    ca, mg, al, t = r.get("calcium"), r.get("magnesium"), r.get("aluminum"), r.values.get("t_effective")
    if inputs.species == "canephora":
        return _liming_conilon(inputs, result, T, V, ca, mg)
    cfg = R.LIMING_ARABICA
    methods, passos = {}, []
    if T is not None and V is not None:
        methods["V"] = max(0.0, T * (cfg["v_target"] - V) / 100)
        passos.append(f"Saturação por bases: NC = T × (V₂ − V₁) / 100 = {fmt(T)} × ({fmt(cfg['v_target'], 0)} − "
                      f"{fmt(V, 1)}) / 100 = {fmt(methods['V'])} t/ha")
    y, y_origin = y_factor(texture)
    if None not in (ca, mg, al, t) and y is not None:
        ca_part = max(0.0, y * (al - cfg["mt"] * t / 100))
        cd_part = max(0.0, cfg["x_factor"] - (ca + mg))
        methods["Al"] = ca_part + cd_part
        passos.append(f"Al e Ca+Mg: NC = Y[Al − (mt·t/100)] + [X − (Ca+Mg)] = {fmt(y)}[{fmt(al)} − "
                      f"({fmt(cfg['mt'], 0)}×{fmt(t)}/100)] + [{fmt(cfg['x_factor'], 0)} − ({fmt(ca)}+{fmt(mg)})] = "
                      f"{fmt(ca_part)} + {fmt(cd_part)} = {fmt(methods['Al'])} t/ha (Y: {y_origin})")
    if not methods:
        result["pendencias"].append(pending("CTC (T) e saturação por bases (V%)", "Ausentes ou com unidade inválida.",
                                            "Sem esses valores não há como calcular a calagem."))
        return result
    nc = min(methods.values())
    if len(methods) == 2:
        passos.append(f"Adotada a menor dose entre os dois métodos ({fmt(nc)} t/ha), como orienta o Manual (p. 40).")
    pf = cfg["pf_with_gypsum_cm"] if with_gypsum else cfg["pf_established_cm"]
    if inputs.stage is None:
        pf = cfg["pf_new_cm"]
    factor = pf / 20
    passos.append(f"Correção para a profundidade: × PF/20 = × {fmt(pf, 0)}/20"
                  + (" (com gessagem, mantém-se a camada de 0–20 cm)" if with_gypsum else
                     " (lavoura formada, incorporação natural sob a saia)"))
    return _finish_liming(inputs, result, nc, factor, passos, ca, mg,
                          trace("QC = NC × (SC/100) × (PF/20) × (100/PRNT)",
                                {"T": f"{fmt(T)} cmolc/dm³" if T is not None else "—",
                                 "V₁": f"{fmt(V, 1)} %" if V is not None else "—", "V₂": "60 %",
                                 "SC": "100 % da faixa de aplicação", "PF": f"{fmt(pf, 0)} cm"},
                                passos, cfg["ref"], f"Calagem, {cfg['pagina']}"))


def _liming_conilon(inputs, result, T, V, ca, mg):
    cfg = R.LIMING_CONILON
    if T is None or V is None:
        result["pendencias"].append(pending("CTC (T) e saturação por bases (V%)", "Ausentes ou com unidade inválida.",
                                            "Sem esses valores não há como calcular a calagem."))
        return result
    v2 = cfg["v_target_high_yield"] if (inputs.yield_target or 0) > cfg["high_yield_sacks"] else cfg["v_target"]
    p = cfg["p_incorporated"] if inputs.stage is None else cfg["p_surface"]
    nc = max(0.0, T * (v2 - V) / 100)
    passos = [f"NC = T × (V₂ − V₁) / 100 = {fmt(T)} × ({fmt(v2, 0)} − {fmt(V, 1)}) / 100 = {fmt(nc)} t/ha",
              f"V₂ = {fmt(v2, 0)} %" + (" (produtividade acima de 60 sc/ha)" if v2 > cfg["v_target"] else ""),
              f"Fator de profundidade p = {fmt(p, 1)} ({'aplicação superficial, sem incorporação' if p < 1 else 'incorporado a 20 cm'})"]
    return _finish_liming(inputs, result, nc, p, passos, ca, mg,
                          trace("QC = T × (V₂ − V₁) / PRNT × p", {"T": f"{fmt(T)} cmolc/dm³", "V₁": f"{fmt(V, 1)} %",
                                                                   "V₂": f"{fmt(v2, 0)} %", "p": fmt(p, 1)},
                                passos, cfg["ref"], f"Calagem, {cfg['pagina']}"))


def _finish_liming(inputs, result, nc, factor, passos, ca, mg, calc):
    result["calculo"] = calc
    result["nc_t_ha"] = round(nc, 2)
    if nc < 0.1:
        result["necessario"] = False
        result["justificativa"] = "A saturação por bases e os teores de Ca e Mg não pedem calagem."
        return result
    result["necessario"] = True
    lot = max(inputs.lime_lots, key=lambda item: (item.prnt is not None, item.balance), default=None)
    prnt = lot.prnt if lot and lot.prnt else None
    ratio = ca / mg if ca and mg else None
    if mg is not None and (mg <= 0.9 or (ratio and ratio > 5)):
        kind = "Calcário dolomítico (rico em magnésio)"
        why = "o magnésio está abaixo do ideal" if mg <= 0.9 else f"a relação Ca:Mg ({fmt(ratio, 1)}:1) está acima de 5:1"
    elif ratio and ratio < 3:
        kind, why = "Calcário calcítico (rico em cálcio)", f"a relação Ca:Mg ({fmt(ratio, 1)}:1) está abaixo de 3:1"
    else:
        kind, why = "Calcário com relação Ca:Mg entre 3 e 5:1", "a relação Ca:Mg atual está adequada"
    result["produto"] = lot.name if lot else kind
    result["estoque"] = {"ref": lot.ref, "saldo": lot.balance, "unidade": lot.unit} if lot else None
    result["tipo_calcario"] = kind
    base = nc * factor
    passos.append(f"Dose para PRNT 100 %: {fmt(nc)} × {fmt(factor)} = {fmt(base)} t/ha")
    if prnt:
        dose = base * 100 / prnt
        passos.append(f"Ajuste ao PRNT do lote ({fmt(prnt, 0)} %): {fmt(base)} × 100/{fmt(prnt, 0)} = {fmt(dose)} t/ha")
        calc["entradas"]["PRNT"] = f"{fmt(prnt, 0)} % (lote em estoque)"
    else:
        dose = None
        result["pendencias"].append(pending("PRNT do calcário", "Nenhum calcário com PRNT registrado no estoque.",
                                            f"Dose com PRNT 100 %: {fmt(base)} t/ha. Divida por PRNT/100 do produto comprado."))
    result["dose_prnt100_t_ha"] = round(base, 2)
    result["dose_t_ha"] = round(dose, 2) if dose else None
    reference = dose or base
    if reference > SAFETY_MAX_LIME_T_HA:
        result["alertas"].append("Dose acima de 6 t/ha: confira CTC, V% e unidades do laudo antes de aplicar.")
    limit = R.LIMING_CONILON["split_above_t_ha"] if inputs.species == "canephora" else R.LIMING_SPLIT_ABOVE_T_HA
    result["parcelas"] = 2 if reference > limit else 1
    result["g_m2"] = round(reference * 100, 0)
    result["total_t"] = round(reference * inputs.area_ha, 2) if inputs.area_ha else None
    result["metodo"] = "A lanço, em faixa sob a projeção da copa, sem incorporação mecânica."
    result["justificativa"] = (f"Elevar a saturação por bases e fornecer Ca e Mg. Indicado {kind.lower()} porque {why}.")
    return result


# ---------------------------------------------------------------- gessagem

def gypsum(inputs: FarmInputs, texture: Texture) -> dict:
    result = {"tipo": "gessagem", "titulo": "Gessagem", "necessario": None, "pendencias": [], "alertas": []}
    sub = inputs.subsurface or (inputs.reading if inputs.reading.is_subsurface else None)
    if not sub:
        result["pendencias"].append(pending("Análise da camada de 20–40 cm",
                                            "A gessagem é decidida pelo subsolo (Ca, Al e m na camada de 20–40 cm).",
                                            "Sem ela, a necessidade de gesso não pode ser avaliada."))
        return result
    if inputs.last_gypsum and sub.sample_date and inputs.last_gypsum > sub.sample_date:
        result["necessario"] = False
        result["justificativa"] = f"Houve gessagem em {inputs.last_gypsum.strftime('%d/%m/%Y')}, depois da coleta do subsolo."
        return result
    ca, al, m = sub.get("calcium"), sub.get("aluminum"), sub.values.get("m")
    c = R.GYPSUM_CRITERIA
    reasons = []
    if ca is not None and ca <= c["calcium_max"]:
        reasons.append(f"Ca no subsolo = {fmt(ca)} cmolc/dm³ (≤ 0,4)")
    if al is not None and al >= c["aluminum_min"]:
        reasons.append(f"Al no subsolo = {fmt(al)} cmolc/dm³ (≥ 0,5)")
    if m is not None and m >= c["m_min"]:
        reasons.append(f"m no subsolo = {fmt(m, 0)} % (≥ 30 %)")
    if ca is None and al is None:
        result["pendencias"].append(pending("Ca e Al da camada de 20–40 cm", "Ausentes no laudo do subsolo.",
                                            "Sem eles a gessagem não pode ser avaliada."))
        return result
    if not reasons:
        result["necessario"] = False
        result["justificativa"] = "O subsolo (20–40 cm) não apresenta deficiência de cálcio nem excesso de alumínio."
        return result
    T, V = sub.get("ctc"), sub.get("base_saturation")
    passos = ["Critérios atendidos: " + "; ".join(reasons) + "."]
    if inputs.species == "canephora":
        if T is None or V is None:
            result["pendencias"].append(pending("CTC e V% da camada de 20–40 cm", "Necessários para o cálculo do Incaper.",
                                                "A gessagem é indicada, mas a dose não pode ser calculada."))
            result["necessario"] = True
            return result
        qc = max(0.0, T * (R.LIMING_CONILON["v_target"] - V) / 100)
        dose = R.GYPSUM_FRACTION_OF_QC_CONILON * qc
        passos.append(f"QC (20–40 cm) = {fmt(T)} × (60 − {fmt(V, 1)}) / 100 = {fmt(qc)} t/ha; QG = 0,3 × QC = {fmt(dose)} t/ha")
        calc = trace("QG = 0,3 × QC(20–40 cm)", {"T (20–40)": f"{fmt(T)} cmolc/dm³", "V (20–40)": f"{fmt(V, 1)} %"},
                     passos, "incaper_conilon", "Gessagem, p. 428–429")
    else:
        dose, origin = gypsum_by_texture(texture_for(sub, inputs.plot_texture))  # camada de 20–40 cm
        if dose is not None:
            passos.append(f"Necessidade de gesso pela {origin}: {fmt(dose)} t/ha (interpolado).")
            formula = "NG pela tabela de P-rem ou argila"
        elif T is not None and V is not None:
            nc = max(0.0, T * (R.LIMING_ARABICA["v_target"] - V) / 100)
            dose = R.GYPSUM_FRACTION_OF_NC_ARABICA * nc
            passos.append(f"NC (20–40 cm) = {fmt(T)} × (60 − {fmt(V, 1)}) / 100 = {fmt(nc)} t/ha; NG = 25 % de NC = {fmt(dose)} t/ha")
            formula = "NG = 0,25 × NC(20–40 cm)"
        else:
            result["necessario"] = True
            result["pendencias"].append(pending("P-rem, argila ou CTC e V% do subsolo",
                                                "Um deles é necessário para calcular o gesso.",
                                                "A gessagem é indicada, mas a dose não pode ser calculada."))
            return result
        passos.append("QG = NG × (SC/100) × (EC/20), com camada de 20 cm: QG = NG.")
        calc = trace(formula, {"Camada": "20–40 cm"}, passos, "manual_cafe_mg", "Gessagem, p. 32–42")
    result.update({
        "necessario": dose >= 0.05, "calculo": calc, "produto": "Gesso agrícola", "dose_t_ha": round(dose, 2),
        "g_m2": round(dose * 100, 0), "total_t": round(dose * inputs.area_ha, 2) if inputs.area_ha else None,
        "enxofre_kg_ha": round(dose * 1000 * 0.13, 1), "parcelas": 1,
        "metodo": "A lanço, em superfície, junto ou logo após a calagem.",
        "justificativa": "Levar cálcio ao subsolo e reduzir o alumínio tóxico, aprofundando as raízes.",
    })
    if dose > SAFETY_MAX_GYPSUM_T_HA:
        result["alertas"].append("Dose de gesso acima de 2 t/ha: confira o laudo do subsolo.")
    return result


# ---------------------------------------------------------------- NPK

def npk(inputs: FarmInputs, texture: Texture) -> tuple[list, list]:
    """Necessidade anual de N, P2O5 e K2O (kg/ha) e pendências."""
    stage, pendencias = inputs.stage, []
    if inputs.species is None:
        pendencias.append(pending("Espécie do café (arábica ou conilon)", "Não informada na variedade nem na cultura.",
                                  "As tabelas de adubação são diferentes para cada espécie."))
        return [], pendencias
    if stage is None:
        pendencias.append(pending("Data de plantio ou fase da lavoura", "Sem ela não se sabe se a lavoura está em formação ou produção.",
                                  "A adubação não pode ser calculada."))
        return [], pendencias
    if inputs.reading.is_subsurface:
        pendencias.append(pending("Análise de 0–20 cm", "A adubação usa a camada superficial.", "Adubação não calculada."))
        return [], pendencias
    if stage == "producao":
        return _production(inputs, texture, pendencias)
    return _formation(inputs, pendencias)


def _need(nutrient, dose, criterio, ref, formula, entradas, passos, origem="tabela"):
    return {"nutriente": nutrient, "dose_kg_ha": round(dose, 1), "origem": origem,
            "calculo": trace(formula, entradas, passos, ref, criterio)}


def _production(inputs, texture, pendencias):
    r = inputs.reading
    target = inputs.yield_target
    if not target or target <= 0:
        pendencias.append(pending("Produtividade esperada (sc/ha)", "Sem estimativa do setor, meta do perfil ou colheita registrada.",
                                  "N, P e K dependem da produtividade esperada."))
        return [], pendencias
    needs, conilon = [], inputs.species == "canephora"
    passos_yield = [f"Produtividade esperada: {fmt(target, 1)} sc/ha ({inputs.yield_source})."]
    if not conilon and inputs.previous_yield and target < inputs.previous_yield / 2:
        adjusted = (target + inputs.previous_yield) / 2
        passos_yield.append(f"Esperada abaixo da metade da colheita anterior ({fmt(inputs.previous_yield, 1)} sc/ha): "
                            f"usa-se a média, {fmt(adjusted, 1)} sc/ha, para não acentuar a bienalidade (Manual, p. 46).")
        target = adjusted
    rows = R.YIELD_ROWS_CONILON if conilon else R.YIELD_ROWS_ARABICA
    row = _row(rows, target)
    ref = "incaper_conilon" if conilon else "manual_cafe_mg"
    band = _band(rows, row)
    n_dose = (R.N_CONILON if conilon else R.N_ARABICA_NO_LEAF)[row]
    needs.append(_need("N", n_dose, "Tabela 7" if conilon else "Tabela 15 (sem análise foliar)", ref,
                       "N = tabela[produtividade]", {"Produtividade": f"{fmt(target, 1)} sc/ha ({band})"},
                       passos_yield + [f"Faixa {band}: {n_dose} kg/ha/ano de N."]))
    if not conilon:
        pendencias.append(pending("Análise foliar de N (opcional)", "Permite ajustar o N pela coluna de teor foliar (Tabela 15).",
                                  "Sem ela, usa-se a dose de referência sem análise foliar."))
    k = r.get("potassium")
    if k is None:
        pendencias.append(pending("Potássio (K) do laudo em mg/dm³", "Ausente ou com unidade inválida.", "K₂O não calculado."))
    else:
        klass = classify(k, R.K_COFFEE_LIMITS, R.K_COFFEE_CLASSES)
        column = R.K_COFFEE_CLASSES.index(klass)
        dose = (R.K2O_CONILON if conilon else R.K2O_ARABICA)[row][column]
        needs.append(_need("K2O", dose, "Tabela 7" if conilon else "Tabela 15", ref,
                           "K₂O = tabela[produtividade, classe de K]",
                           {"K": f"{fmt(k, 0)} mg/dm³ ({klass.replace('_', ' ')})", "Produtividade": band},
                           [f"K = {fmt(k, 0)} mg/dm³ → classe {klass.replace('_', ' ')}; faixa {band}: {dose} kg/ha/ano de K₂O."]))
    p = r.get("phosphorus")
    extractor = r.methods.get("phosphorus") or "Não informado"
    if p is None:
        pendencias.append(pending("Fósforo (P) do laudo", "Ausente.", "P₂O₅ não calculado."))
    elif extractor not in ("Mehlich-1", "Não informado"):
        pendencias.append(pending("Fósforo por Mehlich-1", f"O laudo usa {extractor}; as tabelas são para Mehlich-1.",
                                  "P₂O₅ não calculado."))
    else:
        klass, how = (phosphorus_class_conilon if conilon else phosphorus_class_arabica)(p, texture)
        if not klass:
            pendencias.append(pending("Argila (%) ou P remanescente (P-rem)", "A classe de fósforo depende da textura do solo.",
                                      "P₂O₅ não calculado. Informe no laudo ou a textura no cadastro do setor."))
        else:
            classes = ("muito_baixo", "baixo", "medio", "alto") if conilon else R.CLASSES5
            dose = (R.P2O5_CONILON if conilon else R.P2O5_ARABICA)[row][classes.index(klass)]
            passos = [f"P = {fmt(p, 1)} mg/dm³ → classe {klass.replace('_', ' ')} ({how}); faixa {band}: {dose} kg/ha/ano de P₂O₅."]
            if extractor == "Não informado":
                passos.append("Extrator não informado no laudo: considerado Mehlich-1 (confira).")
            needs.append(_need("P2O5", dose, "Tabela 8" if conilon else "Tabela 14", ref,
                               "P₂O₅ = tabela[produtividade, classe de P]", {"P": f"{fmt(p, 1)} mg/dm³", "Classe": how}, passos))
    return needs, pendencias


def _band(rows, row):
    low = 0 if row == 0 else rows[row - 1]
    high = rows[row]
    return f"até {fmt(high, 0)} sc/ha" if row == 0 else f"acima de {fmt(low, 0)} sc/ha" if high == float("inf") else f"{fmt(low, 0)}–{fmt(high, 0)} sc/ha"


def _formation(inputs, pendencias):
    conilon, age = inputs.species == "canephora", inputs.age_months
    if not inputs.plants_ha:
        pendencias.append(pending("Número de plantas ou espaçamento", "As doses de formação são por planta.",
                                  "Adubação de formação não calculada."))
        return [], pendencias
    k = inputs.reading.get("potassium")
    needs = []
    plants = inputs.plants_ha
    if inputs.stage == "plantio":
        if not conilon:
            pendencias.append(pending("Recomendação de pós-plantio do arábica",
                                      "As tabelas cadastradas cobrem formação (a partir de 12 meses) e produção.",
                                      "Consulte o responsável técnico para a adubação de pegamento."))
            return [], pendencias
        cfg = R.POST_PLANTING_CONILON
        k2o_g = cfg["K2O_high_k"] if k is not None and k > cfg["k_limit"] else cfg["K2O"]
        for nutrient, grams in (("N", cfg["N"]), ("K2O", k2o_g)):
            total_g = grams * cfg["applications"]
            needs.append(_need(nutrient, total_g * plants / 1000, "Pós-plantio, p. 433", "incaper_conilon",
                               "kg/ha = g/planta × aplicações × plantas/ha ÷ 1000",
                               {"Dose": f"{grams} g/planta × {cfg['applications']} aplicações", "Plantas/ha": fmt(plants, 0)},
                               [f"{grams} g × {cfg['applications']} × {fmt(plants, 0)} ÷ 1000 = {fmt(total_g * plants / 1000, 1)} kg/ha"],
                               origem="por planta"))
        return needs, pendencias
    year = 1 if (age or 0) < 24 else 2
    table = (R.FORMATION_CONILON if conilon else R.FORMATION_ARABICA)[year]
    applications = R.FORMATION_CONILON_APPLICATIONS if conilon else 1
    ref = "incaper_conilon" if conilon else "embrapa_cafe_mg"
    criterio = "Tabela 6 (formação)" if conilon else "Tabela 2 (formação)"
    unit = "g/planta por aplicação" if conilon else "g/cova por ano"
    n_g = table["N"] * applications
    needs.append(_need("N", n_g * plants / 1000, criterio, ref, "kg/ha = g/planta × plantas/ha ÷ 1000",
                       {"Ano de formação": f"{year}º", "Dose": f"{table['N']} {unit}", "Plantas/ha": fmt(plants, 0)},
                       [f"{n_g} g/planta/ano × {fmt(plants, 0)} plantas ÷ 1000 = {fmt(n_g * plants / 1000, 1)} kg/ha de N"],
                       origem="por planta"))
    if not conilon:
        pendencias.append(pending("Intervalo da dose de N na formação",
                                  "A Tabela 2 não explicita se o N é anual ou por aplicação.",
                                  "Confirme com o responsável técnico antes de repetir a dose."))
    if k is None:
        pendencias.append(pending("Potássio (K) do laudo", "Ausente ou com unidade inválida.", "K₂O não calculado."))
    else:
        klass = classify(k, R.K_COFFEE_LIMITS, R.K_COFFEE_CLASSES)
        k_g = table["K2O"][R.K_COFFEE_CLASSES.index(klass)] * applications
        needs.append(_need("K2O", k_g * plants / 1000, criterio, ref, "kg/ha = g/planta × plantas/ha ÷ 1000",
                           {"K": f"{fmt(k, 0)} mg/dm³ ({klass.replace('_', ' ')})", "Plantas/ha": fmt(plants, 0)},
                           [f"{k_g} g/planta/ano × {fmt(plants, 0)} ÷ 1000 = {fmt(k_g * plants / 1000, 1)} kg/ha de K₂O"],
                           origem="por planta"))
    return needs, pendencias


# ---------------------------------------------------------------- enxofre

def sulfur(inputs: FarmInputs, texture: Texture, n_dose: float | None, gypsum_s: float) -> tuple[dict | None, list]:
    if not n_dose and inputs.species != "canephora":
        return None, []
    s = inputs.reading.get("sulfur")
    if inputs.species == "canephora":
        if s is None:
            return None, [pending("Enxofre (S) do laudo", "Sem ele não se sabe se o S está baixo.",
                                  "Dose de enxofre não calculada.")]
        klass = sulfur_class(s, texture)
        if klass not in ("baixo", "muito_baixo"):
            return None, []
        dose = R.S_MIN_CONILON_LOW
        passos = [f"S = {fmt(s, 1)} mg/dm³ (baixo): dose mínima de {fmt(dose, 0)} kg/ha de S."]
        ref, criterio = "embrapa_conilon_pa", "Adubação fosfatada, p. 271"
    else:
        klass = sulfur_class(s, texture) if s is not None else "sem_analise"
        fraction = R.S_FRACTION_OF_N.get(klass)
        if not fraction:
            return None, []
        dose = n_dose * fraction
        passos = [f"S {'= ' + fmt(s, 1) + ' mg/dm³' if s is not None else 'sem análise'} ({klass.replace('_', ' ')}): "
                  f"{'1/8' if fraction > 0.1 else '1/16'} da dose de N = {fmt(n_dose, 0)} × {fmt(fraction, 4)} = {fmt(dose, 1)} kg/ha"]
        ref, criterio = "manual_cafe_mg", "Enxofre, p. 47"
    if gypsum_s:
        passos.append(f"Descontado o S do gesso recomendado ({fmt(gypsum_s, 1)} kg/ha).")
        dose = max(0.0, dose - gypsum_s)
    if dose < 1:
        return None, []
    return _need("S", dose, criterio, ref, "S conforme classe no solo", {"S": f"{fmt(s, 1)} mg/dm³" if s is not None else "sem análise"},
                 passos), []


# ---------------------------------------------------------------- micronutrientes

def micronutrients(inputs: FarmInputs) -> tuple[list, list]:
    if inputs.species is None or inputs.stage is None or inputs.reading.is_subsurface:
        return [], []
    conilon = inputs.species == "canephora"
    table = R.MICROS_CONILON if conilon else R.MICROS_ARABICA
    result, pendencias = [], []
    for key in ("boron", "zinc", "copper", "manganese"):
        value = inputs.reading.get(key)
        cfg = table[key]
        if value is None:
            continue
        extractor = micronutrient_extractor(inputs.reading, key)
        limits = cfg.get(extractor)
        if not limits:
            pendencias.append(pending(f"Extrator de {cfg['nutriente']} no laudo",
                                      f"O laudo indica “{extractor}”; as tabelas exigem o método de extração.",
                                      f"Dose de {cfg['nutriente']} não calculada."))
            continue
        classes = ("baixo", "medio", "alto") if conilon else R.MICRO_CLASSES
        klass = classify(value, limits, classes)
        dose = cfg["doses"][classes.index(klass)]
        if dose <= 0:
            continue
        foliar = not conilon and key in ("zinc", "copper")
        ref = "incaper_conilon" if conilon else "manual_cafe_mg"
        passos = [f"{cfg['nutriente']} = {fmt(value)} mg/dm³ ({extractor}) → classe {klass}: {fmt(dose, 1)} kg/ha de {cfg['nutriente']}."]
        if foliar:
            passos.append("Para Zn e Cu o Manual orienta a via foliar (2 a 3 pulverizações), com calda a 0,3 % (Incaper).")
        item = _need(cfg["nutriente"], dose, "Tabela 9" if conilon else "Tabela 19", ref,
                     "dose = tabela[classe no solo]", {cfg["nutriente"]: f"{fmt(value)} mg/dm³"}, passos)
        item["via"] = "foliar" if foliar else "solo"
        result.append(item)
    return result, pendencias


def check_safety(needs: list) -> list[str]:
    alerts = []
    for need in needs:
        limit = SAFETY_MAX_KG_HA.get(need["nutriente"])
        if limit and need["dose_kg_ha"] > limit:
            alerts.append(f"Dose de {need['nutriente']} ({fmt(need['dose_kg_ha'], 0)} kg/ha) acima do limite de segurança "
                          f"({limit} kg/ha); confira plantas/ha, produtividade e unidades.")
            need["bloqueada"] = True
    return alerts
