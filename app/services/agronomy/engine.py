"""Motor de recomendação agronômica do SiSFarm (versão 3).

Fluxo: dados do sistema → leitura e validação do laudo → diagnóstico → cálculos determinísticos
(calagem, gessagem, NPK, S, micronutrientes) → desconto do manejo do ciclo → conversão em
produtos → cronograma → verificações de segurança e compatibilidade. A IA só redige o resumo
e as justificativas sobre o plano já calculado (ver narrative.py).
"""
from __future__ import annotations

from datetime import date, timedelta

from app.services.agronomy import calc, references as R
from app.services.agronomy.context import FarmInputs
from app.services.agronomy.products import GENERIC, NUTRIENT_LABELS, Source, fit_npk
from app.services.agronomy.schedule import (
    compatibility, dry_window, foliar_dates, label, remaining_dates, split_plan,
)
from app.services.agronomy.soil import interpret, texture_for

VERSION = 3
ENGINE_NAME = "Motor agronômico SiSFarm 3.0"
STAGE_LABELS = {"plantio": "Pós-plantio", "formacao": "Formação", "producao": "Produção"}
SPECIES_LABELS = {"arabica": "Café arábica", "canephora": "Café conilon/robusta"}
IRRIGATION_LABELS = {"sequeiro": "Sequeiro", "gotejo": "Gotejamento", "aspersor": "Aspersão"}
fmt = calc.fmt


def _round(value, digits=1):
    return round(value, digits) if value is not None else None


def _scope(inputs: FarmInputs, texture) -> dict:
    r = inputs.reading
    return {
        "fazenda": inputs.farm_name, "setor": inputs.plot_name, "area_ha": inputs.area_ha,
        "cultura": SPECIES_LABELS.get(inputs.species, inputs.culture), "especie": inputs.species,
        "especie_origem": inputs.species_source,
        "fase": inputs.stage, "fase_label": STAGE_LABELS.get(inputs.stage, "Não informada"),
        "fase_origem": inputs.stage_source, "idade_meses": inputs.age_months,
        "plantas_ha": _round(inputs.plants_ha, 0), "plantas_origem": inputs.plants_source,
        "irrigacao": inputs.irrigation, "irrigacao_label": IRRIGATION_LABELS[inputs.irrigation],
        "produtividade_sc_ha": _round(inputs.yield_target), "produtividade_origem": inputs.yield_source,
        "textura": texture.source or "não informada", "regiao": inputs.region,
        "amostra": {"data": r.sample_date.isoformat() if r.sample_date else None, "laboratorio": r.laboratory,
                    "profundidade": f"{fmt(r.depth[0], 0)}–{fmt(r.depth[1], 0)} cm" if r.depth else "não informada"},
        "subsolo": bool(inputs.subsurface),
    }


def _diagnosis(indicators: list, inputs: FarmInputs, corrections: dict) -> list[dict]:
    """Achados priorizados a partir dos indicadores classificados."""
    by_id = {row["id"]: row for row in indicators}
    findings = []

    def add(key, level, title, text, ids):
        findings.append({"id": key, "nivel": level, "titulo": title, "texto": text, "indicadores": ids})

    acid = [i for i in ("ph", "base_saturation", "aluminum", "m") if by_id.get(i, {}).get("status") in ("critico", "baixo")]
    if acid or (corrections.get("calagem") or {}).get("necessario"):
        values = ", ".join(f"{by_id[i]['nome']} {fmt(by_id[i]['valor'])}{by_id[i]['unidade'] and ' ' + by_id[i]['unidade']}"
                           for i in acid)
        add("acidez", "critico" if any(by_id[i]["status"] == "critico" for i in acid) else "atencao",
            "Acidez do solo limita a absorção de nutrientes",
            f"{values or 'Saturação por bases abaixo do alvo'}. A correção da acidez vem antes de qualquer adubação: "
            "sem ela, parte do adubo aplicado não é aproveitada.", acid)
    for key, title, why in (
        ("calcium", "Cálcio baixo", "o cálcio forma raízes novas e dá firmeza aos frutos"),
        ("magnesium", "Magnésio baixo", "o magnésio é essencial para a fotossíntese e o enchimento dos grãos"),
        ("potassium", "Potássio baixo", "o potássio responde pelo enchimento e pela qualidade dos grãos"),
        ("phosphorus", "Fósforo baixo", "o fósforo sustenta raízes e florada"),
        ("sulfur", "Enxofre baixo", "o enxofre acompanha o nitrogênio na formação de proteínas"),
        ("boron", "Boro baixo", "o boro é decisivo para a florada e o pegamento"),
        ("zinc", "Zinco baixo", "o zinco regula o crescimento dos ramos e a florada"),
        ("copper", "Cobre baixo", "o cobre atua no metabolismo e na resistência a doenças"),
        ("manganese", "Manganês baixo", "o manganês participa da fotossíntese"),
    ):
        row = by_id.get(key)
        if row and row["status"] in ("critico", "baixo"):
            add(key, "critico" if row["status"] == "critico" else "atencao", title,
                f"{row['nome']} = {fmt(row['valor'])} {row['unidade']} ({row['classe'].lower()}): {why}.", [key])
    ca, mg = by_id.get("calcium"), by_id.get("magnesium")
    if ca and mg and mg["valor"] > 0:
        ratio = ca["valor"] / mg["valor"]
        if not 3 <= ratio <= 5:
            add("ca_mg", "atencao", "Relação Ca:Mg fora do ideal",
                f"Relação Ca:Mg de {fmt(ratio, 1)}:1 (ideal 3 a 5:1). O tipo de calcário foi escolhido para corrigir.",
                ["calcium", "magnesium"])
    k, mg_row = by_id.get("potassium"), by_id.get("magnesium")
    if k and k["status"] == "alto" and mg_row and mg_row["status"] in ("critico", "baixo", "atencao"):
        add("k_mg", "atencao", "Excesso de potássio pode induzir falta de magnésio",
            "Potássio alto com magnésio abaixo do ideal: a dose de K₂O segue a tabela e o calcário deve repor Mg.",
            ["potassium", "magnesium"])
    om = by_id.get("organic_matter")
    if om and om["status"] in ("critico", "baixo"):
        add("mo", "atencao", "Matéria orgânica baixa",
            "Solo com pouca matéria orgânica retém menos nutrientes e água: mantenha a palhada e considere adubação orgânica.",
            ["organic_matter"])
    if inputs.subsurface or inputs.reading.is_subsurface:
        gyp = corrections.get("gessagem") or {}
        if gyp.get("necessario"):
            add("subsolo", "atencao", "Subsolo com restrição às raízes", gyp["calculo"]["passos"][0], [])
    if not findings:
        add("equilibrio", "ok", "Fertilidade equilibrada",
            "Os indicadores do laudo estão nas faixas adequadas. A adubação de manutenção repõe o que a lavoura exporta.", [])
    order = {"critico": 0, "atencao": 1, "ok": 2}
    return sorted(findings, key=lambda item: order[item["nivel"]])


def _history(inputs: FarmInputs) -> dict:
    applied, scheduled, uncounted = {}, {}, []
    for target, rows in ((applied, inputs.applications), (scheduled, inputs.scheduled)):
        for row in rows:
            for nutrient, value in row.nutrients.items():
                target[nutrient] = target.get(nutrient, 0.0) + value
            uncounted.extend(row.uncounted)
    return {"aplicado": applied, "agendado": scheduled, "nao_contabilizados": sorted(set(uncounted))}


def _product_entry(allocation_source: Source, kg_ha: float, inputs: FarmInputs, stock=None, finalidade=""):
    return {
        "produto": allocation_source.name, "fonte": allocation_source.key if ":" not in allocation_source.key else "estoque",
        "garantia": {NUTRIENT_LABELS[k]: v for k, v in allocation_source.guarantee.items()},
        "origem": "estoque" if stock else "sugerido",
        "estoque": {"ref": stock.ref, "saldo": stock.balance, "unidade": stock.unit, "garantia_origem": stock.origin} if stock else None,
        "kg_ha": round(kg_ha, 1), "g_planta": _round(inputs.per_plant_grams(kg_ha)),
        "total_kg": _round(inputs.total_kg(kg_ha)), "finalidade": finalidade,
        "nota": allocation_source.note,
    }


def _stock_status(product: dict) -> None:
    stock = product.get("estoque")
    if not stock or product.get("total_kg") is None:
        return
    factor = {"kg": 1, "t": 1000, "g": 0.001}.get((stock["unidade"] or "").casefold())
    if not factor:
        stock["situacao"] = "Unidade do estoque diferente de massa: confira o saldo."
        return
    balance = stock["saldo"] * factor
    stock["situacao"] = ("Saldo cobre a quantidade do setor." if balance >= product["total_kg"]
                         else f"Faltam {fmt(product['total_kg'] - balance, 0)} kg para cobrir o setor.")


def build_plan(inputs: FarmInputs) -> dict:
    texture = texture_for(inputs.reading, inputs.plot_texture)
    species = inputs.species or "arabica"
    indicators = interpret(inputs.reading, texture, species)
    pendencias, alertas, premissas = [], list(inputs.reading.issues), []
    if inputs.species_source and inputs.species:
        premissas.append(f"Espécie: {SPECIES_LABELS[inputs.species].lower()} ({inputs.species_source}).")
    if inputs.yield_source:
        premissas.append(f"Produtividade: {fmt(inputs.yield_target, 1)} sc/ha ({inputs.yield_source}).")
    if inputs.stage_source:
        premissas.append(f"Fase: {STAGE_LABELS[inputs.stage].lower()} ({inputs.stage_source}).")
    if texture.source:
        premissas.append(f"Textura do solo: {texture.source}.")

    result = {"versao": VERSION, "motor": ENGINE_NAME, "dados_em": inputs.today.isoformat(),
              "escopo": _scope(inputs, texture), "indicadores": indicators, "modulos_utilizados": sorted(inputs.modules)}
    if not inputs.is_coffee:
        pendencias.append(calc.pending("Cultura café", f"A cultura do setor é “{inputs.culture}”.",
                                       "O motor calcula adubação apenas para café; o diagnóstico do solo segue abaixo."))

    # 1. Corretivos
    gyp = calc.gypsum(inputs, texture) if inputs.is_coffee else {"necessario": None, "pendencias": [], "alertas": []}
    lime = calc.liming(inputs, texture, bool(gyp.get("necessario"))) if inputs.is_coffee else {"necessario": None, "pendencias": [], "alertas": []}
    corrections = {"calagem": lime, "gessagem": gyp}
    for item in (lime, gyp):
        pendencias += item.pop("pendencias", [])
        alertas += item.pop("alertas", [])

    # 2. Necessidades de nutrientes
    needs, npk_pending = calc.npk(inputs, texture) if inputs.is_coffee else ([], [])
    pendencias += npk_pending
    n_dose = next((need["dose_kg_ha"] for need in needs if need["nutriente"] == "N"), None)
    s_need, s_pending = calc.sulfur(inputs, texture, n_dose, gyp.get("enxofre_kg_ha") or 0) if needs else (None, [])
    pendencias += s_pending
    if s_need:
        needs.append(s_need)
    micros, micro_pending = calc.micronutrients(inputs) if inputs.is_coffee else ([], [])
    pendencias += micro_pending
    alertas += calc.check_safety(needs + micros)
    needs = [need for need in needs if not need.get("bloqueada")]
    micros = [need for need in micros if not need.get("bloqueada")]

    # 3. Desconto do manejo do ciclo
    history = _history(inputs)
    for need in needs:
        nutrient = need["nutriente"]
        need["label"] = NUTRIENT_LABELS[nutrient]
        need["aplicado_kg_ha"] = round(history["aplicado"].get(nutrient, 0.0), 1)
        need["agendado_kg_ha"] = round(history["agendado"].get(nutrient, 0.0), 1)
        need["saldo_kg_ha"] = round(max(0.0, need["dose_kg_ha"] - need["aplicado_kg_ha"] - need["agendado_kg_ha"]), 1)
    for need in micros:
        need["label"] = need["nutriente"]
        need["saldo_kg_ha"] = need["dose_kg_ha"]
    if history["nao_contabilizados"]:
        alertas.append("Produtos aplicados ou agendados sem garantia identificável não foram descontados: "
                       + ", ".join(history["nao_contabilizados"][:6]) + ". Registre a fórmula (ex.: 20-05-20) no nome ou no estoque.")
    if not inputs.history_available:
        alertas.append("Sem acesso às aplicações ou à agenda: o saldo considera a dose anual completa.")

    # 4. Produtos e cronograma
    saldo = {need["nutriente"]: need["saldo_kg_ha"] for need in needs}
    fertigation = inputs.irrigation == "gotejo"
    allocations, product_notes = fit_npk(saldo, inputs.stock, fertigation=fertigation)
    premissas += product_notes
    events, products = _events(inputs, allocations, lime, gyp, micros, n_dose or 0)
    for product in products:
        _stock_status(product)
    alertas += _supply_check(allocations, saldo)

    result.update({
        "diagnostico": _diagnosis(indicators, inputs, corrections),
        "corretivos": [item for item in (lime, gyp) if item.get("necessario") is not None or item.get("calculo")],
        "necessidades": needs, "micronutrientes": micros, "produtos": products,
        "cronograma": [{k: v for k, v in event.items() if k != "data_obj"} for event in events],
        "compatibilidade": compatibility(events, corrections),
        "historico": {
            "desde": inputs.cycle_start.isoformat(), "aplicacoes": len(inputs.applications),
            "agendamentos": len(inputs.scheduled),
            "aplicado_kg_ha": {NUTRIENT_LABELS[k]: round(v, 1) for k, v in history["aplicado"].items() if k in NUTRIENT_LABELS},
            "agendado_kg_ha": {NUTRIENT_LABELS[k]: round(v, 1) for k, v in history["agendado"].items() if k in NUTRIENT_LABELS},
        },
        "pendencias": _unique(pendencias, key="dado"), "alertas": list(dict.fromkeys(alertas)),
        "premissas": list(dict.fromkeys(premissas)),
        "referencias": _references_used(needs, micros, lime, gyp),
    })
    result["prioridades"] = _priorities(result)
    return result


def _unique(rows, key):
    seen, output = set(), []
    for row in rows:
        if row[key] not in seen:
            seen.add(row[key])
            output.append(row)
    return output


def _supply_check(allocations, saldo) -> list[str]:
    supplied = {}
    for allocation in allocations:
        for nutrient, amount in allocation.supplies().items():
            supplied[nutrient] = supplied.get(nutrient, 0.0) + amount
    alerts = []
    for nutrient in ("N", "P2O5", "K2O"):
        need = saldo.get(nutrient, 0.0)
        if supplied.get(nutrient, 0.0) > need * 1.05 + 0.5:
            alerts.append(f"Os produtos fornecem mais {NUTRIENT_LABELS[nutrient]} que a necessidade; revise a combinação.")
    return alerts


def _event(day: date, key: str, operation: str, method: str, items: list, purpose: str, note: str = "") -> dict:
    return {"data": day.isoformat(), "data_obj": day, "quando": label(day), "chave": key, "operacao": operation,
            "metodo": method, "itens": items, "finalidade": purpose, "observacao": note, "status": "planejado"}


def _events(inputs: FarmInputs, allocations, lime, gyp, micros, n_total) -> tuple[list, list]:
    events, totals = [], {}

    def item(source: Source, kg_ha, stock=None, purpose=""):
        entry = _product_entry(source, kg_ha, inputs, stock, purpose)
        key = source.key
        if key in totals:
            totals[key]["kg_ha"] = round(totals[key]["kg_ha"] + kg_ha, 1)
            for field in ("g_planta", "total_kg"):
                if totals[key][field] is not None and entry[field] is not None:
                    totals[key][field] = round(totals[key][field] + entry[field], 1)
        else:
            totals[key] = dict(entry)
        return entry

    if lime.get("necessario"):
        day = dry_window(inputs.today)
        dose = lime.get("dose_t_ha") or lime.get("dose_prnt100_t_ha")
        parts = lime.get("parcelas", 1)
        for index in range(parts):
            when = day if index == 0 else day.replace(year=day.year + 1)
            kg_ha = dose * 1000 / parts
            lot = next((lot for lot in inputs.lime_lots if lime.get("estoque") and lot.ref == lime["estoque"]["ref"]), None)
            source = Source(lot.ref if lot else "calcario", lime["produto"], {})
            entry = item(source, kg_ha, lot, "Correção da acidez e fornecimento de Ca e Mg")
            entry["g_m2"] = round(kg_ha / 10, 0)
            note = "" if lime.get("dose_t_ha") else "Dose para PRNT 100 %: ajuste ao PRNT do calcário comprado."
            if parts > 1:
                note = (note + " " if note else "") + f"Parcela {index + 1} de {parts}: intervalo mínimo de 6 meses."
            events.append(_event(when, "calagem", "Calagem", lime["metodo"], [entry],
                                 "Corrigir a acidez antes das adubações (mínimo de 60 dias).", note))
    if gyp.get("necessario") and gyp.get("dose_t_ha"):
        day = dry_window(inputs.today)
        entry = item(GENERIC["gesso"], gyp["dose_t_ha"] * 1000, None, "Cálcio e enxofre no subsolo")
        entry["g_m2"] = gyp["g_m2"]
        events.append(_event(day, "gessagem", "Gessagem", gyp["metodo"], [entry],
                             "Melhorar o ambiente das raízes no subsolo.", "Pode ir junto ou logo após o calcário."))

    # Fósforo de uma vez, localizado, na 1ª parcela (Manual p. 47; Incaper Tab. 8), mesmo vindo do MAP.
    p_only = [a for a in allocations if "Fósforo" in a.roles]
    n_k = [a for a in allocations if a not in p_only]
    month_days, fractions, split_note = split_plan(inputs.species or "arabica", inputs.irrigation, n_total)
    dates, redistributed, next_cycle = remaining_dates(inputs.cycle_start, inputs.today, month_days)
    if len(dates) != len(fractions):
        fractions = fractions[-len(dates):] if len(dates) < len(fractions) else [1 / len(dates)] * len(dates)
        total = sum(fractions)
        fractions = [value / total for value in fractions]
    lime_days = [event["data_obj"] for event in events if event["chave"] == "calagem"]
    if lime_days and not next_cycle:
        earliest = lime_days[0] + timedelta(days=R.LIMING_MIN_DAYS_BEFORE_FERTILIZATION)
        shift = (earliest - dates[0]).days
        if 0 < shift <= 45:  # calcário na seca: adia a 1ª parcela para manter 60 dias de intervalo
            dates = [day + timedelta(days=shift) for day in dates]
    fertigation = inputs.irrigation == "gotejo"
    for index, (day, fraction) in enumerate(zip(dates, fractions)):
        items = [item(a.source, a.kg_ha * fraction, a.stock, ", ".join(a.roles)) for a in n_k]
        if index == 0:
            items += [item(a.source, a.kg_ha, a.stock, ", ".join(a.roles)) for a in p_only]
            items += [item(GENERIC["acido_borico" if m["nutriente"] == "B" else "sulfato_manganes" if m["nutriente"] == "Mn"
                                   else "sulfato_zinco" if m["nutriente"] == "Zn" else "sulfato_cobre"],
                           m["dose_kg_ha"] / (_micro_source(m["nutriente"]).guarantee[m["nutriente"]] / 100), None,
                           f"{m['nutriente']} via solo") for m in micros if m.get("via") == "solo"]
        if not items:
            continue
        method = ("Fertirrigação (fontes solúveis); fósforo e micronutrientes via solo na 1ª parcela"
                  if fertigation else "A lanço sob a saia do cafeeiro, dos dois lados da planta; fósforo localizado")
        note = split_note if index == 0 else ""
        if index == 0 and redistributed:
            note += " As parcelas já vencidas neste ciclo foram redistribuídas nas datas restantes."
        if index == 0 and next_cycle:
            note += " O período de adubação deste ciclo terminou: plano para o próximo ciclo."
        events.append(_event(day, "adubacao", f"Adubação — parcela {index + 1} de {len(dates)}", method, items,
                             "Repor N, P e K conforme a produtividade esperada e o laudo.", note.strip()))
    foliar = [m for m in micros if m.get("via") == "foliar"]
    if foliar:
        sprays = foliar_dates(inputs.cycle_start, inputs.today)
        for m in foliar:
            source = _micro_source(m["nutriente"])
            totals[source.key] = {"produto": source.name, "fonte": source.key, "garantia": {m["nutriente"]: source.guarantee[m["nutriente"]]},
                                  "origem": "sugerido", "estoque": None, "kg_ha": None, "g_planta": None, "total_kg": None,
                                  "dose_texto": f"300 g por 100 L de calda, {len(sprays)} pulverizações",
                                  "finalidade": f"Correção foliar de {m['nutriente']}", "nota": ""}
        for index, day in enumerate(sprays):
            items = []
            for m in foliar:
                source = _micro_source(m["nutriente"])
                grams = R.FOLIAR_CONCENTRATION_PCT * 1000 / 100 * 100  # g por 100 L de calda
                items.append({"produto": source.name, "fonte": source.key, "concentracao": f"{fmt(R.FOLIAR_CONCENTRATION_PCT, 1)} %",
                              "g_100l": grams, "finalidade": f"Correção de {m['nutriente']}", "garantia": {}})
            items.append({"produto": "Cloreto de potássio", "fonte": "kcl", "concentracao": "0,3 %", "g_100l": 300.0,
                          "finalidade": "Melhora a absorção do zinco", "garantia": {}})
            events.append(_event(day, "foliar", f"Pulverização foliar {index + 1} de {len(sprays)}",
                                 "Pulverização foliar (calda)", items, "Corrigir zinco e cobre pela folha.",
                                 "Calda a 0,3 % de cada sal (Incaper, p. 435); intervalo de cerca de 60 dias."))
    for row in inputs.scheduled:
        events.append({"data": row.when.isoformat(), "data_obj": row.when, "quando": label(row.when), "chave": "agendado",
                       "operacao": "Aplicação já agendada", "metodo": row.method, "status": "agendado",
                       "itens": [{"produto": p["produto"], "quantidade": p["quantidade"], "unidade": p["unidade"]}
                                 for p in row.products],
                       "finalidade": "Já descontada do saldo do ciclo.", "observacao": ""})
    events.sort(key=lambda event: event["data_obj"])
    for index, event in enumerate(events, start=1):
        event["ordem"] = index
    products = list(totals.values())
    for product in products:
        product.pop("g_m2", None)
    return events, products


def _micro_source(nutrient: str) -> Source:
    return GENERIC[{"B": "acido_borico", "Zn": "sulfato_zinco", "Cu": "sulfato_cobre", "Mn": "sulfato_manganes"}[nutrient]]


def _references_used(needs, micros, lime, gyp) -> list[dict]:
    codes = []
    for item in [*needs, *micros, lime, gyp]:
        ref = (item.get("calculo") or {}).get("referencia")
        if ref and ref["id"] not in codes:
            codes.append(ref["id"])
    return [{"id": code, **{k: R.REFERENCES[code][k] for k in ("titulo", "url", "escopo")}} for code in codes]


def _priorities(result: dict) -> list[dict]:
    """Até 4 ações, em ordem: corrigir o solo antes de adubar. Cada uma traz quando começa."""
    events = result["cronograma"]

    def when(*keys):
        dates = [e for e in events if e["chave"] in keys]
        if not dates:
            return ""
        first, last = dates[0]["quando"].rsplit("/", 1)[0], dates[-1]["quando"].rsplit("/", 1)[0]
        return first if first == last else f"{first.split('/')[1]}–{last.split('/')[1]}"

    output = []
    lime = next((c for c in result["corretivos"] if c["tipo"] == "calagem"), {})
    gyp = next((c for c in result["corretivos"] if c["tipo"] == "gessagem"), {})
    if lime.get("necessario"):
        dose = lime.get("dose_t_ha") or lime.get("dose_prnt100_t_ha")
        total = f" · {fmt(dose * result['escopo']['area_ha'], 1)} t no setor" if result["escopo"].get("area_ha") else ""
        output.append({"titulo": f"Calagem: {fmt(dose)} t/ha de {lime['tipo_calcario'].split(' (')[0].lower()}"
                                 + ("" if lime.get("dose_t_ha") else " (PRNT 100 %)"),
                       "detalhe": f"{fmt(lime['g_m2'], 0)} g/m² na faixa da saia{total}", "quando": when("calagem")})
    if gyp.get("necessario") and gyp.get("dose_t_ha"):
        output.append({"titulo": f"Gessagem: {fmt(gyp['dose_t_ha'])} t/ha de gesso agrícola",
                       "detalhe": "Junto ou logo após o calcário, para melhorar o subsolo", "quando": when("gessagem")})
    npk = [n for n in result["necessidades"] if n["nutriente"] in ("N", "P2O5", "K2O") and n["saldo_kg_ha"] > 0]
    if npk:
        parcels = len([e for e in events if e["chave"] == "adubacao"])
        output.append({"titulo": "Adubar " + " · ".join(f"{fmt(n['saldo_kg_ha'], 0)} {n['label']}" for n in npk) + " (kg/ha)",
                       "detalhe": f"Em {parcels} parcela{'s' if parcels != 1 else ''}" + ("; fósforo todo na 1ª" if any(
                           n["nutriente"] == "P2O5" for n in npk) else ""), "quando": when("adubacao")})
    if result["micronutrientes"]:
        names = {"B": "boro", "Zn": "zinco", "Cu": "cobre", "Mn": "manganês"}
        soil = [names[m["nutriente"]] for m in result["micronutrientes"] if m.get("via") != "foliar"]
        leaf = [names[m["nutriente"]] for m in result["micronutrientes"] if m.get("via") == "foliar"]
        detail = "; ".join(filter(None, [soil and ", ".join(soil) + " no solo", leaf and ", ".join(leaf) + " pela folha"]))
        output.append({"titulo": "Repor " + ", ".join(names[m["nutriente"]] for m in result["micronutrientes"]),
                       "detalhe": detail.capitalize(), "quando": when("adubacao", "foliar")})
    if not output and result["pendencias"]:
        output.append({"titulo": "Completar os dados indicados", "detalhe": "Eles liberam o cálculo das doses.", "quando": ""})
    return output[:4]


def deterministic_summary(result: dict) -> str:
    scope = result["escopo"]
    critical = [f["titulo"].lower() for f in result["diagnostico"] if f["nivel"] == "critico"]
    attention = [f["titulo"].lower() for f in result["diagnostico"] if f["nivel"] == "atencao"]
    parts = [f"{scope['cultura']} em {scope['fase_label'].lower()} no setor {scope['setor']}."]
    if critical:
        parts.append("Pontos críticos: " + "; ".join(critical) + ".")
    if attention:
        parts.append("Pontos de atenção: " + "; ".join(attention[:4]) + ".")
    if not critical and not attention:
        parts.append("O solo está equilibrado; o plano repõe o que a lavoura exporta.")
    if result["pendencias"]:
        parts.append(f"{len(result['pendencias'])} dado(s) a completar para fechar o plano com segurança.")
    return " ".join(parts)
