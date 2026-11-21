"""Leitura, validação de unidades e interpretação da análise de solo."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from math import isfinite

from app.services.agronomy import references as R
from app.services.soil_measurements import parse_metadata

K_MG_PER_CMOLC = 391.0  # 1 cmolc/dm³ de K = 391 mg/dm³ (Manual do Café, p. 38)

# Faixas físicas plausíveis na unidade canônica — fora delas, a unidade provavelmente está errada.
PLAUSIBLE = {
    "ph": (3.0, 9.0), "organic_matter": (0.05, 15.0), "phosphorus": (0.1, 600.0), "potassium": (3.0, 1500.0),
    "calcium": (0.0, 30.0), "magnesium": (0.0, 12.0), "aluminum": (0.0, 8.0), "h_al": (0.0, 30.0),
    "ctc": (0.5, 60.0), "base_saturation": (0.0, 100.0), "sulfur": (0.0, 500.0), "boron": (0.0, 10.0),
    "zinc": (0.0, 100.0), "copper": (0.0, 60.0), "manganese": (0.0, 300.0), "iron": (0.0, 900.0),
    "clay": (0.0, 100.0), "p_rem": (0.0, 60.0),
}
LABELS = {
    "ph": "pH", "organic_matter": "Matéria orgânica", "phosphorus": "Fósforo (P)", "potassium": "Potássio (K)",
    "calcium": "Cálcio (Ca)", "magnesium": "Magnésio (Mg)", "aluminum": "Alumínio (Al)", "h_al": "H + Al",
    "ctc": "CTC a pH 7 (T)", "base_saturation": "Saturação por bases (V)", "sulfur": "Enxofre (S)",
    "boron": "Boro (B)", "zinc": "Zinco (Zn)", "copper": "Cobre (Cu)", "manganese": "Manganês (Mn)",
    "iron": "Ferro (Fe)", "clay": "Argila", "p_rem": "P remanescente", "sb": "Soma de bases (SB)",
    "t_effective": "CTC efetiva (t)", "m": "Saturação por alumínio (m)",
}
UNITS = {
    "ph": "", "organic_matter": "dag/kg", "phosphorus": "mg/dm³", "potassium": "mg/dm³", "calcium": "cmolc/dm³",
    "magnesium": "cmolc/dm³", "aluminum": "cmolc/dm³", "h_al": "cmolc/dm³", "ctc": "cmolc/dm³",
    "base_saturation": "%", "sulfur": "mg/dm³", "boron": "mg/dm³", "zinc": "mg/dm³", "copper": "mg/dm³",
    "manganese": "mg/dm³", "iron": "mg/dm³", "clay": "%", "p_rem": "mg/L", "sb": "cmolc/dm³",
    "t_effective": "cmolc/dm³", "m": "%",
}
BASE_FIELDS = ("ph", "organic_matter", "phosphorus", "potassium", "calcium", "magnesium", "aluminum", "h_al",
               "ctc", "base_saturation")
EXTRA_FIELDS = ("sulfur", "boron", "zinc", "copper", "manganese", "iron", "clay", "p_rem")


def _num(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if isfinite(number) else None


def to_canonical(field: str, value: float, unit: str | None) -> float | None:
    """Converte para a unidade canônica do motor. Devolve None quando a conversão não é segura."""
    if value is None:
        return None
    if field == "organic_matter":  # canônico: dag/kg (= %)
        if unit is None:  # cadastro antigo: valores acima de 15 só fazem sentido em g/dm³
            return value / 10 if value > 15 else value
        return {"g/dm³": value / 10, "g/kg": value / 10, "%": value}.get(unit)
    if field == "potassium":
        return {"mg/dm³": value, "cmolc/dm³": value * K_MG_PER_CMOLC, "mmolc/dm³": value * K_MG_PER_CMOLC / 10,
                "mg/kg": None, None: value, "": None}.get(unit)
    if field in {"calcium", "magnesium", "aluminum", "h_al", "ctc"}:
        return {"cmolc/dm³": value, "mmolc/dm³": value / 10, None: value, "": None}.get(unit)
    if field == "clay":
        return {"%": value, "g/kg": value / 10, None: value, "": None}.get(unit)
    if unit in ("mg/kg", ""):
        return None
    return value


@dataclass
class SoilReading:
    values: dict = field(default_factory=dict)      # unidade canônica
    declared: dict = field(default_factory=dict)    # campo -> unidade informada (None = não declarada)
    methods: dict = field(default_factory=dict)
    depth: tuple | None = None
    sample_date: date | None = None
    laboratory: str = ""
    issues: list = field(default_factory=list)       # problemas de unidade/consistência
    blocked: set = field(default_factory=set)        # campos descartados por suspeita de unidade

    def get(self, key):
        return None if key in self.blocked else self.values.get(key)

    @property
    def is_subsurface(self) -> bool:
        return bool(self.depth) and self.depth[0] >= 15


def read_soil(analysis) -> SoilReading:
    metadata = parse_metadata(analysis.laboratory_metadata)
    units = metadata.get("units") or {}
    extras = metadata.get("extra_values") or {}
    reading = SoilReading(methods=metadata.get("methods") or {}, laboratory=analysis.laboratory or "")
    reading.sample_date = date.fromisoformat(metadata["collected"]) if metadata.get("collected") else analysis.analysis_date
    if metadata.get("depth_from_cm") is not None and metadata.get("depth_to_cm") is not None:
        reading.depth = (float(metadata["depth_from_cm"]), float(metadata["depth_to_cm"]))
    raw = {name: _num(getattr(analysis, name, None)) for name in BASE_FIELDS}
    raw.update({name: _num(extras.get(name)) for name in EXTRA_FIELDS})
    for name, value in raw.items():
        if value is None:
            continue
        unit = units.get(name) if name in units else None
        reading.declared[name] = unit
        converted = value if name in ("ph", "base_saturation") else to_canonical(name, value, unit)
        if converted is None:
            reading.issues.append(f"{LABELS[name]}: a unidade “{unit or 'não informada'}” não permite cálculo seguro.")
            continue
        reading.values[name] = converted
    _check_plausibility(reading)
    _derive(reading)
    return reading


def _check_plausibility(reading: SoilReading) -> None:
    for name, value in list(reading.values.items()):
        low, high = PLAUSIBLE[name]
        if low <= value <= high:
            continue
        hint = ""
        if name == "potassium" and value < low:
            hint = " O valor parece estar em cmolc/dm³ — confira a unidade."
        elif name in {"calcium", "magnesium", "ctc", "h_al"} and value > high:
            hint = " O valor parece estar em mmolc/dm³ — confira a unidade."
        reading.issues.append(f"{LABELS[name]} = {value:g} está fora da faixa esperada ({low:g}–{high:g} {UNITS[name]})."
                              f"{hint} O dado não foi usado nos cálculos.")
        reading.blocked.add(name)
    for name, unit in reading.declared.items():
        if unit is None and name not in ("ph", "base_saturation") and name not in reading.blocked:
            reading.issues.append(f"{LABELS[name]}: unidade não declarada no cadastro; considerada {UNITS[name]}.")


def _derive(reading: SoilReading) -> None:
    get = reading.get
    ca, mg, k, al, hal = get("calcium"), get("magnesium"), get("potassium"), get("aluminum"), get("h_al")
    values = reading.values
    if None not in (ca, mg, k):
        values["sb"] = ca + mg + k / K_MG_PER_CMOLC
    sb = values.get("sb")
    if sb is not None and al is not None:
        values["t_effective"] = sb + al
        values["m"] = 100 * al / values["t_effective"] if values["t_effective"] > 0 else 0.0
    computed_t = sb + hal if sb is not None and hal is not None else None
    if get("ctc") is None and computed_t:
        values["ctc"] = computed_t
        reading.issues.append("CTC a pH 7 calculada pelo sistema (SB + H+Al).")
    elif computed_t and get("ctc") and abs(computed_t - get("ctc")) / get("ctc") > 0.15:
        reading.issues.append(f"A CTC do laudo ({get('ctc'):.2f}) difere da soma SB + H+Al ({computed_t:.2f}). "
                              "Confira as unidades de K, Ca, Mg e H+Al.")
    if sb is not None and get("ctc"):
        computed_v = 100 * sb / get("ctc")
        if get("base_saturation") is None:
            values["base_saturation"] = computed_v
            reading.issues.append("Saturação por bases calculada pelo sistema (100·SB/T).")
        elif abs(computed_v - get("base_saturation")) > 8:
            reading.issues.append(f"V% do laudo ({get('base_saturation'):.0f}%) difere do calculado ({computed_v:.0f}%). "
                                  "Confira os valores do laudo.")


# ---------------------------------------------------------------- interpretação

def classify(value: float, limits: tuple, classes: tuple = R.CLASSES5) -> str:
    for limit, name in zip(limits, classes):
        if value <= limit:
            return name
    return classes[len(limits)]


def _interp(points: tuple, x: float) -> float:
    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        if x0 <= x <= x1:
            return y0 + (y1 - y0) * (x - x0) / (x1 - x0) if x1 != x0 else y0
    return points[-1][1] if x > points[-1][0] else points[0][1]


TEXTURE_CLAY_RANGE = {"arenoso": (0, 15), "medio": (15, 35), "argiloso": (35, 60)}


@dataclass
class Texture:
    clay: float | None        # % de argila medida
    prem: float | None        # P remanescente medido
    texture_class: str | None  # arenoso | medio | argiloso (cadastro do setor)
    source: str

    def clay_range(self):
        if self.clay is not None:
            for low, high, _ in R.P_CLASSES_BY_CLAY:
                if low <= self.clay <= high:
                    return low, high
        return TEXTURE_CLAY_RANGE.get(self.texture_class or "")


def texture_for(reading: SoilReading, plot_texture: str | None) -> Texture:
    clay, prem = reading.get("clay"), reading.get("p_rem")
    if prem is not None:
        source = "P remanescente do laudo"
    elif clay is not None:
        source = "argila do laudo"
    elif plot_texture in TEXTURE_CLAY_RANGE:
        source = "textura cadastrada no setor"
    else:
        source = ""
    return Texture(clay, prem, plot_texture if plot_texture in TEXTURE_CLAY_RANGE else None, source)


def phosphorus_class_arabica(p: float, texture: Texture) -> tuple[str | None, str]:
    if texture.prem is not None:
        for low, high, limits in R.P_CLASSES_BY_PREM:
            if low <= texture.prem <= high:
                return classify(p, limits), f"Tabela 03 por P-rem ({texture.prem:g} mg/L)"
    span = texture.clay_range()
    if span:
        for low, high, limits in R.P_CLASSES_BY_CLAY:
            if (low, high) == span:
                origin = f"argila {texture.clay:g}%" if texture.clay is not None else f"textura {texture.texture_class} ({low}–{high}% de argila)"
                return classify(p, limits), f"Tabela 03 por {origin}"
    return None, ""


def phosphorus_class_conilon(p: float, texture: Texture) -> tuple[str | None, str]:
    group = None
    if texture.prem is not None:
        group = "argilosa" if texture.prem < 20 else "media" if texture.prem <= 40 else "arenosa"
    elif texture.clay is not None:
        group = "argilosa" if texture.clay >= 35 else "media" if texture.clay >= 15 else "arenosa"
    elif texture.texture_class:
        group = {"argiloso": "argilosa", "medio": "media", "arenoso": "arenosa"}[texture.texture_class]
    if not group:
        return None, ""
    name = classify(p, R.P_CLASSES_CONILON[group], ("muito_baixo", "baixo", "medio", "alto"))
    return name, f"Tabela 8 (textura {group})"


def sulfur_class(s: float, texture: Texture) -> str:
    if texture.prem is not None:
        for low, high, limits in R.S_CLASSES_BY_PREM:
            if low <= texture.prem <= high:
                return classify(s, limits)
    return classify(s, R.S_CLASSES_SIMPLE, ("baixo", "medio", "bom"))


def y_factor(texture: Texture) -> tuple[float | None, str]:
    if texture.prem is not None:
        return _interp(R.Y_BY_PREM, texture.prem), f"Tabela 13 (P-rem {texture.prem:g} mg/L)"
    if texture.clay is not None:
        return _interp(R.Y_BY_CLAY, texture.clay), f"Tabela 12 (argila {texture.clay:g}%)"
    span = texture.clay_range()
    if span:
        middle = sum(span) / 2
        return _interp(R.Y_BY_CLAY, middle), f"Tabela 12 (textura {texture.texture_class}, argila média de {middle:g}%)"
    return None, ""


def gypsum_by_texture(texture: Texture) -> tuple[float | None, str]:
    if texture.prem is not None:
        return _interp(R.GYPSUM_BY_PREM, texture.prem), f"Tabela 08 da 5ª Aproximação (P-rem {texture.prem:g} mg/L)"
    if texture.clay is not None:
        return _interp(R.GYPSUM_BY_CLAY, texture.clay), f"Tabela 09 da 5ª Aproximação (argila {texture.clay:g}%)"
    return None, ""


STATUS_BY_CLASS = {
    "muito_baixo": "critico", "baixo": "baixo", "medio": "atencao", "bom": "adequado", "muito_bom": "alto",
    "adequado": "adequado", "alto": "alto", "muito_alto": "alto",
}


def indicator(key, value, klass, ideal, ref="manual_cafe_mg", status=None):
    return {
        "id": key, "nome": LABELS[key], "valor": round(value, 2), "unidade": UNITS[key],
        "classe": R.CLASS_LABELS.get(klass, klass) if klass else "Sem classe",
        "status": status or STATUS_BY_CLASS.get(klass, "neutro"), "faixa_ideal": ideal, "referencia": ref,
    }


def interpret(reading: SoilReading, texture: Texture, species: str) -> list[dict]:
    """Classifica cada indicador disponível. Retorna a lista na ordem de leitura do laudo."""
    get, rows = reading.get, []
    ph = get("ph")
    ph_method = reading.methods.get("ph") or "Não informado"
    if ph is not None:
        if ph_method in ("Água", "Não informado"):
            klass = classify(ph, R.PH_WATER_LIMITS, ("muito_baixo", "baixo", "bom", "alto", "muito_alto"))
            status = {"muito_baixo": "critico", "baixo": "baixo", "bom": "adequado"}.get(klass, "alto")
            rows.append(indicator("ph", ph, klass, "5,5 a 6,0 (em água)", status=status))
        else:
            rows.append(indicator("ph", ph, None, f"Método {ph_method}: tabela calibrada para pH em água", status="neutro"))
    for key, ideal in (("calcium", "2,41 a 4,00"), ("magnesium", "0,91 a 1,50"), ("ctc", "8,61 a 15,00"),
                       ("base_saturation", "60 a 80"), ("sb", "3,61 a 6,00")):
        value = get(key) if key in reading.values and key not in reading.blocked else None
        if value is not None:
            rows.append(indicator(key, value, classify(value, R.SOIL_CLASSES_MG[key]), ideal))
    for key, ideal in (("aluminum", "até 0,20"), ("m", "até 15"), ("h_al", "até 2,50")):
        value = reading.values.get(key) if key not in reading.blocked else None
        if value is not None:
            klass = classify(value, R.SOIL_CLASSES_MG[key])
            status = {"muito_baixo": "adequado", "baixo": "adequado", "medio": "atencao"}.get(klass, "critico")
            label = {"bom": "alto", "muito_bom": "muito_alto"}.get(klass, klass)
            rows.append(indicator(key, value, label, ideal, status=status))
    om = get("organic_matter")
    if om is not None:
        rows.append(indicator("organic_matter", om, classify(om, R.SOIL_CLASSES_MG["organic_matter_dag"]), "4,01 a 7,00"))
    p = get("phosphorus")
    p_extractor = reading.methods.get("phosphorus") or "Não informado"
    if p is not None:
        klass, how = (phosphorus_class_conilon if species == "canephora" else phosphorus_class_arabica)(p, texture)
        if p_extractor not in ("Mehlich-1", "Não informado"):
            rows.append(indicator("phosphorus", p, None, f"Tabelas calibradas para Mehlich-1 (laudo: {p_extractor})"))
        else:
            rows.append(indicator("phosphorus", p, klass, how or "Informe argila ou P-rem para classificar",
                                  ref="incaper_conilon" if species == "canephora" else "manual_cafe_mg"))
    k = get("potassium")
    if k is not None:
        klass = classify(k, R.K_COFFEE_LIMITS, R.K_COFFEE_CLASSES)
        rows.append(indicator("potassium", k, klass, "120 a 200", status={"muito_bom": "adequado"}.get(klass)))
    s = get("sulfur")
    if s is not None:
        klass = sulfur_class(s, texture)
        rows.append(indicator("sulfur", s, klass, "Classe “bom” da Tabela 04" if texture.prem is not None else "acima de 10"))
    micro_table = R.MICROS_CONILON if species == "canephora" else R.MICROS_ARABICA
    for key in ("boron", "zinc", "copper", "manganese"):
        value = get(key)
        if value is None:
            continue
        extractor = micronutrient_extractor(reading, key)
        limits = micro_table[key].get(extractor)
        if not limits:
            rows.append(indicator(key, value, None, f"Informe o extrator (laudo: {extractor})"))
            continue
        classes = R.MICRO_CLASSES[:len(limits) + 1] if species != "canephora" else ("baixo", "medio", "alto")
        klass = classify(value, limits, classes)
        rows.append(indicator(key, value, klass, "Classe “adequada”" if species != "canephora" else "Classe “alto”",
                              status={"baixo": "baixo", "medio": "atencao"}.get(klass, "adequado"),
                              ref="incaper_conilon" if species == "canephora" else "manual_cafe_mg"))
    if get("iron") is not None:
        rows.append(indicator("iron", get("iron"), None, "Somente informativo", status="neutro"))
    return rows


def micronutrient_extractor(reading: SoilReading, key: str) -> str:
    if key == "boron":
        return reading.methods.get("boron") or "Não informado"
    return reading.methods.get("micronutrients") or "Não informado"
