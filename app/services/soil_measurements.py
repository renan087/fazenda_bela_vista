"""Unidades e métodos declarados pelo laboratório para análises de solo."""
import json
from datetime import date
from math import isfinite

FIELDS = {
    "organic_matter": ("Matéria orgânica", ("g/dm³", "%", "g/kg")),
    "phosphorus": ("Fósforo", ("mg/dm³", "mg/kg")),
    "potassium": ("Potássio", ("mg/dm³", "cmolc/dm³", "mmolc/dm³")),
    "calcium": ("Cálcio", ("cmolc/dm³", "mmolc/dm³")),
    "magnesium": ("Magnésio", ("cmolc/dm³", "mmolc/dm³")),
    "aluminum": ("Alumínio", ("cmolc/dm³", "mmolc/dm³")),
    "h_al": ("H + Al", ("cmolc/dm³", "mmolc/dm³")),
    "ctc": ("CTC", ("cmolc/dm³", "mmolc/dm³")),
    "base_saturation": ("Saturação por bases", ("%",)),
}
EXTRA_FIELDS = {
    "sulfur": ("Enxofre (S)", ("mg/dm³", "mg/kg")),
    "boron": ("Boro (B)", ("mg/dm³", "mg/kg")),
    "zinc": ("Zinco (Zn)", ("mg/dm³", "mg/kg")),
    "copper": ("Cobre (Cu)", ("mg/dm³", "mg/kg")),
    "manganese": ("Manganês (Mn)", ("mg/dm³", "mg/kg")),
    "iron": ("Ferro (Fe)", ("mg/dm³", "mg/kg")),
    "clay": ("Argila", ("%", "g/kg")),
    "p_rem": ("Fósforo remanescente (P-rem)", ("mg/L",)),
}
METHODS = {
    "phosphorus": ("Fósforo", ("Não informado", "Mehlich-1", "Mehlich-3", "Resina", "Olsen")),
    "potassium": ("Potássio", ("Não informado", "Mehlich-1", "Mehlich-3", "Resina")),
    "cations": ("Cálcio, magnésio e alumínio", ("Não informado", "KCl", "Resina")),
    "acidity": ("Acidez potencial (H + Al)", ("Não informado", "SMP", "Acetato de cálcio")),
    "organic_matter": ("Matéria orgânica", ("Não informado", "Walkley-Black", "Combustão seca")),
    "ph": ("pH", ("Não informado", "Água", "CaCl₂", "KCl")),
    "sulfur": ("Enxofre", ("Não informado", "Fosfato monocálcico", "Fosfato de cálcio", "Mehlich-1")),
    "boron": ("Boro", ("Não informado", "Água quente", "Cloreto de bário quente")),
    "micronutrients": ("Zinco, cobre, manganês e ferro", ("Não informado", "Mehlich-1", "Mehlich-3", "DTPA")),
}


def parse_metadata(value):
    try:
        data = json.loads(value) if value else {}
    except (ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def validate_metadata(units, methods, collected, issued, depth_from, depth_to, extra_values=None):
    normalized_units = {}
    for field, (_, options) in {**FIELDS, **EXTRA_FIELDS}.items():
        value = units.get(field)
        if value is None:
            value = options[0]
        if value not in (*options, ""):
            raise ValueError(f"Unidade inválida para {({**FIELDS, **EXTRA_FIELDS})[field][0]}.")
        normalized_units[field] = value
    normalized_methods = {}
    for field, (_, options) in METHODS.items():
        value = methods.get(field) or options[0]
        if value not in options:
            raise ValueError("Método de análise inválido.")
        normalized_methods[field] = value
    try:
        collected_date = date.fromisoformat(collected) if collected else None
        issued_date = date.fromisoformat(issued) if issued else None
        low = float(depth_from) if depth_from not in (None, "") else None
        high = float(depth_to) if depth_to not in (None, "") else None
    except (TypeError, ValueError):
        raise ValueError("Confira as datas e a profundidade da coleta.")
    if collected_date and issued_date and issued_date < collected_date:
        raise ValueError("A emissão do laudo deve ocorrer após a coleta.")
    if (low is None) != (high is None) or (low is not None and (
        not isfinite(low) or not isfinite(high) or low < 0 or high <= low or high > 1000
    )):
        raise ValueError("Informe uma profundidade inicial e final válidas em centímetros.")
    extras = {}
    for field, value in (extra_values or {}).items():
        if field not in EXTRA_FIELDS or value in (None, ""):
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"Valor inválido para {EXTRA_FIELDS[field][0]}.")
        if not isfinite(number) or number < 0 or number > 999999 or (field == "clay" and number > 1000):
            raise ValueError(f"Valor inválido para {EXTRA_FIELDS[field][0]}.")
        extras[field] = number
    return {
        "units": normalized_units, "methods": normalized_methods,
        "extra_values": extras,
        "collected": collected_date.isoformat() if collected_date else None,
        "issued": issued_date.isoformat() if issued_date else None,
        "depth_from_cm": low, "depth_to_cm": high,
    }


def validate_measurements(values):
    for field, value in values.items():
        if value in (None, ""):
            continue
        try:
            number = float(str(value).replace(",", "."))
        except ValueError:
            raise ValueError("Confira os valores dos resultados laboratoriais.")
        if not isfinite(number) or number < 0 or (field == "ph" and number > 14) or (
            field == "base_saturation" and number > 100
        ):
            raise ValueError("Confira os valores dos resultados laboratoriais.")


def canonical_value(field, value, unit):
    if value is None:
        return None
    value = float(value)
    if unit in ("g/kg", "mg/kg"):
        return None
    if field == "potassium":
        return value * {"mg/dm³": 1, "cmolc/dm³": 391, "mmolc/dm³": 39.1}[unit]
    if field in {"calcium", "magnesium", "aluminum", "h_al", "ctc"}:
        return value / 10 if unit == "mmolc/dm³" else value
    if field == "organic_matter":
        return value * 10 if unit == "%" else value
    return value
