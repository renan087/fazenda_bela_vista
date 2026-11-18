"""Validação dos dados declarados de análise, sem classificação automática."""
from datetime import date
from decimal import Decimal, InvalidOperation


def validate_classification(form, *, harvest_date, today, has_report):
    try:
        analysis_date = date.fromisoformat(str(form.get("analysis_date") or ""))
    except ValueError:
        raise ValueError("Informe uma data de análise válida.") from None
    if analysis_date > today or (harvest_date and analysis_date < harvest_date):
        raise ValueError("A data da análise não pode ser futura ou anterior à colheita.")
    source = str(form.get("source") or "")
    if source not in {"informado", "laudo"}:
        raise ValueError("Selecione a origem das informações.")
    if source == "laudo" and not has_report:
        raise ValueError("Anexe o laudo em PDF para usar essa origem.")
    limits = {"evaluator": 180, "method": 180, "coffee_type": 80, "sieve": 80,
              "defects": 180, "beverage": 120, "sensory_notes": 2000, "notes": 2000}
    result = {"analysis_date": analysis_date, "source": source}
    for key, limit in limits.items():
        value = str(form.get(key) or "").strip()
        if len(value) > limit:
            raise ValueError("Um dos campos de texto excedeu o limite permitido.")
        result[key] = value or None
    for key, label in (("moisture", "Umidade"), ("sensory_score", "Nota sensorial")):
        raw = str(form.get(key) or "").strip().replace(",", ".")
        if not raw:
            result[key] = None
            continue
        try:
            value = Decimal(raw)
        except InvalidOperation:
            raise ValueError(f"{label} deve ser um número válido.") from None
        if not value.is_finite() or value < 0 or value > 100 or value.as_tuple().exponent < -2:
            raise ValueError(f"{label} deve estar entre 0 e 100, com até duas casas decimais.")
        result[key] = value
    if not any(result.get(key) is not None for key in ("coffee_type", "sieve", "moisture", "defects", "beverage", "sensory_score", "sensory_notes", "notes")) and not has_report:
        raise ValueError("Informe pelo menos um resultado ou anexe um laudo.")
    return result
