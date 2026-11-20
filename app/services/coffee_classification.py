"""Validação dos dados declarados de análise, sem classificação automática."""
from datetime import date
from decimal import Decimal, InvalidOperation

COFFEE_BEVERAGES = (
    ("Arábica", ("Estritamente Mole", "Mole", "Apenas Mole", "Duro", "Riado", "Rio", "Rio Zona")),
    ("Robusta / Conilon", ("Excelente", "Boa", "Regular", "Anormal")),
)
SENSORY_SCALES = tuple(range(5, 11))
SENSORY_ATTRIBUTES = (("aroma", "Aroma"), ("sabor", "Sabor"), ("acidez", "Acidez"),
                      ("corpo", "Corpo"), ("finalizacao", "Finalização"), ("balanco", "Balanço"),
                      ("docura", "Doçura"), ("uniformidade", "Uniformidade"),
                      ("limpeza", "Limpeza"), ("geral", "Impressão geral"))


def _attribute_overrides(db, organization_id):
    from app.models.coffee_sensory_attribute import CoffeeSensoryAttribute
    return {row.key: row for row in db.query(CoffeeSensoryAttribute).filter_by(organization_id=organization_id).all()}


def attribute_rows(db, organization_id):
    """Atributos da organização, na ordem escolhida: dicts com key, label, hint, active, builtin."""
    overrides = _attribute_overrides(db, organization_id)
    default_keys = [key for key, _ in SENSORY_ATTRIBUTES]
    rows = []
    for index, (key, label) in enumerate(SENSORY_ATTRIBUTES):
        row = overrides.get(key)
        if row and row.deleted:
            continue
        rows.append({"key": key, "label": row.label if row else label, "hint": (row.hint if row else None) or "",
                     "active": row.active if row else True, "builtin": True,
                     "order": row.sort_order if row and row.sort_order is not None else index})
    custom = sorted((row for row in overrides.values() if row.key not in default_keys and not row.deleted),
                    key=lambda row: row.label.casefold())
    for index, row in enumerate(custom):
        rows.append({"key": row.key, "label": row.label, "hint": row.hint or "", "active": row.active, "builtin": False,
                     "order": row.sort_order if row.sort_order is not None else 100 + index})
    rows.sort(key=lambda item: item["order"])
    return rows


def sensory_attributes_for_org(db, organization_id):
    return [(row["key"], row["label"], row["active"]) for row in attribute_rows(db, organization_id)]


def sensory_definitions_for_edit(definitions, profile):
    active = [(key, label) for key, label, enabled in definitions if enabled]
    if not profile:
        return active
    previous = dict(zip(profile.attributes.keys(), (label for label, _ in profile.attribute_pairs)))
    # Keep the original label and any no-longer-active attribute for this analysis.
    result = [(key, previous.get(key, label)) for key, label in active]
    existing = {key for key, _ in result}
    return result + [(key, label) for key, label in previous.items() if key not in existing]


def validate_sensory(form, definitions=None):
    try:
        scale = int(str(form.get("sensory_scale") or "10"))
    except ValueError:
        raise ValueError("Escala sensorial inválida.") from None
    if scale not in SENSORY_SCALES:
        raise ValueError("Selecione uma escala sensorial válida.")
    result = {}
    for key, label in (definitions if definitions is not None else SENSORY_ATTRIBUTES):
        field = "sensory_" + key
        if field not in form:
            continue
        raw = str(form.get(field) or "0").strip().replace(",", ".") or "0"
        try:
            value = Decimal(raw)
        except InvalidOperation:
            raise ValueError(f"Nota de {label.lower()} inválida.") from None
        if not value.is_finite() or value < 0 or value > scale or value.as_tuple().exponent < -2:
            raise ValueError(f"Nota de {label.lower()} deve estar entre 0 e {scale}.")
        result[key] = float(value)
    return scale, result


def validate_classification(form, *, harvest_date, today, has_report, has_sensory=False):
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
    if not any(result.get(key) is not None for key in ("coffee_type", "sieve", "moisture", "defects", "beverage", "sensory_score", "sensory_notes", "notes")) and not has_report and not has_sensory:
        raise ValueError("Informe pelo menos um resultado ou anexe um laudo.")
    return result


SCA_PRESET = ("aroma", "sabor", "finalizacao", "acidez", "corpo", "balanco", "docura", "uniformidade", "geral")
SIMPLE_PRESET = ("aroma", "sabor", "acidez", "corpo", "docura")
ANALYSIS_KINDS = (("fisica", "Classificação física", "tipo, peneira e bebida"),
                  ("ambos", "Física + prova de xícara", "com nota e atributos"),
                  ("xicara", "Só prova de xícara", "nota e atributos"))
COFFEE_TYPES = ("2", "3", "4", "5", "6", "7", "8")
SIEVES = ("17/18", "16 acima", "15 acima", "14 acima", "Moca", "Bica corrida")
FLAVORS = ("Chocolate", "Caramelo", "Frutado", "Cítrico", "Castanhas", "Floral", "Mel", "Cremoso")


def default_scale(db, organization_id) -> int:
    from app.models.coffee_sensory_attribute import CoffeeClassificationSetting
    row = db.get(CoffeeClassificationSetting, organization_id) if organization_id else None
    return row.default_scale if row and row.default_scale in SENSORY_SCALES else 10


def attribute_usage(db, organization_id) -> dict:
    """Quantas análises da organização usaram cada atributo."""
    import json

    from app.models.coffee_classification import CoffeeClassification
    from app.models.coffee_sensory_profile import CoffeeSensoryProfile
    from app.models.farm import Farm
    from app.models.harvest import HarvestRecord
    from app.models.plot import Plot
    rows = (db.query(CoffeeSensoryProfile.attributes_json)
            .join(CoffeeClassification, CoffeeSensoryProfile.classification_id == CoffeeClassification.id)
            .join(HarvestRecord, CoffeeClassification.harvest_id == HarvestRecord.id)
            .join(Plot, HarvestRecord.plot_id == Plot.id).join(Farm, Plot.farm_id == Farm.id)
            .filter(Farm.organization_id == organization_id).all())
    usage = {}
    for (raw,) in rows:
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            continue
        for key in data if isinstance(data, dict) else []:
            usage[key] = usage.get(key, 0) + 1
    return usage


def save_attributes(db, organization_id, payload: dict, *, can_delete: bool) -> None:
    """Grava a lista do modal: ordem, nomes, dicas, ativos, novos e excluídos (só os nunca usados)."""
    from uuid import uuid4

    from app.models.coffee_sensory_attribute import CoffeeClassificationSetting, CoffeeSensoryAttribute
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    deleted = [str(key) for key in payload.get("deleted") or [] if key]
    if len(items) > 30:
        raise ValueError("O limite é de 30 atributos.")
    names = set()
    for item in items:
        label = " ".join(str(item.get("label") or "").split())
        if not label or len(label) > 80:
            raise ValueError("Cada atributo precisa de um nome com até 80 caracteres.")
        if label.casefold() in names:
            raise ValueError(f"O nome “{label}” aparece duas vezes.")
        names.add(label.casefold())
        if len(str(item.get("hint") or "")) > 160:
            raise ValueError("A explicação deve ter até 160 caracteres.")
    if sum(1 for item in items if item.get("active")) == 0:
        raise ValueError("Deixe pelo menos um atributo ativo.")
    current = {row["key"]: row for row in attribute_rows(db, organization_id)}
    usage = attribute_usage(db, organization_id) if deleted else {}
    for key in deleted:
        if key not in current:
            continue
        if not can_delete:
            raise ValueError("Você não tem permissão para excluir atributos.")
        if usage.get(key):
            raise ValueError(f"“{current[key]['label']}” já foi usado em análises. Desative em vez de excluir.")
    overrides = _attribute_overrides(db, organization_id)

    def row_for(key, label):
        row = overrides.get(key)
        if not row:
            row = CoffeeSensoryAttribute(organization_id=organization_id, key=key, label=label, active=True, deleted=False)
            db.add(row)
            overrides[key] = row
        return row

    for key in deleted:
        if key in current:
            row = row_for(key, current[key]["label"])
            row.deleted, row.active = True, False
    for order, item in enumerate(items):
        key = str(item.get("key") or "")
        if key and key not in current:
            continue
        label = " ".join(str(item.get("label") or "").split())
        row = row_for(key or "a" + uuid4().hex, label)
        row.label, row.hint = label, (str(item.get("hint") or "").strip() or None)
        row.active, row.deleted, row.sort_order = bool(item.get("active")), False, order
    scale = payload.get("scale")
    try:
        scale = int(scale)
    except (TypeError, ValueError):
        scale = None
    if scale in SENSORY_SCALES:
        setting = db.get(CoffeeClassificationSetting, organization_id)
        if not setting:
            setting = CoffeeClassificationSetting(organization_id=organization_id, default_scale=scale)
            db.add(setting)
        setting.default_scale = scale
        setting.preset = str(payload.get("preset") or "")[:20] or None


def score_band(score) -> tuple[str, str]:
    """(rótulo, tom) da nota final: especial 80+ (excelente 84+), comercial abaixo."""
    if score is None:
        return "Sem prova de xícara", "none"
    value = float(score)
    if value >= 84:
        return "Especial · excelente", "top"
    if value >= 80:
        return "Especial", "ok"
    return "Comercial", "std"


def radar_points(values, scale, *, cx=120.0, cy=120.0, radius=86.0) -> str:
    """Pontos SVG de um polígono radar; values na escala 0..scale."""
    import math
    n = len(values)
    if n < 3 or not scale:
        return ""
    points = []
    for index, value in enumerate(values):
        angle = -math.pi / 2 + index * 2 * math.pi / n
        k = radius * max(0.0, min(float(value or 0) / float(scale), 1.0))
        points.append(f"{cx + k * math.cos(angle):.1f},{cy + k * math.sin(angle):.1f}")
    return " ".join(points)


def radar_axes(labels, *, cx=120.0, cy=120.0, radius=86.0, pad=20.0) -> list[dict]:
    import math
    n = len(labels)
    result = []
    for index, label in enumerate(labels):
        angle = -math.pi / 2 + index * 2 * math.pi / max(n, 1)
        short = label if len(label) <= 12 else label[:11] + "…"
        result.append({"label": short, "x": round(cx + (radius + pad) * math.cos(angle)),
                       "y": round(cy + (radius + pad) * math.sin(angle) + 4)})
    return result
