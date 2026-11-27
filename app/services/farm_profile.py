"""Ficha da fazenda: pendências do cadastro, percentual de completude e dados das abas.

Tudo aqui é leitura (nenhuma gravação). Os dados que já existem em outros módulos
(setores, safras, adubação, pragas, notas fiscais) são apenas lidos e exibidos,
para não haver duas cópias da mesma informação.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date

DOC_TYPES = [
    "CCIR",
    "ITR",
    "Matrícula",
    "Recibo do CAR",
    "Licença ambiental",
    "Outorga de água",
    "Contrato de arrendamento",
    "Outro",
]
REGISTRY_DOC_TYPES = {"CCIR", "ITR", "Matrícula"}
EXPIRING_DOC_TYPES = {"Licença ambiental", "Outorga de água", "Contrato de arrendamento"}
EXPIRY_WARNING_DAYS = 60

TERRAIN_OPTIONS = [
    "Plano (0 a 3%)",
    "Suave ondulado (3 a 8%)",
    "Ondulado (8 a 20%)",
    "Forte ondulado (20 a 45%)",
    "Montanhoso (acima de 45%)",
]
SUN_OPTIONS = ["Face norte", "Face sul", "Face leste", "Face oeste", "Variada"]
SOIL_OPTIONS = [
    "Latossolo Vermelho",
    "Latossolo Vermelho-Amarelo",
    "Argissolo",
    "Cambissolo",
    "Neossolo",
    "Outro",
]
PEST_OPTIONS = ["Bicho-mineiro", "Broca-do-café", "Ferrugem", "Cercosporiose", "Phoma", "Nematoides"]

USE_COLORS = {"setores": "#446A36", "reserva": "#294127", "app": "#1F5FA8", "outros": "#B9C3B5"}
MONTHS_PT = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]


def br_number(value: float | int | None, digits: int = 2) -> str:
    if value is None:
        return ""
    text = f"{float(value):,.{digits}f}"
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def br_ha(value: float | int | None) -> str:
    return f"{br_number(value or 0)} ha"


def month_label(value: date | None) -> str:
    return value.strftime("%m/%Y") if value else ""


def months_ago(value: date | None, today: date) -> int | None:
    if not value:
        return None
    return max(0, (today.year - value.year) * 12 + (today.month - value.month))


def ago_label(value: date | None, today: date) -> str:
    months = months_ago(value, today)
    if months is None:
        return ""
    if months == 0:
        return "neste mês"
    if months == 1:
        return "há 1 mês"
    return f"há {months} meses"


def reference_year(text: str | None) -> int | None:
    years = [int(m.group(0)) for m in re.finditer(r"(?:19|20)\d{2}", text or "")]
    return max(years) if years else None


@dataclass
class Pending:
    id: str
    title: str
    detail: str
    action: str
    tab: str
    panel: str = ""
    doc_type: str = ""
    href: str = ""


@dataclass
class FarmChecks:
    score: int
    pending: list[Pending] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.pending)

    @property
    def label(self) -> str:
        if not self.pending:
            return "Completo"
        return f"{self.count} pendência" + ("s" if self.count > 1 else "")

    def by_tab(self, tab: str) -> int:
        return sum(1 for p in self.pending if p.tab == tab)


def attachment_status(att, today: date) -> tuple[str, str]:
    """(texto, classe) da situação de um documento anexado."""
    doc_type = (getattr(att, "doc_type", None) or "").strip()
    valid_until = getattr(att, "valid_until", None)
    if valid_until:
        days = (valid_until - today).days
        if days < 0:
            return "Vencido", "err"
        if days <= EXPIRY_WARNING_DAYS:
            months = max(1, round(days / 30))
            return ("Vence em " + (f"{months} meses" if months > 1 else "1 mês")) if days > 31 else f"Vence em {days} dias", "warn"
        return "Em dia", "ok"
    if doc_type == "ITR":
        year = reference_year(getattr(att, "reference", None))
        if year and year < today.year:
            return f"Falta o de {today.year}", "warn"
        return "Em dia", "ok"
    if doc_type == "CCIR":
        year = reference_year(getattr(att, "reference", None))
        if year and year < today.year - 2:
            return "Desatualizado", "warn"
        return "Em dia", "ok"
    if doc_type in {"Matrícula", "Outro", ""}:
        return "Sem validade", "mute"
    return "Em dia", "ok"


def attachment_reference(att) -> str:
    parts = []
    if getattr(att, "reference", None):
        parts.append(att.reference)
    if getattr(att, "valid_until", None):
        parts.append("Vence em " + att.valid_until.strftime("%m/%Y"))
    if not parts and getattr(att, "created_at", None):
        parts.append("Enviado em " + att.created_at.strftime("%d/%m/%Y"))
    return " · ".join(parts) or "—"


def _latest_by_type(attachments) -> dict[str, object]:
    latest: dict[str, object] = {}
    for att in attachments:
        doc_type = (getattr(att, "doc_type", None) or "").strip()
        if not doc_type:
            continue
        current = latest.get(doc_type)
        if current is None or (getattr(att, "id", 0) or 0) > (getattr(current, "id", 0) or 0):
            latest[doc_type] = att
    return latest


def build_farm_checks(farm, *, plots_count: int, attachments, profile, today: date, area_check: dict | None = None) -> FarmChecks:
    """Pendências do cadastro com um percentual simples de itens completos."""
    from app.services.forms import evaluate_area_divergence

    done = 0
    total = 0
    pending: list[Pending] = []

    def check(ok: bool, item: Pending) -> None:
        nonlocal done, total
        total += 1
        if ok:
            done += 1
        else:
            pending.append(item)

    has_map = bool((farm.boundary_geojson or "").strip())
    check(has_map, Pending("perim", "Desenhar o perímetro", "Com o mapa, a área real e a divisão em setores ficam corretas.", "Desenhar", "territorio", "perimetro"))
    check(bool(farm.car_registration_number), Pending("car", "Informar o número do CAR", "Traz a área declarada e o perímetro automaticamente.", "Informar", "documentos", "car"))
    check(plots_count > 0, Pending("set", "Cadastrar os setores", "Organizam adubações, colheitas e custos por talhão.", "Abrir Setores", "resumo", href="/setores"))

    registry = [("CCIR", farm.ccir_number), ("ITR", farm.itr_registration_number), ("Matrícula", farm.property_registration_number)]
    missing_numbers = [name for name, value in registry if not (value or "").strip()]
    check(not missing_numbers, Pending("reg", "Preencher os registros do imóvel", ", ".join(missing_numbers) + ("." if missing_numbers else ""), "Preencher", "documentos", "registros"))

    latest = _latest_by_type(attachments)
    for name, value in registry:
        if not (value or "").strip():
            continue
        att = latest.get(name)
        label = "a matrícula" if name == "Matrícula" else f"o {name}"
        if att is None:
            check(False, Pending(f"doc-{name}", f"Anexar {label}", f"O número {value} está cadastrado; falta o arquivo.", "Anexar", "documentos", "anexo", name))
            continue
        status, css = attachment_status(att, today)
        if css == "warn":
            year = today.year
            title = f"Anexar o ITR {year}" if name == "ITR" else f"Atualizar {label}"
            detail = (
                "O número está cadastrado, mas falta o arquivo do exercício."
                if name == "ITR"
                else f"O {name} anexado é de {reference_year(att.reference) or 'outro ano'}."
            )
            check(False, Pending(f"doc-{name}", title, detail, "Anexar", "documentos", "anexo", name))
        else:
            check(True, Pending("", "", "", "", ""))

    for doc_type, att in latest.items():
        if doc_type not in EXPIRING_DOC_TYPES or not getattr(att, "valid_until", None):
            continue
        status, css = attachment_status(att, today)
        if css in {"warn", "err"}:
            verb = "Renovar" if doc_type != "Contrato de arrendamento" else "Renovar o"
            check(False, Pending(f"exp-{doc_type}", f"{verb} a {doc_type.lower()}" if doc_type != "Contrato de arrendamento" else "Renovar o contrato de arrendamento",
                                 f"{'Venceu' if css == 'err' else 'Vence'} em {att.valid_until.strftime('%m/%Y')}. Anexe o novo quando sair.", "Anexar nova", "documentos", "anexo", doc_type))

    if has_map:
        area = area_check if area_check is not None else evaluate_area_divergence(float(farm.total_area or 0), farm.boundary_geojson)
        if area.get("polygon_area"):
            ack = float(farm.area_ack_ha) if getattr(farm, "area_ack_ha", None) is not None else None
            acknowledged = ack is not None and abs(ack - float(area["polygon_area"])) < 0.01
            check(not area.get("divergent") or acknowledged, Pending("area", "Conferir a área do mapa",
                  f"O mapa soma {br_ha(area['polygon_area'])}; o cadastro, {br_ha(area['declared_area'])}.", "Conferir", "territorio"))

    amb_missing = []
    if not profile or profile.altitude_meters is None:
        amb_missing.append("altitude")
    if not profile or not (profile.terrain_slope or "").strip():
        amb_missing.append("relevo")
    if not profile or not (profile.soil_type or "").strip():
        amb_missing.append("solo")
    if amb_missing == ["altitude"]:
        amb_item = Pending("amb", "Informar a altitude", "Ajuda nas recomendações de adubação e do Pezinho.", "Informar", "agronomia", "ambiente")
    else:
        amb_item = Pending("amb", "Completar o ambiente da lavoura", ", ".join(m.capitalize() if i == 0 else m for i, m in enumerate(amb_missing)) + ".", "Completar", "agronomia", "ambiente")
    check(not amb_missing, amb_item)

    soil_missing = []
    if not profile or not profile.last_liming_date:
        soil_missing.append("calagem")
    if not profile or not profile.last_gypsum_date:
        soil_missing.append("gessagem")
    if len(soil_missing) == 1:
        soil_item = Pending("man", f"Informar a última {soil_missing[0]}", "Usada na recomendação de adubação.", "Informar", "agronomia", "manejo")
    else:
        soil_item = Pending("man", "Informar a última calagem e gessagem", "Usadas na recomendação de adubação.", "Informar", "agronomia", "manejo")
    check(not soil_missing, soil_item)

    score = 100 if not pending else min(96, round(done / total * 100)) if total else 100
    return FarmChecks(score=score, pending=pending)


def area_usage(farm, planted_area: float) -> list[dict]:
    total = float(farm.total_area or 0)
    if total <= 0:
        return []
    has_map = bool((farm.boundary_geojson or "").strip())
    reserve = float(farm.car_legal_reserve_ha or 0)
    app_area = float(farm.car_app_ha or 0)
    if not has_map and not farm.car_registration_number and planted_area <= 0:
        return []
    rows = []
    if planted_area > 0:
        rows.append(("Setores de café", planted_area, USE_COLORS["setores"]))
    if reserve > 0:
        rows.append(("Reserva legal", reserve, USE_COLORS["reserva"]))
    if app_area > 0:
        rows.append(("APP", app_area, USE_COLORS["app"]))
    rest = max(0.0, total - planted_area - reserve - app_area)
    if rest > 0.004:
        rows.append(("Sede, estradas e outros" if planted_area > 0 else "Ainda sem setor", rest, USE_COLORS["outros"]))
    result = []
    for label, value, color in rows:
        pct = value / total * 100 if total else 0
        result.append({
            "label": label,
            "value": br_ha(value),
            "pct": round(pct, 1),
            "pct_label": br_number(round(pct, 1), 1).rstrip("0").rstrip(",") + "%",
            "color": color,
        })
    return result


def _field(label: str, value: str | None, *, note: str = "", warn: str = "", readonly: bool = False) -> dict:
    value = (value or "").strip() if isinstance(value, str) else value
    return {
        "label": label,
        "value": value or "",
        "has": bool(value),
        "note": note if value else "",
        "warn": warn if value else "",
        "add": not value and not readonly,
        "dash": not value and readonly,
    }


def agronomy_sections(farm, *, profile, plots, active_season, derived: dict, last_soil_analysis: date | None, today: date) -> list[dict]:
    has_map = bool((farm.boundary_geojson or "").strip())
    prof = profile
    alt = f"{br_number(prof.altitude_meters, 0)} m" if prof and prof.altitude_meters is not None else ""
    ambiente = {
        "id": "ambiente", "title": "Ambiente", "verb": "ambiente", "desc": "Terreno e clima da propriedade.", "editable": True,
        "fields": [
            _field("Altitude média", alt),
            _field("Relevo", prof.terrain_slope if prof else ""),
            _field("Face de exposição ao sol", prof.sun_exposure if prof else ""),
            _field("Clima", prof.climate if prof else "", note="Sugerido pela localização"),
            _field("Solo predominante", prof.soil_type if prof else ""),
        ],
    }
    del has_map
    lavoura_fields = []
    if plots:
        cultivars = []
        for p in plots:
            name = p.variety.name if getattr(p, "variety", None) else ""
            if name and name not in cultivars:
                cultivars.append(name)
        cult_text = ", ".join(cultivars[:3]) + (f" e mais {len(cultivars) - 3}" if len(cultivars) > 3 else "")
        irrigated = [p for p in plots if (p.irrigation_type or "none") != "none"]
        irrigated_area = sum(float(p.area_hectares or 0) for p in irrigated)
        if irrigated and len(irrigated) < len(plots):
            system = "Sequeiro e irrigado"
        elif irrigated:
            system = "Irrigado"
        else:
            system = "Sequeiro"
        irr_text = (
            f"{derived.get('irrigation_system') or 'Irrigação'} em {len(irrigated)} setor{'es' if len(irrigated) > 1 else ''} ({br_ha(irrigated_area)})"
            if irrigated else "Sem irrigação"
        )
        lavoura_fields = [
            _field("Cultura", derived.get("culture") or "Café arábica", readonly=True),
            _field("Cultivares", cult_text, readonly=True),
            _field("Sistema", system, readonly=True),
            _field("Espaçamento mais comum", derived.get("plant_spacing"), readonly=True),
            _field("Irrigação", irr_text, readonly=True),
            _field("Fase atual", derived.get("crop_stage"), readonly=True, note=active_season.name if active_season else ""),
        ]
    lavoura = {
        "id": "lavoura", "title": "Lavoura", "verb": "", "desc": "Resumo automático dos setores e da safra.", "editable": False,
        "src": "Vem de Setores e Safras",
        "empty": "" if plots else "Cadastre os setores para ver aqui cultivares, espaçamento e irrigação, sem digitar de novo.",
        "fields": lavoura_fields,
        "link": "Abrir Setores", "link_note": "Para mudar, edite o setor.", "link_href": "/setores",
    }
    liming = prof.last_liming_date if prof else None
    liming_months = months_ago(liming, today)
    manejo = {
        "id": "manejo", "title": "Manejo do solo", "verb": "manejo", "desc": "Últimas correções. Usadas na recomendação de adubação.", "editable": True,
        "fields": [
            _field("Última calagem", month_label(liming), note=ago_label(liming, today), warn="Mais de 12 meses" if liming_months is not None and liming_months > 12 else ""),
            _field("Última gessagem", month_label(prof.last_gypsum_date if prof else None), note=ago_label(prof.last_gypsum_date if prof else None, today)),
            _field("Última adubação orgânica", month_label(prof.last_organic_matter_date if prof else None), note=ago_label(prof.last_organic_matter_date if prof else None, today)),
            _field("Última análise de solo", month_label(last_soil_analysis), readonly=True, note="Vem de Adubação"),
            _field("Meta de produtividade", f"{br_number(prof.target_productivity_sacks_per_ha)} sc/ha" if prof and prof.target_productivity_sacks_per_ha is not None else ""),
        ],
        "link": "Abrir Adubação", "link_note": "Análises e recomendações", "link_href": "/fertilizacao",
    }
    pests = (prof.common_pests or "") if prof else ""
    historico = {
        "id": "historico", "title": "Histórico e observações", "verb": "histórico", "desc": "O que o técnico e o Pezinho precisam saber.", "editable": True,
        "fields": [
            _field("Pragas e doenças frequentes", pests or derived.get("top_pests")),
            _field("Histórico de podas", prof.pruning_history if prof else ""),
            _field("Limitações atuais", prof.current_limitations if prof else ""),
            _field("Adubos dos últimos 12 meses", derived.get("top_fertilizers"), readonly=True, note="Vem de Adubação"),
        ],
        "link": "Abrir Pragas e Pulverizações", "link_note": "Ocorrências registradas", "link_href": "/pragas",
    }
    sections = [ambiente, lavoura, manejo, historico]
    for sec in sections:
        fields = sec["fields"]
        missing = sum(1 for f in fields if f["add"])
        filled = sum(1 for f in fields if f["has"])
        sec.setdefault("src", "")
        sec.setdefault("empty", "")
        sec.setdefault("link", "")
        sec["missing"] = missing
        sec["missing_label"] = f"{missing} falta" + ("m" if missing > 1 else "")
        if sec["editable"]:
            sec["status"] = f"{filled} de {len(fields)} preenchidos"
        else:
            sec["status"] = "Preenchido automaticamente" if fields else "Aguardando setores"
    return sections


def car_diff_rows(farm, lookup: dict, polygon_area: float | None) -> list[dict]:
    """Comparação 'hoje' x 'pelo CAR' mostrada antes de aplicar uma atualização do CAR."""
    rows = []
    new_area = lookup.get("total_area")
    rows.append(("Área declarada", br_ha(farm.car_declared_area) if farm.car_declared_area is not None else "—", br_ha(new_area) if new_area is not None else "—"))
    rows.append(("Situação", farm.car_status or "—", lookup.get("status") or "—"))
    rows.append(("Condição", farm.car_condition or "—", lookup.get("condition") or "—"))
    rows.append(("Perímetro", f"{br_ha(polygon_area)} no mapa" if polygon_area else "Sem mapa", br_ha(new_area) if new_area is not None else "Disponível"))
    return [{"label": a, "before": b, "after": c, "changed": b != c} for a, b, c in rows]
