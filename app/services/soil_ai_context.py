import unicodedata
from datetime import date, timedelta
from math import isfinite

from sqlalchemy import or_
from sqlalchemy.orm import Session, joinedload, selectinload

from app.core.permissions_catalog import PAGE_AGENDA, PAGE_INPUTS, PAGE_IRRIGATION, PAGE_OPERATIONS
from app.services.soil_measurements import canonical_value, parse_metadata
from app.models import (
    CropSeason, FertilizationRecord, FertilizationSchedule, InputCatalog, InputRecommendation,
    InputRecommendationItem, IrrigationRecord, PurchasedInput, SoilAnalysis,
)


LAB_FIELDS = {
    "ph": "pH", "organic_matter": "matéria orgânica", "phosphorus": "fósforo",
    "potassium": "potássio", "calcium": "cálcio", "magnesium": "magnésio", "aluminum": "alumínio",
    "h_al": "H+Al", "ctc": "CTC", "base_saturation": "saturação por bases (V%)",
}
PROFILE_FIELDS = (
    "culture", "cultivar", "region", "production_system", "climate", "soil_type", "crop_stage",
    "irrigation_system", "plant_spacing", "drip_spacing", "target_productivity_sacks_per_ha",
    "last_liming_date", "last_gypsum_date", "last_organic_matter_date", "fertilizers_used",
    "pruning_history", "current_limitations",
)


def _number(value):
    return float(value) if value is not None and isfinite(value) else None


def _text(value):
    if isinstance(value, str):
        return value.strip()[:2000] or None
    if isinstance(value, date):
        return value.isoformat()
    return _number(value)


def _lab_values(analysis):
    metadata = parse_metadata(analysis.laboratory_metadata)
    units = metadata.get("units", {})
    values = {}
    for field, label in LAB_FIELDS.items():
        value = _number(getattr(analysis, field))
        if value is not None and field in units:
            value = canonical_value(field, value, units[field]) if units[field] else None
        values[label] = value
    return values


def _recent(query, date_column, id_column, limit=30):
    return query.order_by(date_column.desc(), id_column.desc()).limit(limit + 1).all()


def _record_items(items, farm_id):
    result = []
    for item in items:
        purchase = item.purchased_input
        if purchase and purchase.farm_id != farm_id:
            continue
        catalog = item.input_catalog if isinstance(item, InputRecommendationItem) else None
        product = catalog.name if catalog else purchase.name if purchase else None
        result.append({"produto": product, "quantidade_registrada": _number(item.quantity_per_hectare if isinstance(item, InputRecommendation) else item.quantity),
                       "unidade": item.unit})
    return result


def _normalized(value):
    return "".join(char for char in unicodedata.normalize("NFD", (value or "").casefold())
                   if not unicodedata.combining(char)).strip()


def _same_crop(a, b):
    first, second = _normalized(a), _normalized(b)
    return first == second or (
        first.split()[:1] == second.split()[:1] == ["cafe"] and "cafe" in (first, second)
    )


def build_soil_ai_context(db: Session, analysis: SoilAnalysis, today: date, allowed_pages: set[str]) -> dict:
    farm, plot = analysis.farm, analysis.plot
    if not farm or not plot or plot.farm_id != farm.id or analysis.farm_id != farm.id:
        raise ValueError("O setor da análise de solo não pertence à fazenda.")

    profile = farm.agronomic_profile
    planting = plot.active_planting
    planting_start = planting.planting_date if planting else plot.planting_date
    start = max(today - timedelta(days=365), planting_start or date.min)
    crop = planting.culture if planting else profile.culture if profile else None
    area = _number(plot.area_hectares)
    expected_sacks = _number(planting.estimated_yield_sacks if planting else plot.estimated_yield_sacks)
    scope = {"fazenda": farm.name, "setor": plot.name, "cultura_do_setor": crop,
             "localidade": farm.location, "area_ha": area, "plantio": _text(planting_start),
             "estimativa_setor_sacas_ha": expected_sacks / area if expected_sacks and expected_sacks > 0 and area and area > 0 else None,
             "historico_desde": start.isoformat(), "dados_em": today.isoformat()}

    metadata = parse_metadata(analysis.laboratory_metadata)
    sample_date = date.fromisoformat(metadata["collected"]) if metadata.get("collected") else analysis.analysis_date
    matching_planting = not planting_start or planting_start <= sample_date <= today
    seasons = db.query(CropSeason.id, CropSeason.culture).filter(CropSeason.farm_id == farm.id)
    season_ids = [row.id for row in seasons if not crop or _same_crop(row.culture, crop)]

    applied = _recent(
        db.query(FertilizationRecord).options(selectinload(FertilizationRecord.items)).filter(
            FertilizationRecord.plot_id == plot.id, FertilizationRecord.application_date.between(start, today),
            or_(FertilizationRecord.season_id.is_(None), FertilizationRecord.season_id.in_(season_ids)),
        ), FertilizationRecord.application_date, FertilizationRecord.id,
    ) if matching_planting and PAGE_OPERATIONS in allowed_pages else []
    scheduled = _recent(
        db.query(FertilizationSchedule).options(selectinload(FertilizationSchedule.items)).filter(
            FertilizationSchedule.plot_id == plot.id, FertilizationSchedule.status == "scheduled",
            FertilizationSchedule.fertilization_record_id.is_(None),
            FertilizationSchedule.scheduled_date.between(max(start, today - timedelta(days=30)), today + timedelta(days=90)),
            or_(FertilizationSchedule.season_id.is_(None), FertilizationSchedule.season_id.in_(season_ids)),
        ), FertilizationSchedule.scheduled_date, FertilizationSchedule.id,
    ) if matching_planting and PAGE_AGENDA in allowed_pages else []
    recommendations = db.query(InputRecommendation).options(
        joinedload(InputRecommendation.purchased_input),
        selectinload(InputRecommendation.items).joinedload(InputRecommendationItem.input_catalog),
        selectinload(InputRecommendation.items).joinedload(InputRecommendationItem.purchased_input),
    ).filter(matching_planting and PAGE_OPERATIONS in allowed_pages, InputRecommendation.farm_id == farm.id, or_(
        InputRecommendation.plot_id == plot.id, InputRecommendation.plot_id.is_(None),
    )).order_by(InputRecommendation.id.desc()).limit(31).all()

    stock = db.query(PurchasedInput).join(PurchasedInput.input_catalog).options(
        joinedload(PurchasedInput.input_catalog),
    ).filter(
        matching_planting and PAGE_INPUTS in allowed_pages,
        PurchasedInput.farm_id == farm.id, PurchasedInput.available_quantity > 0,
        or_(PurchasedInput.expiration_date.is_(None), PurchasedInput.expiration_date >= today),
        or_(PurchasedInput.purchase_date.is_(None), PurchasedInput.purchase_date <= today),
        InputCatalog.is_active.is_(True), InputCatalog.category.in_(("Fertilizantes", "Corretivos de Solo")),
    ).order_by(PurchasedInput.available_quantity.desc(), PurchasedInput.id).limit(61).all()
    irrigation = db.query(IrrigationRecord.irrigation_date, IrrigationRecord.volume_liters).filter(
        matching_planting and PAGE_IRRIGATION in allowed_pages, IrrigationRecord.plot_id == plot.id,
        IrrigationRecord.irrigation_date.between(max(start, today - timedelta(days=90)), today),
        IrrigationRecord.origin == "manual",
    ).order_by(IrrigationRecord.irrigation_date.desc(), IrrigationRecord.id.desc()).limit(31).all()
    prior = db.query(SoilAnalysis).filter(
        SoilAnalysis.farm_id == farm.id, SoilAnalysis.plot_id == plot.id,
        SoilAnalysis.analysis_date >= start, SoilAnalysis.analysis_date < analysis.analysis_date,
    ).order_by(SoilAnalysis.analysis_date.desc(), SoilAnalysis.id.desc()).limit(3).all()

    checks = []
    reading = _lab_values(analysis)
    if any(value is not None and (value < 0 or (key == "pH" and not 0 < value <= 14)
           or (key == "saturação por bases (V%)" and value > 100)) for key, value in reading.items()):
        checks.append("Conferir valores laboratoriais fora da faixa física.")
    if not start <= sample_date <= today:
        checks.append("A data da amostra está fora da janela do plantio e histórico atuais.")
    truncated = max(len(applied), len(scheduled), len(recommendations)) > 30
    if truncated:
        checks.append("O histórico de manejo foi limitado aos 30 registros mais recentes por módulo.")

    if PAGE_OPERATIONS not in allowed_pages:
        checks.append("Aplicações e recomendações cadastradas indisponíveis para esta consulta.")
    profile_fields = PROFILE_FIELDS
    if not matching_planting or (profile and crop and not _same_crop(profile.culture, crop)):
        profile_fields = ("region", "climate", "soil_type")
        checks.append("Confirme a fase e o manejo do plantio correspondente a esta amostra.")
    source_pages = sorted({page for page, rows in [
        (PAGE_OPERATIONS, applied or recommendations), (PAGE_AGENDA, scheduled),
        (PAGE_INPUTS, stock), (PAGE_IRRIGATION, irrigation),
    ] if rows})

    return {
        "escopo": scope,
        "modulos_consultados": sorted(allowed_pages & {PAGE_OPERATIONS, PAGE_AGENDA, PAGE_INPUTS, PAGE_IRRIGATION}),
        "modulos_utilizados": source_pages,
        "historico_truncado": truncated,
        "perfil": {field: _text(getattr(profile, field)) for field in profile_fields} if profile else {},
        "solo": {"referencia": "solo", "data": sample_date.isoformat(), "data_analise": analysis.analysis_date.isoformat(), "laboratorio": analysis.laboratory,
                 "resultados": reading, "observacoes": _text(analysis.observations),
                 "metadados_laboratoriais": parse_metadata(analysis.laboratory_metadata),
                 "valores_originais": {label: _number(getattr(analysis, field)) for field, label in LAB_FIELDS.items()},
                 "unidades_resultados_normalizados": {"matéria orgânica": "g/dm³", "fósforo": "mg/dm³",
                     "potássio": "mg/dm³", "cálcio": "cmolc/dm³", "magnésio": "cmolc/dm³",
                     "alumínio": "cmolc/dm³", "H+Al": "cmolc/dm³", "CTC": "cmolc/dm³"} if metadata else {},
                 "aviso_unidades": "Registros sem unidade declarada devem ser confirmados no laudo."},

        "analises_anteriores": [{"data": row.analysis_date.isoformat(), "resultados": _lab_values(row)}
                               for row in prior],
        "aplicacoes": [
            {"referencia": f"aplicacao:{row.id}", "data": row.application_date.isoformat(),
             "metodo": row.application_method,
             "produtos": [{"produto": item.name, "quantidade_total": _number(item.total_quantity),
                           "unidade": item.unit} for item in row.items]
             or [{"produto": row.product, "dose_original": row.dose}]}
            for row in applied[:30]
        ],
        "agendamentos_pendentes": [
            {"referencia": f"agendamento:{row.id}", "data": row.scheduled_date.isoformat(),
             "metodo": row.application_method,
             "produtos": [{"produto": item.name, "quantidade_total": _number(item.quantity), "unidade": item.unit}
                          for item in row.items]}
            for row in scheduled[:30]
        ],
        "recomendacoes_cadastradas_sem_data": [
            {"referencia": f"recomendacao:{row.id}", "nome": row.application_name,
             "abrangencia": "setor" if row.plot_id else "fazenda", "observacoes": _text(row.notes),
             "produtos": _record_items(row.items or [row], farm.id)}
            for row in recommendations[:30]
        ],
        "estoque_atual": [
            {"referencia": f"estoque:{row.id}", "produto": row.name, "saldo": _number(row.available_quantity),
             "unidade": row.package_unit, "validade": _text(row.expiration_date),
             "informacoes_rotulo": _text(row.notes)}
            for row in stock[:60]
        ],
        "irrigacoes_manuais": [{"data": row.irrigation_date.isoformat(), "volume_l": _number(row.volume_liters)}
                               for row in irrigation[:30]],
        "alertas_de_qualidade": checks,
    }
