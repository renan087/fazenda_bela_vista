"""Reúne do banco os dados da fazenda, do setor e do manejo usados pelo motor agronômico."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

from sqlalchemy import or_
from sqlalchemy.orm import Session, selectinload

from app.core.permissions_catalog import PAGE_AGENDA, PAGE_INPUTS, PAGE_OPERATIONS
from app.models import FertilizationRecord, FertilizationSchedule, InputCatalog, PurchasedInput, SoilAnalysis
from app.models.harvest import HarvestRecord
from app.services.agronomy.products import StockLot, is_lime, normalize, parse_guarantee, parse_prnt, to_kg
from app.services.agronomy.soil import SoilReading, read_soil

CANEPHORA_WORDS = ("conilon", "robusta", "canephora")
METHOD_LABELS = {"fertirrigacao": "Fertirrigação", "foliar": "Foliar", "solo": "Solo", "lanco": "A lanço"}


@dataclass
class Application:
    ref: str
    when: date
    method: str
    products: list
    nutrients: dict          # kg/ha por nutriente contabilizado
    uncounted: list          # produtos sem garantia ou unidade conversível


@dataclass
class FarmInputs:
    today: date
    farm_name: str
    plot_name: str
    plot_id: int
    region: str
    culture: str
    species: str | None
    species_source: str
    stage: str | None            # plantio | formacao | producao
    stage_source: str
    age_months: int | None
    area_ha: float | None
    plants_ha: float | None
    plants_source: str
    irrigation: str              # sequeiro | gotejo | aspersor
    yield_target: float | None
    yield_source: str
    previous_yield: float | None
    plot_texture: str | None
    reading: SoilReading
    subsurface: SoilReading | None
    last_liming: date | None
    last_gypsum: date | None
    cycle_start: date
    applications: list = field(default_factory=list)
    scheduled: list = field(default_factory=list)
    stock: list = field(default_factory=list)
    lime_lots: list = field(default_factory=list)
    modules: list = field(default_factory=list)
    history_available: bool = True
    notes: list = field(default_factory=list)

    @property
    def is_coffee(self) -> bool:
        return "cafe" in normalize(self.culture) or self.species is not None

    def per_plant_grams(self, kg_ha: float) -> float | None:
        return kg_ha * 1000 / self.plants_ha if self.plants_ha else None

    def total_kg(self, kg_ha: float) -> float | None:
        return kg_ha * self.area_ha if self.area_ha else None


def _float(value):
    return float(value) if value is not None else None


def cycle_start(today: date) -> date:
    """Ano agrícola do café: de julho a junho (adubação no período chuvoso, colheita no ano seguinte)."""
    return date(today.year if today.month >= 7 else today.year - 1, 7, 1)


def _species(planting, plot, profile, culture: str) -> tuple[str | None, str]:
    variety = getattr(planting, "variety", None) or getattr(plot, "variety", None)
    if variety and variety.species:
        name = normalize(variety.species)
        return ("canephora" if any(w in name for w in CANEPHORA_WORDS) else "arabica"), f"variedade {variety.name}"
    for text, origin in ((culture, "cultura do setor"), (getattr(profile, "cultivar", None), "cultivar do perfil"),
                         (getattr(profile, "culture", None), "cultura do perfil")):
        name = normalize(text)
        if any(w in name for w in CANEPHORA_WORDS):
            return "canephora", origin
        if "arabica" in name:
            return "arabica", origin
    return None, ""


def _stage(planting_date: date | None, profile, today: date) -> tuple[str | None, int | None, str]:
    if planting_date and planting_date <= today:
        months = (today.year - planting_date.year) * 12 + today.month - planting_date.month
        stage = "plantio" if months < 12 else "formacao" if months < 36 else "producao"
        return stage, months, f"plantio em {planting_date.strftime('%m/%Y')}"
    text = normalize(getattr(profile, "crop_stage", None))
    for word, stage in (("produc", "producao"), ("formac", "formacao"), ("plantio", "plantio"), ("implant", "plantio")):
        if word in text:
            return stage, None, "fase informada no perfil agronômico"
    return None, None, ""


def _plants(planting, plot, area) -> tuple[float | None, str]:
    count = getattr(planting, "plant_count", None) or plot.plant_count
    if count and area:
        return count / area, "número de plantas do setor"
    row = _float(getattr(planting, "spacing_row_meters", None) or plot.spacing_row_meters)
    between = _float(getattr(planting, "spacing_plant_meters", None) or plot.spacing_plant_meters)
    if row and between:
        return 10000 / (row * between), f"espaçamento {row:g} × {between:g} m"
    return None, ""


def _yield(planting, plot, profile, area, db, today) -> tuple[float | None, str, float | None]:
    previous = None
    rows = db.query(HarvestRecord.sacks_produced).filter(
        HarvestRecord.plot_id == plot.id, HarvestRecord.harvest_date.between(today - timedelta(days=365), today),
    ).all()
    if rows and area:
        previous = sum(float(row.sacks_produced or 0) for row in rows) / area or None
    estimate = _float(getattr(planting, "estimated_yield_sacks", None) or plot.estimated_yield_sacks)
    if estimate and area:
        return estimate / area, "estimativa de produção do setor", previous
    target = _float(getattr(profile, "target_productivity_sacks_per_ha", None))
    if target:
        return target, "meta de produtividade do perfil da fazenda", previous
    if previous:
        return previous, "colheita registrada nos últimos 12 meses", previous
    return None, "", previous


def _irrigation(plot, profile) -> str:
    kind = (plot.irrigation_type or "none").strip()
    if kind in ("gotejo", "aspersor"):
        return kind
    text = normalize(getattr(profile, "irrigation_system", None))
    if "gotej" in text:
        return "gotejo"
    if "aspers" in text or "pivo" in text:
        return "aspersor"
    return "sequeiro"


def _application_nutrients(items, area) -> tuple[dict, list, list]:
    nutrients, products, uncounted = {}, [], []
    for name, quantity, unit, notes in items:
        products.append({"produto": name, "quantidade": quantity, "unidade": unit})
        guarantee, _ = parse_guarantee(name, notes)
        kg = to_kg(quantity, unit)
        if not guarantee or kg is None or not area:
            uncounted.append(name)
            continue
        for nutrient, pct in guarantee.items():
            nutrients[nutrient] = nutrients.get(nutrient, 0.0) + kg / area * pct / 100
    return nutrients, products, uncounted


def _subsurface(db, analysis, reading) -> SoilReading | None:
    if not reading.depth or reading.depth[0] >= 15:
        return None
    window = timedelta(days=365)
    candidates = db.query(SoilAnalysis).filter(
        SoilAnalysis.plot_id == analysis.plot_id, SoilAnalysis.id != analysis.id,
        SoilAnalysis.analysis_date.between(analysis.analysis_date - window, analysis.analysis_date + window),
    ).order_by(SoilAnalysis.analysis_date.desc()).all()
    for candidate in candidates:
        other = read_soil(candidate)
        if other.is_subsurface:
            return other
    return None


def gather(db: Session, analysis: SoilAnalysis, today: date, allowed_pages: set[str]) -> FarmInputs:
    farm, plot = analysis.farm, analysis.plot
    if not farm or not plot or plot.farm_id != farm.id or analysis.farm_id != farm.id:
        raise ValueError("O setor da análise de solo não pertence à fazenda.")
    profile = farm.agronomic_profile
    planting = plot.active_planting
    culture = (getattr(planting, "culture", None) or getattr(profile, "culture", None) or "Café").strip()
    area = _float(plot.area_hectares) or None
    species, species_source = _species(planting, plot, profile, culture)
    planting_date = getattr(planting, "planting_date", None) or plot.planting_date
    stage, age, stage_source = _stage(planting_date, profile, today)
    plants_ha, plants_source = _plants(planting, plot, area)
    yield_target, yield_source, previous = _yield(planting, plot, profile, area, db, today)
    reading = read_soil(analysis)
    start = cycle_start(today)
    inputs = FarmInputs(
        today=today, farm_name=farm.name, plot_name=plot.name, plot_id=plot.id,
        region=" ".join(filter(None, (getattr(profile, "region", None), farm.location))),
        culture=culture, species=species, species_source=species_source, stage=stage, stage_source=stage_source,
        age_months=age, area_ha=area, plants_ha=plants_ha, plants_source=plants_source,
        irrigation=_irrigation(plot, profile), yield_target=yield_target, yield_source=yield_source,
        previous_yield=previous, plot_texture=plot.soil_texture, reading=reading,
        subsurface=_subsurface(db, analysis, reading),
        last_liming=getattr(profile, "last_liming_date", None), last_gypsum=getattr(profile, "last_gypsum_date", None),
        cycle_start=start,
    )
    if PAGE_OPERATIONS in allowed_pages:
        records = db.query(FertilizationRecord).options(selectinload(FertilizationRecord.items)).filter(
            FertilizationRecord.plot_id == plot.id, FertilizationRecord.application_date.between(start, today),
        ).order_by(FertilizationRecord.application_date).all()
        for row in records:
            items = [(item.name, _float(item.total_quantity), item.unit,
                      item.purchased_input.notes if item.purchased_input else None) for item in row.items]
            if not items:
                items = [(row.product, None, None, None)]
            nutrients, products, uncounted = _application_nutrients(items, area)
            inputs.applications.append(Application(f"aplicacao:{row.id}", row.application_date,
                                                   row.application_method, products, nutrients, uncounted))
        inputs.modules.append(PAGE_OPERATIONS)
    else:
        inputs.history_available = False
    if PAGE_AGENDA in allowed_pages:
        schedules = db.query(FertilizationSchedule).options(selectinload(FertilizationSchedule.items)).filter(
            FertilizationSchedule.plot_id == plot.id, FertilizationSchedule.status == "scheduled",
            FertilizationSchedule.fertilization_record_id.is_(None),
            FertilizationSchedule.scheduled_date.between(today, start.replace(year=start.year + 1)),
        ).order_by(FertilizationSchedule.scheduled_date).all()
        for row in schedules:
            items = [(item.name, _float(item.quantity), item.unit,
                      item.purchased_input.notes if item.purchased_input else None) for item in row.items]
            nutrients, products, uncounted = _application_nutrients(items, area)
            inputs.scheduled.append(Application(f"agendamento:{row.id}", row.scheduled_date,
                                                row.application_method, products, nutrients, uncounted))
        inputs.modules.append(PAGE_AGENDA)
    else:
        inputs.history_available = False
    if PAGE_INPUTS in allowed_pages:
        lots = db.query(PurchasedInput).join(PurchasedInput.input_catalog).filter(
            PurchasedInput.farm_id == farm.id, PurchasedInput.available_quantity > 0,
            or_(PurchasedInput.expiration_date.is_(None), PurchasedInput.expiration_date >= today),
            InputCatalog.is_active.is_(True), InputCatalog.category.in_(("Fertilizantes", "Corretivos de Solo")),
        ).order_by(PurchasedInput.available_quantity.desc()).limit(80).all()
        for lot in lots:
            guarantee, origin = parse_guarantee(lot.name, lot.notes)
            entry = StockLot(f"estoque:{lot.id}", lot.name, float(lot.available_quantity), lot.package_unit,
                             guarantee, origin, parse_prnt(lot.name, lot.notes), lot.notes or "")
            if is_lime(lot.name):
                inputs.lime_lots.append(entry)
            elif guarantee:
                inputs.stock.append(entry)
        inputs.modules.append(PAGE_INPUTS)
    return inputs
