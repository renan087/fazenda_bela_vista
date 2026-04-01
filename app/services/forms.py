import json
import math
import unicodedata
from datetime import date
from decimal import Decimal

from app.core.security import get_password_hash
from app.core.timezone import today_in_app_timezone
from app.models import (
    AgronomicProfile,
    CoffeeVariety,
    CropSeason,
    EquipmentAsset,
    Farm,
    FertilizationSchedule,
    FertilizationScheduleItem,
    FertilizationStockAllocation,
    FertilizationItem,
    FertilizationRecord,
    HarvestRecord,
    InputCatalog,
    InputRecommendation,
    InputRecommendationItem,
    IrrigationRecord,
    PestIncident,
    Plot,
    PurchasedInput,
    RainfallRecord,
    SoilAnalysis,
    StockOutput,
    User,
)
from app.repositories.farm import FarmRepository

MANUAL_STOCK_OUTPUT_REFERENCE = "manual_stock_output"
MANUAL_STOCK_OUTPUT_ALLOCATION = "manual_stock_output_allocation"


def _suggest_crop_season_name(start_date_value: str | date | None, end_date_value: str | date | None) -> str:
    if not start_date_value or not end_date_value:
        return ""
    start = date.fromisoformat(start_date_value) if isinstance(start_date_value, str) else start_date_value
    end = date.fromisoformat(end_date_value) if isinstance(end_date_value, str) else end_date_value
    return f"Safra {start.year}/{end.year}"


def create_farm(repository: FarmRepository, form: dict) -> Farm:
    return repository.create(
        Farm(
            name=form["name"],
            location=form["location"],
            total_area=form["total_area"],
            boundary_geojson=form.get("boundary_geojson"),
            notes=form.get("notes"),
        )
    )


def create_plot(repository: FarmRepository, form: dict) -> Plot:
    plot = Plot(
        name=form["name"],
        area_hectares=form["area_hectares"],
        location=form.get("location"),
        planting_date=date.fromisoformat(form["planting_date"]) if form.get("planting_date") else None,
        plant_count=form["plant_count"],
        spacing_row_meters=form.get("spacing_row_meters"),
        spacing_plant_meters=form.get("spacing_plant_meters"),
        estimated_yield_sacks=form.get("estimated_yield_sacks"),
        centroid_lat=form.get("centroid_lat"),
        centroid_lng=form.get("centroid_lng"),
        boundary_geojson=form.get("boundary_geojson"),
        irrigation_type=form.get("irrigation_type") or "none",
        irrigation_line_count=form.get("irrigation_line_count"),
        irrigation_line_length_meters=form.get("irrigation_line_length_meters"),
        drip_spacing_meters=form.get("drip_spacing_meters"),
        drip_liters_per_hour=form.get("drip_liters_per_hour"),
        sprinkler_count=form.get("sprinkler_count"),
        sprinkler_liters_per_hour=form.get("sprinkler_liters_per_hour"),
        notes=form.get("notes"),
        farm_id=form.get("farm_id"),
        variety_id=form.get("variety_id"),
    )
    return repository.create(plot)


def update_plot(repository: FarmRepository, plot: Plot, form: dict) -> Plot:
    return repository.update(
        plot,
        {
            "name": form["name"],
            "area_hectares": form["area_hectares"],
            "location": form.get("location"),
            "planting_date": date.fromisoformat(form["planting_date"]) if form.get("planting_date") else None,
            "plant_count": form["plant_count"],
            "spacing_row_meters": form.get("spacing_row_meters"),
            "spacing_plant_meters": form.get("spacing_plant_meters"),
            "estimated_yield_sacks": form.get("estimated_yield_sacks"),
            "centroid_lat": form.get("centroid_lat"),
            "centroid_lng": form.get("centroid_lng"),
            "boundary_geojson": form.get("boundary_geojson"),
            "irrigation_type": form.get("irrigation_type") or "none",
            "irrigation_line_count": form.get("irrigation_line_count"),
            "irrigation_line_length_meters": form.get("irrigation_line_length_meters"),
            "drip_spacing_meters": form.get("drip_spacing_meters"),
            "drip_liters_per_hour": form.get("drip_liters_per_hour"),
            "sprinkler_count": form.get("sprinkler_count"),
            "sprinkler_liters_per_hour": form.get("sprinkler_liters_per_hour"),
            "notes": form.get("notes"),
            "farm_id": form.get("farm_id"),
            "variety_id": form.get("variety_id"),
        },
    )


def update_farm(repository: FarmRepository, farm: Farm, form: dict) -> Farm:
    return repository.update(
        farm,
        {
            "name": form["name"],
            "location": form["location"],
            "total_area": form["total_area"],
            "boundary_geojson": form.get("boundary_geojson"),
            "notes": form.get("notes"),
        },
    )


def create_variety(repository: FarmRepository, form: dict) -> CoffeeVariety:
    return repository.create(
        CoffeeVariety(
            name=form["name"],
            species=form["species"],
            maturation_cycle=form["maturation_cycle"],
            flavor_profile=form.get("flavor_profile"),
            notes=form.get("notes"),
        )
    )


def update_variety(repository: FarmRepository, variety: CoffeeVariety, form: dict) -> CoffeeVariety:
    return repository.update(
        variety,
        {
            "name": form["name"],
            "species": form["species"],
            "maturation_cycle": form["maturation_cycle"],
            "flavor_profile": form.get("flavor_profile"),
            "notes": form.get("notes"),
        },
    )


def create_crop_season(repository: FarmRepository, form: dict) -> CropSeason:
    return repository.create(
        CropSeason(
            farm_id=form["farm_id"],
            variety_id=form.get("variety_id"),
            name=(form.get("name") or "").strip() or _suggest_crop_season_name(form.get("start_date"), form.get("end_date")),
            start_date=date.fromisoformat(form["start_date"]),
            end_date=date.fromisoformat(form["end_date"]),
            culture=form["culture"],
            cultivated_area=form["cultivated_area"],
            area_unit=form.get("area_unit") or "ha",
            notes=form.get("notes"),
            status=form.get("status") or "planejada",
        )
    )


def update_crop_season(repository: FarmRepository, crop_season: CropSeason, form: dict) -> CropSeason:
    return repository.update(
        crop_season,
        {
            "farm_id": form["farm_id"],
            "variety_id": form.get("variety_id"),
            "name": (form.get("name") or "").strip() or _suggest_crop_season_name(form.get("start_date"), form.get("end_date")),
            "start_date": date.fromisoformat(form["start_date"]),
            "end_date": date.fromisoformat(form["end_date"]),
            "culture": form["culture"],
            "cultivated_area": form["cultivated_area"],
            "area_unit": form.get("area_unit") or "ha",
            "notes": form.get("notes"),
            "status": form.get("status") or "planejada",
        },
    )


def create_irrigation(repository: FarmRepository, form: dict) -> IrrigationRecord:
    return repository.create(
        IrrigationRecord(
            plot_id=form["plot_id"],
            irrigation_date=date.fromisoformat(form["irrigation_date"]),
            volume_liters=form["volume_liters"],
            duration_minutes=form["duration_minutes"],
            notes=form.get("notes"),
        )
    )


def update_irrigation(repository: FarmRepository, irrigation: IrrigationRecord, form: dict) -> IrrigationRecord:
    return repository.update(
        irrigation,
        {
            "plot_id": form["plot_id"],
            "irrigation_date": date.fromisoformat(form["irrigation_date"]),
            "volume_liters": form["volume_liters"],
            "duration_minutes": form["duration_minutes"],
            "notes": form.get("notes"),
        },
    )


def create_rainfall(repository: FarmRepository, form: dict) -> RainfallRecord:
    return repository.create(
        RainfallRecord(
            farm_id=form["farm_id"],
            rainfall_date=date.fromisoformat(form["rainfall_date"]),
            millimeters=form["millimeters"],
            source=form.get("source"),
            notes=form.get("notes"),
        )
    )


def update_rainfall(repository: FarmRepository, rainfall: RainfallRecord, form: dict) -> RainfallRecord:
    return repository.update(
        rainfall,
        {
            "farm_id": form["farm_id"],
            "rainfall_date": date.fromisoformat(form["rainfall_date"]),
            "millimeters": form["millimeters"],
            "source": form.get("source"),
            "notes": form.get("notes"),
        },
    )


def create_user(repository: FarmRepository, form: dict) -> User:
    return repository.create(
        User(
            name=form["name"],
            email=form["email"].strip().lower(),
            hashed_password=get_password_hash(form["password"]),
            is_active=bool(form.get("is_active", True)),
            is_admin=bool(form.get("is_admin", False)),
        )
    )


def update_user(repository: FarmRepository, user: User, form: dict) -> User:
    payload = {
        "name": form["name"],
        "email": form["email"].strip().lower(),
        "is_active": bool(form.get("is_active", True)),
        "is_admin": bool(form.get("is_admin", False)),
    }
    password = (form.get("password") or "").strip()
    if password:
        payload["hashed_password"] = get_password_hash(password)
    return repository.update(user, payload)


def _normalize_input_name(value: str | None) -> str:
    normalized = unicodedata.normalize("NFKD", (value or "").strip())
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = " ".join(normalized.split())
    return normalized.lower()


def _resolve_input_catalog(
    repository: FarmRepository,
    name: str,
    default_unit: str,
    item_type: str = "insumo_agricola",
    low_stock_threshold: float | None = None,
) -> InputCatalog:
    normalized_name = _normalize_input_name(name)
    existing = repository.get_input_catalog_by_normalized_name(normalized_name)
    if existing:
        if item_type and existing.item_type != item_type:
            existing.item_type = existing.item_type or item_type
        if default_unit and existing.default_unit != default_unit:
            existing.default_unit = existing.default_unit or default_unit
        if low_stock_threshold is not None and low_stock_threshold > 0:
            existing.low_stock_threshold = low_stock_threshold
        repository.db.add(existing)
        repository.db.flush()
        return existing
    catalog = InputCatalog(
        name=" ".join((name or "").strip().split()),
        normalized_name=normalized_name,
        item_type=item_type or "insumo_agricola",
        default_unit=default_unit or "kg",
        low_stock_threshold=low_stock_threshold if low_stock_threshold is not None else None,
        is_active=True,
    )
    repository.db.add(catalog)
    repository.db.flush()
    return catalog


def _catalog_available_stock(repository: FarmRepository, input_id: int | None, farm_id: int | None = None, unit: str | None = None) -> float:
    if not input_id:
        return 0.0
    total = 0.0
    for entry in repository.list_purchased_inputs():
        if entry.input_id != input_id:
            continue
        if farm_id is not None and entry.farm_id not in (None, farm_id):
            continue
        if unit and entry.package_unit != unit:
            continue
        total += float(entry.available_quantity or 0)
    return round(total, 2)


def create_purchased_input(repository: FarmRepository, form: dict) -> PurchasedInput:
    quantity_purchased = float(form["quantity_purchased"])
    package_size = float(form["package_size"])
    unit_price = float(form["unit_price"])
    total_quantity = round(quantity_purchased * package_size, 2)
    low_stock_threshold = float(form.get("low_stock_threshold") or 0)
    item_type = form.get("item_type") or "insumo_agricola"
    catalog = _resolve_input_catalog(
        repository,
        form["name"],
        form["package_unit"],
        item_type,
        low_stock_threshold if low_stock_threshold > 0 else None,
    )
    item = PurchasedInput(
        input_id=catalog.id,
        farm_id=form.get("farm_id"),
        name=catalog.name,
        normalized_name=catalog.normalized_name,
        quantity_purchased=quantity_purchased,
        package_size=package_size,
        package_unit=form["package_unit"],
        unit_price=unit_price,
        purchase_date=date.fromisoformat(form["purchase_date"]) if form.get("purchase_date") else today_in_app_timezone(),
        total_quantity=total_quantity,
        available_quantity=total_quantity,
        total_cost=round(quantity_purchased * unit_price, 2),
        low_stock_threshold=low_stock_threshold,
        notes=form.get("notes"),
    )
    repository.db.add(item)
    repository.db.commit()
    repository.db.refresh(item)
    return item


def update_purchased_input(repository: FarmRepository, item: PurchasedInput, form: dict) -> PurchasedInput:
    quantity_purchased = float(form["quantity_purchased"])
    package_size = float(form["package_size"])
    unit_price = float(form["unit_price"])
    total_quantity = round(quantity_purchased * package_size, 2)
    consumed_quantity = max(float(item.total_quantity or 0) - float(item.available_quantity or 0), 0)
    available_quantity = max(round(total_quantity - consumed_quantity, 2), 0)
    low_stock_threshold = float(form.get("low_stock_threshold") or 0)
    item_type = form.get("item_type") or "insumo_agricola"
    catalog = _resolve_input_catalog(
        repository,
        form["name"],
        form["package_unit"],
        item_type,
        low_stock_threshold if low_stock_threshold > 0 else None,
    )
    return repository.update(
        item,
        {
            "input_id": catalog.id,
            "farm_id": form.get("farm_id"),
            "name": catalog.name,
            "normalized_name": catalog.normalized_name,
            "quantity_purchased": quantity_purchased,
            "package_size": package_size,
            "package_unit": form["package_unit"],
            "unit_price": unit_price,
            "purchase_date": date.fromisoformat(form["purchase_date"]) if form.get("purchase_date") else item.purchase_date,
            "total_quantity": total_quantity,
            "available_quantity": available_quantity,
            "total_cost": round(quantity_purchased * unit_price, 2),
            "low_stock_threshold": low_stock_threshold,
            "notes": form.get("notes"),
        },
    )


def create_equipment_asset(repository: FarmRepository, form: dict) -> EquipmentAsset:
    return repository.create(
        EquipmentAsset(
            farm_id=form.get("farm_id"),
            name=form["name"],
            category=form["category"],
            brand_model=form.get("brand_model"),
            asset_code=form.get("asset_code"),
            acquisition_date=date.fromisoformat(form["acquisition_date"]) if form.get("acquisition_date") else None,
            acquisition_value=form.get("acquisition_value"),
            status=form.get("status") or "ativo",
            notes=form.get("notes"),
        )
    )


def update_equipment_asset(repository: FarmRepository, asset: EquipmentAsset, form: dict) -> EquipmentAsset:
    return repository.update(
        asset,
        {
            "farm_id": form.get("farm_id"),
            "name": form["name"],
            "category": form["category"],
            "brand_model": form.get("brand_model"),
            "asset_code": form.get("asset_code"),
            "acquisition_date": date.fromisoformat(form["acquisition_date"]) if form.get("acquisition_date") else None,
            "acquisition_value": form.get("acquisition_value"),
            "status": form.get("status") or "ativo",
            "notes": form.get("notes"),
        },
    )


def _manual_stock_output_allocations(repository: FarmRepository, output_id: int) -> list[StockOutput]:
    return (
        repository.db.query(StockOutput)
        .filter(
            StockOutput.reference_type == MANUAL_STOCK_OUTPUT_ALLOCATION,
            StockOutput.reference_id == output_id,
        )
        .order_by(StockOutput.id.asc())
        .all()
    )


def create_manual_stock_output(repository: FarmRepository, form: dict) -> StockOutput:
    input_id = int(form["input_id"])
    quantity = float(form["quantity"])
    if quantity <= 0:
        raise ValueError("Informe uma quantidade valida para a saida manual.")

    input_catalog = repository.get_input_catalog(input_id)
    if not input_catalog:
        raise ValueError("Insumo nao encontrado para a saida manual.")

    unit = (form.get("unit") or input_catalog.default_unit or "kg").strip()
    farm_id = form.get("farm_id")
    plot_id = form.get("plot_id")
    movement_date = date.fromisoformat(form["movement_date"]) if form.get("movement_date") else today_in_app_timezone()
    season_id = _resolve_season_for_farm(repository, farm_id, movement_date, form.get("season_id"))

    available = _catalog_available_stock(repository, input_catalog.id, farm_id, unit)
    if quantity > available:
        missing = round(quantity - available, 2)
        raise ValueError(f"Estoque insuficiente. Necessario comprar {missing} {unit} de {input_catalog.name}.")

    candidate_lots = _find_candidate_lots(repository, input_catalog.id, None, input_catalog.name, unit, farm_id)
    candidate_lots = sorted(candidate_lots, key=lambda item: (item.purchase_date or today_in_app_timezone(), item.id))

    output = StockOutput(
        input_id=input_catalog.id,
        purchased_input_id=None,
        farm_id=farm_id,
        plot_id=plot_id,
        season_id=season_id,
        movement_date=movement_date,
        quantity=round(quantity, 2),
        unit=unit,
        origin="manual",
        reference_type=MANUAL_STOCK_OUTPUT_REFERENCE,
        reference_id=None,
        unit_cost=0,
        total_cost=0,
        notes=form.get("notes"),
    )
    repository.db.add(output)
    repository.db.flush()

    remaining = quantity
    total_cost = 0.0
    for lot in candidate_lots:
        if remaining <= 0:
            break
        lot_available = _available_stock_for_input(lot)
        consumed = min(lot_available, remaining)
        unit_cost = float(lot.total_cost or 0) / max(float(lot.total_quantity or 0), 1)
        lot.available_quantity = round(lot_available - consumed, 2)
        allocation = StockOutput(
            input_id=input_catalog.id,
            purchased_input_id=lot.id,
            farm_id=farm_id,
            plot_id=plot_id,
            season_id=season_id,
            movement_date=movement_date,
            quantity=consumed,
            unit=unit,
            origin="manual",
            reference_type=MANUAL_STOCK_OUTPUT_ALLOCATION,
            reference_id=output.id,
            unit_cost=round(unit_cost, 4),
            total_cost=round(consumed * unit_cost, 2),
            notes=form.get("notes"),
        )
        repository.db.add(allocation)
        total_cost = round(total_cost + float(allocation.total_cost or 0), 2)
        remaining = round(remaining - consumed, 2)

    output.total_cost = round(total_cost, 2)
    output.unit_cost = round(total_cost / quantity, 4) if quantity else 0
    repository.db.add(output)
    repository.db.commit()
    repository.db.refresh(output)
    return output


def update_manual_stock_output(repository: FarmRepository, output: StockOutput, form: dict) -> StockOutput:
    if output.reference_type != MANUAL_STOCK_OUTPUT_REFERENCE:
        raise ValueError("Este lancamento nao pode ser editado por aqui.")

    new_quantity = float(form["quantity"])
    if new_quantity <= 0:
        raise ValueError("Informe uma quantidade valida para a saida manual.")

    movement_date = date.fromisoformat(form["movement_date"]) if form.get("movement_date") else today_in_app_timezone()
    input_catalog = repository.get_input_catalog(int(output.input_id)) if output.input_id else None
    if not input_catalog or not input_catalog.is_active:
        raise ValueError("Selecione um item válido para a saída manual.")

    target_unit = (form.get("unit") or output.unit or input_catalog.default_unit or "kg").strip()
    target_plot_id = form.get("plot_id")
    target_plot = repository.get_plot(int(target_plot_id)) if target_plot_id else None
    target_farm_id_raw = form.get("farm_id")
    target_farm_id = int(target_farm_id_raw) if target_farm_id_raw not in (None, "", 0, "0") else None
    if target_plot and target_plot.farm_id:
        target_farm_id = target_plot.farm_id

    allocations = _manual_stock_output_allocations(repository, output.id)
    if allocations:
        for allocation in allocations:
            if allocation.purchased_input:
                allocation.purchased_input.available_quantity = round(
                    float(allocation.purchased_input.available_quantity or 0) + float(allocation.quantity or 0),
                    2,
                )
                repository.db.add(allocation.purchased_input)
            repository.db.delete(allocation)
        repository.db.flush()
    elif output.purchased_input:
        current_available = float(output.purchased_input.available_quantity or 0)
        output.purchased_input.available_quantity = round(current_available + float(output.quantity or 0), 2)
        repository.db.add(output.purchased_input)
    else:
        raise ValueError("Nao foi possivel localizar os lotes vinculados a esta saida manual.")

    candidate_lots = sorted(
        _find_candidate_lots(repository, input_catalog.id, None, input_catalog.name, target_unit, target_farm_id),
        key=lambda item: (item.purchase_date or today_in_app_timezone(), item.id),
    )
    available = _catalog_available_stock(repository, input_catalog.id, target_farm_id, target_unit)
    if new_quantity > available:
        missing = round(new_quantity - available, 2)
        raise ValueError(
            f"Estoque insuficiente. Necessario comprar {missing} {target_unit} de {input_catalog.name}."
        )

    remaining = new_quantity
    total_cost = 0.0
    first_lot = None
    for lot in candidate_lots:
        if remaining <= 0:
            break
        lot_available = _available_stock_for_input(lot)
        consumed = min(lot_available, remaining)
        if consumed <= 0:
            continue
        if first_lot is None:
            first_lot = lot
        unit_cost = float(lot.total_cost or 0) / max(float(lot.total_quantity or 0), 1)
        lot.available_quantity = round(lot_available - consumed, 2)
        allocation = StockOutput(
            input_id=input_catalog.id,
            purchased_input_id=lot.id,
            farm_id=target_farm_id,
            plot_id=target_plot.id if target_plot else None,
            season_id=_resolve_season_for_farm(repository, target_farm_id, movement_date, output.season_id),
            movement_date=movement_date,
            quantity=round(consumed, 2),
            unit=target_unit,
            origin="manual",
            reference_type=MANUAL_STOCK_OUTPUT_ALLOCATION,
            reference_id=output.id,
            unit_cost=round(unit_cost, 4),
            total_cost=round(consumed * unit_cost, 2),
            notes=form.get("notes"),
        )
        repository.db.add(lot)
        repository.db.add(allocation)
        total_cost = round(total_cost + float(allocation.total_cost or 0), 2)
        remaining = round(remaining - consumed, 2)

    if first_lot is None:
        available = _catalog_available_stock(repository, input_catalog.id, target_farm_id, target_unit)
        missing = round(new_quantity - available, 2)
        raise ValueError(
            f"Estoque insuficiente. Necessario comprar {missing} {target_unit} de {input_catalog.name}."
        )

    output.input_id = input_catalog.id
    output.purchased_input_id = None
    output.farm_id = target_farm_id
    output.plot_id = target_plot.id if target_plot else None
    output.movement_date = movement_date
    output.season_id = _resolve_season_for_farm(repository, target_farm_id, movement_date, output.season_id)
    output.quantity = round(new_quantity, 2)
    output.unit = target_unit
    output.unit_cost = round(total_cost / new_quantity, 4)
    output.total_cost = round(total_cost, 2)
    output.notes = form.get("notes")
    repository.db.add(output)
    repository.db.commit()
    repository.db.refresh(output)
    return output


def delete_manual_stock_output(repository: FarmRepository, output: StockOutput) -> None:
    if output.reference_type != MANUAL_STOCK_OUTPUT_REFERENCE:
        raise ValueError("Este lancamento esta vinculado a outro modulo e nao pode ser excluido por aqui.")
    allocations = _manual_stock_output_allocations(repository, output.id)
    if allocations:
        for allocation in allocations:
            if allocation.purchased_input:
                allocation.purchased_input.available_quantity = round(
                    float(allocation.purchased_input.available_quantity or 0) + float(allocation.quantity or 0),
                    2,
                )
                repository.db.add(allocation.purchased_input)
            repository.db.delete(allocation)
    elif output.purchased_input:
        output.purchased_input.available_quantity = round(
            float(output.purchased_input.available_quantity or 0) + float(output.quantity or 0),
            2,
        )
        repository.db.add(output.purchased_input)
    repository.db.delete(output)
    repository.db.commit()


def create_input_recommendation(repository: FarmRepository, form: dict) -> InputRecommendation:
    recommendation = InputRecommendation(
        farm_id=form.get("farm_id"),
        plot_id=form.get("plot_id"),
        application_name=form["application_name"],
        notes=form.get("notes"),
    )
    repository.db.add(recommendation)
    repository.db.flush()
    for item in form.get("items", []):
        input_catalog = repository.get_input_catalog(item["input_id"])
        if not input_catalog or input_catalog.item_type != "insumo_agricola" or not input_catalog.is_active:
            continue
        recommendation.items.append(
            InputRecommendationItem(
                input_id=input_catalog.id,
                unit=item.get("unit") or input_catalog.default_unit,
                quantity=item["quantity"],
            )
        )
    if recommendation.items:
        first_item = recommendation.items[0]
        recommendation.purchased_input_id = None
        recommendation.unit = first_item.unit
        recommendation.quantity_per_hectare = first_item.quantity
    repository.db.add(recommendation)
    repository.db.commit()
    repository.db.refresh(recommendation)
    return recommendation


def update_input_recommendation(repository: FarmRepository, recommendation: InputRecommendation, form: dict) -> InputRecommendation:
    recommendation.farm_id = form.get("farm_id")
    recommendation.plot_id = form.get("plot_id")
    recommendation.application_name = form["application_name"]
    recommendation.notes = form.get("notes")
    recommendation.items.clear()
    repository.db.flush()
    for item in form.get("items", []):
        input_catalog = repository.get_input_catalog(item["input_id"])
        if not input_catalog or input_catalog.item_type != "insumo_agricola" or not input_catalog.is_active:
            continue
        recommendation.items.append(
            InputRecommendationItem(
                input_id=input_catalog.id,
                unit=item.get("unit") or input_catalog.default_unit,
                quantity=item["quantity"],
            )
        )
    first_item = recommendation.items[0] if recommendation.items else None
    recommendation.purchased_input_id = None
    recommendation.unit = first_item.unit if first_item else None
    recommendation.quantity_per_hectare = first_item.quantity if first_item else None
    repository.db.add(recommendation)
    repository.db.commit()
    repository.db.refresh(recommendation)
    return recommendation


def update_fertilization(repository: FarmRepository, fertilization: FertilizationRecord, form: dict) -> FertilizationRecord:
    _restore_fertilization_stock(repository, fertilization)
    repository.db.flush()
    return _save_fertilization(repository, fertilization, form)


def create_fertilization_schedule(repository: FarmRepository, form: dict) -> FertilizationSchedule:
    plot = repository.get_plot(form["plot_id"])
    scheduled_date = date.fromisoformat(form["scheduled_date"])
    schedule = FertilizationSchedule(
        plot_id=form["plot_id"],
        season_id=_resolve_season_for_plot(repository, plot, scheduled_date, form.get("season_id")),
        scheduled_date=scheduled_date,
        status=form.get("status") or "scheduled",
        notes=form.get("notes"),
    )
    repository.db.add(schedule)
    repository.db.flush()
    for item in form.get("items", []):
        input_catalog = repository.get_input_catalog(item["input_id"])
        if not input_catalog or input_catalog.item_type != "insumo_agricola" or not input_catalog.is_active:
            continue
        schedule.items.append(
            FertilizationScheduleItem(
                input_id=input_catalog.id,
                purchased_input_id=None,
                name=input_catalog.name,
                unit=item.get("unit") or input_catalog.default_unit,
                quantity=item["quantity"],
            )
        )
    repository.db.add(schedule)
    repository.db.commit()
    repository.db.refresh(schedule)
    return schedule


def update_fertilization_schedule(repository: FarmRepository, schedule: FertilizationSchedule, form: dict) -> FertilizationSchedule:
    plot = repository.get_plot(form["plot_id"])
    scheduled_date = date.fromisoformat(form["scheduled_date"])
    schedule.plot_id = form["plot_id"]
    schedule.season_id = _resolve_season_for_plot(repository, plot, scheduled_date, form.get("season_id"))
    schedule.scheduled_date = scheduled_date
    schedule.status = form.get("status") or schedule.status
    schedule.notes = form.get("notes")
    schedule.items.clear()
    repository.db.flush()
    for item in form.get("items", []):
        input_catalog = repository.get_input_catalog(item["input_id"])
        if not input_catalog or input_catalog.item_type != "insumo_agricola" or not input_catalog.is_active:
            continue
        schedule.items.append(
            FertilizationScheduleItem(
                input_id=input_catalog.id,
                purchased_input_id=None,
                name=input_catalog.name,
                unit=item.get("unit") or input_catalog.default_unit,
                quantity=item["quantity"],
            )
        )
    repository.db.add(schedule)
    repository.db.commit()
    repository.db.refresh(schedule)
    return schedule


def validate_schedule_stock(repository: FarmRepository, schedule: FertilizationSchedule) -> dict:
    shortages = []
    for item in schedule.items:
        available = _catalog_available_stock(repository, item.input_id, schedule.plot.farm_id if schedule.plot else None, item.unit)
        required = float(item.quantity or 0)
        if required > available:
            shortages.append(
                {
                    "name": item.name,
                    "required": round(required, 2),
                    "available": round(available, 2),
                    "missing": round(required - available, 2),
                    "unit": item.unit,
                }
            )
    return {"ok": not shortages, "shortages": shortages}


def conclude_fertilization_schedule(repository: FarmRepository, schedule: FertilizationSchedule, application_date: str | None = None) -> FertilizationRecord:
    if not schedule.items:
        raise ValueError("Adicione ao menos um insumo no agendamento.")
    schedule_items = [
        {
            "input_id": item.input_id,
            "purchased_input_id": item.purchased_input_id,
            "name": item.name or (item.input_catalog.name if item.input_catalog else ""),
            "unit": item.unit or (item.input_catalog.default_unit if item.input_catalog else ""),
            "quantity": float(item.quantity or 0),
        }
        for item in schedule.items
        if (item.name or (item.input_catalog.name if item.input_catalog else "")).strip()
        and (item.unit or (item.input_catalog.default_unit if item.input_catalog else "")).strip()
        and float(item.quantity or 0) > 0
    ]
    if not schedule_items:
        raise ValueError("Adicione ao menos um insumo valido no agendamento antes de concluir.")
    record = create_fertilization(
        repository,
        {
            "plot_id": schedule.plot_id,
            "application_date": application_date or schedule.scheduled_date.isoformat(),
            "season_id": schedule.season_id,
            "notes": schedule.notes,
            "items": schedule_items,
        },
    )
    schedule.status = "completed"
    schedule.fertilization_record_id = record.id
    repository.db.add(schedule)
    repository.db.commit()
    repository.db.refresh(schedule)
    return record


def delete_fertilization(repository: FarmRepository, fertilization: FertilizationRecord) -> None:
    _restore_fertilization_stock(repository, fertilization)
    repository.db.delete(fertilization)
    repository.db.commit()


def delete_fertilization_schedule(repository: FarmRepository, schedule: FertilizationSchedule) -> None:
    repository.db.delete(schedule)
    repository.db.commit()


def create_harvest(repository: FarmRepository, form: dict, area_hectares: float) -> HarvestRecord:
    sacks = float(form["sacks_produced"])
    productivity = sacks / area_hectares if area_hectares else 0
    return repository.create(
        HarvestRecord(
            plot_id=form["plot_id"],
            harvest_date=date.fromisoformat(form["harvest_date"]),
            sacks_produced=sacks,
            productivity_per_hectare=round(productivity, 2),
            notes=form.get("notes"),
        )
    )


def update_harvest(
    repository: FarmRepository,
    harvest: HarvestRecord,
    form: dict,
    area_hectares: float,
) -> HarvestRecord:
    sacks = float(form["sacks_produced"])
    productivity = sacks / area_hectares if area_hectares else 0
    return repository.update(
        harvest,
        {
            "plot_id": form["plot_id"],
            "harvest_date": date.fromisoformat(form["harvest_date"]),
            "sacks_produced": sacks,
            "productivity_per_hectare": round(productivity, 2),
            "notes": form.get("notes"),
        },
    )


def create_pest_incident(repository: FarmRepository, form: dict) -> PestIncident:
    return repository.create(
        PestIncident(
            plot_id=form["plot_id"],
            occurrence_date=date.fromisoformat(form["occurrence_date"]),
            category=form["category"],
            name=form["name"],
            severity=form["severity"],
            treatment=form.get("treatment"),
            notes=form.get("notes"),
        )
    )


def update_pest_incident(repository: FarmRepository, incident: PestIncident, form: dict) -> PestIncident:
    return repository.update(
        incident,
        {
            "plot_id": form["plot_id"],
            "occurrence_date": date.fromisoformat(form["occurrence_date"]),
            "category": form["category"],
            "name": form["name"],
            "severity": form["severity"],
            "treatment": form.get("treatment"),
            "notes": form.get("notes"),
        },
    )


def create_agronomic_profile(repository: FarmRepository, form: dict) -> AgronomicProfile:
    return repository.create(
        AgronomicProfile(
            farm_id=form["farm_id"],
            culture=form["culture"],
            region=form["region"],
            climate=form.get("climate"),
            soil_type=form.get("soil_type"),
            irrigation_system=form.get("irrigation_system"),
            plant_spacing=form.get("plant_spacing"),
            drip_spacing=form.get("drip_spacing"),
            fertilizers_used=form.get("fertilizers_used"),
            crop_stage=form.get("crop_stage"),
            common_pests=form.get("common_pests"),
        )
    )


def update_agronomic_profile(repository: FarmRepository, profile: AgronomicProfile, form: dict) -> AgronomicProfile:
    return repository.update(
        profile,
        {
            "farm_id": form["farm_id"],
            "culture": form["culture"],
            "region": form["region"],
            "climate": form.get("climate"),
            "soil_type": form.get("soil_type"),
            "irrigation_system": form.get("irrigation_system"),
            "plant_spacing": form.get("plant_spacing"),
            "drip_spacing": form.get("drip_spacing"),
            "fertilizers_used": form.get("fertilizers_used"),
            "crop_stage": form.get("crop_stage"),
            "common_pests": form.get("common_pests"),
        },
    )


def _available_stock_for_input(purchased_input: PurchasedInput | None) -> float:
    return float(purchased_input.available_quantity if purchased_input and purchased_input.available_quantity is not None else 0)


def _resolve_season_for_plot(
    repository: FarmRepository,
    plot: Plot | None,
    movement_date: date,
    season_id: int | None = None,
) -> int | None:
    if not plot or not plot.farm_id:
        return None
    if season_id:
        season = repository.get_crop_season(season_id)
        if (
            season
            and season.farm_id == plot.farm_id
            and season.start_date <= movement_date <= season.end_date
            and (season.variety_id is None or season.variety_id == plot.variety_id)
        ):
            return season.id
    for season in repository.list_crop_seasons(farm_id=plot.farm_id):
        if season.start_date <= movement_date <= season.end_date:
            if season.variety_id is not None and season.variety_id != plot.variety_id:
                continue
            return season.id
    return None


def _resolve_season_for_farm(
    repository: FarmRepository,
    farm_id: int | None,
    movement_date: date,
    season_id: int | None = None,
) -> int | None:
    if not farm_id:
        return None
    if season_id:
        season = repository.get_crop_season(season_id)
        if season and season.farm_id == farm_id and season.start_date <= movement_date <= season.end_date:
            return season.id
    for season in repository.list_crop_seasons(farm_id=farm_id):
        if season.start_date <= movement_date <= season.end_date:
            return season.id
    return None


def _current_average_cost(repository: FarmRepository, farm_id: int | None, input_name: str, unit: str) -> float:
    input_id = None
    normalized_name = _normalize_input_name(input_name)
    if normalized_name:
        catalog = repository.get_input_catalog_by_normalized_name(normalized_name)
        input_id = catalog.id if catalog else None
    lots = [
        item for item in repository.list_purchased_inputs()
        if item.input_id == input_id and item.package_unit == unit and (farm_id is None or item.farm_id in (None, farm_id)) and _available_stock_for_input(item) > 0
    ]
    total_quantity = sum(_available_stock_for_input(item) for item in lots)
    total_value = sum(_available_stock_for_input(item) * (float(item.total_cost or 0) / max(float(item.total_quantity or 0), 1)) for item in lots)
    return round(total_value / total_quantity, 4) if total_quantity else 0


def _find_candidate_lots(repository: FarmRepository, input_id: int | None, purchased_input_id: int | None, name: str | None, unit: str, farm_id: int | None) -> list[PurchasedInput]:
    purchased_inputs = repository.list_purchased_inputs()
    target_input_id = input_id
    if target_input_id is None and purchased_input_id:
        direct = next((item for item in purchased_inputs if item.id == purchased_input_id), None)
        target_input_id = direct.input_id if direct else None
    if target_input_id is None and name:
        catalog = repository.get_input_catalog_by_normalized_name(_normalize_input_name(name))
        target_input_id = catalog.id if catalog else None
    return [
        item for item in purchased_inputs
        if item.input_id == target_input_id and item.package_unit == unit and (farm_id is None or item.farm_id in (None, farm_id)) and _available_stock_for_input(item) > 0
    ]


def _deduct_stock(repository: FarmRepository, fertilization_item: FertilizationItem, farm_id: int | None, requested_quantity: float) -> tuple[float, float]:
    candidate_lots = _find_candidate_lots(
        repository,
        fertilization_item.input_id,
        fertilization_item.purchased_input_id,
        fertilization_item.name,
        fertilization_item.unit,
        farm_id,
    )
    candidate_lots = sorted(candidate_lots, key=lambda item: (item.purchase_date or today_in_app_timezone(), item.id))
    total_available = sum(_available_stock_for_input(item) for item in candidate_lots)
    if requested_quantity > total_available:
        missing = round(requested_quantity - total_available, 2)
        raise ValueError(f"Estoque insuficiente para {fertilization_item.name}. Necessario comprar {missing} {fertilization_item.unit}.")

    average_cost = _current_average_cost(repository, farm_id, fertilization_item.name, fertilization_item.unit)
    remaining = requested_quantity
    for lot in candidate_lots:
        if remaining <= 0:
            break
        lot_available = _available_stock_for_input(lot)
        consumed = min(lot_available, remaining)
        unit_cost = float(lot.total_cost or 0) / max(float(lot.total_quantity or 0), 1)
        lot.available_quantity = round(lot_available - consumed, 2)
        fertilization_item.stock_allocations.append(
            FertilizationStockAllocation(
                purchased_input_id=lot.id,
                quantity_used=consumed,
                unit_cost=round(unit_cost, 4),
                total_cost=round(consumed * unit_cost, 2),
            )
        )
        repository.db.add(
            StockOutput(
                input_id=lot.input_id,
                purchased_input_id=lot.id,
                farm_id=farm_id,
                plot_id=fertilization_item.fertilization.plot_id if fertilization_item.fertilization else None,
                season_id=fertilization_item.fertilization.season_id if fertilization_item.fertilization else None,
                movement_date=fertilization_item.fertilization.application_date if fertilization_item.fertilization else today_in_app_timezone(),
                quantity=consumed,
                unit=fertilization_item.unit,
                origin="fertilizacao",
                reference_type="fertilization_item",
                reference_id=fertilization_item.id,
                unit_cost=round(unit_cost, 4),
                total_cost=round(consumed * unit_cost, 2),
                notes=fertilization_item.name,
            )
        )
        remaining = round(remaining - consumed, 2)
        if fertilization_item.purchased_input_id is None:
            fertilization_item.purchased_input_id = lot.id

    return round(average_cost, 4), round(requested_quantity * average_cost, 2)


def _restore_fertilization_stock(repository: FarmRepository, fertilization: FertilizationRecord) -> None:
    item_ids = [item.id for item in fertilization.items if item.id]
    for item in fertilization.items:
        for allocation in item.stock_allocations:
            if allocation.purchased_input:
                current_available = float(allocation.purchased_input.available_quantity or 0)
                allocation.purchased_input.available_quantity = round(current_available + float(allocation.quantity_used or 0), 2)
        item.stock_allocations.clear()
    if item_ids or fertilization.id:
        for output in repository.list_stock_outputs():
            if (
                output.reference_type == "fertilization_item"
                and output.reference_id in item_ids
            ) or (
                output.reference_type == "fertilization_record"
                and output.reference_id == fertilization.id
            ):
                repository.db.delete(output)


def _save_fertilization(repository: FarmRepository, fertilization: FertilizationRecord | None, form: dict) -> FertilizationRecord:
    items = _normalize_fertilization_items(form.get("items"), form.get("area_hectares"))
    plot = repository.get_plot(form["plot_id"])
    if not plot:
        raise ValueError("Setor nao encontrado para fertilizacao.")
    application_date = date.fromisoformat(form["application_date"])
    season_id = _resolve_season_for_plot(repository, plot, application_date, form.get("season_id"))
    product, dose = _fertilization_summary(items)
    record = fertilization or FertilizationRecord(
        plot_id=form["plot_id"],
        season_id=season_id,
        application_date=application_date,
        product=product,
        dose=dose,
        cost=0,
        notes=form.get("notes"),
    )
    record.plot_id = form["plot_id"]
    record.season_id = season_id
    record.application_date = application_date
    record.product = product
    record.dose = dose
    record.notes = form.get("notes")
    if fertilization:
        record.items.clear()
        repository.db.flush()
    repository.db.add(record)
    repository.db.flush()

    total_cost = Decimal("0")
    for item in items:
        if item.get("input_id"):
            input_catalog = repository.get_input_catalog(item["input_id"])
            if not input_catalog or input_catalog.item_type != "insumo_agricola" or not input_catalog.is_active:
                raise ValueError("Selecione apenas insumos agrícolas válidos para a fertilização.")
        fertilization_item = FertilizationItem(
            fertilization_record_id=record.id,
            input_id=item.get("input_id"),
            purchased_input_id=item.get("purchased_input_id"),
            name=item["name"],
            unit=item["unit"],
            quantity_per_hectare=item["quantity_per_hectare"],
            total_quantity=item["total_quantity"],
        )
        repository.db.add(fertilization_item)
        repository.db.flush()
        unit_cost, item_total_cost = _deduct_stock(repository, fertilization_item, plot.farm_id, float(item["total_quantity"]))
        fertilization_item.unit_cost = unit_cost
        fertilization_item.total_cost = item_total_cost
        total_cost += Decimal(str(item_total_cost))

    record.cost = round(float(total_cost), 2)
    repository.db.add(record)
    repository.db.commit()
    repository.db.refresh(record)
    return record


def create_fertilization(repository: FarmRepository, form: dict) -> FertilizationRecord:
    return _save_fertilization(repository, None, form)


def create_soil_analysis(repository: FarmRepository, form: dict) -> SoilAnalysis:
    return repository.create(
        SoilAnalysis(
            farm_id=form["farm_id"],
            plot_id=form["plot_id"],
            analysis_date=date.fromisoformat(form["analysis_date"]),
            laboratory=form["laboratory"],
            ph=form.get("ph"),
            organic_matter=form.get("organic_matter"),
            phosphorus=form.get("phosphorus"),
            potassium=form.get("potassium"),
            calcium=form.get("calcium"),
            magnesium=form.get("magnesium"),
            aluminum=form.get("aluminum"),
            h_al=form.get("h_al"),
            ctc=form.get("ctc"),
            base_saturation=form.get("base_saturation"),
            observations=form.get("observations"),
            pdf_filename=form.get("pdf_filename"),
            pdf_content_type=form.get("pdf_content_type"),
            pdf_data=form.get("pdf_data"),
            liming_need_t_ha=form.get("liming_need_t_ha"),
            npk_recommendation=form.get("npk_recommendation"),
            micronutrient_recommendation=form.get("micronutrient_recommendation"),
            ai_recommendation=form.get("ai_recommendation"),
            ai_status=form.get("ai_status"),
            ai_model=form.get("ai_model"),
            ai_error=form.get("ai_error"),
            ai_generated_at=form.get("ai_generated_at"),
        )
    )


def update_soil_analysis(repository: FarmRepository, analysis: SoilAnalysis, form: dict) -> SoilAnalysis:
    return repository.update(
        analysis,
        {
            "farm_id": form["farm_id"],
            "plot_id": form["plot_id"],
            "analysis_date": date.fromisoformat(form["analysis_date"]),
            "laboratory": form["laboratory"],
            "ph": form.get("ph"),
            "organic_matter": form.get("organic_matter"),
            "phosphorus": form.get("phosphorus"),
            "potassium": form.get("potassium"),
            "calcium": form.get("calcium"),
            "magnesium": form.get("magnesium"),
            "aluminum": form.get("aluminum"),
            "h_al": form.get("h_al"),
            "ctc": form.get("ctc"),
            "base_saturation": form.get("base_saturation"),
            "observations": form.get("observations"),
            "pdf_filename": form.get("pdf_filename"),
            "pdf_content_type": form.get("pdf_content_type"),
            "pdf_data": form.get("pdf_data"),
            "liming_need_t_ha": form.get("liming_need_t_ha"),
            "npk_recommendation": form.get("npk_recommendation"),
            "micronutrient_recommendation": form.get("micronutrient_recommendation"),
            "ai_recommendation": form.get("ai_recommendation"),
            "ai_status": form.get("ai_status"),
            "ai_model": form.get("ai_model"),
            "ai_error": form.get("ai_error"),
            "ai_generated_at": form.get("ai_generated_at"),
        },
    )


def normalize_geojson(raw_text: str | None) -> str | None:
    if not raw_text:
        return None
    normalized = raw_text.strip()
    if not normalized:
        return None
    try:
        parsed = json.loads(normalized)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict) and parsed.get("type") == "FeatureCollection":
        features = parsed.get("features") or []
        if not features:
            return None
        first_feature = features[0]
        if isinstance(first_feature, dict):
            geometry = first_feature.get("geometry")
            return json.dumps(geometry) if geometry else None
        return None
    if isinstance(parsed, dict) and parsed.get("type") == "Feature":
        geometry = parsed.get("geometry")
        return json.dumps(geometry) if geometry else None
    return json.dumps(parsed)


def extract_geojson_file(raw_bytes: bytes | None) -> str | None:
    if not raw_bytes:
        return None
    try:
        decoded = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        decoded = raw_bytes.decode("latin-1")
    return normalize_geojson(decoded)


def estimate_geojson_centroid(geojson_text: str | None) -> tuple[float | None, float | None]:
    if not geojson_text:
        return None, None
    try:
        geometry = json.loads(geojson_text)
    except json.JSONDecodeError:
        return None, None

    coordinates = _flatten_coordinates(geometry.get("coordinates"))
    if not coordinates:
        return None, None

    longitudes = [point[0] for point in coordinates]
    latitudes = [point[1] for point in coordinates]
    return round(sum(latitudes) / len(latitudes), 6), round(sum(longitudes) / len(longitudes), 6)


def calculate_geojson_area_hectares(geojson_text: str | None) -> float | None:
    if not geojson_text:
        return None
    try:
        geometry = json.loads(geojson_text)
    except json.JSONDecodeError:
        return None

    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates") or []
    rings: list[list[tuple[float, float]]] = []
    if geometry_type == "Polygon":
        if coordinates:
            rings.append(_flatten_coordinates(coordinates[0]))
    elif geometry_type == "MultiPolygon":
        for polygon in coordinates:
            if polygon:
                rings.append(_flatten_coordinates(polygon[0]))

    areas = [_polygon_area_hectares(ring) for ring in rings if len(ring) >= 3]
    valid_areas = [area for area in areas if area > 0]
    return round(sum(valid_areas), 4) if valid_areas else None


def calculate_irrigation_volume(plot: Plot, duration_minutes: int) -> float | None:
    if not plot or not duration_minutes:
        return None
    hours = duration_minutes / 60

    if plot.irrigation_type == "gotejo":
        if not (
            plot.irrigation_line_count
            and plot.irrigation_line_length_meters
            and plot.drip_spacing_meters
            and plot.drip_liters_per_hour
        ):
            return None
        emitters_per_line = float(plot.irrigation_line_length_meters) / float(plot.drip_spacing_meters)
        total_emitters = float(plot.irrigation_line_count) * emitters_per_line
        return round(total_emitters * float(plot.drip_liters_per_hour) * hours, 2)

    if plot.irrigation_type == "aspersor":
        if not (plot.sprinkler_count and plot.sprinkler_liters_per_hour):
            return None
        return round(float(plot.sprinkler_count) * float(plot.sprinkler_liters_per_hour) * hours, 2)

    return None


def calculate_soil_recommendations(form: dict) -> dict:
    ph = _float(form.get("ph"))
    phosphorus = _float(form.get("phosphorus"))
    potassium = _float(form.get("potassium"))
    organic_matter = _float(form.get("organic_matter"))
    ctc = _float(form.get("ctc"))
    base_saturation = _float(form.get("base_saturation"))

    liming_need = None
    if ctc is not None and base_saturation is not None:
        liming_need = max(0.0, ((60 - base_saturation) / 100) * ctc * 2)
    elif ph is not None and ph < 5.5:
        liming_need = round((5.5 - ph) * 1.6, 2)

    npk_parts = []
    if phosphorus is not None:
        if phosphorus < 12:
            npk_parts.append("Elevar fosforo com formulacao rica em P, priorizando MAP ou fosfatado de alta solubilidade.")
        elif phosphorus < 20:
            npk_parts.append("Manutencao moderada de fosforo, ajustando pela meta produtiva do setor.")
        else:
            npk_parts.append("Fosforo em faixa satisfatoria para manutencao.")
    if potassium is not None:
        if potassium < 120:
            npk_parts.append("Reforcar potassio com NPK de cobertura ou fertirrigacao potassica.")
        elif potassium < 180:
            npk_parts.append("Potassio em faixa intermediaria, manter reposicao parcelada.")
        else:
            npk_parts.append("Potassio adequado para manutencao.")
    if organic_matter is not None:
        if organic_matter < 2.5:
            npk_parts.append("Associar materia organica e nitrogenio de arranque para estimular raiz e brotacao.")
        else:
            npk_parts.append("Materia organica favorece resposta a adubacao nitrogenada parcelada.")

    micronutrients = []
    if ph is not None and ph < 5.3:
        micronutrients.append("Monitorar Boro e Zinco apos correcao de acidez.")
    if ph is not None and ph > 6.4:
        micronutrients.append("Atencao a possiveis limitacoes de Zinco, Boro e Manganes em pH mais alto.")
    if organic_matter is not None and organic_matter < 2.0:
        micronutrients.append("Considerar programa com micronutrientes foliares e fontes organicas.")
    if not micronutrients:
        micronutrients.append("Micronutrientes em manutencao, com foco em Boro e Zinco conforme diagnostico foliar.")

    return {
        "liming_need_t_ha": round(liming_need, 2) if liming_need is not None else None,
        "npk_recommendation": " ".join(npk_parts) if npk_parts else "Definir NPK conforme produtividade alvo e historico do setor.",
        "micronutrient_recommendation": " ".join(micronutrients),
    }


def _flatten_coordinates(value) -> list[tuple[float, float]]:
    if not isinstance(value, list):
        return []
    if len(value) >= 2 and all(isinstance(item, (int, float)) for item in value[:2]):
        return [(float(value[0]), float(value[1]))]

    coordinates: list[tuple[float, float]] = []
    for item in value:
        coordinates.extend(_flatten_coordinates(item))
    return coordinates


def _float(value):
    return float(value) if value is not None else None


def _normalize_fertilization_items(items: list[dict] | None, area_hectares: float | None) -> list[dict]:
    normalized: list[dict] = []
    for item in items or []:
        name = (item.get("name") or "").strip()
        unit = (item.get("unit") or "").strip()
        quantity = item.get("quantity")
        if not name or not unit or quantity in (None, ""):
            continue
        quantity_value = round(float(quantity), 2)
        normalized.append(
            {
                "input_id": item.get("input_id"),
                "purchased_input_id": item.get("purchased_input_id"),
                "name": name,
                "unit": unit,
                "quantity_per_hectare": quantity_value,
                "total_quantity": quantity_value,
            }
        )
    return normalized


def _fertilization_summary(items: list[dict]) -> tuple[str, str]:
    if not items:
        return "Aplicacao sem itens", "-"
    first = items[0]
    if len(items) == 1:
        return first["name"], f'{first["quantity_per_hectare"]:.2f} {first["unit"]}'
    return f'{len(items)} insumos aplicados', f'{first["quantity_per_hectare"]:.2f} {first["unit"]} + {len(items) - 1} item(ns)'


def _polygon_area_hectares(points: list[tuple[float, float]]) -> float:
    if len(points) < 3:
        return 0.0
    if points[0] == points[-1]:
        points = points[:-1]
    avg_lat = sum(lat for _, lat in points) / len(points)
    meters_per_degree_lat = 111_320
    meters_per_degree_lng = 111_320 * math.cos(math.radians(avg_lat))
    projected = [(lng * meters_per_degree_lng, lat * meters_per_degree_lat) for lng, lat in points]
    area = 0.0
    for index, (x1, y1) in enumerate(projected):
        x2, y2 = projected[(index + 1) % len(projected)]
        area += (x1 * y2) - (x2 * y1)
    return abs(area) / 2 / 10_000
