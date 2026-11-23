from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base
from app.db.init_db import backfill_input_catalog_organizations
from app.models import (
    CropSeason,
    EquipmentAsset,
    EquipmentFueling,
    Farm,
    FertilizationItem,
    FertilizationRecord,
    FertilizationSchedule,
    FertilizationScheduleItem,
    InputCatalog,
    InputRecommendation,
    InputRecommendationItem,
    Organization,
    Plot,
    PurchasedInput,
    StockAssistantDraft,
    StockInventoryItem,
    StockInventorySession,
    StockOutput,
    User,
)
from app.repositories.farm import FarmRepository
from app.services import inputs_hub as ih
from app.services import purchase_note as pn
from app.services.forms import create_purchased_input
from app.web import routes


def _setup_two_orgs():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    db = Session(engine)
    org_a = Organization(name="Organização Alpha", slug="alpha", is_active=True)
    org_b = Organization(name="Organização Beta", slug="beta", is_active=True)
    db.add_all([org_a, org_b])
    db.flush()

    farm_a = Farm(name="Fazenda Alpha", location="MG", total_area=50, organization_id=org_a.id)
    farm_b = Farm(name="Fazenda Beta", location="SP", total_area=80, organization_id=org_b.id)
    db.add_all([farm_a, farm_b])
    db.flush()

    db.commit()
    return db, org_a, org_b, farm_a, farm_b


def test_purchase_wizard_and_catalog_are_strictly_isolated_by_organization():
    db, org_a, org_b, farm_a, farm_b = _setup_two_orgs()
    try:
        repo = FarmRepository(db)

        # Org A compra "Insumo Exclusivo Alpha" e "Ureia" em sacas (sc)
        create_purchased_input(
            repo,
            {
                "farm_id": farm_a.id,
                "item_type": "insumo_agricola",
                "category": "Fertilizantes",
                "name": "Insumo Exclusivo Alpha",
                "quantity_purchased": 5,
                "package_size": 1,
                "package_unit": "sc",
                "unit_price": 180.0,
                "purchase_date": "2026-11-10",
                "low_stock_threshold": 10,
            },
        )
        create_purchased_input(
            repo,
            {
                "farm_id": farm_a.id,
                "item_type": "insumo_agricola",
                "category": "Fertilizantes",
                "name": "Ureia",
                "quantity_purchased": 10,
                "package_size": 1,
                "package_unit": "sc",
                "unit_price": 200.0,
                "purchase_date": "2026-11-10",
                "low_stock_threshold": 15,
            },
        )

        # Org B compra "Insumo Exclusivo Beta" e também "Ureia", porém em kg
        pn.register_purchase_note(
            repo,
            farm_id=farm_b.id,
            supplier="Fornecedor Beta",
            invoice="NF-900",
            purchase_date=date(2026, 11, 12),
            items=[
                {
                    "name": "Insumo Exclusivo Beta",
                    "item_type": "insumo_agricola",
                    "category": "Herbicida",
                    "package_unit": "L",
                    "packs": "4",
                    "size": "5",
                    "price": "95",
                    "minimum": "5",
                },
                {
                    "name": "Ureia",
                    "item_type": "insumo_agricola",
                    "category": "Fertilizantes",
                    "package_unit": "kg",
                    "packs": "20",
                    "size": "50",
                    "price": "150",
                    "minimum": "100",
                },
            ],
            payment={"mode": "none"},
            finance_account=None,
            credit_card=None,
            notes=None,
        )

        scope_a = {
            "active_organization_id": org_a.id,
            "active_farm_id": farm_a.id,
            "active_farm": farm_a,
        }
        scope_b = {
            "active_organization_id": org_b.id,
            "active_farm_id": farm_b.id,
            "active_farm": farm_b,
        }

        wiz_a = routes._purchase_wizard_data(repo, scope_a)
        wiz_b = routes._purchase_wizard_data(repo, scope_b)

        names_a = {item["name"]: item for item in wiz_a["catalog"]}
        names_b = {item["name"]: item for item in wiz_b["catalog"]}

        assert set(names_a) == {"Insumo Exclusivo Alpha", "Ureia"}
        assert set(names_b) == {"Insumo Exclusivo Beta", "Ureia"}

        # Mesmo nome ("Ureia") com unidades e mínimos completamente independentes
        assert names_a["Ureia"]["unit"] == "sc"
        assert names_a["Ureia"]["minimum"] == 15.0
        assert names_b["Ureia"]["unit"] == "kg"
        assert names_b["Ureia"]["minimum"] == 100.0

        # Validação de unidade de compra na Org B não herda a unidade de Org A
        resolved_b = routes._resolve_wizard_item(
            repo,
            {"name": "Ureia", "item_type": "insumo_agricola", "category": "Fertilizantes", "unit": "kg"},
            1,
            False,
            organization_id=org_b.id,
        )
        assert resolved_b["package_unit"] == "kg"

        # Consulta por ID bloqueia acesso cruzado entre organizações
        cat_a_only = repo.get_input_catalog_by_normalized_name("insumo exclusivo alpha", organization_id=org_a.id)
        assert cat_a_only is not None
        assert repo.get_input_catalog(cat_a_only.id, organization_id=org_b.id) is None

        # Estoque também isolado
        stock_a = {i["name"] for i in ih.build_stock_items(repo, farm_a.id, date(2026, 11, 20))}
        stock_b = {i["name"] for i in ih.build_stock_items(repo, farm_b.id, date(2026, 11, 20))}
        assert stock_a == {"Insumo Exclusivo Alpha", "Ureia"}
        assert stock_b == {"Insumo Exclusivo Beta", "Ureia"}
    finally:
        db.close()


def test_backfill_input_catalog_splits_shared_items_and_is_idempotent():
    db, org_a, org_b, farm_a, farm_b = _setup_two_orgs()
    try:
        user_b = User(name="Bruno", email="bruno@beta.com", hashed_password="x", organization_id=org_b.id, is_active=True)
        plot_a = Plot(farm_id=farm_a.id, name="Talhão A", area_hectares=10, plant_count=1000)
        plot_b = Plot(farm_id=farm_b.id, name="Talhão B", area_hectares=12, plant_count=1200)
        season_b = CropSeason(
            name="Safra 2026",
            farm_id=farm_b.id,
            culture="Café",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 12, 31),
            cultivated_area=12,
            area_unit="ha",
            status="ativa",
        )
        asset_b = EquipmentAsset(farm_id=farm_b.id, name="Trator Beta", category="Máquina", status="ativo")
        db.add_all([user_b, plot_a, plot_b, season_b, asset_b])
        db.flush()

        # Simula registros legados sem organization_id
        only_a = InputCatalog(organization_id=None, name="Só Alpha", normalized_name="so alpha", default_unit="kg")
        only_b = InputCatalog(organization_id=None, name="Só Beta", normalized_name="so beta", default_unit="L")
        shared = InputCatalog(organization_id=None, name="Diesel S10", normalized_name="diesel s10", item_type="combustivel", default_unit="L")
        db.add_all([only_a, only_b, shared])
        db.flush()

        p_only_a = PurchasedInput(
            input_id=only_a.id, farm_id=farm_a.id, name=only_a.name, normalized_name=only_a.normalized_name,
            quantity_purchased=1, package_size=10, package_unit="kg", unit_price=10, total_quantity=10,
            available_quantity=10, total_cost=10, purchase_date=date(2026, 11, 1),
        )
        p_only_b = PurchasedInput(
            input_id=only_b.id, farm_id=farm_b.id, name=only_b.name, normalized_name=only_b.normalized_name,
            quantity_purchased=1, package_size=10, package_unit="L", unit_price=10, total_quantity=10,
            available_quantity=10, total_cost=10, purchase_date=date(2026, 11, 1),
        )
        p_shared_a = PurchasedInput(
            input_id=shared.id, farm_id=farm_a.id, name=shared.name, normalized_name=shared.normalized_name,
            quantity_purchased=1, package_size=100, package_unit="L", unit_price=600, total_quantity=100,
            available_quantity=100, total_cost=600, purchase_date=date(2026, 11, 1),
        )
        p_shared_b = PurchasedInput(
            input_id=shared.id, farm_id=farm_b.id, name=shared.name, normalized_name=shared.normalized_name,
            quantity_purchased=1, package_size=200, package_unit="L", unit_price=1200, total_quantity=200,
            available_quantity=200, total_cost=1200, purchase_date=date(2026, 11, 2),
        )
        db.add_all([p_only_a, p_only_b, p_shared_a, p_shared_b])
        db.flush()

        # Referências extras da Org B em todas as tabelas filhas
        out_b = StockOutput(
            input_id=shared.id, purchased_input_id=p_shared_b.id, farm_id=farm_b.id,
            movement_date=date(2026, 11, 3), quantity=20, unit="L", unit_cost=6, total_cost=120, origin="Consumo",
        )
        fuel_b = EquipmentFueling(
            asset_id=asset_b.id, farm_id=farm_b.id, input_id=shared.id, fueling_date=date(2026, 11, 3),
            liters=20, total_cost=120,
        )
        inv_sess_b = StockInventorySession(farm_id=farm_b.id, inventory_date=date(2026, 11, 4))
        rec_b = InputRecommendation(farm_id=farm_b.id, plot_id=plot_b.id, application_name="Rec B")
        fert_b = FertilizationRecord(plot_id=plot_b.id, application_date=date(2026, 11, 4), product="Diesel S10", dose="10 L", cost=60)
        sched_b = FertilizationSchedule(plot_id=plot_b.id, scheduled_date=date(2026, 11, 10), status="scheduled")
        draft_b = StockAssistantDraft(
            id="draft-beta-1", user_id=user_b.id, organization_id=org_b.id, farm_id=farm_b.id,
            season_id=season_b.id, input_id=shared.id, movement_date=date(2026, 11, 5),
            quantity=10, unit="L", expires_at=datetime.now(timezone.utc) + timedelta(minutes=30),
        )
        db.add_all([out_b, fuel_b, inv_sess_b, rec_b, fert_b, sched_b, draft_b])
        db.flush()

        inv_item_b = StockInventoryItem(session_id=inv_sess_b.id, input_id=shared.id, unit="L", system_quantity=180, counted_quantity=180, difference_quantity=0)
        rec_item_b = InputRecommendationItem(recommendation_id=rec_b.id, input_id=shared.id, quantity=10, unit="L")
        fert_item_b = FertilizationItem(fertilization_record_id=fert_b.id, input_id=shared.id, name="Diesel S10", unit="L", quantity_per_hectare=1, total_quantity=10)
        sched_item_b = FertilizationScheduleItem(schedule_id=sched_b.id, input_id=shared.id, name="Diesel S10", unit="L", quantity=10)
        db.add_all([inv_item_b, rec_item_b, fert_item_b, sched_item_b])
        db.commit()

        migrated = backfill_input_catalog_organizations(db)
        db.commit()
        assert migrated == 3

        # Reexecução deve ser totalmente idempotente
        assert backfill_input_catalog_organizations(db) == 0

        db.expire_all()
        assert db.get(InputCatalog, only_a.id).organization_id == org_a.id
        assert db.get(InputCatalog, only_b.id).organization_id == org_b.id

        # O compartilhado foi mantido na Org A e clonado para a Org B
        shared_a = db.get(InputCatalog, shared.id)
        assert shared_a.organization_id == org_a.id
        shared_b = (
            db.query(InputCatalog)
            .filter(InputCatalog.organization_id == org_b.id, InputCatalog.normalized_name == "diesel s10")
            .one()
        )
        assert shared_b.id != shared_a.id

        # Compras e todas as referências filhas da Org B apontam para a cópia isolada
        assert db.get(PurchasedInput, p_shared_a.id).input_id == shared_a.id
        assert db.get(PurchasedInput, p_shared_b.id).input_id == shared_b.id
        assert db.get(StockOutput, out_b.id).input_id == shared_b.id
        assert db.get(EquipmentFueling, fuel_b.id).input_id == shared_b.id
        assert db.get(StockInventoryItem, inv_item_b.id).input_id == shared_b.id
        assert db.get(InputRecommendationItem, rec_item_b.id).input_id == shared_b.id
        assert db.get(FertilizationItem, fert_item_b.id).input_id == shared_b.id
        assert db.get(FertilizationScheduleItem, sched_item_b.id).input_id == shared_b.id
        assert db.get(StockAssistantDraft, draft_b.id).input_id == shared_b.id
    finally:
        db.close()
