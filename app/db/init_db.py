from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import get_password_hash
from app.db.base import Base
from app.db.session import engine
from app.models import (
    AgronomicProfile,
    CoffeeVariety,
    CropSeason,
    EquipmentAsset,
    Farm,
    FertilizationItem,
    FertilizationSchedule,
    FertilizationScheduleItem,
    FertilizationStockAllocation,
    FertilizationRecord,
    HarvestRecord,
    InputCatalog,
    InputRecommendation,
    InputRecommendationItem,
    IrrigationRecord,
    LoginVerificationCode,
    PestIncident,
    Plot,
    PurchasedInput,
    RainfallRecord,
    SoilAnalysis,
    StockOutput,
    TrustedBrowserToken,
    User,
)


def create_tables() -> None:
    Base.metadata.create_all(bind=engine)
    _sync_schema()


def _sync_schema() -> None:
    # Lightweight Postgres-friendly sync for new nullable columns added after first deploy.
    statements = [
        """
        CREATE TABLE IF NOT EXISTS farms (
            id SERIAL PRIMARY KEY,
            name VARCHAR(160) UNIQUE NOT NULL,
            location VARCHAR(180) NOT NULL,
            total_area NUMERIC(12,2) NOT NULL,
            boundary_geojson TEXT,
            notes TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS agronomic_profiles (
            id SERIAL PRIMARY KEY,
            farm_id INTEGER UNIQUE NOT NULL REFERENCES farms(id),
            culture VARCHAR(120) NOT NULL,
            region VARCHAR(180) NOT NULL,
            climate VARCHAR(180),
            soil_type VARCHAR(180),
            irrigation_system VARCHAR(120),
            plant_spacing VARCHAR(120),
            drip_spacing VARCHAR(120),
            fertilizers_used TEXT,
            crop_stage TEXT,
            common_pests TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS crop_seasons (
            id SERIAL PRIMARY KEY,
            farm_id INTEGER NOT NULL REFERENCES farms(id) ON DELETE CASCADE,
            variety_id INTEGER REFERENCES coffee_varieties(id) ON DELETE SET NULL,
            name VARCHAR(160) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            culture VARCHAR(120) NOT NULL,
            cultivated_area NUMERIC(12,2) NOT NULL,
            area_unit VARCHAR(20) NOT NULL DEFAULT 'ha',
            notes TEXT,
            status VARCHAR(20) NOT NULL DEFAULT 'planejada'
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fertilization_items (
            id SERIAL PRIMARY KEY,
            fertilization_record_id INTEGER NOT NULL REFERENCES fertilization_records(id) ON DELETE CASCADE,
            name VARCHAR(120) NOT NULL,
            unit VARCHAR(40) NOT NULL,
            quantity_per_hectare NUMERIC(10,2) NOT NULL,
            total_quantity NUMERIC(10,2) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS rainfall_records (
            id SERIAL PRIMARY KEY,
            farm_id INTEGER NOT NULL REFERENCES farms(id) ON DELETE CASCADE,
            rainfall_date DATE NOT NULL,
            millimeters NUMERIC(10,2) NOT NULL,
            source VARCHAR(120),
            notes TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS input_catalog (
            id SERIAL PRIMARY KEY,
            name VARCHAR(160) NOT NULL,
            normalized_name VARCHAR(180) UNIQUE NOT NULL,
            item_type VARCHAR(40) NOT NULL DEFAULT 'insumo_agricola',
            default_unit VARCHAR(20) NOT NULL DEFAULT 'kg',
            low_stock_threshold NUMERIC(10,2),
            is_active BOOLEAN NOT NULL DEFAULT TRUE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS equipment_assets (
            id SERIAL PRIMARY KEY,
            farm_id INTEGER REFERENCES farms(id) ON DELETE SET NULL,
            name VARCHAR(180) NOT NULL,
            category VARCHAR(120) NOT NULL,
            brand_model VARCHAR(180),
            asset_code VARCHAR(120),
            acquisition_date DATE,
            acquisition_value NUMERIC(12,2),
            status VARCHAR(60) NOT NULL DEFAULT 'ativo',
            notes TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS purchased_inputs (
            id SERIAL PRIMARY KEY,
            input_id INTEGER REFERENCES input_catalog(id) ON DELETE SET NULL,
            farm_id INTEGER REFERENCES farms(id) ON DELETE SET NULL,
            name VARCHAR(160) NOT NULL,
            normalized_name VARCHAR(180),
            quantity_purchased NUMERIC(10,2) NOT NULL,
            package_size NUMERIC(10,2) NOT NULL,
            package_unit VARCHAR(20) NOT NULL,
            unit_price NUMERIC(10,2) NOT NULL,
            purchase_date DATE,
            total_quantity NUMERIC(12,2) NOT NULL,
            available_quantity NUMERIC(12,2),
            total_cost NUMERIC(12,2) NOT NULL,
            low_stock_threshold NUMERIC(10,2),
            notes TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS input_recommendations (
            id SERIAL PRIMARY KEY,
            farm_id INTEGER REFERENCES farms(id) ON DELETE SET NULL,
            plot_id INTEGER REFERENCES plots(id) ON DELETE SET NULL,
            application_name VARCHAR(160) NOT NULL,
            purchased_input_id INTEGER REFERENCES purchased_inputs(id) ON DELETE CASCADE,
            unit VARCHAR(20),
            quantity_per_hectare NUMERIC(10,2),
            notes TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS input_recommendation_items (
            id SERIAL PRIMARY KEY,
            recommendation_id INTEGER NOT NULL REFERENCES input_recommendations(id) ON DELETE CASCADE,
            input_id INTEGER REFERENCES input_catalog(id) ON DELETE CASCADE,
            purchased_input_id INTEGER REFERENCES purchased_inputs(id) ON DELETE CASCADE,
            unit VARCHAR(20) NOT NULL,
            quantity NUMERIC(10,2) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fertilization_schedules (
            id SERIAL PRIMARY KEY,
            plot_id INTEGER NOT NULL REFERENCES plots(id) ON DELETE CASCADE,
            season_id INTEGER REFERENCES crop_seasons(id) ON DELETE SET NULL,
            scheduled_date DATE NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'scheduled',
            notes TEXT,
            fertilization_record_id INTEGER REFERENCES fertilization_records(id) ON DELETE SET NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fertilization_schedule_items (
            id SERIAL PRIMARY KEY,
            schedule_id INTEGER NOT NULL REFERENCES fertilization_schedules(id) ON DELETE CASCADE,
            input_id INTEGER REFERENCES input_catalog(id) ON DELETE CASCADE,
            purchased_input_id INTEGER REFERENCES purchased_inputs(id) ON DELETE CASCADE,
            name VARCHAR(160) NOT NULL,
            unit VARCHAR(20) NOT NULL,
            quantity NUMERIC(10,2) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS stock_outputs (
            id SERIAL PRIMARY KEY,
            input_id INTEGER NOT NULL REFERENCES input_catalog(id) ON DELETE CASCADE,
            purchased_input_id INTEGER REFERENCES purchased_inputs(id) ON DELETE SET NULL,
            farm_id INTEGER REFERENCES farms(id) ON DELETE SET NULL,
            plot_id INTEGER REFERENCES plots(id) ON DELETE SET NULL,
            season_id INTEGER REFERENCES crop_seasons(id) ON DELETE SET NULL,
            movement_date DATE NOT NULL,
            quantity NUMERIC(10,2) NOT NULL,
            unit VARCHAR(20) NOT NULL,
            origin VARCHAR(40) NOT NULL,
            reference_type VARCHAR(40),
            reference_id INTEGER,
            unit_cost NUMERIC(10,4),
            total_cost NUMERIC(12,2),
            notes TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS login_verification_codes (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            code_hash VARCHAR(255) NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            attempts_count INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 5,
            used_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS trusted_browser_tokens (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            token_hash VARCHAR(255) NOT NULL UNIQUE,
            user_agent_hash VARCHAR(255) NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            last_used_at TIMESTAMPTZ,
            revoked_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fertilization_stock_allocations (
            id SERIAL PRIMARY KEY,
            fertilization_item_id INTEGER NOT NULL REFERENCES fertilization_items(id) ON DELETE CASCADE,
            purchased_input_id INTEGER NOT NULL REFERENCES purchased_inputs(id) ON DELETE CASCADE,
            quantity_used NUMERIC(10,2) NOT NULL,
            unit_cost NUMERIC(10,4) NOT NULL,
            total_cost NUMERIC(12,2) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS soil_analyses (
            id SERIAL PRIMARY KEY,
            farm_id INTEGER NOT NULL REFERENCES farms(id),
            plot_id INTEGER NOT NULL REFERENCES plots(id),
            analysis_date DATE NOT NULL,
            laboratory VARCHAR(180) NOT NULL,
            ph NUMERIC(6,2),
            organic_matter NUMERIC(8,2),
            phosphorus NUMERIC(8,2),
            potassium NUMERIC(8,2),
            calcium NUMERIC(8,2),
            magnesium NUMERIC(8,2),
            aluminum NUMERIC(8,2),
            h_al NUMERIC(8,2),
            ctc NUMERIC(8,2),
            base_saturation NUMERIC(8,2),
            observations TEXT,
            pdf_filename VARCHAR(255),
            pdf_content_type VARCHAR(120),
            pdf_data BYTEA,
            liming_need_t_ha NUMERIC(8,2),
            npk_recommendation TEXT,
            micronutrient_recommendation TEXT,
            ai_recommendation TEXT,
            ai_status VARCHAR(40),
            ai_model VARCHAR(120),
            ai_error TEXT,
            ai_generated_at TIMESTAMPTZ
        )
        """,
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS active_farm_id INTEGER",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS active_season_id INTEGER",
        "ALTER TABLE input_catalog ADD COLUMN IF NOT EXISTS item_type VARCHAR(40) DEFAULT 'insumo_agricola'",
        "ALTER TABLE purchased_inputs ADD COLUMN IF NOT EXISTS input_id INTEGER",
        "ALTER TABLE purchased_inputs ADD COLUMN IF NOT EXISTS normalized_name VARCHAR(180)",
        "ALTER TABLE input_recommendation_items ADD COLUMN IF NOT EXISTS input_id INTEGER",
        "ALTER TABLE fertilization_items ADD COLUMN IF NOT EXISTS input_id INTEGER",
        "ALTER TABLE fertilization_schedule_items ADD COLUMN IF NOT EXISTS input_id INTEGER",
        "ALTER TABLE coffee_varieties ADD COLUMN IF NOT EXISTS flavor_profile VARCHAR(180)",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS planting_date DATE",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS farm_id INTEGER",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS spacing_row_meters NUMERIC(8,2)",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS spacing_plant_meters NUMERIC(8,2)",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS estimated_yield_sacks NUMERIC(10,2)",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS centroid_lat NUMERIC(10,6)",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS centroid_lng NUMERIC(10,6)",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS boundary_geojson TEXT",
        "ALTER TABLE farms ADD COLUMN IF NOT EXISTS boundary_geojson TEXT",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS irrigation_type VARCHAR(40) DEFAULT 'none'",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS irrigation_line_count INTEGER",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS irrigation_line_length_meters NUMERIC(10,2)",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS drip_spacing_meters NUMERIC(8,3)",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS drip_liters_per_hour NUMERIC(10,2)",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS sprinkler_count INTEGER",
        "ALTER TABLE plots ADD COLUMN IF NOT EXISTS sprinkler_liters_per_hour NUMERIC(10,2)",
        "ALTER TABLE farms ADD COLUMN IF NOT EXISTS notes TEXT",
        "ALTER TABLE plots ALTER COLUMN location DROP NOT NULL",
        "ALTER TABLE irrigation_records ADD COLUMN IF NOT EXISTS water_volume_mm NUMERIC(10,2)",
        "ALTER TABLE irrigation_records ADD COLUMN IF NOT EXISTS method VARCHAR(80)",
        "ALTER TABLE irrigation_records ADD COLUMN IF NOT EXISTS volume_liters NUMERIC(12,2)",
        "ALTER TABLE irrigation_records ADD COLUMN IF NOT EXISTS duration_minutes INTEGER",
        "ALTER TABLE harvest_records ADD COLUMN IF NOT EXISTS productivity_per_hectare NUMERIC(10,2)",
        "ALTER TABLE purchased_inputs ADD COLUMN IF NOT EXISTS purchase_date DATE",
        "ALTER TABLE purchased_inputs ADD COLUMN IF NOT EXISTS available_quantity NUMERIC(12,2)",
        "ALTER TABLE purchased_inputs ADD COLUMN IF NOT EXISTS low_stock_threshold NUMERIC(10,2)",
        "ALTER TABLE fertilization_records ADD COLUMN IF NOT EXISTS season_id INTEGER",
        "ALTER TABLE fertilization_schedules ADD COLUMN IF NOT EXISTS season_id INTEGER",
        "ALTER TABLE stock_outputs ADD COLUMN IF NOT EXISTS season_id INTEGER",
        "ALTER TABLE input_recommendations ADD COLUMN IF NOT EXISTS plot_id INTEGER",
        "ALTER TABLE fertilization_items ADD COLUMN IF NOT EXISTS purchased_input_id INTEGER",
        "ALTER TABLE fertilization_items ADD COLUMN IF NOT EXISTS unit_cost NUMERIC(10,4)",
        "ALTER TABLE fertilization_items ADD COLUMN IF NOT EXISTS total_cost NUMERIC(12,2)",
    ]
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
        connection.execute(text("ALTER TABLE irrigation_records ALTER COLUMN water_volume_mm DROP NOT NULL"))
        connection.execute(text("ALTER TABLE irrigation_records ALTER COLUMN method DROP NOT NULL"))
        connection.execute(text("ALTER TABLE input_recommendations ALTER COLUMN purchased_input_id DROP NOT NULL"))
        connection.execute(text("ALTER TABLE input_recommendations ALTER COLUMN unit DROP NOT NULL"))
        connection.execute(text("ALTER TABLE input_recommendations ALTER COLUMN quantity_per_hectare DROP NOT NULL"))
        connection.execute(text("ALTER TABLE input_recommendation_items ALTER COLUMN purchased_input_id DROP NOT NULL"))
        connection.execute(text("ALTER TABLE fertilization_schedule_items ALTER COLUMN purchased_input_id DROP NOT NULL"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_trusted_browser_tokens_user_id ON trusted_browser_tokens(user_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_trusted_browser_tokens_token_hash ON trusted_browser_tokens(token_hash)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_trusted_browser_tokens_expires_at ON trusted_browser_tokens(expires_at)"))
        connection.execute(text("UPDATE plots SET irrigation_type = 'none' WHERE irrigation_type IS NULL"))
        connection.execute(text("ALTER TABLE plots ALTER COLUMN irrigation_type SET DEFAULT 'none'"))
        connection.execute(text("UPDATE purchased_inputs SET purchase_date = CURRENT_DATE WHERE purchase_date IS NULL"))
        connection.execute(text("UPDATE purchased_inputs SET available_quantity = total_quantity WHERE available_quantity IS NULL"))
        connection.execute(text("UPDATE purchased_inputs SET low_stock_threshold = 0 WHERE low_stock_threshold IS NULL"))
        connection.execute(text("UPDATE input_catalog SET item_type = 'insumo_agricola' WHERE item_type IS NULL OR item_type = ''"))
        connection.execute(
            text(
                """
                UPDATE purchased_inputs
                SET normalized_name = lower(
                    regexp_replace(
                        translate(trim(name), 'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇç', 'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCc'),
                        '\s+',
                        ' ',
                        'g'
                    )
                )
                WHERE normalized_name IS NULL OR normalized_name = ''
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO input_catalog (name, normalized_name, default_unit, low_stock_threshold, is_active)
                SELECT MIN(name) AS name, normalized_name, MIN(package_unit) AS default_unit, MAX(COALESCE(low_stock_threshold, 0)) AS low_stock_threshold, TRUE
                FROM purchased_inputs
                WHERE normalized_name IS NOT NULL AND normalized_name <> ''
                GROUP BY normalized_name
                ON CONFLICT (normalized_name) DO UPDATE
                SET
                    name = EXCLUDED.name,
                    default_unit = COALESCE(input_catalog.default_unit, EXCLUDED.default_unit),
                    low_stock_threshold = GREATEST(COALESCE(input_catalog.low_stock_threshold, 0), COALESCE(EXCLUDED.low_stock_threshold, 0)),
                    is_active = TRUE
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE purchased_inputs entry
                SET input_id = catalog.id
                FROM input_catalog catalog
                WHERE entry.normalized_name = catalog.normalized_name
                  AND (entry.input_id IS NULL OR entry.input_id <> catalog.id)
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE input_recommendation_items item
                SET input_id = entry.input_id
                FROM purchased_inputs entry
                WHERE item.purchased_input_id = entry.id
                  AND (item.input_id IS NULL OR item.input_id <> entry.input_id)
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE fertilization_items item
                SET input_id = entry.input_id
                FROM purchased_inputs entry
                WHERE item.purchased_input_id = entry.id
                  AND (item.input_id IS NULL OR item.input_id <> entry.input_id)
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE fertilization_schedule_items item
                SET input_id = entry.input_id
                FROM purchased_inputs entry
                WHERE item.purchased_input_id = entry.id
                  AND (item.input_id IS NULL OR item.input_id <> entry.input_id)
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO input_recommendation_items (recommendation_id, purchased_input_id, unit, quantity)
                SELECT id, purchased_input_id, COALESCE(unit, 'kg'), COALESCE(quantity_per_hectare, 0)
                FROM input_recommendations recommendation
                WHERE recommendation.purchased_input_id IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM input_recommendation_items item
                    WHERE item.recommendation_id = recommendation.id
                  )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO stock_outputs (
                    input_id,
                    purchased_input_id,
                    farm_id,
                    plot_id,
                    season_id,
                    movement_date,
                    quantity,
                    unit,
                    origin,
                    reference_type,
                    reference_id,
                    unit_cost,
                    total_cost,
                    notes
                )
                SELECT
                    entry.input_id,
                    allocation.purchased_input_id,
                    plot.farm_id,
                    record.plot_id,
                    record.season_id,
                    record.application_date,
                    allocation.quantity_used,
                    item.unit,
                    'fertilizacao',
                    'fertilization_record',
                    record.id,
                    allocation.unit_cost,
                    allocation.total_cost,
                    'Saida gerada automaticamente a partir da aplicacao historica'
                FROM fertilization_stock_allocations allocation
                JOIN fertilization_items item ON item.id = allocation.fertilization_item_id
                JOIN fertilization_records record ON record.id = item.fertilization_record_id
                LEFT JOIN plots plot ON plot.id = record.plot_id
                LEFT JOIN purchased_inputs entry ON entry.id = allocation.purchased_input_id
                WHERE entry.input_id IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1
                    FROM stock_outputs output
                    WHERE output.reference_type = 'fertilization_record'
                      AND output.reference_id = record.id
                      AND output.purchased_input_id = allocation.purchased_input_id
                      AND output.quantity = allocation.quantity_used
                  )
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE fertilization_records record
                SET season_id = season.id
                FROM plots plot
                JOIN crop_seasons season
                  ON season.farm_id = plot.farm_id
                WHERE record.plot_id = plot.id
                  AND record.application_date BETWEEN season.start_date AND season.end_date
                  AND (season.variety_id IS NULL OR season.variety_id = plot.variety_id)
                  AND record.season_id IS NULL
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE fertilization_schedules schedule
                SET season_id = season.id
                FROM plots plot
                JOIN crop_seasons season
                  ON season.farm_id = plot.farm_id
                WHERE schedule.plot_id = plot.id
                  AND schedule.scheduled_date BETWEEN season.start_date AND season.end_date
                  AND (season.variety_id IS NULL OR season.variety_id = plot.variety_id)
                  AND schedule.season_id IS NULL
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE stock_outputs output
                SET season_id = season.id
                FROM crop_seasons season
                WHERE output.farm_id = season.farm_id
                  AND output.movement_date BETWEEN season.start_date AND season.end_date
                  AND output.season_id IS NULL
                """
            )
        )
        connection.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE constraint_name = 'plots_farm_id_fkey'
                    ) THEN
                        ALTER TABLE plots
                        ADD CONSTRAINT plots_farm_id_fkey
                        FOREIGN KEY (farm_id) REFERENCES farms(id);
                    END IF;
                END $$;
                """
            )
        )
        connection.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE constraint_name = 'fertilization_records_season_id_fkey'
                    ) THEN
                        ALTER TABLE fertilization_records
                        ADD CONSTRAINT fertilization_records_season_id_fkey
                        FOREIGN KEY (season_id) REFERENCES crop_seasons(id) ON DELETE SET NULL;
                    END IF;
                END $$;
                """
            )
        )
        connection.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE constraint_name = 'fertilization_schedules_season_id_fkey'
                    ) THEN
                        ALTER TABLE fertilization_schedules
                        ADD CONSTRAINT fertilization_schedules_season_id_fkey
                        FOREIGN KEY (season_id) REFERENCES crop_seasons(id) ON DELETE SET NULL;
                    END IF;
                END $$;
                """
            )
        )
        connection.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE constraint_name = 'stock_outputs_season_id_fkey'
                    ) THEN
                        ALTER TABLE stock_outputs
                        ADD CONSTRAINT stock_outputs_season_id_fkey
                        FOREIGN KEY (season_id) REFERENCES crop_seasons(id) ON DELETE SET NULL;
                    END IF;
                END $$;
                """
            )
        )
        connection.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE constraint_name = 'purchased_inputs_input_id_fkey'
                    ) THEN
                        ALTER TABLE purchased_inputs
                        ADD CONSTRAINT purchased_inputs_input_id_fkey
                        FOREIGN KEY (input_id) REFERENCES input_catalog(id);
                    END IF;
                END $$;
                """
            )
        )
        connection.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE constraint_name = 'input_recommendation_items_input_id_fkey'
                    ) THEN
                        ALTER TABLE input_recommendation_items
                        ADD CONSTRAINT input_recommendation_items_input_id_fkey
                        FOREIGN KEY (input_id) REFERENCES input_catalog(id);
                    END IF;
                END $$;
                """
            )
        )
        connection.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE constraint_name = 'fertilization_items_input_id_fkey'
                    ) THEN
                        ALTER TABLE fertilization_items
                        ADD CONSTRAINT fertilization_items_input_id_fkey
                        FOREIGN KEY (input_id) REFERENCES input_catalog(id);
                    END IF;
                END $$;
                """
            )
        )
        connection.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE constraint_name = 'fertilization_schedule_items_input_id_fkey'
                    ) THEN
                        ALTER TABLE fertilization_schedule_items
                        ADD CONSTRAINT fertilization_schedule_items_input_id_fkey
                        FOREIGN KEY (input_id) REFERENCES input_catalog(id);
                    END IF;
                END $$;
                """
            )
        )


def seed_admin(db: Session) -> None:
    settings = get_settings()
    existing = db.query(User).filter(User.email == settings.admin_email).first()
    if existing:
        if not existing.is_admin:
            existing.is_admin = True
            db.add(existing)
            db.commit()
        return

    admin = User(
        name="Administrador",
        email=settings.admin_email,
        hashed_password=get_password_hash(settings.admin_password),
        is_active=True,
        is_admin=True,
    )
    db.add(admin)
    db.commit()


def seed_demo_data(db: Session) -> None:
    # Never inject demo data into an environment that already has operational records.
    if db.query(Farm).count() or db.query(Plot).count():
        return

    catuai = db.query(CoffeeVariety).filter(CoffeeVariety.name == "Catuai 144").first()
    if not catuai:
        catuai = CoffeeVariety(
            name="Catuai 144",
            species="Arabica",
            maturation_cycle="Media",
            flavor_profile="Doce, caramelo e frutas amarelas",
            notes="Alta adaptacao em cafeicultura de montanha.",
        )
        db.add(catuai)
        db.flush()

    mundo_novo = db.query(CoffeeVariety).filter(CoffeeVariety.name == "Mundo Novo").first()
    if not mundo_novo:
        mundo_novo = CoffeeVariety(
            name="Mundo Novo",
            species="Arabica",
            maturation_cycle="Tardia",
            flavor_profile="Chocolate e castanhas",
            notes="Boa produtividade e vigor.",
        )
        db.add(mundo_novo)
        db.flush()

    farm = Farm(
        name="Fazenda Bela Vista",
        location="Manhuacu - MG",
        total_area=28.5,
        boundary_geojson='{"type":"Polygon","coordinates":[[[-42.8784,-20.7415],[-42.8686,-20.7415],[-42.8686,-20.7472],[-42.8784,-20.7472],[-42.8784,-20.7415]]]}',
        notes="Unidade principal com foco em cafe especial.",
    )
    db.add(farm)
    db.flush()

    db.add(
        AgronomicProfile(
            farm_id=farm.id,
            culture="Cafe arabica",
            region="Bahia - Brasil",
            climate="Tropical semiarido com estacao chuvosa no verao",
            soil_type="Argilosa a media",
            irrigation_system="Gotejamento",
            plant_spacing="60 a 70 cm",
            drip_spacing="30 cm",
            fertilizers_used="MAP\nNPK\nUreia\nEsterco curtido",
            crop_stage="implantacao\nformacao de raiz\ndesenvolvimento vegetativo",
            common_pests="bicho-mineiro\nbroca-do-cafe\nferrugem",
        )
    )

    plot_alpha = Plot(
        name="Setor Alpha",
        area_hectares=12.5,
        location=farm.location,
        planting_date=date.fromisoformat("2019-10-12"),
        plant_count=42000,
        spacing_row_meters=3.5,
        spacing_plant_meters=0.6,
        estimated_yield_sacks=420,
        centroid_lat=-20.743218,
        centroid_lng=-42.874221,
        boundary_geojson='{"type":"Polygon","coordinates":[[[-42.8758,-20.7428],[-42.8736,-20.7428],[-42.8736,-20.7442],[-42.8758,-20.7442],[-42.8758,-20.7428]]]}',
        irrigation_type="gotejo",
        irrigation_line_count=12,
        irrigation_line_length_meters=90,
        drip_spacing_meters=0.3,
        drip_liters_per_hour=1.6,
        notes="Talhao com maior vigor vegetativo.",
        farm_id=farm.id,
        variety_id=catuai.id,
    )
    plot_beta = Plot(
        name="Setor Beta",
        area_hectares=8.2,
        location=farm.location,
        planting_date=date.fromisoformat("2017-11-05"),
        plant_count=25500,
        spacing_row_meters=3.4,
        spacing_plant_meters=0.7,
        estimated_yield_sacks=255,
        centroid_lat=-20.745418,
        centroid_lng=-42.870411,
        boundary_geojson='{"type":"Polygon","coordinates":[[[-42.8717,-20.7447],[-42.8694,-20.7447],[-42.8694,-20.7461],[-42.8717,-20.7461],[-42.8717,-20.7447]]]}',
        irrigation_type="aspersor",
        sprinkler_count=24,
        sprinkler_liters_per_hour=480,
        notes="Area com foco em qualidade de bebida.",
        farm_id=farm.id,
        variety_id=mundo_novo.id,
    )
    db.add_all([plot_alpha, plot_beta])
    db.flush()

    db.add_all(
        [
            IrrigationRecord(plot_id=plot_alpha.id, irrigation_date=date.fromisoformat("2026-03-10"), volume_liters=18000, duration_minutes=90, notes="Irrigacao preventiva."),
            IrrigationRecord(plot_id=plot_beta.id, irrigation_date=date.fromisoformat("2026-03-12"), volume_liters=12500, duration_minutes=70, notes="Complemento apos estiagem."),
            FertilizationRecord(plot_id=plot_alpha.id, application_date=date.fromisoformat("2026-02-05"), product="NPK 20-05-20", dose="320 kg/ha", cost=4850, notes="Cobertura 1."),
            FertilizationRecord(plot_id=plot_beta.id, application_date=date.fromisoformat("2026-02-18"), product="Organomineral 16-06-16", dose="280 kg/ha", cost=3620, notes="Aplicacao em sulco."),
            HarvestRecord(plot_id=plot_alpha.id, harvest_date=date.fromisoformat("2024-07-14"), sacks_produced=395, productivity_per_hectare=31.6, notes="Safra forte."),
            HarvestRecord(plot_id=plot_alpha.id, harvest_date=date.fromisoformat("2025-07-18"), sacks_produced=408, productivity_per_hectare=32.6, notes="Boa uniformidade."),
            HarvestRecord(plot_id=plot_beta.id, harvest_date=date.fromisoformat("2024-07-22"), sacks_produced=228, productivity_per_hectare=27.8, notes="Oscilacao por estiagem."),
            HarvestRecord(plot_id=plot_beta.id, harvest_date=date.fromisoformat("2025-07-20"), sacks_produced=241, productivity_per_hectare=29.4, notes="Recuperacao nutricional."),
            PestIncident(plot_id=plot_alpha.id, occurrence_date=date.fromisoformat("2026-03-08"), category="Praga", name="Bicho-mineiro", severity=2, treatment="Aplicacao seletiva", notes="Monitoramento em andamento."),
            SoilAnalysis(
                farm_id=farm.id,
                plot_id=plot_alpha.id,
                analysis_date=date.fromisoformat("2026-02-12"),
                laboratory="Laboratorio Referencia Cafe",
                ph=5.2,
                organic_matter=2.1,
                phosphorus=10.5,
                potassium=105,
                calcium=2.9,
                magnesium=0.8,
                aluminum=0.2,
                h_al=4.8,
                ctc=7.6,
                base_saturation=38,
                observations="Analise inicial para planejamento de implantacao.",
                liming_need_t_ha=3.34,
                npk_recommendation="Elevar fosforo e potassio com manejo parcelado.",
                micronutrient_recommendation="Monitorar Boro e Zinco.",
                ai_status="seeded",
            ),
            RainfallRecord(farm_id=farm.id, rainfall_date=date.fromisoformat("2026-03-02"), millimeters=18.5, source="Pluviometro", notes="Chuva isolada no inicio do mes."),
            RainfallRecord(farm_id=farm.id, rainfall_date=date.fromisoformat("2026-03-09"), millimeters=7.2, source="Pluviometro", notes="Precipitacao leve no fim da tarde."),
            RainfallRecord(farm_id=farm.id, rainfall_date=date.fromisoformat("2026-03-18"), millimeters=24.0, source="Pluviometro", notes="Evento mais intenso da quinzena."),
        ]
    )
    db.flush()

    purchased_input = PurchasedInput(
        farm_id=farm.id,
        name="MAP",
        quantity_purchased=20,
        package_size=50,
        package_unit="kg",
        unit_price=210,
        total_quantity=1000,
        total_cost=4200,
        notes="Lote inicial para implantacao.",
    )
    db.add(purchased_input)
    db.flush()

    db.add(
        InputRecommendation(
            farm_id=farm.id,
            application_name="Fertilizacao de Base",
            purchased_input_id=purchased_input.id,
            unit="kg",
            quantity_per_hectare=1.5,
            notes="Dose padrao de abertura.",
        )
    )

    for record in db.query(FertilizationRecord).all():
        if record.items:
            continue
        quantity_text = (record.dose or "0").split(" ")[0].replace(",", ".")
        try:
            quantity_value = float(quantity_text)
        except ValueError:
            quantity_value = 0
        area = float(record.plot.area_hectares) if record.plot and record.plot.area_hectares else 0
        db.add(
            FertilizationItem(
                fertilization_record_id=record.id,
                name=record.product,
                unit="kg",
                quantity_per_hectare=quantity_value,
                total_quantity=round(quantity_value * area, 2),
            )
        )
    db.commit()
