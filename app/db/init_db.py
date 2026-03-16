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
    Farm,
    FertilizationRecord,
    HarvestRecord,
    IrrigationRecord,
    PestIncident,
    Plot,
    SoilAnalysis,
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
    ]
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
        connection.execute(text("ALTER TABLE irrigation_records ALTER COLUMN water_volume_mm DROP NOT NULL"))
        connection.execute(text("ALTER TABLE irrigation_records ALTER COLUMN method DROP NOT NULL"))
        connection.execute(text("UPDATE plots SET irrigation_type = 'none' WHERE irrigation_type IS NULL"))
        connection.execute(text("ALTER TABLE plots ALTER COLUMN irrigation_type SET DEFAULT 'none'"))
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


def seed_admin(db: Session) -> None:
    settings = get_settings()
    existing = db.query(User).filter(User.email == settings.admin_email).first()
    if existing:
        return

    admin = User(
        name="Administrador",
        email=settings.admin_email,
        hashed_password=get_password_hash(settings.admin_password),
        is_active=True,
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
        ]
    )
    db.commit()
