from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base
from app.models import InputCatalog, PurchasedInput, StockOutput
from app.models.stock_assistant_draft import StockAssistantDraft
from app.repositories.farm import FarmRepository
from app.services.stock_assistant import claim_draft, confirm_consumption, parse_consumption


@pytest.mark.parametrize("message", [
    "Hoje usei dois sacos de Vita Nit S de 20 kg no Setor 3.",
    "Hoje usei dois sacos de 20 kg de Vita Nit S no Setor 3.",
    "Hoje usei 40 kg de Vita Nit S no Setor 3.",
])
def test_parses_total_quantity(message):
    fields = parse_consumption(message, date(2026, 11, 15))
    assert fields == {"quantity": Decimal("40"), "unit": "kg", "product": "vita nit s",
                      "plot": "setor 3", "movement_date": date(2026, 11, 15)}


@pytest.mark.parametrize("message", [
    "Amanhã vou aplicar 40 kg de Vita Nit S no Setor 3.",
    "Hoje usei 40 kg/ha de Vita Nit S no Setor 3.",
    "Hoje usei Vita Nit S no Setor 3.",
    "31/02/2026 usei 40 kg de Vita Nit S no Setor 3.",
])
def test_refuses_missing_or_unsafe_values(message):
    with pytest.raises(ValueError):
        parse_consumption(message, date(2026, 11, 15))


@pytest.fixture
def scenario():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        repo = FarmRepository(db)
        product = InputCatalog(name="Vita Nit S", normalized_name="vita nit s")
        db.add(product)
        db.flush()
        lot_fields = dict(input_id=product.id, name=product.name, quantity_purchased=1,
                          package_size=50, package_unit="kg", unit_price=50,
                          total_quantity=50, available_quantity=50, total_cost=50)
        lot = PurchasedInput(farm_id=1, **lot_fields)
        shared_lot = PurchasedInput(farm_id=None, **lot_fields)
        draft = StockAssistantDraft(id="draft", user_id=1, farm_id=1, season_id=1, plot_id=1,
                                    input_id=product.id, movement_date=date(2026, 11, 15),
                                    quantity=40, unit="kg", expires_at=datetime.now(timezone.utc) + timedelta(minutes=30))
        db.add_all([lot, shared_lot, draft])
        db.commit()
        yield repo, draft, lot, shared_lot


def test_confirm_consumes_once(scenario):
    repo, draft, lot, shared_lot = scenario
    assert repo.db.query(StockOutput).count() == 0 and lot.available_quantity == 50
    output = confirm_consumption(repo, draft)
    repo.db.commit()
    assert output.quantity == 40 and output.origin == "Assistente SiSFarm"
    assert confirm_consumption(repo, draft) is None
    assert lot.available_quantity == 10 and shared_lot.available_quantity == 50
    assert repo.db.query(StockOutput).filter_by(purchased_input_id=None).count() == 1


def test_insufficient_farm_stock_rolls_back_claim(scenario):
    repo, draft, lot, _ = scenario
    draft.quantity = 60
    repo.db.commit()
    with pytest.raises(ValueError, match="Estoque insuficiente"):
        confirm_consumption(repo, draft)
    repo.db.rollback()
    assert draft.status == "pending" and lot.available_quantity == 50
    assert repo.db.query(StockOutput).count() == 0


def test_discard_prevents_confirmation(scenario):
    repo, draft, lot, _ = scenario
    assert claim_draft(repo.db, draft, "discarded")
    repo.db.commit()
    assert confirm_consumption(repo, draft) is None and lot.available_quantity == 50


def test_expired_draft_cannot_be_confirmed(scenario):
    repo, draft, lot, _ = scenario
    draft.expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    repo.db.commit()
    assert confirm_consumption(repo, draft) is None and lot.available_quantity == 50
