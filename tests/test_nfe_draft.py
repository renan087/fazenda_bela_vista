from datetime import date
from decimal import Decimal
from types import SimpleNamespace
import json
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.db.base import Base
from app.services.nfe_draft import create_draft


def test_draft_is_an_independent_snapshot_and_no_fiscal_number():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    db = Session(engine)
    row = SimpleNamespace(id=7, farm_id=1, sale_code='V-0001', buyer_id=4, buyer_name='Comprador',
                          sale_date=date(2026, 11, 20), total_value=Decimal('120.10'),
                          equivalent_sacks=Decimal('2.0000'), lot_label='Lote 1')
    draft = create_draft(db, organization_id=2, farm_id=1, user_id=3, records=[row])
    row.buyer_name = 'Outro'
    snapshot = json.loads(draft.snapshot_json)
    assert snapshot['comprador'] == 'Comprador' and snapshot['valor_bruto'] == '120.10'
    assert not {'numero', 'chave', 'autorizada'} & set(snapshot)
    with pytest.raises(ValueError):
        create_draft(db, organization_id=2, farm_id=999, user_id=3, records=[row])
    db.close()
