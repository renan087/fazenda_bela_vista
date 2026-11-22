"""Preparação local: não reserva número, não assina e não transmite."""
import json
from decimal import Decimal
from uuid import uuid4
from app.models.nfe_draft import NfeDraft


def build_sale_snapshot(records):
    if not records:
        raise ValueError('Venda não encontrada.')
    first = records[0]
    if any((r.farm_id, r.sale_code, r.buyer_id, r.buyer_name) !=
           (first.farm_id, first.sale_code, first.buyer_id, first.buyer_name) for r in records):
        raise ValueError('Os dados da venda estão inconsistentes. Confira antes de preparar o rascunho.')
    lines = []
    for row in records:
        amount = Decimal(str(row.total_value))
        quantity = Decimal(str(row.equivalent_sacks))
        if not amount.is_finite() or not quantity.is_finite() or amount <= 0 or quantity <= 0:
            raise ValueError('Confira as quantidades e valores da venda.')
        lines.append({'lote': row.lot_label, 'quantidade_sacas': str(quantity), 'valor_bruto': str(amount)})
    return {'versao': 1, 'venda': first.sale_code, 'origem_ids': sorted(r.id for r in records), 'comprador': first.buyer_name,
            'comprador_id': first.buyer_id, 'data_venda': first.sale_date.isoformat(),
            'itens': lines, 'valor_bruto': str(sum((Decimal(i['valor_bruto']) for i in lines), Decimal('0'))),
            'aviso': 'Dados comerciais para revisão. Não são uma nota fiscal.'}


def create_draft(db, *, organization_id, farm_id, user_id, records):
    snapshot = build_sale_snapshot(records)
    if any(row.farm_id != farm_id for row in records):
        raise ValueError('Venda fora da fazenda selecionada.')
    draft = NfeDraft(id=uuid4().hex, organization_id=organization_id, farm_id=farm_id,
                     sale_code=records[0].sale_code, created_by=user_id,
                     snapshot_json=json.dumps(snapshot, ensure_ascii=False))
    db.add(draft)
    db.commit()
    return draft
