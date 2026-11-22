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


def clean_issuer(data):
    """Valida formato sem presumir credenciamento ou enquadramento tributário."""
    import re
    from app.services.coffee_buyers import valid_cpf
    limits = {'name': 180, 'state_registration': 30, 'street': 180, 'number': 30,
              'complement': 120, 'district': 120, 'city': 120}
    cleaned = {}
    for field, limit in limits.items():
        value = ' '.join(str(data.get(field) or '').split())
        if len(value) > limit:
            raise ValueError('Um dos campos excede o tamanho permitido.')
        cleaned[field] = value
    for field, size in [('cpf', 11), ('postal_code', 8), ('municipality_code', 7)]:
        raw = str(data.get(field) or '').strip()
        if re.search(r'[^0-9.\-/\s]', raw):
            raise ValueError('CPF, CEP e código do município devem conter apenas números.')
        value = re.sub(r'\D', '', raw)
        if value and len(value) != size:
            raise ValueError('Confira CPF, CEP e código IBGE do município.')
        cleaned[field] = value
    if cleaned['cpf'] and not valid_cpf(cleaned['cpf']):
        raise ValueError('CPF inválido.')
    cleaned['state'] = str(data.get('state') or 'BA').strip().upper()
    if cleaned['state'] != 'BA':
        raise ValueError('Por enquanto o cadastro fiscal está disponível apenas para a Bahia.')
    return cleaned


def buyer_prefill(buyer):
    """Sugestão inicial, nunca uma afirmação de conformidade fiscal."""
    if not buyer or buyer.person_type != 'pf':
        return {}
    return {'recipient_name': buyer.name or '', 'recipient_cpf': buyer.document or '',
            'recipient_ie': buyer.state_registration or '', 'recipient_postal_code': buyer.postal_code or '',
            'recipient_street': buyer.street or '', 'recipient_number': buyer.number or '',
            'recipient_complement': buyer.complement or '', 'recipient_district': buyer.district or '',
            'recipient_city': buyer.city or '', 'recipient_state': buyer.state or ''}


def clean_draft_details(data):
    import re
    from app.services.coffee_buyers import valid_cpf
    limits = {'recipient_name': 180, 'recipient_ie': 30, 'recipient_street': 180,
              'recipient_number': 30, 'recipient_complement': 120, 'recipient_district': 120,
              'recipient_city': 120, 'product_description': 180, 'operation_nature': 120}
    cleaned = {}
    for name, limit in limits.items():
        text = ' '.join(str(data.get(name) or '').split())
        if len(text) > limit:
            raise ValueError('Um dos campos excede o tamanho permitido.')
        cleaned[name] = text
    for field, length in [('recipient_cpf', 11), ('recipient_postal_code', 8),
                          ('recipient_municipality_code', 7), ('product_ncm', 8), ('operation_cfop', 4)]:
        raw = str(data.get(field) or '').strip()
        if re.search(r'[^0-9.\-/\s]', raw):
            raise ValueError('Confira os campos numéricos informados.')
        value = re.sub(r'\D', '', raw)
        if value and len(value) != length:
            raise ValueError('Confira o tamanho dos campos numéricos informados.')
        cleaned[field] = value
    if cleaned['recipient_cpf'] and not valid_cpf(cleaned['recipient_cpf']):
        raise ValueError('CPF do destinatário inválido.')
    state = str(data.get('recipient_state') or '').strip().upper()
    if state not in ('', 'BA'):
        raise ValueError('Esta etapa aceita apenas destinatário na Bahia.')
    cleaned['recipient_state'] = state
    indicator = str(data.get('recipient_ie_indicator') or '').strip()
    if indicator not in ('', 'contribuinte', 'isento', 'nao_contribuinte'):
        raise ValueError('Indicador de inscrição estadual inválido.')
    cleaned['recipient_ie_indicator'] = indicator
    return cleaned
