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
    from app.services.ibge_municipalities import resolve_municipality_code
    mun_code = getattr(buyer, 'municipality_code', None) or resolve_municipality_code(buyer.city, buyer.state)
    ie_indicator = getattr(buyer, 'ie_indicator', None) or ''
    return {'recipient_name': buyer.name or '', 'recipient_cpf': buyer.document or '',
            'recipient_ie': buyer.state_registration or '', 'recipient_ie_indicator': ie_indicator,
            'recipient_postal_code': buyer.postal_code or '',
            'recipient_street': buyer.street or '', 'recipient_number': buyer.number or '',
            'recipient_complement': buyer.complement or '', 'recipient_district': buyer.district or '',
            'recipient_city': buyer.city or '', 'recipient_municipality_code': mun_code or '',
            'recipient_state': buyer.state or ''}


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


def review_preparation(snapshot, issuer, details, *, details_saved):
    """Revisão de completude; não substitui XSD, regras fiscais ou autorização."""
    from datetime import date
    groups = []

    issuer_fields = [('name', 'Nome do produtor'), ('cpf', 'CPF do produtor'),
                     ('state_registration', 'Inscrição estadual do produtor'), ('postal_code', 'CEP do produtor'),
                     ('street', 'Logradouro do produtor'), ('number', 'Número do produtor'),
                     ('district', 'Bairro ou localidade do produtor'), ('city', 'Município do produtor'),
                     ('municipality_code', 'Código IBGE do município do produtor'), ('state', 'UF do produtor')]
    issues = [label for field, label in issuer_fields if not issuer or not str(getattr(issuer, field, '') or '').strip()]
    if issuer:
        try:
            clean_issuer({field: getattr(issuer, field, '') for field, _ in issuer_fields})
        except ValueError:
            issues.append('Revise o formato dos dados do produtor')
    groups.append({'title': 'Produtor e estabelecimento', 'issues': issues, 'target': 'produtor'})

    recipient_fields = [('recipient_name', 'Nome do destinatário'), ('recipient_cpf', 'CPF do destinatário'),
                        ('recipient_postal_code', 'CEP do destinatário'), ('recipient_street', 'Logradouro do destinatário'),
                        ('recipient_number', 'Número do destinatário'), ('recipient_district', 'Bairro ou localidade do destinatário'),
                        ('recipient_city', 'Município do destinatário'), ('recipient_municipality_code', 'Código IBGE do município do destinatário'),
                        ('recipient_state', 'UF do destinatário'), ('recipient_ie_indicator', 'Indicador de inscrição estadual')]
    issues = [label for field, label in recipient_fields if not str(details.get(field) or '').strip()]
    if not details_saved:
        issues.append('Confirme e salve os dados do destinatário no rascunho')
    if details.get('recipient_ie_indicator') == 'contribuinte' and not details.get('recipient_ie'):
        issues.append('Inscrição estadual do destinatário contribuinte')
    try:
        clean_draft_details(details)
    except ValueError:
        issues.append('Revise o formato ou a UF dos dados do destinatário e da operação')
    groups.append({'title': 'Destinatário', 'issues': issues, 'target': 'destinatario'})

    product_fields = [('product_description', 'Descrição fiscal do produto'), ('product_ncm', 'NCM'),
                      ('operation_nature', 'Natureza da operação'), ('operation_cfop', 'CFOP')]
    issues = [label for field, label in product_fields if not str(details.get(field) or '').strip()]
    if not details_saved:
        issues.append('Salve os dados do produto e da operação no rascunho')
    groups.append({'title': 'Produto e operação', 'issues': issues, 'target': 'destinatario'})

    issues = []
    try:
        date.fromisoformat(snapshot.get('data_venda') or '')
        items = snapshot.get('itens')
        if not isinstance(items, list) or not items:
            raise ValueError
        total = Decimal('0')
        for item in items:
            quantity = Decimal(str(item.get('quantidade_sacas', '')))
            amount = Decimal(str(item.get('valor_bruto', '')))
            if not item.get('lote') or not quantity.is_finite() or not amount.is_finite() or quantity <= 0 or amount <= 0:
                raise ValueError
            total += amount
        stored_total = Decimal(str(snapshot.get('valor_bruto', '')))
        if not stored_total.is_finite() or total != stored_total:
            issues.append('Total dos itens não confere com o total comercial registrado')
    except (ValueError, TypeError, AttributeError, ArithmeticError):
        issues.append('Revise data, lotes, quantidades e valores comerciais')
    groups.append({'title': 'Dados comerciais', 'issues': issues, 'target': 'dados-comerciais'})
    return {'groups': groups, 'pending_count': sum(len(g['issues']) for g in groups)}


def clean_technical_data(data, snapshot):
    """Somente formato e vínculo com o snapshot; não valida enquadramento fiscal."""
    import re
    from datetime import date
    raw_date = str(data.get('emission_date') or '').strip()
    if raw_date:
        try:
            if date.fromisoformat(raw_date).isoformat() != raw_date:
                raise ValueError
        except ValueError:
            raise ValueError('Informe uma data de emissão válida.') from None
    modality = str(data.get('freight_mode') or '').strip()
    if modality not in ('', '0', '1', '2', '3', '4', '9'):
        raise ValueError('Selecione uma modalidade de frete válida.')
    ids = snapshot.get('origem_ids')
    items = snapshot.get('itens')
    if not isinstance(ids, list) or not isinstance(items, list) or len(ids) != len(items) or not ids or len(set(ids)) != len(ids):
        raise ValueError('Os lotes do rascunho estão inconsistentes.')
    codes = [('origin', 1), ('icms_cst', 2), ('ibs_cbs_cst', 3), ('ibs_cbs_class', 6)]
    result = []
    for item_id in ids:
        if not isinstance(item_id, int) or isinstance(item_id, bool) or item_id <= 0:
            raise ValueError('Identificação inválida do lote.')
    for index in range(1, len(items) + 1):
        row = {'item_index': index}
        for field, size in codes:
            value = str(data.get(f'item_{index}_{field}') or '').strip()
            if value and not re.fullmatch(r'[0-9]{' + str(size) + '}', value):
                raise ValueError('Confira o formato dos códigos tributários de cada lote.')
            row[field] = value
        for field in ('commercial_unit', 'tax_unit'):
            value = str(data.get(f'item_{index}_{field}') or '').strip().upper()
            if value and not re.fullmatch(r'[A-Z0-9]{1,6}', value):
                raise ValueError('Unidades devem conter até seis letras ou números, sem espaços.')
            row[field] = value
        for field, scale in [('commercial_quantity', 4), ('tax_quantity', 4), ('commercial_unit_price', 10), ('tax_unit_price', 10)]:
            value = str(data.get(f'item_{index}_{field}') or '').strip().replace(',', '.')
            if value:
                if not re.fullmatch(r'[0-9]{1,11}(?:\.[0-9]{1,' + str(scale) + r'})?', value) or Decimal(value) <= 0:
                    raise ValueError('Confira a quantidade ou o valor unitário: use número positivo e precisão permitida.')
                value = format(Decimal(value), 'f')
            row[field] = value
        for field in ('icms_base', 'icms_value', 'ibs_cbs_base', 'ibs_uf_value', 'ibs_municipal_value', 'cbs_value'):
            raw = data.get(f'item_{index}_{field}')
            value = str(raw if raw is not None else '').strip().replace(',', '.')
            if value:
                if not re.fullmatch(r'[0-9]{1,13}(?:\.[0-9]{1,2})?', value):
                    raise ValueError('Confira os valores tributários: use valores não negativos com até duas casas decimais.')
                value = format(Decimal(value), 'f')
            row[field] = value
        for field in ('icms_rate', 'ibs_uf_rate', 'ibs_municipal_rate', 'cbs_rate'):
            raw = data.get(f'item_{index}_{field}')
            value = str(raw if raw is not None else '').strip().replace(',', '.')
            if value:
                if not re.fullmatch(r'[0-9]{1,3}(?:\.[0-9]{1,4})?', value) or Decimal(value) > 100:
                    raise ValueError('Confira as alíquotas de teste: informe percentual entre 0 e 100 com até quatro casas decimais.')
                value = format(Decimal(value), 'f')
            row[field] = value
        result.append(row)
    return {'version': 1, 'emission_date': raw_date, 'freight_mode': modality, 'items': result}


def review_item_values(snapshot, technical):
    """Confere valores informados, sem substituir as regras tributárias da NF-e."""
    from decimal import localcontext, ROUND_HALF_UP, InvalidOperation
    items = snapshot.get('itens') or []
    rows = technical.get('items') or []
    by_index = {row.get('item_index'): row for row in rows if isinstance(row, dict)}
    result = []
    for index, item in enumerate(items, 1):
        data = by_index.get(index, {})
        issues = []
        for prefix, label in [('commercial', 'comercial'), ('tax', 'tributável')]:
            unit, quantity, price = (data.get(prefix + '_unit'), data.get(prefix + '_quantity'), data.get(prefix + '_unit_price'))
            if not all((unit, quantity, price)):
                issues.append(f'Informe unidade, quantidade e valor unitário {label}.')
                continue
            try:
                with localcontext() as ctx:
                    ctx.prec = 50
                    qty, unit_price, gross = Decimal(str(quantity)), Decimal(str(price)), Decimal(str(item['valor_bruto']))
                    if not all(v.is_finite() and v > 0 for v in (qty, unit_price, gross)):
                        raise ValueError
                    calculated = (qty * unit_price).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    if calculated != gross:
                        issues.append(f'Quantidade × valor unitário {label} difere do valor bruto comercial; confira.')
            except (InvalidOperation, ValueError, TypeError, KeyError):
                issues.append(f'Confira os valores {label} informados.')
        result.append({'item_index': index, 'issues': issues})
    return result


def summarize_test_tax_values(snapshot, technical):
    """Soma somente valores explicitamente informados, sem determinar sua aplicabilidade."""
    from decimal import localcontext, InvalidOperation
    fields = [('icms_base', 'Base de ICMS'), ('icms_value', 'ICMS'), ('ibs_cbs_base', 'Base de IBS/CBS'),
              ('ibs_uf_value', 'IBS estadual'), ('ibs_municipal_value', 'IBS municipal'), ('cbs_value', 'CBS')]
    items = snapshot.get('itens') or []
    rows = technical.get('items') or []
    by_index = {row.get('item_index'): row for row in rows if isinstance(row, dict)}
    result = []
    for field, label in fields:
        total, missing = Decimal('0'), 0
        with localcontext() as ctx:
            ctx.prec = 50
            for index in range(1, len(items) + 1):
                raw = by_index.get(index, {}).get(field)
                if raw is None or str(raw).strip() == '':
                    missing += 1
                    continue
                try:
                    value = Decimal(str(raw))
                    if not value.is_finite() or value < 0 or value.as_tuple().exponent < -2:
                        raise ValueError
                    total += value
                except (InvalidOperation, ValueError):
                    missing += 1
        result.append({'field': field, 'label': label, 'total': format(total, '.2f') if items and missing == 0 else None,
                       'missing': missing if items else 0})
    return result


def review_test_tax_fields(snapshot, technical):
    """Inventaria campos salvos de teste, sem julgar obrigatoriedade ou validade fiscal."""
    groups = [
        ('ICMS', [('origin', 'origem'), ('icms_cst', 'CST'), ('icms_base', 'base'),
                  ('icms_rate', 'alíquota'), ('icms_value', 'valor')]),
        ('IBS/CBS', [('ibs_cbs_cst', 'CST'), ('ibs_cbs_class', 'classificação'),
                     ('ibs_cbs_base', 'base'), ('ibs_uf_rate', 'alíquota IBS estadual'),
                     ('ibs_uf_value', 'valor IBS estadual'), ('ibs_municipal_rate', 'alíquota IBS municipal'),
                     ('ibs_municipal_value', 'valor IBS municipal'), ('cbs_rate', 'alíquota CBS'),
                     ('cbs_value', 'valor CBS')]),
    ]
    items = snapshot.get('itens') or []
    rows = technical.get('items') or []
    by_index = {row.get('item_index'): row for row in rows if isinstance(row, dict)}
    result = {}
    for index in range(1, len(items) + 1):
        saved = by_index.get(index, {})
        result[index] = []
        for label, fields in groups:
            missing = [name for key, name in fields if saved.get(key) is None or str(saved.get(key)).strip() == '']
            result[index].append({'label': label, 'missing': missing, 'all_absent': len(missing) == len(fields)})
    return result
