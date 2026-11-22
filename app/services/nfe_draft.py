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
        unit_price = getattr(row, 'unit_price', None)
        lines.append({
            'lote': row.lot_label,
            'quantidade_sacas': str(quantity),
            'valor_bruto': str(amount),
            'unidade': getattr(row, 'sale_unit', None) or 'sc_60',
            'preco_unitario': str(Decimal(str(unit_price))) if unit_price is not None else '',
        })
    freight_val = getattr(first, 'freight_value', None)
    return {
        'versao': 1,
        'venda': first.sale_code,
        'origem_ids': sorted(r.id for r in records),
        'comprador': first.buyer_name,
        'comprador_id': first.buyer_id,
        'data_venda': first.sale_date.isoformat(),
        'coffee_type': getattr(first, 'coffee_type', None) or 'beneficiado',
        'payment_condition': getattr(first, 'payment_condition', None) or 'a_vista',
        'payment_method': getattr(first, 'payment_method', None) or '',
        'freight_value': str(Decimal(str(freight_val))) if freight_val is not None else '0',
        'itens': lines,
        'valor_bruto': str(sum((Decimal(i['valor_bruto']) for i in lines), Decimal('0'))),
        'aviso': 'Dados comerciais para revisão. Não são uma nota fiscal.',
    }


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


def map_sale_payment_and_freight(snapshot: dict) -> dict:
    """Traduz pagamento e frete gravados na venda para exibição amigável e códigos de teste."""
    raw_method = str((snapshot or {}).get('payment_method') or '').strip()
    raw_cond = str((snapshot or {}).get('payment_condition') or 'a_vista').strip().lower()
    lower_method = raw_method.lower()
    if 'pix' in lower_method:
        t_pag = '17'
    elif 'dinheiro' in lower_method:
        t_pag = '01'
    elif 'boleto' in lower_method:
        t_pag = '15'
    elif 'transfer' in lower_method or 'ted' in lower_method or 'doc' in lower_method:
        t_pag = '18'
    elif 'crédito' in lower_method or 'credito' in lower_method:
        t_pag = '03'
    elif 'débito' in lower_method or 'debito' in lower_method:
        t_pag = '04'
    else:
        t_pag = '99' if raw_method else ''

    cond_label = 'a prazo' if raw_cond == 'a_prazo' else 'à vista'
    payment_label = f'{raw_method} · {cond_label}' if raw_method else f'A definir · {cond_label}'

    try:
        freight_num = Decimal(str((snapshot or {}).get('freight_value') or '0'))
    except Exception:
        freight_num = Decimal('0')
    default_freight_mode = '9' if freight_num <= 0 else '0'
    return {
        'payment_label': payment_label,
        't_pag': t_pag,
        'ind_pag': '1' if raw_cond == 'a_prazo' else '0',
        'default_freight_mode': default_freight_mode,
    }


def apply_tax_profile_to_draft(db, *, draft, profile, buyer, user_id: int) -> None:
    """Aplica regras aprovadas de um NfeTaxProfile aos dados salvos do rascunho (1 clique)."""
    from app.models.nfe_draft import NfeDraftDetails, NfeDraftTechnicalData
    from app.services.nfe_snapshots import record_draft_event

    snapshot = json.loads(draft.snapshot_json)
    rules = json.loads(profile.rules_json or '{}')
    sale_map = map_sale_payment_and_freight(snapshot)

    # 1. Atualiza/cria NfeDraftDetails preservando destinatário salvo (ou prefill do comprador).
    details_row = db.query(NfeDraftDetails).filter_by(
        draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id
    ).first()
    details_data = json.loads(details_row.data_json) if details_row else buyer_prefill(buyer)
    for key in ('product_description', 'product_ncm', 'operation_nature', 'operation_cfop'):
        if rules.get(key):
            details_data[key] = str(rules[key]).strip()
    cleaned_details = clean_draft_details(details_data)
    if not details_row:
        details_row = NfeDraftDetails(
            draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id, updated_by=user_id
        )
        db.add(details_row)
    details_row.data_json = json.dumps(cleaned_details, ensure_ascii=False)
    details_row.updated_by = user_id

    # 2. Atualiza/cria NfeDraftTechnicalData por lote sem inventar impostos fora do perfil.
    tech_row = db.query(NfeDraftTechnicalData).filter_by(
        draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id
    ).first()
    existing_tech = json.loads(tech_row.data_json) if tech_row else {}
    existing_by_idx = {i.get('item_index'): i for i in existing_tech.get('items', []) if isinstance(i, dict)}

    unit = str(rules.get('unit') or 'SC').strip().upper()[:6]
    form_like = {
        'emission_date': existing_tech.get('emission_date') or snapshot.get('data_venda') or '',
        'freight_mode': existing_tech.get('freight_mode') or sale_map['default_freight_mode'],
    }
    for idx, item in enumerate(snapshot.get('itens') or [], 1):
        prev = existing_by_idx.get(idx, {})
        qty = Decimal(str(item.get('quantidade_sacas') or '0'))
        gross = Decimal(str(item.get('valor_bruto') or '0'))
        unit_price = (gross / qty) if qty > 0 else Decimal('0')
        form_like[f'item_{idx}_origin'] = str(rules.get('origin') or prev.get('origin') or '0')
        form_like[f'item_{idx}_icms_cst'] = str(rules.get('icms_cst') or prev.get('icms_cst') or '')
        form_like[f'item_{idx}_ibs_cbs_cst'] = str(rules.get('ibs_cbs_cst') or prev.get('ibs_cbs_cst') or '')
        form_like[f'item_{idx}_ibs_cbs_class'] = str(rules.get('ibs_cbs_class') or prev.get('ibs_cbs_class') or '')
        form_like[f'item_{idx}_commercial_unit'] = unit
        form_like[f'item_{idx}_tax_unit'] = unit
        if qty > 0:
            qty_str = format(qty.quantize(Decimal('0.0001')), 'f')
            price_str = format(unit_price.quantize(Decimal('0.0000000001')), 'f')
            form_like[f'item_{idx}_commercial_quantity'] = qty_str
            form_like[f'item_{idx}_tax_quantity'] = qty_str
            form_like[f'item_{idx}_commercial_unit_price'] = price_str
            form_like[f'item_{idx}_tax_unit_price'] = price_str
        for carry in ('icms_base', 'icms_value', 'icms_rate', 'ibs_cbs_base', 'ibs_uf_value',
                      'ibs_uf_rate', 'ibs_municipal_value', 'ibs_municipal_rate', 'cbs_value', 'cbs_rate'):
            if prev.get(carry) is not None and str(prev.get(carry)) != '':
                form_like[f'item_{idx}_{carry}'] = str(prev[carry])

    cleaned_tech = clean_technical_data(form_like, snapshot)
    if not tech_row:
        tech_row = NfeDraftTechnicalData(
            draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id, updated_by=user_id
        )
        db.add(tech_row)
    tech_row.data_json = json.dumps(cleaned_tech, ensure_ascii=False)
    tech_row.schema_version = 1
    tech_row.updated_by = user_id

    record_draft_event(
        db,
        draft_id=draft.id,
        organization_id=draft.organization_id,
        farm_id=draft.farm_id,
        event_type='detalhes_salvos',
        summary=f'Perfil fiscal "{profile.name}" (v{profile.version}) aplicado ao rascunho.',
        user_id=user_id,
        metadata={'profile_id': profile.id, 'profile_version': profile.version},
    )
    db.commit()
