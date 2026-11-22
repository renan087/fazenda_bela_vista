"""Motor modular de montagem estrutural do XML NF-e 4.00 (somente simulação offline).

Este módulo prepara a estrutura de tags na ordem do leiaute NF-e 4.00 para testes
locais. Ele NUNCA assina o documento, NUNCA transmite à SEFAZ e exige que todos os
códigos fiscais (CST, CRT, CFOP, NCM) sejam informados explicitamente pelo chamador.
"""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from xml.etree import ElementTree as ET

from app.services.nfe_access_key import UF_IBGE_CODES, build_access_key
from app.services.nfe_preview import _element

NS = 'http://www.portalfiscal.inf.br/nfe'
SUPPORTED_ICMS_CSTS = frozenset({'00', '40', '41', '50', '60', '90'})
SUPPORTED_CRTS = frozenset({'1', '2', '3', '4'})


def _sub(parent, name, value=None):
    if value is None:
        return ET.SubElement(parent, f'{{{NS}}}{name}')
    text = str(value).strip()
    if text == '':
        return None
    return _element(parent, f'{{{NS}}}{name}', text)


def _fmt_decimal(raw, places: int) -> str:
    try:
        val = Decimal(str(raw))
        if not val.is_finite() or val < 0:
            raise ValueError
    except (InvalidOperation, ValueError):
        raise ValueError(f'Valor decimal inválido: {raw!r}') from None
    quantum = Decimal('1').scaleb(-places)
    return format(val.quantize(quantum, rounding=ROUND_HALF_UP), 'f')


def _collect_missing(*, snapshot, issuer, details, technical, identification, issuer_crt):
    missing = []
    if not issuer:
        missing.append('Cadastro do produtor (emitente)')
    else:
        for attr, label in (
            ('cpf', 'CPF do produtor'),
            ('name', 'Nome do produtor'),
            ('street', 'Logradouro do produtor'),
            ('number', 'Número do produtor'),
            ('district', 'Bairro do produtor'),
            ('municipality_code', 'Código IBGE do município do produtor'),
            ('city', 'Município do produtor'),
            ('state', 'UF do produtor'),
            ('postal_code', 'CEP do produtor'),
            ('state_registration', 'Inscrição estadual do produtor'),
        ):
            if not str(getattr(issuer, attr, '') or '').strip():
                missing.append(label)

    if str(issuer_crt or '').strip() not in SUPPORTED_CRTS:
        missing.append('CRT explícito do emitente (1, 2, 3 ou 4)')

    for field, label in (
        ('recipient_cpf', 'CPF do destinatário'),
        ('recipient_name', 'Nome do destinatário'),
        ('recipient_street', 'Logradouro do destinatário'),
        ('recipient_number', 'Número do destinatário'),
        ('recipient_district', 'Bairro do destinatário'),
        ('recipient_municipality_code', 'Código IBGE do município do destinatário'),
        ('recipient_city', 'Município do destinatário'),
        ('recipient_state', 'UF do destinatário'),
        ('recipient_postal_code', 'CEP do destinatário'),
        ('recipient_ie_indicator', 'Indicador de IE do destinatário'),
        ('product_description', 'Descrição fiscal do produto'),
        ('product_ncm', 'NCM do produto'),
        ('operation_nature', 'Natureza da operação'),
        ('operation_cfop', 'CFOP da operação'),
    ):
        if not str((details or {}).get(field) or '').strip():
            missing.append(label)

    if (details or {}).get('recipient_ie_indicator') == 'contribuinte' and not str((details or {}).get('recipient_ie') or '').strip():
        missing.append('Inscrição estadual do destinatário contribuinte')

    if not str((technical or {}).get('emission_date') or '').strip():
        missing.append('Data de emissão pretendida')
    if not str((technical or {}).get('freight_mode') or '').strip():
        missing.append('Modalidade de frete')

    if not identification:
        missing.append('Identificação e numeração fictícia reservada')
    else:
        if not str(getattr(identification, 'consumer_indicator', '') or '').strip():
            missing.append('Indicador de consumidor final (indFinal)')
        if not str(getattr(identification, 'presence_indicator', '') or '').strip():
            missing.append('Indicador de presença (indPres)')
        if not str(getattr(identification, 'payment_method', '') or '').strip():
            missing.append('Forma de pagamento (tPag)')

    items = (snapshot or {}).get('itens') or []
    tech_by_index = {row.get('item_index'): row for row in (technical or {}).get('items', []) if isinstance(row, dict)}
    if not items:
        missing.append('Lotes comerciais da venda')
    for idx in range(1, len(items) + 1):
        row = tech_by_index.get(idx, {})
        for field, label in (
            ('origin', 'origem'),
            ('icms_cst', 'CST ICMS'),
            ('commercial_unit', 'unidade comercial'),
            ('commercial_quantity', 'quantidade comercial'),
            ('commercial_unit_price', 'valor unitário comercial'),
            ('tax_unit', 'unidade tributável'),
            ('tax_quantity', 'quantidade tributável'),
            ('tax_unit_price', 'valor unitário tributável'),
        ):
            if not str(row.get(field) or '').strip():
                missing.append(f'Lote {idx}: {label}')
        cst = str(row.get('icms_cst') or '').strip()
        if cst == '00':
            for field, label in (('icms_base', 'base ICMS'), ('icms_rate', 'alíquota ICMS'), ('icms_value', 'valor ICMS')):
                if str(row.get(field) or '').strip() == '':
                    missing.append(f'Lote {idx}: {label} (exigido para CST 00)')
    return missing


def _build_icms_group(imposto_node, tech_row: dict) -> tuple[Decimal, Decimal]:
    orig = str(tech_row['origin']).strip()
    cst = str(tech_row['icms_cst']).strip()
    if cst not in SUPPORTED_ICMS_CSTS:
        raise ValueError(f'CST ICMS {cst!r} ainda não suportado na simulação estrutural.')
    icms = _sub(imposto_node, 'ICMS')
    if cst == '00':
        grp = _sub(icms, 'ICMS00')
        _sub(grp, 'orig', orig)
        _sub(grp, 'CST', '00')
        _sub(grp, 'modBC', '3')
        v_bc = Decimal(_fmt_decimal(tech_row['icms_base'], 2))
        p_icms = _fmt_decimal(tech_row['icms_rate'], 4)
        v_icms = Decimal(_fmt_decimal(tech_row['icms_value'], 2))
        _sub(grp, 'vBC', format(v_bc, '.2f'))
        _sub(grp, 'pICMS', p_icms)
        _sub(grp, 'vICMS', format(v_icms, '.2f'))
        return v_bc, v_icms
    if cst in ('40', '41', '50'):
        grp = _sub(icms, 'ICMS40')
        _sub(grp, 'orig', orig)
        _sub(grp, 'CST', cst)
        return Decimal('0.00'), Decimal('0.00')
    if cst == '60':
        grp = _sub(icms, 'ICMS60')
        _sub(grp, 'orig', orig)
        _sub(grp, 'CST', '60')
        return Decimal('0.00'), Decimal('0.00')
    # CST 90 mínimo estrutural de teste
    grp = _sub(icms, 'ICMS90')
    _sub(grp, 'orig', orig)
    _sub(grp, 'CST', '90')
    return Decimal('0.00'), Decimal('0.00')


def build_simulation_xml(
    *,
    snapshot: dict,
    issuer,
    details: dict,
    technical: dict,
    identification,
    issuer_crt: str,
    extra_data: dict | None = None,
    emission_time: str = '12:00:00',
    timezone_offset: str = '-03:00',
    simulation_only: bool = True,
) -> dict:
    """Monta XML estrutural NF-e 4.00 sem assinatura, exclusivamente para simulação offline."""
    if simulation_only is not True:
        raise PermissionError('A geração de XML fora do modo simulação não está habilitada nesta etapa.')
    if identification is not None and getattr(identification, 'environment', '') != 'teste_ficticio':
        raise PermissionError('Apenas identificações do ambiente teste_ficticio são aceitas nesta etapa.')

    missing = _collect_missing(
        snapshot=snapshot,
        issuer=issuer,
        details=details,
        technical=technical,
        identification=identification,
        issuer_crt=issuer_crt,
    )
    if missing:
        return {
            'ready': False,
            'xml_bytes': None,
            'access_key': '',
            'missing_fields': missing,
            'warnings': ['Complete os campos ausentes antes de montar o XML estrutural de simulação.'],
        }

    key_info = build_access_key(
        state_uf=issuer.state,
        emission_date=technical['emission_date'],
        issuer_document=issuer.cpf,
        document_type='cpf',
        model=identification.model,
        series=identification.series,
        number=identification.number,
        emission_type=identification.emission_type,
        numeric_code=identification.numeric_code,
    )

    ET.register_namespace('', NS)
    root = ET.Element(f'{{{NS}}}NFe')
    root.append(ET.Comment(' SIMULACAO ESTRUTURAL DE TESTE - SEM VALIDADE FISCAL - NAO TRANSMITIR '))
    inf = ET.SubElement(root, f'{{{NS}}}infNFe', {'versao': '4.00', 'Id': key_info['id_attribute']})

    # 1. ide
    ide = _sub(inf, 'ide')
    id_dest = '1' if str(issuer.state).upper() == str(details['recipient_state']).upper() else '2'
    _sub(ide, 'cUF', key_info['uf_code'])
    _sub(ide, 'cNF', key_info['numeric_code'])
    _sub(ide, 'natOp', details['operation_nature'][:60])
    _sub(ide, 'mod', identification.model)
    _sub(ide, 'serie', str(identification.series))
    _sub(ide, 'nNF', str(identification.number))
    _sub(ide, 'dhEmi', f"{technical['emission_date']}T{emission_time}{timezone_offset}")
    _sub(ide, 'tpNF', '1')
    _sub(ide, 'idDest', id_dest)
    _sub(ide, 'cMunFG', issuer.municipality_code)
    _sub(ide, 'tpImp', '1')
    _sub(ide, 'tpEmis', identification.emission_type)
    _sub(ide, 'cDV', key_info['check_digit'])
    _sub(ide, 'tpAmb', '2')
    _sub(ide, 'finNFe', identification.finality)
    _sub(ide, 'indFinal', identification.consumer_indicator)
    _sub(ide, 'indPres', identification.presence_indicator)
    _sub(ide, 'procEmi', '0')
    _sub(ide, 'verProc', 'SiSFarm-Teste-0.1')

    # 2. emit
    emit = _sub(inf, 'emit')
    _sub(emit, 'CPF', issuer.cpf)
    _sub(emit, 'xNome', issuer.name[:60])
    ender_emit = _sub(emit, 'enderEmit')
    _sub(ender_emit, 'xLgr', issuer.street[:60])
    _sub(ender_emit, 'nro', issuer.number[:60])
    _sub(ender_emit, 'xCpl', getattr(issuer, 'complement', '')[:60])
    _sub(ender_emit, 'xBairro', issuer.district[:60])
    _sub(ender_emit, 'cMun', issuer.municipality_code)
    _sub(ender_emit, 'xMun', issuer.city[:60])
    _sub(ender_emit, 'UF', issuer.state)
    _sub(ender_emit, 'CEP', issuer.postal_code)
    _sub(emit, 'IE', issuer.state_registration)
    _sub(emit, 'CRT', str(issuer_crt).strip())

    # 3. dest
    dest = _sub(inf, 'dest')
    _sub(dest, 'CPF', details['recipient_cpf'])
    _sub(dest, 'xNome', details['recipient_name'][:60])
    ender_dest = _sub(dest, 'enderDest')
    _sub(ender_dest, 'xLgr', details['recipient_street'][:60])
    _sub(ender_dest, 'nro', details['recipient_number'][:60])
    _sub(ender_dest, 'xCpl', str(details.get('recipient_complement') or '')[:60])
    _sub(ender_dest, 'xBairro', details['recipient_district'][:60])
    _sub(ender_dest, 'cMun', details['recipient_municipality_code'])
    _sub(ender_dest, 'xMun', details['recipient_city'][:60])
    _sub(ender_dest, 'UF', details['recipient_state'])
    _sub(ender_dest, 'CEP', details['recipient_postal_code'])
    ind_ie = {'contribuinte': '1', 'isento': '2', 'nao_contribuinte': '9'}[details['recipient_ie_indicator']]
    _sub(dest, 'indIEDest', ind_ie)
    if ind_ie == '1':
        _sub(dest, 'IE', details['recipient_ie'])

    # 4. det (itens)
    items = snapshot['itens']
    tech_by_index = {row['item_index']: row for row in technical.get('items', []) if isinstance(row, dict)}
    total_vprod = Decimal('0.00')
    total_vbc = Decimal('0.00')
    total_vicms = Decimal('0.00')

    for idx, item in enumerate(items, 1):
        tech = tech_by_index[idx]
        det = ET.SubElement(inf, f'{{{NS}}}det', {'nItem': str(idx)})
        prod = _sub(det, 'prod')
        v_prod = Decimal(_fmt_decimal(item['valor_bruto'], 2))
        total_vprod += v_prod
        _sub(prod, 'cProd', f'LOTE-{idx}')
        _sub(prod, 'cEAN', 'SEM GTIN')
        _sub(prod, 'xProd', details['product_description'][:120])
        _sub(prod, 'NCM', details['product_ncm'])
        _sub(prod, 'CFOP', details['operation_cfop'])
        _sub(prod, 'uCom', tech['commercial_unit'])
        _sub(prod, 'qCom', _fmt_decimal(tech['commercial_quantity'], 4))
        _sub(prod, 'vUnCom', _fmt_decimal(tech['commercial_unit_price'], 10))
        _sub(prod, 'vProd', format(v_prod, '.2f'))
        _sub(prod, 'cEANTrib', 'SEM GTIN')
        _sub(prod, 'uTrib', tech['tax_unit'])
        _sub(prod, 'qTrib', _fmt_decimal(tech['tax_quantity'], 4))
        _sub(prod, 'vUnTrib', _fmt_decimal(tech['tax_unit_price'], 10))
        _sub(prod, 'indTot', '1')

        imposto = _sub(det, 'imposto')
        item_vbc, item_vicms = _build_icms_group(imposto, tech)
        total_vbc += item_vbc
        total_vicms += item_vicms

    # 5. total
    total_node = _sub(inf, 'total')
    icms_tot = _sub(total_node, 'ICMSTot')
    _sub(icms_tot, 'vBC', format(total_vbc, '.2f'))
    _sub(icms_tot, 'vICMS', format(total_vicms, '.2f'))
    for zero_tag in ('vICMSDeson', 'vFCP', 'vBCST', 'vST', 'vFCPST', 'vFCPSTRet'):
        _sub(icms_tot, zero_tag, '0.00')
    _sub(icms_tot, 'vProd', format(total_vprod, '.2f'))
    for zero_tag in ('vFrete', 'vSeg', 'vDesc', 'vII', 'vIPI', 'vIPIDevol', 'vPIS', 'vCOFINS', 'vOutro'):
        _sub(icms_tot, zero_tag, '0.00')
    _sub(icms_tot, 'vNF', format(total_vprod, '.2f'))

    # 6. transp
    extra = extra_data or {}
    transp = _sub(inf, 'transp')
    _sub(transp, 'modFrete', technical['freight_mode'])
    if extra.get('carrier_name'):
        transporta = _sub(transp, 'transporta')
        _sub(transporta, 'xNome', extra['carrier_name'][:60])
    if extra.get('vehicle_plate') and extra.get('vehicle_uf'):
        veic = _sub(transp, 'veicTransp')
        _sub(veic, 'placa', extra['vehicle_plate'])
        _sub(veic, 'UF', extra['vehicle_uf'])
    if any(extra.get(k) for k in ('volume_quantity', 'volume_species', 'net_weight_kg', 'gross_weight_kg')):
        vol = _sub(transp, 'vol')
        _sub(vol, 'qVol', extra.get('volume_quantity'))
        _sub(vol, 'esp', extra.get('volume_species'))
        _sub(vol, 'pesoL', extra.get('net_weight_kg'))
        _sub(vol, 'pesoB', extra.get('gross_weight_kg'))

    # 7. pag
    pag = _sub(inf, 'pag')
    det_pag = _sub(pag, 'detPag')
    _sub(det_pag, 'indPag', extra.get('payment_indicator'))
    _sub(det_pag, 'tPag', identification.payment_method)
    if identification.payment_method == '90':
        v_pag = '0.00'
    elif extra.get('payment_amount'):
        v_pag = _fmt_decimal(extra['payment_amount'], 2)
    else:
        v_pag = format(total_vprod, '.2f')
    _sub(det_pag, 'vPag', v_pag)

    # 8. infAdic
    if extra.get('additional_info'):
        inf_adic = _sub(inf, 'infAdic')
        _sub(inf_adic, 'infCpl', extra['additional_info'])

    return {
        'ready': True,
        'xml_bytes': ET.tostring(root, encoding='utf-8', xml_declaration=True),
        'access_key': key_info['key'],
        'missing_fields': [],
        'warnings': [
            'XML estrutural gerado apenas para simulação offline (tpAmb=2), sem assinatura digital e sem validade fiscal.',
        ],
    }
