"""Montagem parcial para testes locais; não pode gerar documento fiscal transmissível."""
from decimal import Decimal, InvalidOperation
from xml.etree import ElementTree as ET

from app.services.nfe_preview import _element

NS = 'http://www.portalfiscal.inf.br/nfe'
ET.register_namespace('', NS)


def _add(parent, name, value):
    if value is not None and str(value).strip():
        _element(parent, f'{{{NS}}}{name}', str(value).strip())


def _address(parent, name, source, fields):
    values = [(tag, source.get(key)) for tag, key in fields]
    if any(value for _, value in values):
        child = ET.SubElement(parent, f'{{{NS}}}{name}')
        for tag, value in values:
            _add(child, tag, value)


def build_incomplete_candidate(snapshot, issuer, details, technical):
    """Somente campos já salvos; propositalmente sem Id, assinatura ou cálculo fiscal."""
    items = snapshot.get('itens')
    if not isinstance(items, list) or not 1 <= len(items) <= 500 or not all(isinstance(i, dict) for i in items):
        raise ValueError('Lotes indisponíveis.')
    if not isinstance(details, dict) or not isinstance(technical, dict):
        raise ValueError('Dados do rascunho indisponíveis.')
    ET.register_namespace('', NS)
    root = ET.Element(f'{{{NS}}}NFe')
    root.append(ET.Comment(' SIMULACAO INCOMPLETA SEM VALOR FISCAL - NAO TRANSMITIR '))
    info = ET.SubElement(root, f'{{{NS}}}infNFe', {'versao': '4.00'})
    ide = ET.SubElement(info, f'{{{NS}}}ide')
    _add(ide, 'natOp', details.get('operation_nature'))
    _add(ide, 'mod', '55')
    _add(ide, 'tpAmb', '2')
    producer = ET.SubElement(info, f'{{{NS}}}emit')
    src = {key: getattr(issuer, key, '') if issuer else '' for key in ('cpf', 'name', 'street', 'number', 'complement', 'district', 'municipality_code', 'city', 'state', 'postal_code', 'state_registration')}
    _add(producer, 'CPF', src['cpf'])
    _add(producer, 'xNome', src['name'])
    _address(producer, 'enderEmit', src, [('xLgr','street'), ('nro','number'), ('xCpl','complement'), ('xBairro','district'), ('cMun','municipality_code'), ('xMun','city'), ('UF','state'), ('CEP','postal_code')])
    _add(producer, 'IE', src['state_registration'])
    recipient = ET.SubElement(info, f'{{{NS}}}dest')
    _add(recipient, 'CPF', details.get('recipient_cpf'))
    _add(recipient, 'xNome', details.get('recipient_name'))
    _address(recipient, 'enderDest', details, [('xLgr','recipient_street'), ('nro','recipient_number'), ('xCpl','recipient_complement'), ('xBairro','recipient_district'), ('cMun','recipient_municipality_code'), ('xMun','recipient_city'), ('UF','recipient_state'), ('CEP','recipient_postal_code')])
    _add(recipient, 'indIEDest', {'contribuinte':'1', 'isento':'2', 'nao_contribuinte':'9'}.get(details.get('recipient_ie_indicator')))
    _add(recipient, 'IE', details.get('recipient_ie'))
    tech_items = technical.get('items', [])
    if not isinstance(tech_items, list) or not all(isinstance(i, dict) for i in tech_items):
        raise ValueError('Dados técnicos de itens inválidos.')
    by_index = {}
    for tech in tech_items:
        index = tech.get('item_index')
        if type(index) is not int or index < 1 or index > len(items) or index in by_index:
            raise ValueError('Dados técnicos de itens inconsistentes.')
        by_index[index] = tech
    for index, item in enumerate(items, 1):
        line = ET.SubElement(info, f'{{{NS}}}det', {'nItem': str(index)})
        prod = ET.SubElement(line, f'{{{NS}}}prod')
        _add(prod, 'xProd', details.get('product_description'))
        _add(prod, 'NCM', details.get('product_ncm'))
        _add(prod, 'CFOP', details.get('operation_cfop'))
        tech = by_index.get(index, {})
        _add(prod, 'uCom', tech.get('commercial_unit'))
        _add(prod, 'qCom', tech.get('commercial_quantity'))
        _add(prod, 'vUnCom', tech.get('commercial_unit_price'))
        try:
            value = Decimal(str(item.get('valor_bruto', '')))
            if not value.is_finite() or value <= 0:
                raise ValueError
        except (InvalidOperation, ValueError):
            raise ValueError('Valor comercial inválido.') from None
        _add(prod, 'vProd', format(value, 'f'))
        _add(prod, 'uTrib', tech.get('tax_unit'))
        _add(prod, 'qTrib', tech.get('tax_quantity'))
        _add(prod, 'vUnTrib', tech.get('tax_unit_price'))
    freight = technical.get('freight_mode')
    if freight is not None and str(freight) != '':
        if str(freight) not in ('0', '1', '2', '3', '4', '9'):
            raise ValueError('Modalidade de frete inválida.')
        transp = ET.SubElement(info, f'{{{NS}}}transp')
        _add(transp, 'modFrete', freight)
    # Uma estrutura incompleta não pode passar por validação XSD nem ser assinada/transmitida.
    return ET.tostring(root, encoding='utf-8', xml_declaration=True)
