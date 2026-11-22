"""Prévia interna dos dados; não produz NF-e, chave, assinatura ou XML SEFAZ."""
from xml.etree import ElementTree as ET

ISSUER_FIELDS = ('name', 'cpf', 'state_registration', 'postal_code', 'street', 'number',
                 'complement', 'district', 'city', 'municipality_code', 'state')
RECIPIENT_FIELDS = ('recipient_name', 'recipient_cpf', 'recipient_ie', 'recipient_ie_indicator',
                    'recipient_postal_code', 'recipient_street', 'recipient_number', 'recipient_complement',
                    'recipient_district', 'recipient_city', 'recipient_municipality_code', 'recipient_state')
PRODUCT_FIELDS = ('product_description', 'product_ncm', 'operation_nature', 'operation_cfop')
TECH_FIELDS = ('origin', 'icms_cst', 'ibs_cbs_cst', 'ibs_cbs_class',
               'commercial_unit', 'commercial_quantity', 'commercial_unit_price',
               'tax_unit', 'tax_quantity', 'tax_unit_price',
               'icms_base', 'icms_value', 'ibs_cbs_base', 'ibs_uf_value',
               'ibs_municipal_value', 'cbs_value')


def _element(parent, name, value):
    text = '' if value is None else str(value)
    if any(not (ord(c) in (9, 10, 13) or 0x20 <= ord(c) <= 0xD7FF or 0xE000 <= ord(c) <= 0xFFFD or 0x10000 <= ord(c) <= 0x10FFFF) for c in text):
        raise ValueError('O rascunho contém caracteres incompatíveis com XML.')
    node = ET.SubElement(parent, name)
    node.text = text
    return node


def build_preview(snapshot, issuer, details, technical):
    """Serializa somente campos conhecidos; dados omitidos continuam vazios."""
    items = snapshot.get('itens')
    if not isinstance(items, list) or not 1 <= len(items) <= 500 or not all(isinstance(i, dict) for i in items):
        raise ValueError('O rascunho não possui lotes válidos para pré-visualização.')
    if not isinstance(details, dict) or not isinstance(technical, dict):
        raise ValueError('Os dados do rascunho estão indisponíveis.')
    tech_items = technical.get('items', [])
    if not isinstance(tech_items, list) or not all(isinstance(i, dict) for i in tech_items):
        raise ValueError('Os dados de teste dos lotes estão inconsistentes.')
    by_index = {}
    for item in tech_items:
        index = item.get('item_index')
        if type(index) is not int or not 1 <= index <= len(items) or index in by_index:
            raise ValueError('Os dados de teste dos lotes estão inconsistentes.')
        by_index[index] = item
    root = ET.Element('preparacao_sisfarm', {'versao': '1', 'natureza': 'simulacao_sem_validade_fiscal'})
    _element(root, 'aviso', 'Não é NF-e. Não contém chave fiscal, assinatura ou autorização. Não transmitir à SEFAZ.')
    commercial = ET.SubElement(root, 'venda')
    for field in ('venda', 'data_venda', 'comprador', 'valor_bruto'):
        _element(commercial, field, snapshot.get(field))
    producer = ET.SubElement(root, 'produtor')
    for field in ISSUER_FIELDS:
        _element(producer, field, getattr(issuer, field, '') if issuer else '')
    recipient = ET.SubElement(root, 'destinatario')
    for field in RECIPIENT_FIELDS:
        _element(recipient, field, details.get(field))
    operation = ET.SubElement(root, 'produto_e_operacao')
    for field in PRODUCT_FIELDS:
        _element(operation, field, details.get(field))
    _element(operation, 'emission_date', technical.get('emission_date'))
    _element(operation, 'freight_mode', technical.get('freight_mode'))
    lines = ET.SubElement(root, 'lotes')
    for index, item in enumerate(items, 1):
        line = ET.SubElement(lines, 'lote', {'posicao': str(index)})
        for field in ('lote', 'quantidade_sacas', 'valor_bruto'):
            _element(line, field, item.get(field))
        for field in TECH_FIELDS:
            _element(line, field, by_index.get(index, {}).get(field))
    return ET.tostring(root, encoding='utf-8', xml_declaration=True)
