"""Montagem offline de eventos fiscais NF-e (Cancelamento e CC-e) e parser de retorno.

Opera exclusivamente em modo de teste/homologação offline (tpAmb=2): não assina
eventos e não realiza comunicação com a SEFAZ.
"""
import re
from lxml import etree

from app.services.coffee_buyers import valid_cnpj, valid_cpf
from app.services.nfe_access_key import UF_IBGE_CODES, validate_access_key
from app.services.nfe_schema import MAX_XML_BYTES, UnsafeXML
from app.services.nfe_sefaz_protocol import NFE_NS, _first_local_text, _safe_parser, _wrap_soap12

EVENT_CANCELAMENTO = '110111'
EVENT_CARTA_CORRECAO = '110110'
CCE_COND_USO = (
    'A Carta de Correcao e disciplinada pelo paragrafo 1o-A do art. 7o do Convenio S/N, '
    'de 15 de dezembro de 1970 e pode ser utilizada para regularizacao de erro ocorrido na '
    'emissao de documento fiscal, desde que o erro nao esteja relacionado com: I - as variaveis '
    'que determinam o valor do imposto tais como: base de calculo, aliquota, diferenca de preco, '
    'quantidade, valor da operacao ou da prestacao; II - a correcao de dados cadastrais que '
    'implique mudanca do remetente ou do destinatario; III - a data de emissao ou de saida.'
)


def _check_xml_text(value: str, label: str) -> str:
    text = ' '.join(str(value or '').split())
    if any(
        not (ord(c) in (9, 10, 13) or 0x20 <= ord(c) <= 0xD7FF or 0xE000 <= ord(c) <= 0xFFFD or 0x10000 <= ord(c) <= 0x10FFFF)
        for c in text
    ):
        raise ValueError(f'{label} contém caracteres incompatíveis com XML.')
    return text


def _validate_author(document: str, document_type: str) -> tuple[str, str]:
    doc_type = str(document_type or 'cpf').strip().lower()
    digits = re.sub(r'\D', '', str(document or ''))
    if doc_type == 'cpf':
        if len(digits) != 11 or not valid_cpf(digits):
            raise ValueError('CPF do autor do evento inválido.')
        return 'CPF', digits
    if doc_type == 'cnpj':
        if len(digits) != 14 or not valid_cnpj(digits):
            raise ValueError('CNPJ do autor do evento inválido.')
        return 'CNPJ', digits
    raise ValueError('Tipo de documento do autor inválido.')


def _build_event_envelope(
    *,
    state_uf: str,
    author_document: str,
    author_type: str,
    access_key: str,
    event_type: str,
    seq: int,
    event_datetime: str,
    batch_id: str,
    detail_builder,
    tp_amb: str = '2',
) -> dict:
    if str(tp_amb) != '2':
        raise PermissionError('Apenas tpAmb=2 (homologação) é permitido na montagem de testes.')
    uf = str(state_uf or '').strip().upper()
    if uf not in UF_IBGE_CODES:
        raise ValueError('UF inválida para o evento fiscal.')
    if not validate_access_key(access_key):
        raise ValueError('Chave de acesso inválida para o evento fiscal.')
    if not re.fullmatch(r'[0-9]{1,15}', str(batch_id or '').strip()):
        raise ValueError('Identificador de lote (idLote) inválido.')
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}', str(event_datetime or '').strip()):
        raise ValueError('Data/hora do evento deve estar no formato ISO-8601 com fuso horário.')

    doc_tag, doc_digits = _validate_author(author_document, author_type)
    event_id = f'ID{event_type}{access_key}{seq:02d}'

    env = etree.Element(f'{{{NFE_NS}}}envEvento', {'versao': '1.00'}, nsmap={None: NFE_NS})
    etree.SubElement(env, f'{{{NFE_NS}}}idLote').text = str(batch_id).strip()
    evento = etree.SubElement(env, f'{{{NFE_NS}}}evento', {'versao': '1.00'})
    inf = etree.SubElement(evento, f'{{{NFE_NS}}}infEvento', {'Id': event_id})
    etree.SubElement(inf, f'{{{NFE_NS}}}cOrgao').text = UF_IBGE_CODES[uf]
    etree.SubElement(inf, f'{{{NFE_NS}}}tpAmb').text = '2'
    etree.SubElement(inf, f'{{{NFE_NS}}}{doc_tag}').text = doc_digits
    etree.SubElement(inf, f'{{{NFE_NS}}}chNFe').text = access_key
    etree.SubElement(inf, f'{{{NFE_NS}}}dhEvento').text = str(event_datetime).strip()
    etree.SubElement(inf, f'{{{NFE_NS}}}tpEvento').text = event_type
    etree.SubElement(inf, f'{{{NFE_NS}}}nSeqEvento').text = str(seq)
    etree.SubElement(inf, f'{{{NFE_NS}}}verEvento').text = '1.00'
    det = etree.SubElement(inf, f'{{{NFE_NS}}}detEvento', {'versao': '1.00'})
    detail_builder(det)

    xml_bytes = etree.tostring(env, encoding='utf-8', xml_declaration=True)
    soap_bytes = _wrap_soap12('NFeRecepcaoEvento4', env)
    return {
        'event_id': event_id,
        'event_type': event_type,
        'seq': seq,
        'xml_bytes': xml_bytes,
        'soap_bytes': soap_bytes,
        'signed': False,
    }


def build_cancellation_event(
    *,
    state_uf: str,
    author_document: str,
    author_type: str = 'cpf',
    access_key: str,
    protocol_number: str,
    justification: str,
    event_datetime: str,
    batch_id: str = '1',
    tp_amb: str = '2',
) -> dict:
    """Monta evento de Cancelamento (110111) offline, sem assinatura."""
    prot = re.sub(r'\D', '', str(protocol_number or ''))
    if len(prot) != 15:
        raise ValueError('O protocolo de autorização (nProt) deve conter 15 dígitos.')
    just = _check_xml_text(justification, 'Justificativa de cancelamento')
    if not (15 <= len(just) <= 255):
        raise ValueError('A justificativa de cancelamento deve ter entre 15 e 255 caracteres.')

    def _fill(det):
        etree.SubElement(det, f'{{{NFE_NS}}}descEvento').text = 'Cancelamento'
        etree.SubElement(det, f'{{{NFE_NS}}}nProt').text = prot
        etree.SubElement(det, f'{{{NFE_NS}}}xJust').text = just

    return _build_event_envelope(
        state_uf=state_uf,
        author_document=author_document,
        author_type=author_type,
        access_key=access_key,
        event_type=EVENT_CANCELAMENTO,
        seq=1,
        event_datetime=event_datetime,
        batch_id=batch_id,
        detail_builder=_fill,
        tp_amb=tp_amb,
    )


def build_correction_letter_event(
    *,
    state_uf: str,
    author_document: str,
    author_type: str = 'cpf',
    access_key: str,
    correction_text: str,
    seq: int = 1,
    event_datetime: str,
    batch_id: str = '1',
    tp_amb: str = '2',
) -> dict:
    """Monta evento de Carta de Correção Eletrônica (110110) offline, sem assinatura."""
    if isinstance(seq, bool) or not isinstance(seq, int) or not (1 <= seq <= 20):
        raise ValueError('A sequência da Carta de Correção deve estar entre 1 e 20.')
    text = _check_xml_text(correction_text, 'Texto da Carta de Correção')
    if not (15 <= len(text) <= 1000):
        raise ValueError('O texto da Carta de Correção deve ter entre 15 e 1000 caracteres.')

    def _fill(det):
        etree.SubElement(det, f'{{{NFE_NS}}}descEvento').text = 'Carta de Correcao'
        etree.SubElement(det, f'{{{NFE_NS}}}xCorrecao').text = text
        etree.SubElement(det, f'{{{NFE_NS}}}xCondUso').text = CCE_COND_USO

    return _build_event_envelope(
        state_uf=state_uf,
        author_document=author_document,
        author_type=author_type,
        access_key=access_key,
        event_type=EVENT_CARTA_CORRECAO,
        seq=seq,
        event_datetime=event_datetime,
        batch_id=batch_id,
        detail_builder=_fill,
        tp_amb=tp_amb,
    )


def parse_event_response(xml_bytes: bytes) -> dict:
    """Interpreta retorno XML/SOAP de evento (retEnvEvento) com parser seguro."""
    if not isinstance(xml_bytes, bytes) or len(xml_bytes) > MAX_XML_BYTES:
        raise UnsafeXML('Resposta XML inválida ou maior que o limite permitido.')
    try:
        root = etree.fromstring(xml_bytes, _safe_parser())
    except etree.XMLSyntaxError:
        raise UnsafeXML('Resposta XML malformada ou insegura.') from None
    if root.getroottree().docinfo.doctype:
        raise UnsafeXML('DTD não é permitida na resposta de evento.')

    inf_list = root.xpath('.//*[local-name()="infEvento"]')
    target = inf_list[0] if inf_list else root
    c_stat = _first_local_text(target, 'cStat') or _first_local_text(root, 'cStat')
    x_motivo = _first_local_text(target, 'xMotivo') or _first_local_text(root, 'xMotivo')

    if c_stat in ('135', '136'):
        category = 'evento_registrado'
    elif c_stat.isdigit() and 200 <= int(c_stat) <= 999:
        category = 'rejeitado'
    else:
        category = 'desconhecido'

    return {
        'c_stat': c_stat,
        'x_motivo': x_motivo,
        'category': category,
        'event_type': _first_local_text(target, 'tpEvento'),
        'seq': _first_local_text(target, 'nSeqEvento'),
        'protocol_number': _first_local_text(target, 'nProt'),
        'access_key': _first_local_text(target, 'chNFe'),
    }
