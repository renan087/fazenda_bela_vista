"""Protocolo SOAP 1.2 offline, parser seguro de retornos SEFAZ e trava de certificado.

Opera exclusivamente em memória/banco local nesta etapa: não realiza chamadas HTTP,
não acessa certificados reais e não transmite documentos à SEFAZ.
"""
from abc import ABC, abstractmethod
from hashlib import sha256
import re
from uuid import uuid4
from lxml import etree

from app.models.nfe_draft import NfeTransmissionAttempt
from app.services.nfe_access_key import UF_IBGE_CODES, validate_access_key
from app.services.nfe_schema import MAX_XML_BYTES, UnsafeXML

SOAP12_NS = 'http://www.w3.org/2003/05/soap-envelope'
NFE_NS = 'http://www.portalfiscal.inf.br/nfe'
NFE_WSDL_BASE = 'http://www.portalfiscal.inf.br/nfe/wsdl'

ALLOWED_OPERATIONS = frozenset({'status_servico', 'autorizacao', 'consulta_situacao', 'evento'})


class CertificateProvider(ABC):
    """Contrato abstrato para assinatura/mTLS futuro sem acoplar armazenamento de segredos."""

    @abstractmethod
    def sign_xml(self, xml_bytes: bytes, reference_id: str) -> bytes:
        raise NotImplementedError


class DisabledCertificateProvider(CertificateProvider):
    """Implementação segura padrão enquanto certificados digitais não estão habilitados."""

    def sign_xml(self, xml_bytes: bytes, reference_id: str) -> bytes:
        raise PermissionError('Assinatura digital com certificado A1 ainda não está habilitada nesta etapa.')


def _safe_parser():
    return etree.XMLParser(resolve_entities=False, load_dtd=False, no_network=True, huge_tree=False)


def _wrap_soap12(wsdl_service: str, payload_element) -> bytes:
    env = etree.Element(f'{{{SOAP12_NS}}}Envelope', nsmap={'soap12': SOAP12_NS})
    body = etree.SubElement(env, f'{{{SOAP12_NS}}}Body')
    dados = etree.SubElement(body, f'{{{NFE_WSDL_BASE}/{wsdl_service}}}nfeDadosMsg')
    dados.append(payload_element)
    return etree.tostring(env, encoding='utf-8', xml_declaration=True)


def build_soap_status_service(*, state_uf: str = 'BA', tp_amb: str = '2') -> bytes:
    """Monta envelope SOAP 1.2 offline para NfeStatusServico4 (consStatServ)."""
    uf = str(state_uf or '').strip().upper()
    if uf not in UF_IBGE_CODES:
        raise ValueError('UF inválida para consulta de status.')
    if str(tp_amb) != '2':
        raise PermissionError('Apenas tpAmb=2 (homologação) é permitido na montagem de testes.')
    cons = etree.Element(f'{{{NFE_NS}}}consStatServ', {'versao': '4.00'}, nsmap={None: NFE_NS})
    etree.SubElement(cons, f'{{{NFE_NS}}}tpAmb').text = '2'
    etree.SubElement(cons, f'{{{NFE_NS}}}cUF').text = UF_IBGE_CODES[uf]
    etree.SubElement(cons, f'{{{NFE_NS}}}xServ').text = 'STATUS'
    return _wrap_soap12('NFeStatusServico4', cons)


def build_soap_consult_protocol(*, access_key: str, tp_amb: str = '2') -> bytes:
    """Monta envelope SOAP 1.2 offline para NfeConsultaProtocolo4 (consSitNFe)."""
    if not validate_access_key(access_key):
        raise ValueError('Chave de acesso inválida para consulta de situação.')
    if str(tp_amb) != '2':
        raise PermissionError('Apenas tpAmb=2 (homologação) é permitido na montagem de testes.')
    cons = etree.Element(f'{{{NFE_NS}}}consSitNFe', {'versao': '4.00'}, nsmap={None: NFE_NS})
    etree.SubElement(cons, f'{{{NFE_NS}}}tpAmb').text = '2'
    etree.SubElement(cons, f'{{{NFE_NS}}}xServ').text = 'CONSULTAR'
    etree.SubElement(cons, f'{{{NFE_NS}}}chNFe').text = access_key
    return _wrap_soap12('NFeConsultaProtocolo4', cons)


def build_soap_authorize_batch(*, nfe_xml_bytes: bytes, batch_id: str, sync: bool = True) -> bytes:
    """Monta envelope SOAP 1.2 offline para NfeAutorizacao4 (enviNFe) sem transmitir."""
    if not isinstance(nfe_xml_bytes, bytes) or len(nfe_xml_bytes) > MAX_XML_BYTES:
        raise UnsafeXML('XML inválido ou acima do limite permitido.')
    clean_batch = str(batch_id or '').strip()
    if not re.fullmatch(r'[0-9]{1,15}', clean_batch):
        raise ValueError('Identificador de lote (idLote) deve conter de 1 a 15 dígitos.')
    try:
        nfe_root = etree.fromstring(nfe_xml_bytes, _safe_parser())
    except etree.XMLSyntaxError:
        raise UnsafeXML('XML malformado ou inseguro.') from None
    if nfe_root.getroottree().docinfo.doctype:
        raise UnsafeXML('DTD não é permitida no XML da NF-e.')
    if etree.QName(nfe_root).localname != 'NFe':
        raise ValueError('O elemento raiz deve ser <NFe>.')

    envi = etree.Element(f'{{{NFE_NS}}}enviNFe', {'versao': '4.00'}, nsmap={None: NFE_NS})
    etree.SubElement(envi, f'{{{NFE_NS}}}idLote').text = clean_batch
    etree.SubElement(envi, f'{{{NFE_NS}}}indSinc').text = '1' if sync else '0'
    envi.append(nfe_root)
    return _wrap_soap12('NFeAutorizacao4', envi)


def classify_sefaz_status(c_stat: str) -> str:
    """Classifica o código cStat da SEFAZ em categoria operacional clara."""
    code = str(c_stat or '').strip()
    if code in ('100', '150'):
        return 'autorizada'
    if code == '107':
        return 'servico_em_operacao'
    if code in ('103', '105'):
        return 'em_processamento'
    if code in ('108', '109'):
        return 'servico_indisponivel'
    if code in ('110', '301', '302', '303'):
        return 'denegada'
    if code.isdigit() and 200 <= int(code) <= 999:
        return 'rejeitada'
    return 'desconhecido'


def _first_local_text(node, local_name: str) -> str:
    matches = node.xpath(f'.//*[local-name()="{local_name}"]/text()')
    return str(matches[0]).strip() if matches else ''


def parse_sefaz_response(xml_bytes: bytes) -> dict:
    """Interpreta retorno XML/SOAP da SEFAZ com parser endurecido contra XXE/DTD."""
    if not isinstance(xml_bytes, bytes) or len(xml_bytes) > MAX_XML_BYTES:
        raise UnsafeXML('Resposta XML inválida ou maior que o limite permitido.')
    try:
        root = etree.fromstring(xml_bytes, _safe_parser())
    except etree.XMLSyntaxError:
        raise UnsafeXML('Resposta XML malformada ou insegura.') from None
    if root.getroottree().docinfo.doctype:
        raise UnsafeXML('DTD não é permitida na resposta SEFAZ.')

    # Quando vier protNFe/infProt dentro de retEnviNFe ou retConsSitNFe, prioriza o cStat do protocolo.
    inf_prot_list = root.xpath('.//*[local-name()="infProt"]')
    target = inf_prot_list[0] if inf_prot_list else root

    c_stat = _first_local_text(target, 'cStat') or _first_local_text(root, 'cStat')
    x_motivo = _first_local_text(target, 'xMotivo') or _first_local_text(root, 'xMotivo')
    protocol = _first_local_text(target, 'nProt')
    access_key = _first_local_text(target, 'chNFe') or _first_local_text(root, 'chNFe')
    received_at = _first_local_text(target, 'dhRecbto') or _first_local_text(root, 'dhRecbto')

    return {
        'root_tag': etree.QName(root).localname,
        'c_stat': c_stat,
        'x_motivo': x_motivo,
        'category': classify_sefaz_status(c_stat),
        'protocol_number': protocol,
        'access_key': access_key,
        'received_at': received_at,
    }


def record_simulated_attempt(
    db,
    *,
    draft_id: str,
    organization_id: int,
    farm_id: int,
    idempotency_key: str,
    uf: str,
    operation: str,
    request_bytes: bytes,
    response_bytes: bytes,
    user_id: int,
) -> NfeTransmissionAttempt:
    """Registra tentativa simulada com idempotência e hashes SHA-256 (ambiente teste_ficticio)."""
    clean_key = str(idempotency_key or '').strip()
    if not clean_key or len(clean_key) > 64:
        raise ValueError('Chave de idempotência inválida.')
    if operation not in ALLOWED_OPERATIONS:
        raise ValueError('Operação SEFAZ não suportada.')

    existing = (
        db.query(NfeTransmissionAttempt)
        .filter_by(organization_id=organization_id, farm_id=farm_id, idempotency_key=clean_key)
        .first()
    )
    if existing:
        return existing

    parsed = parse_sefaz_response(response_bytes)
    row = NfeTransmissionAttempt(
        id=uuid4().hex,
        draft_id=draft_id,
        organization_id=organization_id,
        farm_id=farm_id,
        idempotency_key=clean_key,
        environment='teste_ficticio',
        uf=str(uf or 'BA').strip().upper(),
        operation=operation,
        status_category=parsed['category'],
        c_stat=parsed['c_stat'][:10],
        x_motivo=parsed['x_motivo'][:300],
        protocol_number=parsed['protocol_number'][:40],
        access_key=parsed['access_key'][:44],
        request_sha256=sha256(request_bytes).hexdigest(),
        response_sha256=sha256(response_bytes).hexdigest(),
        created_by=user_id,
    )
    db.add(row)
    db.commit()
    return row
