"""Repositório imutável local de documentos XML simulados (nfeProc e eventos de teste).

Opera exclusivamente no ambiente 'teste_ficticio': prepara a política de guarda,
integridade SHA-256 e bloqueio de exclusão para a futura fase de homologação/produção.
"""
from hashlib import sha256
import hmac
from uuid import uuid4
from lxml import etree

from app.models.nfe_draft import NfeStoredDocument
from app.services.nfe_access_key import validate_access_key
from app.services.nfe_schema import MAX_XML_BYTES, UnsafeXML
from app.services.nfe_sefaz_protocol import NFE_NS, _first_local_text, _safe_parser

ALLOWED_DOCUMENT_TYPES = frozenset({
    'nfe_simulacao',
    'nfe_proc_simulacao',
    'evento_simulacao',
    'cce_simulacao',
})


def build_simulated_nfe_proc(*, nfe_xml_bytes: bytes, prot_nfe_xml_bytes: bytes) -> bytes:
    """Combina <NFe> e <protNFe> em <nfeProc versao="4.00"> apenas para simulação (tpAmb=2)."""
    for payload in (nfe_xml_bytes, prot_nfe_xml_bytes):
        if not isinstance(payload, bytes) or len(payload) > MAX_XML_BYTES:
            raise UnsafeXML('Documento XML inválido ou acima do limite permitido.')
    try:
        nfe_root = etree.fromstring(nfe_xml_bytes, _safe_parser())
        prot_root = etree.fromstring(prot_nfe_xml_bytes, _safe_parser())
    except etree.XMLSyntaxError:
        raise UnsafeXML('XML malformado ou inseguro.') from None
    if nfe_root.getroottree().docinfo.doctype or prot_root.getroottree().docinfo.doctype:
        raise UnsafeXML('DTD não é permitida na montagem de nfeProc.')
    if etree.QName(nfe_root).localname != 'NFe':
        raise ValueError('O primeiro XML deve ter raiz <NFe>.')
    if etree.QName(prot_root).localname != 'protNFe':
        raise ValueError('O segundo XML deve ter raiz <protNFe>.')

    nfe_tp_amb = _first_local_text(nfe_root, 'tpAmb')
    prot_tp_amb = _first_local_text(prot_root, 'tpAmb')
    if nfe_tp_amb != '2' or (prot_tp_amb and prot_tp_amb != '2'):
        raise PermissionError('A montagem simulada de nfeProc aceita apenas tpAmb=2.')

    inf_nodes = nfe_root.xpath('.//*[local-name()="infNFe"]')
    inf_id = str(inf_nodes[0].get('Id') or '') if inf_nodes else ''
    prot_key = _first_local_text(prot_root, 'chNFe')
    if not validate_access_key(prot_key) or inf_id != f'NFe{prot_key}':
        raise ValueError('A chave de acesso da NF-e não confere com a chave do protocolo.')

    proc = etree.Element(f'{{{NFE_NS}}}nfeProc', {'versao': '4.00'}, nsmap={None: NFE_NS})
    proc.append(etree.Comment(' SIMULACAO DE NFEPROC SEM VALOR FISCAL - NAO TRANSMITIDO '))
    proc.append(nfe_root)
    proc.append(prot_root)
    return etree.tostring(proc, encoding='utf-8', xml_declaration=True)


def store_simulated_document(
    db,
    *,
    draft_id: str,
    organization_id: int,
    farm_id: int,
    document_type: str,
    access_key: str,
    xml_bytes: bytes,
    user_id: int,
    protocol_number: str = '',
) -> NfeStoredDocument:
    """Armazena XML de simulação com SHA-256, deduplicação idempotente e trava de exclusão."""
    if document_type not in ALLOWED_DOCUMENT_TYPES:
        raise ValueError('Tipo de documento fiscal simulado não suportado.')
    if not validate_access_key(access_key):
        raise ValueError('Chave de acesso inválida para armazenamento.')
    if not isinstance(xml_bytes, bytes) or not xml_bytes or len(xml_bytes) > MAX_XML_BYTES:
        raise UnsafeXML('XML inválido ou acima do limite permitido.')

    xml_text = xml_bytes.decode('utf-8')
    digest = sha256(xml_text.encode('utf-8')).hexdigest()

    existing = (
        db.query(NfeStoredDocument)
        .filter_by(
            organization_id=organization_id,
            farm_id=farm_id,
            environment='teste_ficticio',
            document_type=document_type,
            access_key=access_key,
            xml_sha256=digest,
        )
        .first()
    )
    if existing:
        return existing

    row = NfeStoredDocument(
        id=uuid4().hex,
        draft_id=draft_id,
        organization_id=organization_id,
        farm_id=farm_id,
        environment='teste_ficticio',
        document_type=document_type,
        access_key=access_key,
        protocol_number=str(protocol_number or '').strip()[:40],
        xml_content=xml_text,
        xml_sha256=digest,
        is_locked=True,
        created_by=user_id,
    )
    db.add(row)
    db.commit()
    return row


def verify_stored_document(doc: NfeStoredDocument) -> bool:
    """Confere em tempo constante se o XML salvo bate com o SHA-256 original."""
    if not doc or not isinstance(doc.xml_content, str) or not isinstance(doc.xml_sha256, str):
        return False
    computed = sha256(doc.xml_content.encode('utf-8')).hexdigest()
    return hmac.compare_digest(computed, doc.xml_sha256.lower())


def assert_document_can_be_deleted(doc: NfeStoredDocument) -> None:
    """Impede exclusão de documentos fiscais travados pela política de retenção."""
    if doc and doc.is_locked:
        raise PermissionError('Documentos fiscais arquivados são protegidos contra exclusão.')
