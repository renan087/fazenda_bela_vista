"""Cofre criptografado de Certificado A1 (AES-256-GCM) e Assinador XMLDSig (NF-e 4.00).

A chave mestra (NFE_CERT_MASTER_KEY) nunca é armazenada no banco de dados.
A assinatura XMLDSig segue o padrão enveloped-signature + C14N 1.0 + RSA-SHA1 do MOC.
"""
import base64
from copy import deepcopy
from hashlib import sha1
import json
import os
import secrets
from uuid import uuid4

from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from lxml import etree

from app.models.nfe_draft import NfeCertificateVault
from app.services.certificate_a1 import parse_and_validate_pfx
from app.services.nfe_schema import MAX_XML_BYTES, UnsafeXML
from app.services.nfe_sefaz_protocol import CertificateProvider, _safe_parser

DS_NS = 'http://www.w3.org/2000/09/xmldsig#'
C14N_ALG = 'http://www.w3.org/TR/2001/REC-xml-c14n-20010315'
ENV_SIG_ALG = 'http://www.w3.org/2000/09/xmldsig#enveloped-signature'
RSA_SHA1_ALG = 'http://www.w3.org/2000/09/xmldsig#rsa-sha1'
SHA1_ALG = 'http://www.w3.org/2000/09/xmldsig#sha1'


def _configured_master_key() -> str:
    """Lê a chave do ambiente; se ausente, do .env (via Settings)."""
    raw = os.environ.get('NFE_CERT_MASTER_KEY', '')
    if raw.strip():
        return raw
    try:
        from app.core.config import get_settings
        return get_settings().nfe_cert_master_key or ''
    except Exception:
        return ''


def is_master_key_configured() -> bool:
    return len(_configured_master_key().strip()) >= 32


def _get_master_key(master_key: str | None = None) -> bytes:
    raw = master_key if master_key is not None else _configured_master_key()
    if not isinstance(raw, str) or len(raw.strip()) < 32:
        raise ValueError(
            'Chave mestra NFE_CERT_MASTER_KEY ausente ou muito curta (mínimo 32 caracteres).'
        )
    return raw.strip().encode('utf-8')


def _derive_aes_key(master_key: bytes, salt: bytes, aad: bytes) -> bytes:
    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        info=b'sisfarm-nfe-cert-vault-v1:' + aad,
    ).derive(master_key)


def _aad(organization_id: int, farm_id: int, environment: str) -> bytes:
    return f'org={organization_id}|farm={farm_id}|env={environment}'.encode('utf-8')


def store_encrypted_certificate(
    db,
    *,
    organization_id: int,
    farm_id: int,
    environment: str,
    pfx_bytes: bytes,
    password: str,
    user_id: int,
    master_key: str | None = None,
) -> NfeCertificateVault:
    """Valida, criptografa (AES-256-GCM) e armazena o certificado A1 no cofre."""
    env = str(environment or '').strip().lower()
    if env not in ('homologacao', 'producao'):
        raise ValueError('Ambiente do certificado deve ser homologacao ou producao.')

    mkey = _get_master_key(master_key)
    meta = parse_and_validate_pfx(pfx_bytes, password)

    salt = secrets.token_bytes(16)
    nonce = secrets.token_bytes(12)
    aad = _aad(organization_id, farm_id, env)
    aes_key = _derive_aes_key(mkey, salt, aad)
    secret_payload = json.dumps(
        {'pfx_b64': base64.b64encode(pfx_bytes).decode('ascii'), 'password': password},
        separators=(',', ':'),
    ).encode('utf-8')
    ciphertext = AESGCM(aes_key).encrypt(nonce, secret_payload, aad)

    # Serializa a troca com a Central de Certificados no mesmo ambiente.
    from app.services.digital_certificates import clear_fiscal_selection, lock_fiscal_farm
    lock_fiscal_farm(db, organization_id, farm_id)
    clear_fiscal_selection(db, organization_id, farm_id, env)

    # Desativa certificados anteriores do mesmo escopo/ambiente.
    db.query(NfeCertificateVault).filter_by(
        organization_id=organization_id, farm_id=farm_id, environment=env, is_active=True
    ).update({'is_active': False})

    row = NfeCertificateVault(
        id=uuid4().hex,
        organization_id=organization_id,
        farm_id=farm_id,
        environment=env,
        subject_cn=meta['subject_cn'],
        issuer_cn=meta['issuer_cn'],
        serial_number=meta['serial_number'],
        fingerprint_sha256=meta['fingerprint_sha256'],
        valid_from=meta['valid_from'],
        valid_until=meta['valid_until'],
        salt_b64=base64.b64encode(salt).decode('ascii'),
        nonce_b64=base64.b64encode(nonce).decode('ascii'),
        ciphertext_b64=base64.b64encode(ciphertext).decode('ascii'),
        is_active=True,
        uploaded_by=user_id,
    )
    db.add(row)
    db.commit()
    return row


def decrypt_certificate_from_vault(
    row: NfeCertificateVault,
    *,
    master_key: str | None = None,
) -> tuple[bytes, str]:
    """Descriptografa o PFX e senha em memória validando a tag AES-GCM e o escopo AAD."""
    mkey = _get_master_key(master_key)
    salt = base64.b64decode(row.salt_b64)
    nonce = base64.b64decode(row.nonce_b64)
    ciphertext = base64.b64decode(row.ciphertext_b64)
    aad = _aad(row.organization_id, row.farm_id, row.environment)
    aes_key = _derive_aes_key(mkey, salt, aad)
    try:
        plaintext = AESGCM(aes_key).decrypt(nonce, ciphertext, aad)
        data = json.loads(plaintext.decode('utf-8'))
        return base64.b64decode(data['pfx_b64']), data['password']
    except Exception:
        raise ValueError('Falha ao descriptografar o certificado A1 no cofre.') from None


class Pkcs12CertificateProvider(CertificateProvider):
    """Assinador XMLDSig Enveloped Signature (C14N + RSA-SHA1) para NF-e 4.00."""

    def __init__(self, pfx_bytes: bytes, password: str):
        parsed = parse_and_validate_pfx(pfx_bytes, password)
        self._private_key: rsa.RSAPrivateKey = parsed['private_key']
        self._certificate: x509.Certificate = parsed['certificate']
        self._cert_b64 = base64.b64encode(
            self._certificate.public_bytes(serialization.Encoding.DER)
        ).decode('ascii')

    @classmethod
    def from_vault(cls, row: NfeCertificateVault, *, master_key: str | None = None, db=None) -> 'Pkcs12CertificateProvider':
        from app.services.digital_certificates import CENTRAL, load_fiscal_pkcs12
        if row.environment == CENTRAL:
            if db is None:
                raise ValueError("O certificado central exige uma seleção fiscal ativa.")
            pfx_bytes, password = load_fiscal_pkcs12(row, db)
        else:
            pfx_bytes, password = decrypt_certificate_from_vault(row, master_key=master_key)
        return cls(pfx_bytes, password)

    def export_pem_pair(self) -> tuple[bytes, bytes]:
        """Exporta cert PEM e chave privada PEM em memória para configuração de SSLContext mTLS."""
        cert_pem = self._certificate.public_bytes(serialization.Encoding.PEM)
        key_pem = self._private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        return cert_pem, key_pem

    def sign_xml(self, xml_bytes: bytes, reference_id: str) -> bytes:
        """Assina o elemento alvo (ex.: infNFe ou infEvento) com XMLDSig Enveloped Signature."""
        if not isinstance(xml_bytes, bytes) or len(xml_bytes) > MAX_XML_BYTES:
            raise UnsafeXML('XML inválido ou acima do limite permitido.')
        try:
            root = etree.fromstring(xml_bytes, _safe_parser())
        except etree.XMLSyntaxError:
            raise UnsafeXML('XML malformado ou inseguro.') from None
        if root.getroottree().docinfo.doctype:
            raise UnsafeXML('DTD não é permitida na assinatura XML.')

        targets = root.xpath(f'.//*[@Id="{reference_id}"]')
        if len(targets) != 1:
            raise ValueError(f'Elemento alvo com Id={reference_id!r} não encontrado de forma única.')
        target = targets[0]
        parent = target.getparent()
        if parent is None:
            raise ValueError('Elemento alvo precisa estar contido em um nó pai (<NFe> ou <evento>).')
        if parent.xpath('./ds:Signature', namespaces={'ds': DS_NS}):
            raise ValueError('O documento já possui uma assinatura XMLDSig.')

        # 1. Canonicalização C14N 1.0 e DigestValue SHA-1 do elemento alvo (sem comentários).
        target_c14n = etree.tostring(target, method='c14n', exclusive=False, with_comments=False)
        digest_b64 = base64.b64encode(sha1(target_c14n).digest()).decode('ascii')

        # 2. Monta <Signature><SignedInfo>...</SignedInfo></Signature> e anexa ao pai ANTES
        #    da canonicalização de SignedInfo (garante herança exata de namespaces no C14N 1.0).
        sig = etree.Element(f'{{{DS_NS}}}Signature', nsmap={None: DS_NS})
        signed_info = etree.SubElement(sig, f'{{{DS_NS}}}SignedInfo')
        etree.SubElement(signed_info, f'{{{DS_NS}}}CanonicalizationMethod', {'Algorithm': C14N_ALG})
        etree.SubElement(signed_info, f'{{{DS_NS}}}SignatureMethod', {'Algorithm': RSA_SHA1_ALG})
        ref = etree.SubElement(signed_info, f'{{{DS_NS}}}Reference', {'URI': f'#{reference_id}'})
        transforms = etree.SubElement(ref, f'{{{DS_NS}}}Transforms')
        etree.SubElement(transforms, f'{{{DS_NS}}}Transform', {'Algorithm': ENV_SIG_ALG})
        etree.SubElement(transforms, f'{{{DS_NS}}}Transform', {'Algorithm': C14N_ALG})
        etree.SubElement(ref, f'{{{DS_NS}}}DigestMethod', {'Algorithm': SHA1_ALG})
        etree.SubElement(ref, f'{{{DS_NS}}}DigestValue').text = digest_b64
        parent.append(sig)

        # 3. Canonicaliza SignedInfo já inserido na árvore e assina com RSA PKCS#1 v1.5 + SHA-1.
        signed_info_c14n = etree.tostring(signed_info, method='c14n', exclusive=False, with_comments=False)
        signature_raw = self._private_key.sign(signed_info_c14n, padding.PKCS1v15(), hashes.SHA1())
        sig_value_b64 = base64.b64encode(signature_raw).decode('ascii')

        etree.SubElement(sig, f'{{{DS_NS}}}SignatureValue').text = sig_value_b64
        key_info = etree.SubElement(sig, f'{{{DS_NS}}}KeyInfo')
        x509_data = etree.SubElement(key_info, f'{{{DS_NS}}}X509Data')
        etree.SubElement(x509_data, f'{{{DS_NS}}}X509Certificate').text = self._cert_b64

        return etree.tostring(root, encoding='utf-8', xml_declaration=True)


def verify_xml_signature(signed_xml_bytes: bytes) -> bool:
    """Verifica matematicamente DigestValue e SignatureValue de um XML assinado."""
    if not isinstance(signed_xml_bytes, bytes) or len(signed_xml_bytes) > MAX_XML_BYTES:
        return False
    try:
        root = etree.fromstring(signed_xml_bytes, _safe_parser())
    except etree.XMLSyntaxError:
        return False
    if root.getroottree().docinfo.doctype:
        return False

    sigs = root.xpath('.//ds:Signature', namespaces={'ds': DS_NS})
    if len(sigs) != 1:
        return False
    sig = sigs[0]
    signed_info = sig.find(f'{{{DS_NS}}}SignedInfo')
    sig_val_node = sig.find(f'{{{DS_NS}}}SignatureValue')
    cert_node = sig.find(f'.//{{{DS_NS}}}X509Certificate')
    ref = sig.find(f'.//{{{DS_NS}}}Reference')
    digest_node = sig.find(f'.//{{{DS_NS}}}DigestValue')
    if any(n is None for n in (signed_info, sig_val_node, cert_node, ref, digest_node)):
        return False

    uri = str(ref.get('URI') or '')
    if not uri.startswith('#') or len(uri) < 2:
        return False
    ref_id = uri[1:]

    # Verifica DigestValue sobre uma cópia sem o nó <Signature>.
    tree_copy = deepcopy(root)
    for s in tree_copy.xpath('.//ds:Signature', namespaces={'ds': DS_NS}):
        s.getparent().remove(s)
    targets = tree_copy.xpath(f'.//*[@Id="{ref_id}"]')
    if len(targets) != 1:
        return False
    target_c14n = etree.tostring(targets[0], method='c14n', exclusive=False, with_comments=False)
    expected_digest = base64.b64encode(sha1(target_c14n).digest()).decode('ascii')
    if expected_digest != (digest_node.text or '').strip():
        return False

    # Verifica SignatureValue com a chave pública do certificado embutido.
    try:
        cert = x509.load_der_x509_certificate(base64.b64decode((cert_node.text or '').strip()))
        signed_info_c14n = etree.tostring(signed_info, method='c14n', exclusive=False, with_comments=False)
        sig_raw = base64.b64decode((sig_val_node.text or '').strip())
        cert.public_key().verify(sig_raw, signed_info_c14n, padding.PKCS1v15(), hashes.SHA1())
        return True
    except (InvalidSignature, ValueError, TypeError):
        return False
