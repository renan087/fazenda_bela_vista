"""Parsing of local A1 certificate material shared by both consumers."""
from datetime import datetime, timezone
from hashlib import sha256

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12


def _extract_cn(name: x509.Name) -> str:
    attrs = name.get_attributes_for_oid(x509.NameOID.COMMON_NAME)
    return str(attrs[0].value)[:240] if attrs else ''


def parse_and_validate_pfx(pfx_bytes: bytes, password: str, *, now: datetime | None = None) -> dict:
    """Valida um arquivo PKCS#12 (.pfx/.p12), verifica chave RSA e vigência do X.509."""
    if not isinstance(pfx_bytes, bytes) or not pfx_bytes or len(pfx_bytes) > 200_000:
        raise ValueError('Arquivo de certificado A1 inválido ou acima do limite permitido.')
    if not isinstance(password, str) or not password:
        raise ValueError('Informe a senha do certificado A1.')
    try:
        private_key, cert, additional = pkcs12.load_key_and_certificates(pfx_bytes, password.encode('utf-8'))
    except Exception:
        raise ValueError('Não foi possível abrir o certificado A1. Confira o arquivo e a senha.') from None

    if private_key is None or cert is None or not isinstance(private_key, rsa.RSAPrivateKey):
        raise ValueError('O certificado A1 deve conter uma chave privada RSA e um certificado X.509 válido.')

    ref_now = now or datetime.now(timezone.utc)
    valid_from = cert.not_valid_before_utc
    valid_until = cert.not_valid_after_utc
    if ref_now < valid_from or ref_now > valid_until:
        raise ValueError('O certificado A1 está fora do período de validade.')

    der_bytes = cert.public_bytes(serialization.Encoding.DER)
    return {
        'private_key': private_key,
        'certificate': cert,
        'chain': additional or [],
        'subject_cn': _extract_cn(cert.subject),
        'issuer_cn': _extract_cn(cert.issuer),
        'serial_number': format(cert.serial_number, 'x')[:80],
        'fingerprint_sha256': sha256(der_bytes).hexdigest(),
        'valid_from': valid_from,
        'valid_until': valid_until,
    }
