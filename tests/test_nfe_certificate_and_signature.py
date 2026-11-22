from datetime import datetime, timedelta, timezone
import json
import shutil
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base
from app.services.nfe_certificate_vault import (
    Pkcs12CertificateProvider,
    decrypt_certificate_from_vault,
    parse_and_validate_pfx,
    store_encrypted_certificate,
    verify_xml_signature,
)
from app.services.nfe_events import build_cancellation_event
from app.services.nfe_schema import SCHEMA_DIRECTORY, validate_xml
from app.services.nfe_xml_builder import build_simulation_xml
from tests.test_nfe_xml_builder import _sample_inputs

TEST_MASTER_KEY = 'chave-mestra-apenas-para-testes-unitarios-32bytes!'


def _new_db():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    return Session(engine)


def _generate_ephemeral_pfx(*, password: str = 'senha-teste-123', expired: bool = False) -> bytes:
    """Gera certificado RSA-2048 autoassinado efêmero apenas em memória para testes."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(timezone.utc)
    if expired:
        not_before = now - timedelta(days=400)
        not_after = now - timedelta(days=10)
    else:
        not_before = now - timedelta(days=1)
        not_after = now + timedelta(days=365)

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, 'BR'),
        x509.NameAttribute(NameOID.COMMON_NAME, 'PRODUTOR FICTICIO DE TESTE:52998224725'),
    ])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .sign(key, hashes.SHA256())
    )
    return pkcs12.serialize_key_and_certificates(
        name=b'teste-a1',
        key=key,
        cert=cert,
        cas=None,
        encryption_algorithm=serialization.BestAvailableEncryption(password.encode('utf-8')),
    )


def test_certificate_vault_encrypts_validates_and_binds_scope():
    db = _new_db()
    pfx = _generate_ephemeral_pfx(password='segredo123')
    expired_pfx = _generate_ephemeral_pfx(password='segredo123', expired=True)

    # Rejeita certificado expirado, senha errada e chave mestra curta.
    with pytest.raises(ValueError):
        parse_and_validate_pfx(expired_pfx, 'segredo123')
    with pytest.raises(ValueError):
        parse_and_validate_pfx(pfx, 'senha-errada')
    with pytest.raises(ValueError):
        store_encrypted_certificate(
            db, organization_id=1, farm_id=10, environment='homologacao',
            pfx_bytes=pfx, password='segredo123', user_id=5, master_key='curta',
        )

    vault_row = store_encrypted_certificate(
        db, organization_id=1, farm_id=10, environment='homologacao',
        pfx_bytes=pfx, password='segredo123', user_id=5, master_key=TEST_MASTER_KEY,
    )
    assert vault_row.is_active is True
    assert 'PRODUTOR FICTICIO DE TESTE' in vault_row.subject_cn
    assert 'segredo123' not in vault_row.ciphertext_b64

    # Descriptografa com a chave mestra correta.
    out_pfx, out_pwd = decrypt_certificate_from_vault(vault_row, master_key=TEST_MASTER_KEY)
    assert out_pfx == pfx and out_pwd == 'segredo123'

    # Rejeita chave mestra errada.
    with pytest.raises(ValueError):
        decrypt_certificate_from_vault(vault_row, master_key='outra-chave-mestra-totalmente-diferente-32bytes!')

    # Rejeita tentativa de copiar o blob criptografado para outra fazenda (AAD mismatch).
    vault_row.farm_id = 999
    with pytest.raises(ValueError):
        decrypt_certificate_from_vault(vault_row, master_key=TEST_MASTER_KEY)
    db.close()


def test_xmldsig_signature_verifies_mathematically_and_passes_temp_xsd(tmp_path):
    pfx = _generate_ephemeral_pfx(password='segredo123')
    provider = Pkcs12CertificateProvider(pfx, 'segredo123')

    snapshot, issuer, details, technical, identification, extra_data = _sample_inputs(cst='40')
    built = build_simulation_xml(
        snapshot=snapshot, issuer=issuer, details=details, technical=technical,
        identification=identification, issuer_crt='3', extra_data=extra_data,
    )
    assert built['ready'] is True

    signed_xml = provider.sign_xml(built['xml_bytes'], 'NFe' + built['access_key'])
    assert verify_xml_signature(signed_xml) is True

    # Adulteração de 1 caractere invalida a assinatura imediatamente.
    tampered = signed_xml.replace(b'CAFE FICTICIO DE TESTE', b'CAFE ADULTERADO TESTE')
    assert verify_xml_signature(tampered) is False

    # Validação contra cópia temporária do XSD 4.00 (incluindo xmldsig-core-schema_v1.01.xsd).
    temp_schema = tmp_path / 'schemas'
    shutil.copytree(SCHEMA_DIRECTORY, temp_schema)
    manifest_path = temp_schema / 'manifest.json'
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    manifest['reviewed'] = True
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')

    validation = validate_xml(signed_xml, schema_directory=temp_schema)
    assert validation['valid'] is True, f'Erros XSD no XML assinado: {validation["errors"]}'

    # Também assina e verifica evento de cancelamento (infEvento).
    cancel = build_cancellation_event(
        state_uf='BA', author_document='52998224725', access_key=built['access_key'],
        protocol_number='129260000000001',
        justification='Cancelamento ficticio de teste assinado em memoria.',
        event_datetime='2026-11-21T15:00:00-03:00',
    )
    signed_event = provider.sign_xml(cancel['xml_bytes'], cancel['event_id'])
    assert verify_xml_signature(signed_event) is True
