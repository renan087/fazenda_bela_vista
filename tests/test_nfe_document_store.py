import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base
from app.services.nfe_access_key import build_access_key
from app.services.nfe_document_store import (
    assert_document_can_be_deleted,
    build_simulated_nfe_proc,
    store_simulated_document,
    verify_stored_document,
)
from app.services.nfe_schema import UnsafeXML


def _new_db():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    return Session(engine)


def _sample_key():
    return build_access_key(
        state_uf='BA',
        emission_date='2026-11-21',
        issuer_document='52998224725',
        series=920,
        number=1,
        numeric_code='24681357',
    )['key']


def test_build_simulated_nfe_proc_validates_matching_key_tpamb_and_xxe():
    key = _sample_key()
    nfe_xml = (
        f'<NFe xmlns="http://www.portalfiscal.inf.br/nfe">'
        f'<infNFe versao="4.00" Id="NFe{key}"><ide><tpAmb>2</tpAmb></ide></infNFe></NFe>'
    ).encode('utf-8')
    prot_xml = (
        f'<protNFe xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00">'
        f'<infProt><tpAmb>2</tpAmb><chNFe>{key}</chNFe><nProt>129260000000001</nProt><cStat>100</cStat></infProt></protNFe>'
    ).encode('utf-8')

    proc = build_simulated_nfe_proc(nfe_xml_bytes=nfe_xml, prot_nfe_xml_bytes=prot_xml)
    assert b'<nfeProc' in proc and b'SIMULACAO DE NFEPROC SEM VALOR FISCAL' in proc

    # Rejeita tpAmb=1 (produção).
    prod_nfe = nfe_xml.replace(b'<tpAmb>2</tpAmb>', b'<tpAmb>1</tpAmb>')
    with pytest.raises(PermissionError):
        build_simulated_nfe_proc(nfe_xml_bytes=prod_nfe, prot_nfe_xml_bytes=prot_xml)

    # Rejeita chave divergente entre NFe e protNFe.
    other_key = build_access_key(
        state_uf='BA', emission_date='2026-11-21', issuer_document='52998224725',
        series=920, number=2, numeric_code='86429753',
    )['key']
    wrong_prot = prot_xml.replace(key.encode(), other_key.encode())
    with pytest.raises(ValueError):
        build_simulated_nfe_proc(nfe_xml_bytes=nfe_xml, prot_nfe_xml_bytes=wrong_prot)

    # Rejeita DTD/XXE.
    with pytest.raises(UnsafeXML):
        build_simulated_nfe_proc(nfe_xml_bytes=b'<!DOCTYPE a><NFe/>', prot_nfe_xml_bytes=prot_xml)


def test_store_simulated_document_is_idempotent_locked_and_detects_tampering():
    db = _new_db()
    key = _sample_key()
    xml_bytes = b'<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00"/>'

    doc1 = store_simulated_document(
        db, draft_id='d' * 32, organization_id=1, farm_id=10,
        document_type='nfe_proc_simulacao', access_key=key,
        xml_bytes=xml_bytes, user_id=5, protocol_number='129260000000001',
    )
    doc2 = store_simulated_document(
        db, draft_id='d' * 32, organization_id=1, farm_id=10,
        document_type='nfe_proc_simulacao', access_key=key,
        xml_bytes=xml_bytes, user_id=5, protocol_number='129260000000001',
    )
    assert doc1.id == doc2.id
    assert doc1.environment == 'teste_ficticio'
    assert doc1.is_locked is True
    assert verify_stored_document(doc1) is True

    # Política de retenção bloqueia exclusão de documento travado.
    with pytest.raises(PermissionError):
        assert_document_can_be_deleted(doc1)

    # Detecta adulteração direta no banco.
    doc1.xml_content = doc1.xml_content + ' '
    db.commit()
    assert verify_stored_document(doc1) is False
    db.close()
