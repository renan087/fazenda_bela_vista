import json
from pathlib import Path
import shutil
from types import SimpleNamespace
from xml.etree import ElementTree as ET
import pytest

from app.services.nfe_access_key import validate_access_key
from app.services.nfe_schema import SCHEMA_DIRECTORY, validate_xml
from app.services.nfe_xml_builder import build_simulation_xml

DS_NS = 'http://www.w3.org/2000/09/xmldsig#'
NFE_NS = 'http://www.portalfiscal.inf.br/nfe'


def _sample_inputs(cst='40'):
    snapshot = {
        'venda': 'V-0001',
        'data_venda': '2026-11-21',
        'comprador': 'Comprador Ficticio',
        'valor_bruto': '250.00',
        'itens': [
            {'lote': 'L1', 'quantidade_sacas': '1', 'valor_bruto': '100.00'},
            {'lote': 'L2', 'quantidade_sacas': '1.5', 'valor_bruto': '150.00'},
        ],
    }
    issuer = SimpleNamespace(
        cpf='52998224725',
        name='PRODUTOR FICTICIO DE TESTE',
        street='RUA TESTE',
        number='10',
        complement='',
        district='ZONA RURAL',
        municipality_code='2927408',
        city='SALVADOR',
        state='BA',
        postal_code='40000000',
        state_registration='123456',
    )
    details = {
        'recipient_cpf': '11144477735',
        'recipient_name': 'DESTINATARIO FICTICIO DE TESTE',
        'recipient_street': 'AV TESTE',
        'recipient_number': '20',
        'recipient_complement': '',
        'recipient_district': 'CENTRO',
        'recipient_municipality_code': '2927408',
        'recipient_city': 'SALVADOR',
        'recipient_state': 'BA',
        'recipient_postal_code': '40000000',
        'recipient_ie_indicator': 'nao_contribuinte',
        'recipient_ie': '',
        'product_description': 'CAFE FICTICIO DE TESTE',
        'product_ncm': '09011110',
        'operation_nature': 'VENDA FICTICIA DE TESTE',
        'operation_cfop': '5102',
    }
    item1 = {
        'item_index': 1, 'origin': '0', 'icms_cst': cst,
        'commercial_unit': 'SC', 'commercial_quantity': '1', 'commercial_unit_price': '100',
        'tax_unit': 'SC', 'tax_quantity': '1', 'tax_unit_price': '100',
    }
    item2 = {
        'item_index': 2, 'origin': '0', 'icms_cst': cst,
        'commercial_unit': 'SC', 'commercial_quantity': '1.5', 'commercial_unit_price': '100',
        'tax_unit': 'SC', 'tax_quantity': '1.5', 'tax_unit_price': '100',
    }
    if cst == '00':
        item1.update({'icms_base': '100.00', 'icms_rate': '12', 'icms_value': '12.00'})
        item2.update({'icms_base': '150.00', 'icms_rate': '12', 'icms_value': '18.00'})
    technical = {'emission_date': '2026-11-21', 'freight_mode': '9', 'items': [item1, item2]}
    identification = SimpleNamespace(
        environment='teste_ficticio',
        model='55',
        series=920,
        number=1,
        numeric_code='24681357',
        emission_type='1',
        finality='1',
        consumer_indicator='1',
        presence_indicator='1',
        payment_method='17',
    )
    extra_data = {
        'carrier_name': 'TRANSPORTADOR FICTICIO',
        'vehicle_plate': 'ABC1D23',
        'vehicle_uf': 'BA',
        'volume_quantity': '3',
        'volume_species': 'SACAS',
        'net_weight_kg': '150.000',
        'gross_weight_kg': '152.500',
        'payment_indicator': '0',
        'payment_amount': '250.00',
        'additional_info': 'DOCUMENTO FICTICIO PARA TESTE ESTRUTURAL.',
    }
    return snapshot, issuer, details, technical, identification, extra_data


def _attach_dummy_signature_for_xsd_test(xml_bytes: bytes, ref_id: str) -> bytes:
    """Anexa uma assinatura artificial (AA==) somente em memória para testar estrutura XSD."""
    dummy_sig = (
        f'<ds:Signature xmlns:ds="{DS_NS}">'
        '<ds:SignedInfo>'
        '<ds:CanonicalizationMethod Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/>'
        '<ds:SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>'
        f'<ds:Reference URI="#{ref_id}">'
        '<ds:Transforms>'
        '<ds:Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>'
        '<ds:Transform Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/>'
        '</ds:Transforms>'
        '<ds:DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/>'
        '<ds:DigestValue>AA==</ds:DigestValue>'
        '</ds:Reference>'
        '</ds:SignedInfo>'
        '<ds:SignatureValue>AA==</ds:SignatureValue>'
        '<ds:KeyInfo><ds:X509Data><ds:X509Certificate>AA==</ds:X509Certificate></ds:X509Data></ds:KeyInfo>'
        '</ds:Signature>'
    )
    text = xml_bytes.decode('utf-8')
    return text.replace('</NFe>', dummy_sig + '</NFe>').encode('utf-8')


def test_simulation_xml_builder_enforces_simulation_guard_and_reports_missing():
    snapshot, issuer, details, technical, identification, extra_data = _sample_inputs()
    with pytest.raises(PermissionError):
        build_simulation_xml(
            snapshot=snapshot, issuer=issuer, details=details, technical=technical,
            identification=identification, issuer_crt='3', simulation_only=False,
        )
    prod_ident = SimpleNamespace(**{**identification.__dict__, 'environment': 'producao'})
    with pytest.raises(PermissionError):
        build_simulation_xml(
            snapshot=snapshot, issuer=issuer, details=details, technical=technical,
            identification=prod_ident, issuer_crt='3',
        )

    incomplete = build_simulation_xml(
        snapshot=snapshot, issuer=None, details={}, technical={}, identification=None, issuer_crt='',
    )
    assert incomplete['ready'] is False and incomplete['xml_bytes'] is None
    assert len(incomplete['missing_fields']) > 5


def test_simulation_xml_builder_sums_icms_and_validates_against_temp_xsd_copy(tmp_path):
    # Prepara cópia temporária do pacote recebido apenas para validar estrutura XSD no teste.
    temp_schema = tmp_path / 'schemas'
    shutil.copytree(SCHEMA_DIRECTORY, temp_schema)
    manifest_path = temp_schema / 'manifest.json'
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    manifest['reviewed'] = True
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')

    for cst, expected_vbc, expected_vicms in (('40', '0.00', '0.00'), ('00', '250.00', '30.00')):
        snapshot, issuer, details, technical, identification, extra_data = _sample_inputs(cst=cst)
        result = build_simulation_xml(
            snapshot=snapshot, issuer=issuer, details=details, technical=technical,
            identification=identification, issuer_crt='3', extra_data=extra_data,
        )
        assert result['ready'] is True
        assert validate_access_key(result['access_key']) is True
        assert b'<Signature' not in result['xml_bytes']

        root = ET.fromstring(result['xml_bytes'])
        ns = {'nfe': NFE_NS}
        assert root.findtext('nfe:infNFe/nfe:ide/nfe:tpAmb', namespaces=ns) == '2'
        assert root.findtext('nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vProd', namespaces=ns) == '250.00'
        assert root.findtext('nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vBC', namespaces=ns) == expected_vbc
        assert root.findtext('nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vICMS', namespaces=ns) == expected_vicms
        assert root.findtext('nfe:infNFe/nfe:infAdic/nfe:infCpl', namespaces=ns) == 'DOCUMENTO FICTICIO PARA TESTE ESTRUTURAL.'

        # Com assinatura fictícia em memória, o XSD 4.00 valida toda a estrutura gerada.
        with_dummy_sig = _attach_dummy_signature_for_xsd_test(result['xml_bytes'], 'NFe' + result['access_key'])
        validation = validate_xml(with_dummy_sig, schema_directory=temp_schema)
        assert validation['valid'] is True, f'Erros XSD para CST {cst}: {validation["errors"]}'


def test_simulation_xml_builder_supports_optional_ibscbs_validated_by_temp_xsd(tmp_path):
    temp_schema = tmp_path / 'schemas'
    shutil.copytree(SCHEMA_DIRECTORY, temp_schema)
    manifest_path = temp_schema / 'manifest.json'
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    manifest['reviewed'] = True
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')

    snapshot, issuer, details, technical, identification, extra_data = _sample_inputs(cst='40')
    technical['items'][0].update({
        'ibs_cbs_cst': '000', 'ibs_cbs_class': '000001', 'ibs_cbs_base': '100.00',
        'ibs_uf_rate': '0.1000', 'ibs_uf_value': '0.10',
        'ibs_municipal_rate': '0.0000', 'ibs_municipal_value': '0.00',
        'cbs_rate': '0.9000', 'cbs_value': '0.90',
    })
    technical['items'][1].update({
        'ibs_cbs_cst': '000', 'ibs_cbs_class': '000001', 'ibs_cbs_base': '150.00',
        'ibs_uf_rate': '0.1000', 'ibs_uf_value': '0.15',
        'ibs_municipal_rate': '0.0000', 'ibs_municipal_value': '0.00',
        'cbs_rate': '0.9000', 'cbs_value': '1.35',
    })

    result = build_simulation_xml(
        snapshot=snapshot, issuer=issuer, details=details, technical=technical,
        identification=identification, issuer_crt='3', extra_data=extra_data,
        include_ibscbs=True,
    )
    assert result['ready'] is True
    root = ET.fromstring(result['xml_bytes'])
    ns = {'nfe': NFE_NS}
    assert root.findtext('nfe:infNFe/nfe:total/nfe:IBSCBSTot/nfe:vBCIBSCBS', namespaces=ns) == '250.00'
    assert root.findtext('nfe:infNFe/nfe:total/nfe:IBSCBSTot/nfe:gIBS/nfe:vIBS', namespaces=ns) == '0.25'
    assert root.findtext('nfe:infNFe/nfe:total/nfe:IBSCBSTot/nfe:gCBS/nfe:vCBS', namespaces=ns) == '2.25'

    with_dummy_sig = _attach_dummy_signature_for_xsd_test(result['xml_bytes'], 'NFe' + result['access_key'])
    validation = validate_xml(with_dummy_sig, schema_directory=temp_schema)
    assert validation['valid'] is True, f'Erros XSD para IBSCBS: {validation["errors"]}'
