import json
from types import SimpleNamespace
import pytest
from fastapi import HTTPException

from app.models.nfe_draft import NfeDraft
from app.services.nfe_access_key import build_access_key
from app.services.nfe_danfe_preview import build_danfe_preview_pdf
from app.services.nfe_events import (
    EVENT_CANCELAMENTO,
    EVENT_CARTA_CORRECAO,
    build_cancellation_event,
    build_correction_letter_event,
    parse_event_response,
)
from app.services.nfe_schema import UnsafeXML


def _sample_key():
    return build_access_key(
        state_uf='BA',
        emission_date='2026-11-21',
        issuer_document='52998224725',
        series=920,
        number=1,
        numeric_code='24681357',
    )['key']


def test_build_cancellation_and_cce_events_offline():
    key = _sample_key()
    cancel = build_cancellation_event(
        state_uf='BA',
        author_document='529.982.247-25',
        author_type='cpf',
        access_key=key,
        protocol_number='129260000000001',
        justification='Cancelamento ficticio de teste para validacao de estrutura.',
        event_datetime='2026-11-21T13:00:00-03:00',
    )
    assert cancel['event_type'] == EVENT_CANCELAMENTO
    assert cancel['event_id'] == f'ID110111{key}01'
    assert cancel['signed'] is False
    assert b'<tpAmb>2</tpAmb>' in cancel['xml_bytes']
    assert b'<CPF>52998224725</CPF>' in cancel['xml_bytes']
    assert b'NFeRecepcaoEvento4' in cancel['soap_bytes']

    cce = build_correction_letter_event(
        state_uf='BA',
        author_document='11.222.333/0001-81',
        author_type='cnpj',
        access_key=key,
        correction_text='Correcao ficticia do endereco de entrega para teste estrutural.',
        seq=2,
        event_datetime='2026-11-21T14:00:00-03:00',
    )
    assert cce['event_type'] == EVENT_CARTA_CORRECAO
    assert cce['event_id'] == f'ID110110{key}02'
    assert b'<CNPJ>11222333000181</CNPJ>' in cce['xml_bytes']
    assert b'<xCondUso>' in cce['xml_bytes']

    # Rejeita produção, justificativa curta, protocolo inválido e sequência fora da faixa.
    with pytest.raises(PermissionError):
        build_cancellation_event(
            state_uf='BA', author_document='52998224725', access_key=key,
            protocol_number='129260000000001', justification='Justificativa valida de teste',
            event_datetime='2026-11-21T13:00:00-03:00', tp_amb='1',
        )
    with pytest.raises(ValueError):
        build_cancellation_event(
            state_uf='BA', author_document='52998224725', access_key=key,
            protocol_number='129260000000001', justification='curta',
            event_datetime='2026-11-21T13:00:00-03:00',
        )
    with pytest.raises(ValueError):
        build_correction_letter_event(
            state_uf='BA', author_document='52998224725', access_key=key,
            correction_text='Texto valido de carta de correcao de teste.', seq=21,
            event_datetime='2026-11-21T14:00:00-03:00',
        )


def test_parse_event_response_and_xxe_block():
    ok_xml = (
        b'<retEnvEvento xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.00">'
        b'<retEvento versao="1.00"><infEvento>'
        b'<cStat>135</cStat><xMotivo>Evento registrado e vinculado a NF-e</xMotivo>'
        b'<tpEvento>110111</tpEvento><nSeqEvento>1</nSeqEvento>'
        b'<nProt>129260000000099</nProt>'
        b'<chNFe>29261100052998224725559200000000011246813570</chNFe>'
        b'</infEvento></retEvento></retEnvEvento>'
    )
    parsed = parse_event_response(ok_xml)
    assert parsed['category'] == 'evento_registrado'
    assert parsed['c_stat'] == '135' and parsed['protocol_number'] == '129260000000099'

    rej_xml = b'<retEnvEvento xmlns="http://www.portalfiscal.inf.br/nfe"><retEvento><infEvento><cStat>573</cStat><xMotivo>Rejeicao: Duplicidade de Evento</xMotivo></infEvento></retEvento></retEnvEvento>'
    assert parse_event_response(rej_xml)['category'] == 'rejeitado'

    with pytest.raises(UnsafeXML):
        parse_event_response(b'<!DOCTYPE x [<!ENTITY a "b">]><retEnvEvento/>')


def test_danfe_preview_pdf_generation_and_route_isolation(monkeypatch):
    from app.web import routes
    from tests.test_stock_assistant import _route_env

    snapshot = {
        'venda': 'V-0001',
        'data_venda': '2026-11-21',
        'comprador': 'Comprador Ficticio',
        'valor_bruto': '100.00',
        'itens': [{'lote': 'L1', 'quantidade_sacas': '1', 'valor_bruto': '100.00'}],
    }
    issuer = SimpleNamespace(
        name='Produtor Ficticio', cpf='52998224725', state_registration='123456',
        street='Rua Teste', number='10', district='Zona Rural', city='Salvador',
        state='BA', postal_code='40000000',
    )
    ident = SimpleNamespace(
        environment='teste_ficticio', model='55', series=920, number=1,
        numeric_code='24681357', emission_type='1',
    )
    pdf_bytes = build_danfe_preview_pdf(
        snapshot=snapshot,
        issuer=issuer,
        details={'recipient_name': 'Comprador Ficticio', 'recipient_cpf': '11144477735', 'recipient_state': 'BA'},
        technical={'emission_date': '2026-11-21', 'items': [{'item_index': 1, 'commercial_unit': 'SC', 'commercial_quantity': '1', 'commercial_unit_price': '100.00'}]},
        identification=ident,
        extra_data={'additional_info': 'Observacao de teste'},
    )
    assert pdf_bytes.startswith(b'%PDF-')
    assert len(pdf_bytes) > 1500

    # Sem identificação reservada também gera PDF sem erro.
    pdf_minimal = build_danfe_preview_pdf(snapshot=snapshot, issuer=None, details={}, technical={})
    assert pdf_minimal.startswith(b'%PDF-')

    # Rota bloqueia rascunho de outra organização.
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    other_draft = NfeDraft(
        id='p' * 32, organization_id=user.organization_id + 999,
        farm_id=scope['active_farm_id'], sale_code='V-1', created_by=user.id,
        snapshot_json=json.dumps(snapshot),
    )
    db.add(other_draft)
    db.commit()
    monkeypatch.setattr(routes, '_launch_scope_or_redirect', lambda *args: (scope, None))
    with pytest.raises(HTTPException) as exc:
        routes.fiscal_preview_danfe_pdf(other_draft.id, SimpleNamespace(), db, user)
    assert exc.value.status_code == 404
    db.close()
