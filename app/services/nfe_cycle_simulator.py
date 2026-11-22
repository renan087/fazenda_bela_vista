"""Orquestrador do ciclo completo de NF-e em modo 100% offline/simulado.

Exercita ponta a ponta: montagem XML -> assinatura (via provider de teste) ->
envelope SOAP enviNFe -> retorno simulado retEnviNFe -> tentativa idempotente ->
montagem nfeProc -> guarda imutável com SHA-256 -> histórico local.
Nunca realiza chamadas de rede nem transmite à SEFAZ.
"""
from datetime import datetime, timezone

from app.services.nfe_document_store import build_simulated_nfe_proc, store_simulated_document
from app.services.nfe_sefaz_protocol import (
    CertificateProvider,
    build_soap_authorize_batch,
    record_simulated_attempt,
)
from app.services.nfe_snapshots import ALLOWED_EVENT_TYPES, record_draft_event
from app.services.nfe_xml_builder import build_simulation_xml


def _build_mock_sefaz_response(access_key: str, *, c_stat: str = '100', x_motivo: str = 'Autorizado o uso da NF-e (SIMULACAO)', protocol_number: str = '129260000000001') -> tuple[bytes, bytes]:
    now_iso = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S-03:00')
    prot_xml = (
        f'<protNFe xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00">'
        f'<infProt>'
        f'<tpAmb>2</tpAmb>'
        f'<verAplic>BA_SIMULADOR_4.00</verAplic>'
        f'<chNFe>{access_key}</chNFe>'
        f'<dhRecbto>{now_iso}</dhRecbto>'
        f'<nProt>{protocol_number}</nProt>'
        f'<digVal>AA==</digVal>'
        f'<cStat>{c_stat}</cStat>'
        f'<xMotivo>{x_motivo}</xMotivo>'
        f'</infProt></protNFe>'
    ).encode('utf-8')
    ret_xml = (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<retEnviNFe xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00">'
        b'<tpAmb>2</tpAmb><cStat>104</cStat><xMotivo>Lote processado (SIMULACAO)</xMotivo>'
        + prot_xml
        + b'</retEnviNFe>'
    )
    return ret_xml, prot_xml


def run_offline_cycle_simulation(
    db,
    *,
    draft_id: str,
    organization_id: int,
    farm_id: int,
    user_id: int,
    snapshot: dict,
    issuer,
    details: dict,
    technical: dict,
    identification,
    issuer_crt: str,
    extra_data: dict | None = None,
    signer: CertificateProvider | None = None,
    include_ibscbs: bool = False,
    simulated_c_stat: str = '100',
    simulated_x_motivo: str = 'Autorizado o uso da NF-e (SIMULACAO)',
    simulated_protocol: str = '129260000000001',
    idempotency_key: str = '',
) -> dict:
    """Executa o fluxo completo localmente em ambiente teste_ficticio sem rede."""
    built = build_simulation_xml(
        snapshot=snapshot,
        issuer=issuer,
        details=details,
        technical=technical,
        identification=identification,
        issuer_crt=issuer_crt,
        extra_data=extra_data,
        include_ibscbs=include_ibscbs,
        simulation_only=True,
    )
    if not built['ready']:
        return {
            'ok': False,
            'stage': 'montagem_xml',
            'missing_fields': built['missing_fields'],
            'attempt': None,
            'stored_document': None,
        }

    access_key = built['access_key']
    nfe_xml = built['xml_bytes']
    signed = False
    if signer is not None:
        nfe_xml = signer.sign_xml(nfe_xml, f'NFe{access_key}')
        signed = True

    soap_req = build_soap_authorize_batch(nfe_xml_bytes=nfe_xml, batch_id=str(identification.number), sync=True)
    ret_xml, prot_xml = _build_mock_sefaz_response(
        access_key,
        c_stat=simulated_c_stat,
        x_motivo=simulated_x_motivo,
        protocol_number=simulated_protocol,
    )

    idem = idempotency_key.strip() if idempotency_key else f'sim-{draft_id[:12]}-{identification.series}-{identification.number}-{simulated_c_stat}'
    attempt = record_simulated_attempt(
        db,
        draft_id=draft_id,
        organization_id=organization_id,
        farm_id=farm_id,
        idempotency_key=idem,
        uf=issuer.state,
        operation='autorizacao',
        request_bytes=soap_req,
        response_bytes=ret_xml,
        user_id=user_id,
    )

    stored_doc = None
    if attempt.status_category == 'autorizada':
        nfe_proc = build_simulated_nfe_proc(nfe_xml_bytes=nfe_xml, prot_nfe_xml_bytes=prot_xml)
        stored_doc = store_simulated_document(
            db,
            draft_id=draft_id,
            organization_id=organization_id,
            farm_id=farm_id,
            document_type='nfe_proc_simulacao',
            access_key=access_key,
            xml_bytes=nfe_proc,
            user_id=user_id,
            protocol_number=attempt.protocol_number,
        )

    if 'simulacao_ciclo_offline' in ALLOWED_EVENT_TYPES:
        record_draft_event(
            db,
            draft_id=draft_id,
            organization_id=organization_id,
            farm_id=farm_id,
            event_type='simulacao_ciclo_offline',
            summary=f'Simulação offline executada (cStat {attempt.c_stat}: {attempt.status_category}).',
            user_id=user_id,
            metadata={'c_stat': attempt.c_stat, 'access_key': access_key, 'signed': signed},
            commit=True,
        )

    return {
        'ok': attempt.status_category == 'autorizada',
        'stage': 'concluido_simulacao',
        'access_key': access_key,
        'signed': signed,
        'attempt': attempt,
        'stored_document': stored_doc,
        'missing_fields': [],
    }
