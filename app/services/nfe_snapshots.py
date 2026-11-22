"""Snapshots imutáveis com hash SHA-256 e histórico local de rascunhos NF-e.

Opera exclusivamente em modo preparatório/fictício: não substitui protocolo,
assinatura digital, autorização da SEFAZ nem arquivamento fiscal legal.
"""
from hashlib import sha256
import hmac
import json
from uuid import uuid4

from sqlalchemy import func

from app.models.nfe_draft import NfeDraftEvent, NfeDraftSnapshot
from app.services.nfe_draft import review_preparation
from app.services.nfe_numbering import build_simulated_access_key_summary
from app.services.nfe_preview import ISSUER_FIELDS

SNAPSHOT_STATUS_TEST = 'congelado_teste'
ALLOWED_EVENT_TYPES = frozenset({
    'rascunho_criado',
    'emitente_salvo',
    'detalhes_salvos',
    'dados_tecnicos_salvos',
    'identificacao_teste_salva',
    'complementos_salvos',
    'snapshot_congelado',
    'simulacao_ciclo_offline',
    'cancelamento_simulado',
})


def record_draft_event(
    db,
    *,
    draft_id: str,
    organization_id: int,
    farm_id: int,
    event_type: str,
    summary: str,
    user_id: int,
    metadata: dict | None = None,
    commit: bool = False,
) -> NfeDraftEvent:
    """Registra um marco local na linha do tempo do rascunho."""
    if event_type not in ALLOWED_EVENT_TYPES:
        raise ValueError('Tipo de evento local de rascunho não permitido.')
    clean_summary = ' '.join(str(summary or '').split())[:240]
    if not clean_summary:
        raise ValueError('Resumo do evento é obrigatório.')
    row = NfeDraftEvent(
        id=uuid4().hex,
        draft_id=draft_id,
        organization_id=organization_id,
        farm_id=farm_id,
        event_type=event_type,
        summary=clean_summary,
        metadata_json=json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
        created_by=user_id,
    )
    db.add(row)
    if commit:
        db.commit()
    return row


def build_snapshot_payload(
    *,
    sale_snapshot: dict,
    issuer,
    details: dict,
    details_saved: bool,
    technical: dict,
    identification,
    extra_data: dict | None = None,
) -> tuple[str, str]:
    """Serializa o estado salvo do rascunho de forma canônica e calcula seu SHA-256."""
    if not isinstance(sale_snapshot, dict):
        raise ValueError('Snapshot comercial do rascunho indisponível.')
    saved_details = dict(details) if (details_saved and isinstance(details, dict)) else {}
    saved_technical = dict(technical) if isinstance(technical, dict) else {}
    saved_extra = dict(extra_data) if isinstance(extra_data, dict) else {}

    issuer_copy = {
        field: str(getattr(issuer, field, '') or '')
        for field in ISSUER_FIELDS
    } if issuer else {}

    key_summary = build_simulated_access_key_summary(issuer, saved_technical, identification)
    ident_copy = {
        'ambiente': identification.environment,
        'modelo': identification.model,
        'serie': identification.series,
        'numero': identification.number,
        'codigo_numerico': identification.numeric_code,
        'tipo_emissao': identification.emission_type,
        'finalidade': identification.finality,
        'ind_final': identification.consumer_indicator,
        'ind_pres': identification.presence_indicator,
        'forma_pagamento': identification.payment_method,
        'chave_simulada': key_summary.get('key', ''),
    } if identification else None

    review = review_preparation(sale_snapshot, issuer, saved_details, details_saved=bool(details_saved))

    payload = {
        'versao_formato': 1,
        'natureza': 'snapshot_local_teste_sem_validade_fiscal',
        'aviso': 'Snapshot local de teste sem validade fiscal. Não representa NF-e emitida, assinada ou transmitida.',
        'venda': sale_snapshot,
        'produtor': issuer_copy,
        'destinatario_e_operacao': saved_details,
        'dados_tecnicos': saved_technical,
        'identificacao_teste': ident_copy,
        'complementos_teste': saved_extra,
        'pendencias_preparo': review,
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    digest = sha256(canonical.encode('utf-8')).hexdigest()
    return canonical, digest


def verify_snapshot_integrity(snapshot_row: NfeDraftSnapshot) -> bool:
    """Confere se o conteúdo salvo corresponde exatamente ao SHA-256 gravado."""
    if not snapshot_row or not isinstance(snapshot_row.payload_json, str) or not isinstance(snapshot_row.payload_sha256, str):
        return False
    try:
        json.loads(snapshot_row.payload_json)
    except ValueError:
        return False
    computed = sha256(snapshot_row.payload_json.encode('utf-8')).hexdigest()
    return hmac.compare_digest(computed, snapshot_row.payload_sha256.lower())


def freeze_draft_snapshot(
    db,
    *,
    draft,
    issuer,
    details: dict,
    details_saved: bool,
    technical: dict,
    identification,
    user_id: int,
    note: str = '',
    extra_data: dict | None = None,
) -> NfeDraftSnapshot:
    """Congela uma nova versão imutável de teste do rascunho e registra no histórico."""
    clean_note = ' '.join(str(note or '').split())
    if len(clean_note) > 240:
        raise ValueError('A observação da versão deve ter no máximo 240 caracteres.')

    sale_snapshot = json.loads(draft.snapshot_json)
    canonical, digest = build_snapshot_payload(
        sale_snapshot=sale_snapshot,
        issuer=issuer,
        details=details,
        details_saved=details_saved,
        technical=technical,
        identification=identification,
        extra_data=extra_data,
    )

    last_version = (
        db.query(func.max(NfeDraftSnapshot.version))
        .filter_by(draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id)
        .scalar()
    ) or 0
    next_version = int(last_version) + 1

    row = NfeDraftSnapshot(
        id=uuid4().hex,
        draft_id=draft.id,
        organization_id=draft.organization_id,
        farm_id=draft.farm_id,
        version=next_version,
        status=SNAPSHOT_STATUS_TEST,
        payload_json=canonical,
        payload_sha256=digest,
        note=clean_note,
        created_by=user_id,
    )
    db.add(row)
    record_draft_event(
        db,
        draft_id=draft.id,
        organization_id=draft.organization_id,
        farm_id=draft.farm_id,
        event_type='snapshot_congelado',
        summary=f'Versão de teste v{next_version} congelada (SHA-256 {digest[:12]}…).',
        user_id=user_id,
        metadata={'version': next_version, 'sha256': digest},
        commit=False,
    )
    db.commit()
    return row


def list_draft_snapshots(db, *, draft_id: str, organization_id: int, farm_id: int) -> list[dict]:
    """Lista as versões congeladas do rascunho da mais recente para a mais antiga."""
    rows = (
        db.query(NfeDraftSnapshot)
        .filter_by(draft_id=draft_id, organization_id=organization_id, farm_id=farm_id)
        .order_by(NfeDraftSnapshot.version.desc())
        .all()
    )
    result = []
    for row in rows:
        integrity_ok = verify_snapshot_integrity(row)
        pending_count = None
        simulated_key = ''
        if integrity_ok:
            data = json.loads(row.payload_json)
            pending_count = (data.get('pendencias_preparo') or {}).get('pending_count')
            simulated_key = (data.get('identificacao_teste') or {}).get('chave_simulada') or ''
        result.append({
            'id': row.id,
            'version': row.version,
            'status': row.status,
            'note': row.note,
            'sha256': row.payload_sha256,
            'short_hash': row.payload_sha256[:12],
            'integrity_ok': integrity_ok,
            'pending_count': pending_count,
            'simulated_key': simulated_key,
            'created_at': row.created_at,
        })
    return result


def list_draft_events(db, *, draft_id: str, organization_id: int, farm_id: int, limit: int = 30) -> list[NfeDraftEvent]:
    """Lista os eventos locais do rascunho do mais recente para o mais antigo."""
    return (
        db.query(NfeDraftEvent)
        .filter_by(draft_id=draft_id, organization_id=organization_id, farm_id=farm_id)
        .order_by(NfeDraftEvent.created_at.desc(), NfeDraftEvent.id.desc())
        .limit(limit)
        .all()
    )


def export_snapshot_json_bytes(snapshot_row: NfeDraftSnapshot) -> bytes:
    """Exporta o snapshot congelado em JSON após verificar sua integridade SHA-256."""
    if not verify_snapshot_integrity(snapshot_row):
        raise ValueError('A integridade desta versão congelada não pôde ser confirmada.')
    envelope = {
        'versao': snapshot_row.version,
        'status': snapshot_row.status,
        'observacao': snapshot_row.note,
        'sha256_conteudo_canonico': snapshot_row.payload_sha256,
        'conteudo': json.loads(snapshot_row.payload_json),
    }
    return json.dumps(envelope, ensure_ascii=False, indent=2).encode('utf-8')
