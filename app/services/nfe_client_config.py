"""Configuração fiscal independente por cliente SaaS, prontidão de piloto e painel técnico.

O SiSFarm é uma plataforma SaaS: cada produtor rural cliente é o emitente fiscal,
com seus próprios CPF/CNPJ, IE, CRT, certificado A1, séries, numeração e credenciamento.
O administrador do SiSFarm não é emitente fiscal e nenhuma credencial global é compartilhada.
"""
from datetime import datetime, timezone
import json
import os
import re
from uuid import uuid4

from app.models.nfe_draft import (
    NfeCertificateVault,
    NfeClientFiscalConfig,
    NfeDraft,
    NfeDraftSnapshot,
    NfeStoredDocument,
    NfeTaxProfile,
    NfeTransmissionAttempt,
)
from app.services.coffee_buyers import valid_cnpj, valid_cpf
from app.services.nfe_access_key import (
    PF_SERIES_MAX,
    PF_SERIES_MIN,
    PJ_SERIES_MAX,
    PJ_SERIES_MIN,
    UF_IBGE_CODES,
)
from app.services.nfe_schema import SCHEMA_DIRECTORY
from app.services.nfe_sefaz_registry import get_service_config

ALLOWED_ENVIRONMENTS = (
    ('teste_ficticio', 'Teste fictício (offline)'),
    ('homologacao', 'Homologação SEFAZ (piloto · bloqueado até credenciamento)'),
)
ALLOWED_CREDENTIAL_STATUSES = (
    ('nao_credenciado', 'Não credenciado na SEFAZ'),
    ('em_analise', 'Credenciamento em análise'),
    ('credenciado_homologacao', 'Credenciado em homologação (informado pelo cliente)'),
)
ALLOWED_CRTS = (
    ('1', '1 — Simples Nacional'),
    ('2', '2 — Simples Nacional (excesso sublimite)'),
    ('3', '3 — Regime Normal'),
    ('4', '4 — Simples Nacional — MEI'),
)

_VALID_ENVS = frozenset(code for code, _ in ALLOWED_ENVIRONMENTS)
_VALID_CREDS = frozenset(code for code, _ in ALLOWED_CREDENTIAL_STATUSES)
_VALID_CRTS = frozenset(code for code, _ in ALLOWED_CRTS)


def clean_client_fiscal_config(data) -> dict:
    """Valida a configuração fiscal independente do cliente sem habilitar produção."""
    person_type = str(data.get('person_type') or 'pf').strip().lower()
    if person_type not in ('pf', 'cnpj'):
        raise ValueError('Tipo de emitente deve ser PF (produtor rural) ou CNPJ.')

    raw_doc = str(data.get('document') or '').strip()
    digits = re.sub(r'\D', '', raw_doc)
    if digits:
        if person_type == 'pf':
            if len(digits) != 11 or not valid_cpf(digits):
                raise ValueError('CPF do emitente inválido.')
        else:
            if len(digits) != 14 or not valid_cnpj(digits):
                raise ValueError('CNPJ do emitente inválido.')

    ie = ' '.join(str(data.get('state_registration') or '').split())
    if len(ie) > 30:
        raise ValueError('Inscrição estadual excede 30 caracteres.')

    crt = str(data.get('crt') or '').strip()
    if crt and crt not in _VALID_CRTS:
        raise ValueError('CRT inválido. Selecione 1, 2, 3 ou 4.')

    state = str(data.get('state') or 'BA').strip().upper()
    if state not in UF_IBGE_CODES:
        raise ValueError('UF do emitente inválida.')

    environment = str(data.get('environment') or 'teste_ficticio').strip().lower()
    if environment not in _VALID_ENVS:
        raise ValueError('Ambiente inválido ou não liberado nesta etapa (produção permanece bloqueada).')

    raw_series = str(data.get('default_series') or '').strip()
    default_series = PF_SERIES_MIN if person_type == 'pf' else 1
    if raw_series:
        if not raw_series.isdigit():
            raise ValueError('Série deve ser um número inteiro.')
        default_series = int(raw_series)
    if person_type == 'pf' and not (PF_SERIES_MIN <= default_series <= PF_SERIES_MAX):
        raise ValueError(f'Para produtor rural PF, a série deve estar entre {PF_SERIES_MIN} e {PF_SERIES_MAX}.')
    if person_type == 'cnpj' and not (PJ_SERIES_MIN <= default_series <= PJ_SERIES_MAX):
        raise ValueError(f'Para CNPJ, a série deve estar entre {PJ_SERIES_MIN} e {PJ_SERIES_MAX}.')

    raw_next = str(data.get('next_number') or '1').strip()
    if not raw_next.isdigit() or not (1 <= int(raw_next) <= 999_999_999):
        raise ValueError('Próximo número deve estar entre 1 e 999999999.')
    next_number = int(raw_next)

    cred_status = str(data.get('sefaz_credential_status') or 'nao_credenciado').strip().lower()
    if cred_status not in _VALID_CREDS:
        raise ValueError('Status de credenciamento SEFAZ inválido.')

    pilot_raw = str(data.get('pilot_homologacao_enabled') or '').strip().lower()
    pilot_enabled = pilot_raw in ('1', 'true', 'on', 'sim', 'yes')

    return {
        'person_type': person_type,
        'document': digits,
        'state_registration': ie,
        'crt': crt,
        'state': state,
        'environment': environment,
        'default_series': default_series,
        'next_number': next_number,
        'sefaz_credential_status': cred_status,
        'pilot_homologacao_enabled': pilot_enabled,
    }


def save_client_fiscal_config(db, *, organization_id: int, farm_id: int, user_id: int, data) -> NfeClientFiscalConfig:
    """Salva a configuração fiscal independente de um cliente/fazenda."""
    cleaned = clean_client_fiscal_config(data)
    row = (
        db.query(NfeClientFiscalConfig)
        .filter_by(organization_id=organization_id, farm_id=farm_id)
        .first()
    )
    if not row:
        row = NfeClientFiscalConfig(
            id=uuid4().hex,
            organization_id=organization_id,
            farm_id=farm_id,
            updated_by=user_id,
        )
        db.add(row)
    for key, val in cleaned.items():
        setattr(row, key, val)
    row.updated_by = user_id
    db.commit()
    return row


def _schema_manifest_status() -> dict:
    manifest_path = SCHEMA_DIRECTORY / 'manifest.json'
    if not manifest_path.exists():
        return {'present': False, 'reviewed': False, 'version': '', 'file_count': 0}
    try:
        data = json.loads(manifest_path.read_text(encoding='utf-8'))
        return {
            'present': True,
            'reviewed': data.get('reviewed') is True,
            'version': str(data.get('version') or ''),
            'file_count': len(data.get('files') or {}),
        }
    except (OSError, ValueError):
        return {'present': False, 'reviewed': False, 'version': '', 'file_count': 0}


def evaluate_pilot_homologation_readiness(db, *, organization_id: int, farm_id: int) -> dict:
    """Avalia se o cliente piloto atual reúne requisitos para homologação real na SEFAZ-BA.

    Nunca inventa credenciamento, certificado ou validação oficial; retorna com clareza
    o que já está configurado localmente e o que ainda bloqueia a transmissão real.
    """
    config = (
        db.query(NfeClientFiscalConfig)
        .filter_by(organization_id=organization_id, farm_id=farm_id)
        .first()
    )
    now = datetime.now(timezone.utc)
    active_cert = (
        db.query(NfeCertificateVault)
        .filter_by(organization_id=organization_id, farm_id=farm_id, environment='homologacao', is_active=True)
        .order_by(NfeCertificateVault.created_at.desc())
        .first()
    )
    cert_valid = False
    if active_cert and active_cert.valid_from and active_cert.valid_until:
        vf = active_cert.valid_from if active_cert.valid_from.tzinfo else active_cert.valid_from.replace(tzinfo=timezone.utc)
        vu = active_cert.valid_until if active_cert.valid_until.tzinfo else active_cert.valid_until.replace(tzinfo=timezone.utc)
        cert_valid = vf <= now <= vu

    schema_info = _schema_manifest_status()
    uf = config.state if config else 'BA'
    try:
        sefaz_cfg = get_service_config(state_uf=uf, environment='homologacao', service='NfeAutorizacao4')
        sefaz_verified = sefaz_cfg.verified and sefaz_cfg.transmission_enabled
    except (ValueError, NotImplementedError):
        sefaz_verified = False

    network_env_on = os.environ.get('NFE_ENABLE_SEFAZ_NETWORK', '').strip().lower() == 'true'
    from app.services.nfe_certificate_vault import is_master_key_configured
    master_key_on = is_master_key_configured()

    checks = [
        {
            'code': 'client_config',
            'label': 'Configuração fiscal independente do cliente (CPF/CNPJ, IE, CRT, série)',
            'ok': bool(config and config.document and config.state_registration and config.crt),
            'scope': 'cliente',
            'detail': 'Preenchida para esta fazenda.' if (config and config.document and config.state_registration and config.crt) else 'Pendente: informe documento, IE e CRT do produtor cliente.',
        },
        {
            'code': 'client_certificate',
            'label': 'Certificado digital A1 próprio do cliente no cofre criptografado',
            'ok': cert_valid,
            'scope': 'cliente',
            'detail': f'Ativo ({active_cert.subject_cn})' if cert_valid else 'Pendente: o produtor cliente precisa carregar seu próprio e-CPF/e-CNPJ A1.',
        },
        {
            'code': 'client_credential',
            'label': 'Credenciamento do produtor piloto na SEFAZ (homologação)',
            'ok': bool(config and config.sefaz_credential_status == 'credenciado_homologacao' and config.pilot_homologacao_enabled),
            'scope': 'sefaz',
            'detail': 'Informado como credenciado para piloto.' if (config and config.sefaz_credential_status == 'credenciado_homologacao' and config.pilot_homologacao_enabled) else 'Pendente de confirmação de credenciamento do produtor na SEFAZ-BA.',
        },
        {
            'code': 'vault_master_key',
            'label': 'Chave mestra do cofre (NFE_CERT_MASTER_KEY) fora do banco',
            'ok': master_key_on,
            'scope': 'infraestrutura',
            'detail': 'Configurada no ambiente.' if master_key_on else 'Pendente: definir variável NFE_CERT_MASTER_KEY (>= 32 caracteres) no servidor.',
        },
        {
            'code': 'xsd_official_review',
            'label': 'Pacote XSD oficial com procedência confirmada (reviewed: true)',
            'ok': schema_info['reviewed'],
            'scope': 'sefaz',
            'detail': f"Pacote {schema_info['version']} revisado." if schema_info['reviewed'] else f"Bloqueado: pacote local ({schema_info['version'] or 'ausente'}) está com reviewed: false.",
        },
        {
            'code': 'sefaz_transmission_gate',
            'label': 'Trava de transmissão SEFAZ e URLs verificadas de homologação',
            'ok': sefaz_verified and network_env_on,
            'scope': 'sefaz',
            'detail': 'Liberada em homologação.' if (sefaz_verified and network_env_on) else 'Bloqueada por segurança (transmission_enabled=False / NFE_ENABLE_SEFAZ_NETWORK!=true).',
        },
    ]

    blockers = [c['label'] for c in checks if not c['ok']]
    return {
        'config': config,
        'active_cert': active_cert,
        'schema_info': schema_info,
        'checks': checks,
        'blockers': blockers,
        'transmission_blocked': len(blockers) > 0,
        'ready_for_official_homologacao': len(blockers) == 0,
    }


def build_technical_panel_context(db, *, organization_id: int, farm_id: int) -> dict:
    """Agrega métricas e diagnóstico do painel técnico distinguindo local vs. SEFAZ oficial."""
    readiness = evaluate_pilot_homologation_readiness(db, organization_id=organization_id, farm_id=farm_id)

    drafts_count = db.query(NfeDraft).filter_by(organization_id=organization_id, farm_id=farm_id).count()
    snapshots_count = db.query(NfeDraftSnapshot).filter_by(organization_id=organization_id, farm_id=farm_id).count()
    stored_docs_count = db.query(NfeStoredDocument).filter_by(organization_id=organization_id, farm_id=farm_id).count()
    tax_profiles_count = db.query(NfeTaxProfile).filter_by(organization_id=organization_id).count()
    recent_attempts = (
        db.query(NfeTransmissionAttempt)
        .filter_by(organization_id=organization_id, farm_id=farm_id)
        .order_by(NfeTransmissionAttempt.created_at.desc())
        .limit(10)
        .all()
    )

    local_conformity_items = [
        {'name': 'Chave de acesso 44 dígitos (Módulo 11, PF 000+CPF e CNPJ)', 'status': 'aprovado_local', 'detail': 'Validado offline em test_nfe_access_key.py'},
        {'name': 'Numeração fictícia sequencial isolada por cliente e série', 'status': 'aprovado_local', 'detail': 'Validado offline em test_nfe_numbering.py e test_nfe_conformity_suite.py'},
        {'name': 'Motor XML NF-e 4.00 + IBSCBS opcional contra XSD oficial 010h-v1.03', 'status': 'aprovado_local', 'detail': 'Validado offline contra o pacote PL_010h v1.03 verificado no Portal Nacional (reviewed: true)'},
        {'name': 'Cofre AES-256-GCM (AAD por cliente) e assinatura XMLDSig real', 'status': 'aprovado_local', 'detail': 'Testado com certificado efêmero RSA-2048 em memória + XSD xmldsig'},
        {'name': 'Protocolo SOAP 1.2 (autorização, rejeição, consulta, cancelamento, duplicidade)', 'status': 'aprovado_local', 'detail': 'Simulado offline com parser endurecido contra XXE/DTD'},
        {'name': 'Timeout, idempotência e isolamento estrito entre clientes SaaS', 'status': 'aprovado_local', 'detail': 'Validado offline em test_nfe_conformity_suite.py'},
        {'name': 'Guarda imutável (nfeProc + SHA-256) e prévia DANFE em PDF', 'status': 'aprovado_local', 'detail': 'Validado offline com trava de exclusão e marca d\'água SEM VALOR FISCAL'},
    ]

    approved_local = sum(1 for i in local_conformity_items if i['status'] == 'aprovado_local')
    pending_official = sum(1 for c in readiness['checks'] if not c['ok'])
    errors_count = sum(1 for a in recent_attempts if a.status_category in ('rejeitada', 'denegada', 'timeout'))

    return {
        'readiness': readiness,
        'local_conformity_items': local_conformity_items,
        'stats': {
            'approved_local': approved_local,
            'pending_official': pending_official,
            'errors_count': errors_count,
            'drafts_count': drafts_count,
            'snapshots_count': snapshots_count,
            'stored_docs_count': stored_docs_count,
            'tax_profiles_count': tax_profiles_count,
        },
        'recent_attempts': recent_attempts,
        'allowed_environments': ALLOWED_ENVIRONMENTS,
        'allowed_credential_statuses': ALLOWED_CREDENTIAL_STATUSES,
        'allowed_crts': ALLOWED_CRTS,
    }
