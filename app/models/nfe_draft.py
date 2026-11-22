from datetime import datetime
from sqlalchemy import Boolean, DateTime, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base


class NfeDraft(Base):
    """Rascunho isolado; não representa NF-e nem possui validade fiscal."""
    __tablename__ = 'nfe_drafts'
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    sale_code: Mapped[str] = mapped_column(String(32), nullable=False)
    snapshot_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class NfeIssuerProfile(Base):
    """Cadastro preparatório, sem certificado, credenciamento ou autorização fiscal."""
    __tablename__ = 'nfe_issuer_profiles'
    __table_args__ = (UniqueConstraint('organization_id', 'farm_id', name='uq_nfe_issuer_farm'),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(180), nullable=False, default='')
    cpf: Mapped[str] = mapped_column(String(11), nullable=False, default='')
    state_registration: Mapped[str] = mapped_column(String(30), nullable=False, default='')
    postal_code: Mapped[str] = mapped_column(String(8), nullable=False, default='')
    street: Mapped[str] = mapped_column(String(180), nullable=False, default='')
    number: Mapped[str] = mapped_column(String(30), nullable=False, default='')
    complement: Mapped[str] = mapped_column(String(120), nullable=False, default='')
    district: Mapped[str] = mapped_column(String(120), nullable=False, default='')
    city: Mapped[str] = mapped_column(String(120), nullable=False, default='')
    municipality_code: Mapped[str] = mapped_column(String(7), nullable=False, default='')
    state: Mapped[str] = mapped_column(String(2), nullable=False, default='BA')
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class NfeDraftDetails(Base):
    """Dados editáveis apenas para preparação, sem regras fiscais presumidas."""
    __tablename__ = 'nfe_draft_details'
    draft_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    data_json: Mapped[str] = mapped_column(Text, nullable=False, default='{}')
    updated_by: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class NfeDraftTechnicalData(Base):
    """Entradas versionadas para futura montagem, sem cálculo nem XML fiscal."""
    __tablename__ = 'nfe_draft_technical_data'
    draft_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    data_json: Mapped[str] = mapped_column(Text, nullable=False, default='{}')
    updated_by: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class NfeTestSequence(Base):
    """Contador de numeração fictícia isolado por fazenda e série; nunca usado em produção."""
    __tablename__ = 'nfe_test_sequences'
    __table_args__ = (
        UniqueConstraint('organization_id', 'farm_id', 'environment', 'model', 'series', name='uq_nfe_test_sequence_scope'),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    environment: Mapped[str] = mapped_column(String(20), nullable=False, default='teste_ficticio')
    model: Mapped[str] = mapped_column(String(2), nullable=False, default='55')
    series: Mapped[int] = mapped_column(Integer, nullable=False, default=920)
    last_number: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class NfeDraftIdentification(Base):
    """Identificação e numeração fictícia de teste vinculada ao rascunho; sem validade fiscal."""
    __tablename__ = 'nfe_draft_identifications'
    __table_args__ = (
        UniqueConstraint('organization_id', 'farm_id', 'environment', 'model', 'series', 'number', name='uq_nfe_draft_test_number'),
    )
    draft_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    environment: Mapped[str] = mapped_column(String(20), nullable=False, default='teste_ficticio')
    model: Mapped[str] = mapped_column(String(2), nullable=False, default='55')
    series: Mapped[int] = mapped_column(Integer, nullable=False, default=920)
    number: Mapped[int] = mapped_column(Integer, nullable=False)
    numeric_code: Mapped[str] = mapped_column(String(8), nullable=False)
    emission_type: Mapped[str] = mapped_column(String(1), nullable=False, default='1')
    finality: Mapped[str] = mapped_column(String(1), nullable=False, default='1')
    consumer_indicator: Mapped[str] = mapped_column(String(1), nullable=False, default='')
    presence_indicator: Mapped[str] = mapped_column(String(1), nullable=False, default='')
    payment_method: Mapped[str] = mapped_column(String(2), nullable=False, default='')
    updated_by: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class NfeDraftSnapshot(Base):
    """Versão imutável local do rascunho para conferência; não possui validade fiscal."""
    __tablename__ = 'nfe_draft_snapshots'
    __table_args__ = (
        UniqueConstraint('draft_id', 'version', name='uq_nfe_draft_snapshot_version'),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    draft_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default='congelado_teste')
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    note: Mapped[str] = mapped_column(String(240), nullable=False, default='')
    created_by: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class NfeDraftEvent(Base):
    """Histórico local de alterações preparatórias do rascunho; não é evento SEFAZ."""
    __tablename__ = 'nfe_draft_events'
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    draft_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    summary: Mapped[str] = mapped_column(String(240), nullable=False)
    metadata_json: Mapped[str] = mapped_column(Text, nullable=False, default='{}')
    created_by: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class NfeTaxProfile(Base):
    """Perfil fiscal versionado por UF e operação; exige revisão contábil antes do uso real."""
    __tablename__ = 'nfe_tax_profiles'
    __table_args__ = (
        UniqueConstraint(
            'organization_id', 'operation_code', 'origin_state', 'destination_state',
            'issuer_person_type', 'recipient_person_type', 'version',
            name='uq_nfe_tax_profile_version',
        ),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    operation_code: Mapped[str] = mapped_column(String(60), nullable=False, default='venda_cafe_interna')
    origin_state: Mapped[str] = mapped_column(String(2), nullable=False, default='BA')
    destination_state: Mapped[str] = mapped_column(String(2), nullable=False, default='BA')
    issuer_person_type: Mapped[str] = mapped_column(String(10), nullable=False, default='pf')
    recipient_person_type: Mapped[str] = mapped_column(String(10), nullable=False, default='pf')
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default='rascunho')
    valid_from: Mapped[str] = mapped_column(String(10), nullable=False, default='')
    valid_until: Mapped[str] = mapped_column(String(10), nullable=False, default='')
    approved_by_name: Mapped[str] = mapped_column(String(160), nullable=False, default='')
    reference_notes: Mapped[str] = mapped_column(Text, nullable=False, default='')
    rules_json: Mapped[str] = mapped_column(Text, nullable=False, default='{}')
    created_by: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class NfeDraftExtraData(Base):
    """Complementos preparatórios de transporte, pagamento e informações adicionais (teste)."""
    __tablename__ = 'nfe_draft_extra_data'
    draft_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    data_json: Mapped[str] = mapped_column(Text, nullable=False, default='{}')
    updated_by: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class NfeTransmissionAttempt(Base):
    """Registro idempotente de tentativas simuladas/futuras com a SEFAZ; nunca armazena segredos."""
    __tablename__ = 'nfe_transmission_attempts'
    __table_args__ = (
        UniqueConstraint('organization_id', 'farm_id', 'idempotency_key', name='uq_nfe_transmission_idempotency'),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    draft_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    environment: Mapped[str] = mapped_column(String(20), nullable=False, default='teste_ficticio')
    uf: Mapped[str] = mapped_column(String(2), nullable=False, default='BA')
    operation: Mapped[str] = mapped_column(String(40), nullable=False)
    status_category: Mapped[str] = mapped_column(String(30), nullable=False, default='desconhecido')
    c_stat: Mapped[str] = mapped_column(String(10), nullable=False, default='')
    x_motivo: Mapped[str] = mapped_column(String(300), nullable=False, default='')
    protocol_number: Mapped[str] = mapped_column(String(40), nullable=False, default='')
    access_key: Mapped[str] = mapped_column(String(44), nullable=False, default='')
    request_sha256: Mapped[str] = mapped_column(String(64), nullable=False, default='')
    response_sha256: Mapped[str] = mapped_column(String(64), nullable=False, default='')
    created_by: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class NfeStoredDocument(Base):
    """Guarda imutável local de XMLs de simulação/teste com SHA-256; sem validade fiscal."""
    __tablename__ = 'nfe_stored_documents'
    __table_args__ = (
        UniqueConstraint(
            'organization_id', 'farm_id', 'environment', 'document_type', 'access_key', 'xml_sha256',
            name='uq_nfe_stored_doc_dedup',
        ),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    draft_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    environment: Mapped[str] = mapped_column(String(20), nullable=False, default='teste_ficticio')
    document_type: Mapped[str] = mapped_column(String(30), nullable=False)
    access_key: Mapped[str] = mapped_column(String(44), index=True, nullable=False)
    protocol_number: Mapped[str] = mapped_column(String(40), nullable=False, default='')
    xml_content: Mapped[str] = mapped_column(Text, nullable=False)
    xml_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    is_locked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_by: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class NfeCertificateVault(Base):
    """Cofre criptografado (AES-256-GCM) de certificado A1; chave mestra fica fora do banco."""
    __tablename__ = 'nfe_certificate_vaults'
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    environment: Mapped[str] = mapped_column(String(20), nullable=False, default='homologacao')
    subject_cn: Mapped[str] = mapped_column(String(240), nullable=False, default='')
    issuer_cn: Mapped[str] = mapped_column(String(240), nullable=False, default='')
    serial_number: Mapped[str] = mapped_column(String(80), nullable=False, default='')
    fingerprint_sha256: Mapped[str] = mapped_column(String(64), nullable=False, default='')
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    salt_b64: Mapped[str] = mapped_column(String(64), nullable=False)
    nonce_b64: Mapped[str] = mapped_column(String(64), nullable=False)
    ciphertext_b64: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    uploaded_by: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class NfeClientFiscalConfig(Base):
    """Configuração fiscal independente de cada cliente/fazenda no SaaS SiSFarm."""
    __tablename__ = 'nfe_client_fiscal_configs'
    __table_args__ = (
        UniqueConstraint('organization_id', 'farm_id', name='uq_nfe_client_fiscal_config'),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    person_type: Mapped[str] = mapped_column(String(10), nullable=False, default='pf')
    document: Mapped[str] = mapped_column(String(14), nullable=False, default='')
    state_registration: Mapped[str] = mapped_column(String(30), nullable=False, default='')
    crt: Mapped[str] = mapped_column(String(1), nullable=False, default='')
    state: Mapped[str] = mapped_column(String(2), nullable=False, default='BA')
    environment: Mapped[str] = mapped_column(String(20), nullable=False, default='teste_ficticio')
    default_series: Mapped[int] = mapped_column(Integer, nullable=False, default=920)
    next_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    sefaz_credential_status: Mapped[str] = mapped_column(String(30), nullable=False, default='nao_credenciado')
    pilot_homologacao_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    updated_by: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
