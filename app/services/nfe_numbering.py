"""Controle local de numeração fictícia e identificação de teste para rascunhos NF-e.

Isolado em ambiente 'teste_ficticio': nunca reserva número real na SEFAZ, não
substitui coordenação com outros emissores e não autoriza emissão fiscal.
"""
from uuid import uuid4

from app.models.nfe_draft import NfeDraftIdentification, NfeTestSequence
from app.services.nfe_access_key import (
    PF_SERIES_MAX,
    PF_SERIES_MIN,
    build_access_key,
    generate_numeric_code,
)

TEST_ENVIRONMENT = 'teste_ficticio'
DEFAULT_MODEL = '55'
DEFAULT_PF_SERIES = 920

CONSUMER_INDICATORS = (
    ('0', 'Normal'),
    ('1', 'Consumidor final'),
)
PRESENCE_INDICATORS = (
    ('0', 'Não se aplica'),
    ('1', 'Operação presencial'),
    ('2', 'Operação não presencial, pela internet'),
    ('3', 'Operação não presencial, teleatendimento'),
    ('5', 'Operação presencial, fora do estabelecimento'),
    ('9', 'Operação não presencial, outros'),
)
PAYMENT_METHODS = (
    ('01', 'Dinheiro'),
    ('03', 'Cartão de crédito'),
    ('04', 'Cartão de débito'),
    ('15', 'Boleto bancário'),
    ('17', 'PIX'),
    ('18', 'Transferência bancária / carteira digital'),
    ('90', 'Sem pagamento'),
    ('99', 'Outros'),
)

_VALID_CONSUMER = frozenset(code for code, _ in CONSUMER_INDICATORS)
_VALID_PRESENCE = frozenset(code for code, _ in PRESENCE_INDICATORS)
_VALID_PAYMENT = frozenset(code for code, _ in PAYMENT_METHODS)


def clean_identification_input(data) -> dict:
    """Valida formato dos campos de identificação de teste sem presumir regra fiscal."""
    raw_series = str(data.get('series') or '').strip()
    if raw_series == '':
        series = DEFAULT_PF_SERIES
    else:
        if not raw_series.isdigit():
            raise ValueError('Informe uma série numérica válida entre 920 e 969.')
        series = int(raw_series)
        if not (PF_SERIES_MIN <= series <= PF_SERIES_MAX):
            raise ValueError(
                f'Para produtor rural pessoa física, a série de teste deve estar entre {PF_SERIES_MIN} e {PF_SERIES_MAX}.'
            )

    consumer = str(data.get('consumer_indicator') or '').strip()
    if consumer and consumer not in _VALID_CONSUMER:
        raise ValueError('Selecione um indicador de consumidor final válido.')

    presence = str(data.get('presence_indicator') or '').strip()
    if presence and presence not in _VALID_PRESENCE:
        raise ValueError('Selecione um indicador de presença válido.')

    payment = str(data.get('payment_method') or '').strip()
    if payment and payment not in _VALID_PAYMENT:
        raise ValueError('Selecione uma forma de pagamento válida.')

    return {
        'series': series,
        'consumer_indicator': consumer,
        'presence_indicator': presence,
        'payment_method': payment,
    }


def _next_sequence_number(db, *, organization_id: int, farm_id: int, series: int) -> int:
    query = db.query(NfeTestSequence).filter_by(
        organization_id=organization_id,
        farm_id=farm_id,
        environment=TEST_ENVIRONMENT,
        model=DEFAULT_MODEL,
        series=series,
    )
    bind = db.get_bind()
    if bind is not None and bind.dialect.name != 'sqlite':
        query = query.with_for_update()
    seq = query.first()
    if not seq:
        seq = NfeTestSequence(
            id=uuid4().hex,
            organization_id=organization_id,
            farm_id=farm_id,
            environment=TEST_ENVIRONMENT,
            model=DEFAULT_MODEL,
            series=series,
            last_number=0,
        )
        db.add(seq)
        db.flush()
    seq.last_number += 1
    return seq.last_number


def reserve_or_update_test_identification(
    db,
    *,
    organization_id: int,
    farm_id: int,
    draft_id: str,
    user_id: int,
    data,
) -> NfeDraftIdentification:
    """Reserva numeração fictícia isolada (idempotente na mesma série) e salva identificação."""
    cleaned = clean_identification_input(data)
    existing = (
        db.query(NfeDraftIdentification)
        .filter_by(draft_id=draft_id, organization_id=organization_id, farm_id=farm_id)
        .first()
    )
    if existing and existing.series == cleaned['series']:
        existing.consumer_indicator = cleaned['consumer_indicator']
        existing.presence_indicator = cleaned['presence_indicator']
        existing.payment_method = cleaned['payment_method']
        existing.updated_by = user_id
        db.commit()
        return existing

    next_number = _next_sequence_number(
        db,
        organization_id=organization_id,
        farm_id=farm_id,
        series=cleaned['series'],
    )
    numeric_code = generate_numeric_code(next_number)
    if existing:
        existing.series = cleaned['series']
        existing.number = next_number
        existing.numeric_code = numeric_code
        existing.consumer_indicator = cleaned['consumer_indicator']
        existing.presence_indicator = cleaned['presence_indicator']
        existing.payment_method = cleaned['payment_method']
        existing.updated_by = user_id
        row = existing
    else:
        row = NfeDraftIdentification(
            draft_id=draft_id,
            organization_id=organization_id,
            farm_id=farm_id,
            environment=TEST_ENVIRONMENT,
            model=DEFAULT_MODEL,
            series=cleaned['series'],
            number=next_number,
            numeric_code=numeric_code,
            emission_type='1',
            finality='1',
            consumer_indicator=cleaned['consumer_indicator'],
            presence_indicator=cleaned['presence_indicator'],
            payment_method=cleaned['payment_method'],
            updated_by=user_id,
        )
        db.add(row)
    db.commit()
    return row


def build_simulated_access_key_summary(issuer, technical: dict, identification) -> dict:
    """Monta a chave de acesso simulada quando os campos necessários já estão salvos."""
    if not identification:
        return {
            'reserved': False,
            'key': '',
            'id_attribute': '',
            'check_digit': '',
            'missing': ['Reserve a numeração fictícia de teste'],
        }

    missing = []
    state = str(getattr(issuer, 'state', '') or '').strip().upper() if issuer else ''
    cpf = str(getattr(issuer, 'cpf', '') or '').strip() if issuer else ''
    emission_date = str((technical or {}).get('emission_date') or '').strip()

    if not state:
        missing.append('UF do produtor (em Produtor e estabelecimento)')
    if not cpf:
        missing.append('CPF válido do produtor (em Produtor e estabelecimento)')
    if not emission_date:
        missing.append('Data de emissão pretendida (em Dados fiscais e transporte)')

    if missing:
        return {
            'reserved': True,
            'key': '',
            'id_attribute': '',
            'check_digit': '',
            'missing': missing,
        }

    try:
        built = build_access_key(
            state_uf=state,
            emission_date=emission_date,
            issuer_document=cpf,
            document_type='cpf',
            model=identification.model,
            series=identification.series,
            number=identification.number,
            emission_type=identification.emission_type,
            numeric_code=identification.numeric_code,
        )
    except ValueError as exc:
        return {
            'reserved': True,
            'key': '',
            'id_attribute': '',
            'check_digit': '',
            'missing': [str(exc)],
        }

    return {
        'reserved': True,
        'key': built['key'],
        'id_attribute': built['id_attribute'],
        'check_digit': built['check_digit'],
        'uf_code': built['uf_code'],
        'year_month': built['year_month'],
        'missing': [],
    }
