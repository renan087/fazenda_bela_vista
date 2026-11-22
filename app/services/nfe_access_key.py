"""Montagem e validação matemática da chave de acesso da NF-e (44 dígitos).

Opera apenas como cálculo local/simulado nesta etapa; não reserva número na SEFAZ,
não assina XML e não autoriza emissão fiscal.
"""
from datetime import date
import re
import secrets

from app.services.coffee_buyers import valid_cnpj, valid_cpf

# Códigos IBGE das UFs brasileiras (tabela oficial do MOC/IBGE).
UF_IBGE_CODES = {
    'RO': '11', 'AC': '12', 'AM': '13', 'RR': '14', 'PA': '15', 'AP': '16', 'TO': '17',
    'MA': '21', 'PI': '22', 'CE': '23', 'RN': '24', 'PB': '25', 'PE': '26', 'AL': '27',
    'SE': '28', 'BA': '29', 'MG': '31', 'ES': '32', 'RJ': '33', 'SP': '35', 'PR': '41',
    'SC': '42', 'RS': '43', 'MS': '50', 'MT': '51', 'GO': '52', 'DF': '53',
}
VALID_UF_CODES = frozenset(UF_IBGE_CODES.values())

# Códigos numéricos (cNF) rejeitados pela NT 2019.001 (rejeição 897 / lista de padrões triviais).
FORBIDDEN_CNF_VALUES = frozenset({
    '00000000', '11111111', '22222222', '33333333', '44444444',
    '55555555', '66666666', '77777777', '88888888', '99999999',
    '12345678', '23456789', '34567890', '45678901', '56789012',
    '67890123', '78901234', '89012345', '90123456', '01234567',
    '98765432', '87654321', '76543210', '65432109', '54321098',
    '43210987', '32109876', '21098765', '10987654', '09876543',
    '01010101', '10101010', '12121212', '21212121',
})

PF_SERIES_MIN = 920
PF_SERIES_MAX = 969
PJ_SERIES_MIN = 0
PJ_SERIES_MAX = 889


def calculate_check_digit(base_43: str) -> str:
    """Calcula o dígito verificador (cDV) da NF-e pelo Módulo 11 (pesos 2 a 9)."""
    if not isinstance(base_43, str) or not re.fullmatch(r'[0-9]{43}', base_43):
        raise ValueError('A base da chave de acesso deve conter exatamente 43 dígitos numéricos.')
    total = 0
    weight = 2
    for digit in reversed(base_43):
        total += int(digit) * weight
        weight = 2 if weight == 9 else weight + 1
    remainder = total % 11
    return '0' if remainder in (0, 1) else str(11 - remainder)


def validate_numeric_code(code: str, nf_number: int | None = None) -> str:
    """Valida o código numérico aleatório (cNF) conforme regras de formato e NT 2019.001."""
    cleaned = str(code or '').strip()
    if not re.fullmatch(r'[0-9]{8}', cleaned):
        raise ValueError('O código numérico (cNF) deve conter exatamente 8 dígitos.')
    if cleaned in FORBIDDEN_CNF_VALUES:
        raise ValueError('O código numérico (cNF) usa um padrão não permitido pela NF-e.')
    if nf_number is not None and int(cleaned) == int(nf_number):
        raise ValueError('O código numérico (cNF) não pode ser igual ao número da nota.')
    return cleaned


def generate_numeric_code(nf_number: int) -> str:
    """Gera um cNF aleatório de 8 dígitos usando fonte criptográfica."""
    if isinstance(nf_number, bool) or not isinstance(nf_number, int) or not 1 <= nf_number <= 999_999_999:
        raise ValueError('Número da nota inválido para geração de cNF.')
    for _ in range(100):
        candidate = f'{secrets.randbelow(100_000_000):08d}'
        if candidate not in FORBIDDEN_CNF_VALUES and int(candidate) != nf_number:
            return candidate
    raise RuntimeError('Não foi possível gerar um código numérico válido.')


def _normalize_date(emission_date) -> date:
    if isinstance(emission_date, date):
        return emission_date
    raw = str(emission_date or '').strip()
    try:
        parsed = date.fromisoformat(raw)
    except ValueError:
        raise ValueError('Data de emissão inválida para composição da chave.') from None
    if parsed.isoformat() != raw:
        raise ValueError('Data de emissão inválida para composição da chave.')
    return parsed


def _normalize_document(issuer_document: str, document_type: str) -> str:
    doc_type = str(document_type or 'cpf').strip().lower()
    digits = re.sub(r'\D', '', str(issuer_document or ''))
    if doc_type == 'cpf':
        if len(digits) != 11 or not valid_cpf(digits):
            raise ValueError('CPF do emitente inválido para composição da chave.')
        # NT 2018.001: emitente pessoa física preenche os 14 dígitos com 3 zeros à esquerda + CPF.
        return f'000{digits}'
    if doc_type == 'cnpj':
        if len(digits) != 14 or not valid_cnpj(digits):
            raise ValueError('CNPJ do emitente inválido para composição da chave.')
        return digits
    raise ValueError('Tipo de documento do emitente não suportado.')


def build_access_key(
    *,
    state_uf: str,
    emission_date,
    issuer_document: str,
    document_type: str = 'cpf',
    model: str = '55',
    series: int,
    number: int,
    emission_type: str = '1',
    numeric_code: str | None = None,
) -> dict:
    """Monta a chave de acesso de 44 dígitos no padrão NF-e 4.00 / NT 2018.001."""
    uf = str(state_uf or '').strip().upper()
    uf_code = UF_IBGE_CODES.get(uf)
    if not uf_code:
        raise ValueError('UF inválida para composição da chave.')

    parsed_date = _normalize_date(emission_date)
    year_month = f'{parsed_date.year % 100:02d}{parsed_date.month:02d}'

    doc_type = str(document_type or 'cpf').strip().lower()
    padded_doc = _normalize_document(issuer_document, doc_type)

    mod = str(model or '').strip()
    if mod != '55':
        raise ValueError('Somente NF-e modelo 55 é suportada nesta etapa.')

    if isinstance(series, bool) or not isinstance(series, int):
        raise ValueError('Série da NF-e inválida.')
    if doc_type == 'cpf' and not (PF_SERIES_MIN <= series <= PF_SERIES_MAX):
        raise ValueError(f'Para produtor rural pessoa física, a série deve estar entre {PF_SERIES_MIN} e {PF_SERIES_MAX}.')
    if doc_type == 'cnpj' and not (PJ_SERIES_MIN <= series <= PJ_SERIES_MAX):
        raise ValueError(f'Para CNPJ, a série normal deve estar entre {PJ_SERIES_MIN} e {PJ_SERIES_MAX}.')

    if isinstance(number, bool) or not isinstance(number, int) or not 1 <= number <= 999_999_999:
        raise ValueError('Número da NF-e deve estar entre 1 e 999999999.')

    tp_emis = str(emission_type or '').strip()
    if not re.fullmatch(r'[1-9]', tp_emis):
        raise ValueError('Tipo de emissão inválido.')

    cnf = validate_numeric_code(numeric_code, nf_number=number) if numeric_code is not None else generate_numeric_code(number)

    base_43 = f'{uf_code}{year_month}{padded_doc}{mod}{series:03d}{number:09d}{tp_emis}{cnf}'
    check_digit = calculate_check_digit(base_43)
    full_key = f'{base_43}{check_digit}'
    return {
        'key': full_key,
        'id_attribute': f'NFe{full_key}',
        'uf_code': uf_code,
        'year_month': year_month,
        'padded_document': padded_doc,
        'model': mod,
        'series': series,
        'number': number,
        'emission_type': tp_emis,
        'numeric_code': cnf,
        'check_digit': check_digit,
        'is_simulated': True,
    }


def validate_access_key(key: str) -> bool:
    """Valida formato estrutural e dígito verificador Módulo 11 de uma chave de 44 dígitos."""
    if not isinstance(key, str) or not re.fullmatch(r'[0-9]{44}', key):
        return False
    uf_code = key[0:2]
    month = int(key[4:6])
    model = key[20:22]
    number = int(key[25:34])
    emission_type = key[34:35]
    if uf_code not in VALID_UF_CODES or not 1 <= month <= 12:
        return False
    if model not in ('55', '65') or number <= 0 or emission_type == '0':
        return False
    return calculate_check_digit(key[:43]) == key[43]
