from datetime import date
import pytest

from app.services.nfe_access_key import (
    FORBIDDEN_CNF_VALUES,
    UF_IBGE_CODES,
    build_access_key,
    calculate_check_digit,
    generate_numeric_code,
    validate_access_key,
    validate_numeric_code,
)


def test_check_digit_modulo_11_and_edge_cases():
    # Base de 43 dígitos com cálculo manual verificado (pesos 2..9 da direita para esquerda).
    base = '2926110005299822472555920000000001124681357'
    dv = calculate_check_digit(base)
    assert len(dv) == 1 and dv.isdigit()
    assert validate_access_key(base + dv) is True
    wrong_dv = str((int(dv) + 1) % 10)
    assert validate_access_key(base + wrong_dv) is False

    # Resto 0 e resto 1 devem resultar sempre em DV '0'.
    assert calculate_check_digit('0' * 43) == '0'
    # Soma = 1 * 2 = 2 -> resto 2 -> 11 - 2 = 9.
    assert calculate_check_digit('0' * 42 + '1') == '9'
    # Soma = 5*2 + 1*1? Para resto 1: 42 zeros + '6' -> 6*2 = 12 % 11 = 1 -> DV '0'.
    assert calculate_check_digit('0' * 42 + '6') == '0'

    with pytest.raises(ValueError):
        calculate_check_digit('123')
    with pytest.raises(ValueError):
        calculate_check_digit('A' * 43)


def test_build_access_key_for_bahia_rural_producer_cpf():
    result = build_access_key(
        state_uf='BA',
        emission_date='2026-11-21',
        issuer_document='529.982.247-25',
        document_type='cpf',
        model='55',
        series=920,
        number=1,
        emission_type='1',
        numeric_code='24681357',
    )
    assert result['uf_code'] == '29'
    assert result['year_month'] == '2611'
    assert result['padded_document'] == '00052998224725'
    assert len(result['key']) == 44
    assert result['id_attribute'] == 'NFe' + result['key']
    assert result['is_simulated'] is True
    assert result['key'].startswith('2926110005299822472555920000000001124681357')
    assert validate_access_key(result['key']) is True


def test_build_access_key_supports_future_cnpj_and_other_states():
    assert len(UF_IBGE_CODES) == 27
    result = build_access_key(
        state_uf='MG',
        emission_date=date(2026, 12, 5),
        issuer_document='11.222.333/0001-81',
        document_type='cnpj',
        model='55',
        series=1,
        number=42,
        numeric_code='86429753',
    )
    assert result['uf_code'] == '31'
    assert result['year_month'] == '2612'
    assert result['padded_document'] == '11222333000181'
    assert validate_access_key(result['key']) is True


def test_access_key_rejects_invalid_series_documents_and_cnf():
    # Produtor rural PF deve usar faixa 920..969 (NT 2018.001).
    for invalid_series in (0, 1, 919, 970, 999):
        with pytest.raises(ValueError):
            build_access_key(
                state_uf='BA',
                emission_date='2026-11-21',
                issuer_document='52998224725',
                document_type='cpf',
                series=invalid_series,
                number=1,
                numeric_code='24681357',
            )

    # Códigos numéricos proibidos pela NT 2019.001 ou iguais ao número da nota.
    for forbidden in ('00000000', '11111111', '12345678', '98765432'):
        assert forbidden in FORBIDDEN_CNF_VALUES
        with pytest.raises(ValueError):
            validate_numeric_code(forbidden, nf_number=1)

    with pytest.raises(ValueError):
        validate_numeric_code('00000042', nf_number=42)

    # Geração aleatória nunca retorna código proibido nem igual ao número da nota.
    for nf_number in (1, 42, 12345678):
        code = generate_numeric_code(nf_number)
        assert len(code) == 8 and code.isdigit()
        assert code not in FORBIDDEN_CNF_VALUES
        assert int(code) != nf_number

    # UF, CPF, data e modelo inválidos.
    with pytest.raises(ValueError):
        build_access_key(state_uf='XX', emission_date='2026-11-21', issuer_document='52998224725', series=920, number=1)
    with pytest.raises(ValueError):
        build_access_key(state_uf='BA', emission_date='2026-11-21', issuer_document='11111111111', series=920, number=1)
    with pytest.raises(ValueError):
        build_access_key(state_uf='BA', emission_date='2026-02-30', issuer_document='52998224725', series=920, number=1)
    with pytest.raises(ValueError):
        build_access_key(state_uf='BA', emission_date='2026-11-21', issuer_document='52998224725', model='65', series=920, number=1)
