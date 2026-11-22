"""Tabela local de códigos IBGE de municípios (7 dígitos) para apoio ao cadastro NF-e.

Permite sugerir/preencher automaticamente o código cMun sem depender de consulta externa,
priorizando a Bahia (regiões cafeeiras e polos) e principais polos cafeeiros nacionais.
"""
from __future__ import annotations

import re
import unicodedata


def _norm(text: str | None) -> str:
    folded = unicodedata.normalize('NFKD', text or '').encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'\s+', ' ', folded).strip().lower()


# Chave: (UF_MAIUSCULA, nome_normalizado) -> código IBGE de 7 dígitos.
_MUNICIPALITIES: dict[tuple[str, str], str] = {
    # Bahia (29) — polos cafeeiros (Planalto, Chapada Diamantina, Oeste, Extremo Sul) e principais cidades
    ('BA', 'salvador'): '2927408',
    ('BA', 'vitoria da conquista'): '2933307',
    ('BA', 'barra do choca'): '2902906',
    ('BA', 'planalto'): '2925006',
    ('BA', 'pocoes'): '2925105',
    ('BA', 'encruzilhada'): '2910404',
    ('BA', 'candido sales'): '2906709',
    ('BA', 'anage'): '2901205',
    ('BA', 'belo campo'): '2903508',
    ('BA', 'piata'): '2924306',
    ('BA', 'mucuge'): '2921906',
    ('BA', 'ibicoara'): '2912202',
    ('BA', 'barra da estiva'): '2902807',
    ('BA', 'seabra'): '2929909',
    ('BA', 'boninal'): '2904001',
    ('BA', 'rio de contas'): '2926707',
    ('BA', 'abaíra'): '2900108',
    ('BA', 'abaira'): '2900108',
    ('BA', 'jussiape'): '2918605',
    ('BA', 'jaguaquara'): '2917607',
    ('BA', 'brejoes'): '2904308',
    ('BA', 'itirucu'): '2916906',
    ('BA', 'maracas'): '2920502',
    ('BA', 'jequie'): '2918001',
    ('BA', 'barreiras'): '2903201',
    ('BA', 'luis eduardo magalhaes'): '2919553',
    ('BA', 'sao desiderio'): '2928901',
    ('BA', 'cocorobo'): '2908200',
    ('BA', 'eunapolis'): '2910727',
    ('BA', 'itabela'): '2914653',
    ('BA', 'guaratinga'): '2911808',
    ('BA', 'teixeira de freitas'): '2931350',
    ('BA', 'itamaraju'): '2915601',
    ('BA', 'porto seguro'): '2925303',
    ('BA', 'ilheus'): '2913606',
    ('BA', 'itabuna'): '2914802',
    ('BA', 'feira de santana'): '2910800',
    ('BA', 'brumado'): '2904605',
    ('BA', 'guanambi'): '2911709',
    ('BA', 'juazeiro'): '2918407',
    ('BA', 'alagoinhas'): '2900702',
    ('BA', 'camacari'): '2905701',
    ('BA', 'lauro de freitas'): '2919207',
    ('BA', 'itapetinga'): '2916401',
    ('BA', 'caetite'): '2905206',
    ('BA', 'livramento de nossa senhora'): '2919504',
    # Minas Gerais (31) — expansão futura
    ('MG', 'belo horizonte'): '3106200',
    ('MG', 'varginha'): '3170701',
    ('MG', 'guaxupe'): '3128709',
    ('MG', 'patrocinio'): '3148103',
    ('MG', 'manhuacu'): '3139409',
    ('MG', 'caratinga'): '3113404',
    ('MG', 'tres pontas'): '3169406',
    ('MG', 'alfenas'): '3101607',
    ('MG', 'uberlandia'): '3170206',
    # Espírito Santo (32) — expansão futura
    ('ES', 'vitoria'): '3205309',
    ('ES', 'linhares'): '3203205',
    ('ES', 'colatina'): '3201506',
    ('ES', 'venda nova do imigrante'): '3205069',
    # São Paulo (35) — expansão futura
    ('SP', 'sao paulo'): '3550308',
    ('SP', 'santos'): '3548500',
    ('SP', 'franca'): '3516200',
    ('SP', 'garca'): '3516705',
    ('SP', 'campinas'): '3509502',
}


def resolve_municipality_code(city: str | None, state: str | None) -> str:
    """Retorna o código IBGE de 7 dígitos se o município estiver na tabela local, ou ''."""
    uf = str(state or '').strip().upper()
    name = _norm(city)
    if not uf or not name:
        return ''
    return _MUNICIPALITIES.get((uf, name), '')
