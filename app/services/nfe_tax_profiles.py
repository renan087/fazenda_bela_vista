"""Perfis fiscais versionados e dados complementares preparatórios (modo teste).

Nenhuma regra tributária é presumida automaticamente. Perfis servem apenas para
registrar e versionar parâmetros revisados por profissional contábil.
"""
from datetime import date
from decimal import Decimal
import json
import re
from uuid import uuid4

from sqlalchemy import func

from app.models.nfe_draft import NfeTaxProfile
from app.services.nfe_access_key import UF_IBGE_CODES

ALLOWED_PERSON_TYPES = frozenset({'pf', 'cnpj'})
ALLOWED_PROFILE_STATUSES = frozenset({
    'rascunho',
    'aguardando_revisao_contabil',
    'aprovado_teste',
    'arquivado',
})
PAYMENT_INDICATORS = (
    ('0', 'Pagamento à vista'),
    ('1', 'Pagamento a prazo'),
)
_VALID_PAYMENT_IND = frozenset(code for code, _ in PAYMENT_INDICATORS)


def _validate_iso_date(raw: str, field_label: str) -> str:
    value = str(raw or '').strip()
    if not value:
        return ''
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise ValueError(f'{field_label} inválida.') from None
    if parsed.isoformat() != value:
        raise ValueError(f'{field_label} inválida.')
    return value


def clean_tax_profile_input(data) -> dict:
    """Valida metadados de um perfil fiscal versionado sem presumir tributação."""
    name = ' '.join(str(data.get('name') or '').split())
    if not name or len(name) > 160:
        raise ValueError('Informe um nome válido para o perfil fiscal (até 160 caracteres).')

    operation_code = str(data.get('operation_code') or 'venda_cafe_interna').strip().lower()
    if not re.fullmatch(r'[a-z0-9_]{3,60}', operation_code):
        raise ValueError('Código da operação deve usar apenas letras minúsculas, números e sublinhado.')

    origin_state = str(data.get('origin_state') or 'BA').strip().upper()
    destination_state = str(data.get('destination_state') or 'BA').strip().upper()
    if origin_state not in UF_IBGE_CODES or destination_state not in UF_IBGE_CODES:
        raise ValueError('UF de origem ou destino inválida.')

    issuer_type = str(data.get('issuer_person_type') or 'pf').strip().lower()
    recipient_type = str(data.get('recipient_person_type') or 'pf').strip().lower()
    if issuer_type not in ALLOWED_PERSON_TYPES or recipient_type not in ALLOWED_PERSON_TYPES:
        raise ValueError('Tipo de pessoa do emitente ou destinatário inválido.')

    valid_from = _validate_iso_date(data.get('valid_from') or '', 'Data inicial de vigência')
    valid_until = _validate_iso_date(data.get('valid_until') or '', 'Data final de vigência')
    if valid_from and valid_until and valid_from > valid_until:
        raise ValueError('A data inicial de vigência não pode ser posterior à data final.')

    approved_by_name = ' '.join(str(data.get('approved_by_name') or '').split())
    if len(approved_by_name) > 160:
        raise ValueError('O nome do responsável pela revisão excede 160 caracteres.')

    reference_notes = str(data.get('reference_notes') or '').strip()
    if len(reference_notes) > 2000:
        raise ValueError('As notas de referência excedem 2000 caracteres.')

    raw_rules = data.get('rules')
    if raw_rules is None:
        rules = {}
    elif isinstance(raw_rules, dict):
        rules = raw_rules
    else:
        raise ValueError('As regras do perfil devem ser informadas como objeto estruturado.')

    return {
        'name': name,
        'operation_code': operation_code,
        'origin_state': origin_state,
        'destination_state': destination_state,
        'issuer_person_type': issuer_type,
        'recipient_person_type': recipient_type,
        'valid_from': valid_from,
        'valid_until': valid_until,
        'approved_by_name': approved_by_name,
        'reference_notes': reference_notes,
        'rules': rules,
    }


def create_tax_profile(db, *, organization_id: int, user_id: int, data: dict) -> NfeTaxProfile:
    """Cria nova versão de perfil fiscal sempre iniciando em 'rascunho'."""
    cleaned = clean_tax_profile_input(data)
    last_version = (
        db.query(func.max(NfeTaxProfile.version))
        .filter_by(
            organization_id=organization_id,
            operation_code=cleaned['operation_code'],
            origin_state=cleaned['origin_state'],
            destination_state=cleaned['destination_state'],
            issuer_person_type=cleaned['issuer_person_type'],
            recipient_person_type=cleaned['recipient_person_type'],
        )
        .scalar()
    ) or 0
    row = NfeTaxProfile(
        id=uuid4().hex,
        organization_id=organization_id,
        name=cleaned['name'],
        operation_code=cleaned['operation_code'],
        origin_state=cleaned['origin_state'],
        destination_state=cleaned['destination_state'],
        issuer_person_type=cleaned['issuer_person_type'],
        recipient_person_type=cleaned['recipient_person_type'],
        version=int(last_version) + 1,
        status='rascunho',
        valid_from=cleaned['valid_from'],
        valid_until=cleaned['valid_until'],
        approved_by_name=cleaned['approved_by_name'],
        reference_notes=cleaned['reference_notes'],
        rules_json=json.dumps(cleaned['rules'], ensure_ascii=False, sort_keys=True),
        created_by=user_id,
    )
    db.add(row)
    db.commit()
    return row


def transition_tax_profile_status(
    db,
    profile: NfeTaxProfile,
    *,
    new_status: str,
    approved_by_name: str | None = None,
    reference_notes: str | None = None,
) -> NfeTaxProfile:
    """Altera o status de governança do perfil, exigindo responsável para aprovação de teste."""
    target = str(new_status or '').strip()
    if target not in ALLOWED_PROFILE_STATUSES:
        raise ValueError('Status de perfil fiscal inválido ou não habilitado nesta etapa.')
    if profile.status == 'arquivado' and target != 'arquivado':
        raise ValueError('Perfis arquivados são imutáveis; crie uma nova versão.')
    if approved_by_name is not None:
        profile.approved_by_name = ' '.join(str(approved_by_name).split())[:160]
    if reference_notes is not None:
        profile.reference_notes = str(reference_notes).strip()[:2000]
    if target == 'aprovado_teste':
        if not profile.approved_by_name or not profile.reference_notes:
            raise ValueError('Para aprovar para teste, informe o responsável pela revisão e o embasamento.')
    profile.status = target
    db.commit()
    return profile


def find_matching_tax_profiles(
    db,
    *,
    organization_id: int,
    origin_state: str,
    destination_state: str,
    issuer_person_type: str = 'pf',
    recipient_person_type: str = 'pf',
    operation_code: str = 'venda_cafe_interna',
    reference_date: str = '',
) -> list[NfeTaxProfile]:
    """Localiza perfis compatíveis com a operação e vigência, sem aplicá-los automaticamente."""
    rows = (
        db.query(NfeTaxProfile)
        .filter_by(
            organization_id=organization_id,
            operation_code=operation_code,
            origin_state=origin_state.upper(),
            destination_state=destination_state.upper(),
            issuer_person_type=issuer_person_type.lower(),
            recipient_person_type=recipient_person_type.lower(),
        )
        .order_by(NfeTaxProfile.version.desc())
        .all()
    )
    if not reference_date:
        return rows
    ref = _validate_iso_date(reference_date, 'Data de referência')
    matched = []
    for row in rows:
        if row.valid_from and ref < row.valid_from:
            continue
        if row.valid_until and ref > row.valid_until:
            continue
        matched.append(row)
    return matched


def _check_xml_safe_text(text: str, field_label: str) -> str:
    if any(
        not (ord(c) in (9, 10, 13) or 0x20 <= ord(c) <= 0xD7FF or 0xE000 <= ord(c) <= 0xFFFD or 0x10000 <= ord(c) <= 0x10FFFF)
        for c in text
    ):
        raise ValueError(f'{field_label} contém caracteres incompatíveis com XML.')
    return text


def clean_extra_data(data) -> dict:
    """Valida campos preparatórios de transporte complementar, pagamento e infCpl."""
    carrier_name = _check_xml_safe_text(' '.join(str(data.get('carrier_name') or '').split()), 'Nome do transportador')
    if len(carrier_name) > 120:
        raise ValueError('Nome do transportador excede 120 caracteres.')

    plate = re.sub(r'[^A-Za-z0-9]', '', str(data.get('vehicle_plate') or '')).upper()
    if plate and not re.fullmatch(r'[A-Z]{3}[0-9][A-Z0-9][0-9]{2}', plate):
        raise ValueError('Placa do veículo inválida. Use formato padrão ou Mercosul (7 caracteres).')

    plate_uf = str(data.get('vehicle_uf') or '').strip().upper()
    if plate_uf and plate_uf not in UF_IBGE_CODES:
        raise ValueError('UF da placa do veículo inválida.')

    volume_species = _check_xml_safe_text(' '.join(str(data.get('volume_species') or '').split()), 'Espécie dos volumes')
    if len(volume_species) > 60:
        raise ValueError('Espécie dos volumes excede 60 caracteres.')

    raw_qty = str(data.get('volume_quantity') or '').strip()
    if raw_qty:
        if not raw_qty.isdigit() or int(raw_qty) <= 0 or len(raw_qty) > 9:
            raise ValueError('Quantidade de volumes deve ser um número inteiro positivo.')
        volume_quantity = str(int(raw_qty))
    else:
        volume_quantity = ''

    weights = {}
    for field, label in (('net_weight_kg', 'Peso líquido'), ('gross_weight_kg', 'Peso bruto')):
        raw = str(data.get(field) or '').strip().replace(',', '.')
        if raw:
            if not re.fullmatch(r'[0-9]{1,12}(?:\.[0-9]{1,3})?', raw):
                raise ValueError(f'{label} deve ser número não negativo com até três casas decimais.')
            weights[field] = format(Decimal(raw), '.3f')
        else:
            weights[field] = ''
    if weights['net_weight_kg'] and weights['gross_weight_kg']:
        if Decimal(weights['net_weight_kg']) > Decimal(weights['gross_weight_kg']):
            raise ValueError('O peso líquido não pode ser maior que o peso bruto.')

    payment_indicator = str(data.get('payment_indicator') or '').strip()
    if payment_indicator and payment_indicator not in _VALID_PAYMENT_IND:
        raise ValueError('Indicador da forma de pagamento inválido.')

    raw_pay = str(data.get('payment_amount') or '').strip().replace(',', '.')
    if raw_pay:
        if not re.fullmatch(r'[0-9]{1,13}(?:\.[0-9]{1,2})?', raw_pay):
            raise ValueError('Valor de pagamento deve ser não negativo com até duas casas decimais.')
        payment_amount = format(Decimal(raw_pay), '.2f')
    else:
        payment_amount = ''

    additional_info = _check_xml_safe_text(str(data.get('additional_info') or '').strip(), 'Informações complementares')
    if len(additional_info) > 2000:
        raise ValueError('Informações complementares excedem o limite de 2000 caracteres.')

    return {
        'carrier_name': carrier_name,
        'vehicle_plate': plate,
        'vehicle_uf': plate_uf,
        'volume_quantity': volume_quantity,
        'volume_species': volume_species,
        'net_weight_kg': weights['net_weight_kg'],
        'gross_weight_kg': weights['gross_weight_kg'],
        'payment_indicator': payment_indicator,
        'payment_amount': payment_amount,
        'additional_info': additional_info,
    }
