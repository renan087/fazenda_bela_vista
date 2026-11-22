"""Cadastros fiscais da fazenda: produto fiscal, operação fiscal, logotipo e texto padrão.

Não há validação contábil: o próprio usuário ativa a operação e assume a responsabilidade
pelas informações. O SiSFarm confere apenas o formato dos códigos.
"""
import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from io import BytesIO
from types import SimpleNamespace
from uuid import uuid4

from app.models.nfe_draft import (
    NfeDraftEvent,
    NfeDraftExtraData,
    NfeFarmFiscalPrefs,
    NfeFiscalOperation,
    NfeFiscalProduct,
    NfeIssuerLogo,
    NfeTaxProfile,
)
from app.services.harvest_lots import LOT_STAGES

UNITS = (('SC', 'SC · saca'), ('KG', 'KG · quilo'))
ORIGINS = (('0', '0 · Nacional'), ('1', '1 · Estrangeira, importação direta'), ('2', '2 · Estrangeira, mercado interno'))
FINALITIES = (('1', 'Saída · Normal'), ('2', 'Saída · Complementar'), ('4', 'Saída · Devolução'))
FUNRURAL_OPTIONS = (
    ('retido_comprador', 'Retido pelo comprador'),
    ('recolhido_produtor', 'Recolhido pelo produtor'),
    ('nao_aplica', 'Não se aplica'),
)
FUNRURAL_LABELS = dict(FUNRURAL_OPTIONS)
LOT_STAGE_CHOICES = tuple(LOT_STAGES)
STATUS_LABELS = {'ativa': 'Ativa', 'rascunho': 'Rascunho', 'substituida': 'Substituída'}

PRODUCT_TEMPLATES = (
    {'key': 'arabica', 'name': 'Café arábica beneficiado', 'description': 'CAFE ARABICA EM GRAO CRU BENEFICIADO',
     'code': 'CAF-ARB', 'ncm': '09011110', 'commercial_unit': 'SC', 'tax_unit': 'KG', 'lot_stages': ['beneficiado', 'armazem']},
    {'key': 'conilon', 'name': 'Café conilon beneficiado', 'description': 'CAFE CONILON EM GRAO CRU BENEFICIADO',
     'code': 'CAF-CON', 'ncm': '09011110', 'commercial_unit': 'SC', 'tax_unit': 'KG', 'lot_stages': []},
    {'key': 'coco', 'name': 'Café em coco', 'description': 'CAFE EM COCO', 'code': 'CAF-COCO', 'ncm': '',
     'commercial_unit': 'KG', 'tax_unit': 'KG', 'lot_stages': ['secagem', 'tulha']},
)
OPERATION_TEMPLATES = (
    {'key': 'venda', 'name': 'Venda de produção', 'nature': 'Venda de produção do estabelecimento', 'cfop_in': '5101', 'cfop_out': '6101'},
    {'key': 'armazem', 'name': 'Remessa para armazém', 'nature': 'Remessa para depósito fechado ou armazém geral', 'cfop_in': '5905', 'cfop_out': '6905'},
    {'key': 'retorno', 'name': 'Retorno de armazém', 'nature': 'Retorno de mercadoria depositada', 'cfop_in': '5906', 'cfop_out': '6906'},
    {'key': 'exportacao', 'name': 'Venda com fim de exportação', 'nature': 'Venda com fim específico de exportação', 'cfop_in': '5501', 'cfop_out': '6501'},
)

LOGO_MAX_BYTES = 600_000
_UNSAFE_TEXT = re.compile(r'[<>&\x00-\x08\x0b\x0c\x0e-\x1f]')


def _text(data, key: str, limit: int, label: str, *, required: bool = False, upper: bool = False) -> str:
    value = ' '.join(str(data.get(key) or '').split())
    if upper:
        value = value.upper()
    if required and not value:
        raise ValueError(f'Informe {label}.')
    if len(value) > limit:
        raise ValueError(f'{label[:1].upper() + label[1:]} pode ter no máximo {limit} caracteres.')
    if _UNSAFE_TEXT.search(value):
        raise ValueError(f'{label[:1].upper() + label[1:]} tem caracteres não permitidos (< > &).')
    return value


def _digits(data, key: str, size: int, label: str) -> str:
    raw = str(data.get(key) or '').strip()
    if re.search(r'[^0-9.\-\s]', raw):
        raise ValueError(f'{label} aceita só números.')
    value = re.sub(r'\D', '', raw)
    if value and len(value) != size:
        raise ValueError(f'{label} deve ter {size} dígitos.')
    return value


def _getlist(data, key: str) -> list[str]:
    if hasattr(data, 'getlist'):
        return [str(v) for v in data.getlist(key)]
    value = data.get(key)
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value]
    return [str(value)] if value else []


def _now():
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------- produto fiscal

def clean_product_input(data) -> dict:
    name = _text(data, 'name', 120, 'o nome do produto', required=True)
    description = _text(data, 'description', 120, 'a descrição no DANFE', upper=True) or name.upper()
    code = _text(data, 'code', 60, 'o código do produto')
    ncm = _digits(data, 'ncm', 8, 'O NCM')
    origin = str(data.get('origin') or '0').strip()
    if origin not in dict(ORIGINS):
        raise ValueError('Selecione a origem da mercadoria.')
    units = dict(UNITS)
    commercial_unit = str(data.get('commercial_unit') or 'SC').strip().upper()
    tax_unit = str(data.get('tax_unit') or commercial_unit).strip().upper()
    if commercial_unit not in units or tax_unit not in units:
        raise ValueError('Selecione unidades válidas (SC ou KG).')
    raw_kg = str(data.get('kg_per_unit') or '60').strip().replace(',', '.')
    try:
        kg = Decimal(raw_kg)
    except InvalidOperation:
        raise ValueError('Informe quantos kg tem uma saca (ex.: 60).') from None
    if not kg.is_finite() or kg <= 0 or kg > 1000:
        raise ValueError('Informe quantos kg tem uma saca (ex.: 60).')
    valid_stages = {code for code, _ in LOT_STAGE_CHOICES}
    stages = []
    for stage in _getlist(data, 'lot_stages'):
        if stage not in valid_stages:
            raise ValueError('Situação de lote inválida.')
        if stage not in stages:
            stages.append(stage)
    return {
        'name': name, 'description': description, 'code': code, 'ncm': ncm, 'origin': origin,
        'commercial_unit': commercial_unit, 'tax_unit': tax_unit, 'kg_per_unit': format(kg.normalize(), 'f'),
        'lot_stages': stages, 'is_default': str(data.get('is_default') or '') in ('1', 'on', 'true'),
    }


def save_product(db, *, organization_id: int, farm_id: int, user_id: int, data, product_id: str | None = None) -> NfeFiscalProduct:
    values = clean_product_input(data)
    product = None
    if product_id:
        product = db.query(NfeFiscalProduct).filter_by(id=product_id, organization_id=organization_id, farm_id=farm_id).first()
        if not product:
            raise ValueError('Produto fiscal não encontrado.')
    else:
        product = NfeFiscalProduct(id=uuid4().hex, organization_id=organization_id, farm_id=farm_id, created_by=user_id)
    others = db.query(NfeFiscalProduct).filter(
        NfeFiscalProduct.organization_id == organization_id, NfeFiscalProduct.farm_id == farm_id,
        NfeFiscalProduct.id != product.id).all()
    for other in others:
        taken = set(json.loads(other.lot_stages_json or '[]'))
        if taken & set(values['lot_stages']):
            labels = dict(LOT_STAGE_CHOICES)
            dup = ', '.join(labels[s] for s in values['lot_stages'] if s in taken)
            raise ValueError(f'“{dup}” já está ligado ao produto “{other.name}”. Cada situação de lote usa um só produto.')
        if values['is_default'] and other.is_default:
            other.is_default = False
    for key in ('name', 'description', 'code', 'ncm', 'origin', 'commercial_unit', 'tax_unit', 'kg_per_unit', 'is_default'):
        setattr(product, key, values[key])
    product.lot_stages_json = json.dumps(values['lot_stages'])
    if product not in db:
        db.add(product)
    if not others and not product.is_default:
        product.is_default = True
    db.commit()
    return product


def create_product_from_template(db, *, organization_id: int, farm_id: int, user_id: int, key: str) -> NfeFiscalProduct:
    tpl = next((t for t in PRODUCT_TEMPLATES if t['key'] == key), None)
    if not tpl:
        raise ValueError('Modelo de produto não encontrado.')
    used = set()
    for other in db.query(NfeFiscalProduct).filter_by(organization_id=organization_id, farm_id=farm_id).all():
        used |= set(json.loads(other.lot_stages_json or '[]'))
    data = {**tpl, 'lot_stages': [s for s in tpl['lot_stages'] if s not in used], 'origin': '0', 'kg_per_unit': '60'}
    return save_product(db, organization_id=organization_id, farm_id=farm_id, user_id=user_id, data=data)


def product_is_complete(product) -> bool:
    return bool(product and product.description and len(product.ncm or '') == 8)


def product_view(product) -> dict:
    stages = json.loads(product.lot_stages_json or '[]')
    labels = dict(LOT_STAGE_CHOICES)
    unit_line = f'vende em {product.commercial_unit}'
    if product.tax_unit != product.commercial_unit:
        unit_line += f' · tributável em {product.tax_unit}'
    ncm = product.ncm
    return {
        'obj': product, 'complete': product_is_complete(product), 'stages': stages,
        'line': (f'NCM {ncm[:4]}.{ncm[4:6]}.{ncm[6:]} · ' if len(ncm) == 8 else 'Falta o NCM · ') + unit_line,
        'links': ('Usado para lotes: ' + ', '.join(labels.get(s, s) for s in stages)) if stages else 'Nenhuma situação de lote ligada',
    }


def list_products(db, *, organization_id: int, farm_id: int) -> list[NfeFiscalProduct]:
    return (db.query(NfeFiscalProduct).filter_by(organization_id=organization_id, farm_id=farm_id)
            .order_by(NfeFiscalProduct.is_default.desc(), NfeFiscalProduct.name.asc()).all())


def suggest_product(db, *, organization_id: int, farm_id: int, lot_stage: str | None):
    products = list_products(db, organization_id=organization_id, farm_id=farm_id)
    if lot_stage:
        for product in products:
            if lot_stage in json.loads(product.lot_stages_json or '[]'):
                return product
    return next((p for p in products if p.is_default), products[0] if products else None)


# ---------------------------------------------------------------- operação fiscal

def clean_operation_input(data) -> dict:
    name = _text(data, 'name', 120, 'o nome da operação', required=True)
    nature = _text(data, 'nature', 60, 'a natureza da operação')
    finality = str(data.get('finality') or '1').strip()
    if finality not in dict(FINALITIES):
        raise ValueError('Selecione o tipo e a finalidade.')
    cfop_in = _digits(data, 'cfop_in', 4, 'O CFOP para comprador do estado')
    cfop_out = _digits(data, 'cfop_out', 4, 'O CFOP para comprador de outro estado')
    if cfop_in and cfop_in[0] != '5':
        raise ValueError('O CFOP para comprador do mesmo estado começa com 5.')
    if cfop_out and cfop_out[0] != '6':
        raise ValueError('O CFOP para comprador de outro estado começa com 6.')
    icms_cst = _digits(data, 'icms_cst', 2, 'O CST do ICMS')
    funrural = str(data.get('funrural') or 'retido_comprador').strip()
    if funrural not in FUNRURAL_LABELS:
        raise ValueError('Selecione como fica o Funrural.')
    ibs_cbs_cst = _digits(data, 'ibs_cbs_cst', 3, 'O CST do IBS/CBS')
    ibs_cbs_class = _digits(data, 'ibs_cbs_class', 6, 'A classificação do IBS/CBS')
    info = str(data.get('info') or '').strip()
    if len(info) > 1000:
        raise ValueError('As informações ao fisco podem ter no máximo 1000 caracteres.')
    if _UNSAFE_TEXT.search(info):
        raise ValueError('As informações ao fisco têm caracteres não permitidos (< > &).')
    return {'name': name, 'nature': nature, 'finality': finality, 'cfop_in': cfop_in, 'cfop_out': cfop_out,
            'icms_cst': icms_cst, 'funrural': funrural, 'ibs_cbs_cst': ibs_cbs_cst, 'ibs_cbs_class': ibs_cbs_class,
            'info': info}


def operation_missing(op) -> list[str]:
    missing = []
    if not op.nature:
        missing.append('natureza da operação')
    if not op.cfop_in:
        missing.append('CFOP para comprador do estado')
    if not op.icms_cst:
        missing.append('CST do ICMS')
    return missing


def _get_operation(db, organization_id, farm_id, operation_id):
    op = db.query(NfeFiscalOperation).filter_by(id=operation_id, organization_id=organization_id, farm_id=farm_id).first()
    if not op:
        raise ValueError('Operação fiscal não encontrada.')
    return op


def save_operation(db, *, organization_id: int, farm_id: int, user_id: int, data, operation_id: str | None = None) -> NfeFiscalOperation:
    """Salva rascunho. Uma operação ativa nunca é alterada: nasce a próxima versão em rascunho."""
    values = clean_operation_input(data)
    if operation_id:
        current = _get_operation(db, organization_id, farm_id, operation_id)
        if current.status == 'rascunho':
            op = current
        else:
            pending = db.query(NfeFiscalOperation).filter_by(
                root_id=current.root_id, organization_id=organization_id, farm_id=farm_id, status='rascunho').first()
            latest = (db.query(NfeFiscalOperation).filter_by(root_id=current.root_id, organization_id=organization_id, farm_id=farm_id)
                      .order_by(NfeFiscalOperation.version.desc()).first())
            op = pending or NfeFiscalOperation(id=uuid4().hex, root_id=current.root_id, organization_id=organization_id,
                                               farm_id=farm_id, version=latest.version + 1, status='rascunho', created_by=user_id)
            if not pending:
                db.add(op)
    else:
        new_id = uuid4().hex
        op = NfeFiscalOperation(id=new_id, root_id=new_id, organization_id=organization_id, farm_id=farm_id,
                                version=1, status='rascunho', created_by=user_id)
        db.add(op)
    for key, value in values.items():
        setattr(op, key, value)
    db.commit()
    return op


def create_operation_from_template(db, *, organization_id: int, farm_id: int, user_id: int, key: str) -> NfeFiscalOperation:
    tpl = next((t for t in OPERATION_TEMPLATES if t['key'] == key), None)
    if not tpl:
        raise ValueError('Modelo de operação não encontrado.')
    return save_operation(db, organization_id=organization_id, farm_id=farm_id, user_id=user_id, data=tpl)


def activate_operation(db, *, organization_id: int, farm_id: int, user_id: int, operation_id: str, accepted: bool) -> NfeFiscalOperation:
    if not accepted:
        raise ValueError('Marque a confirmação de que conferiu os dados para ativar a operação.')
    op = _get_operation(db, organization_id, farm_id, operation_id)
    if op.status == 'ativa':
        return op
    if op.status != 'rascunho':
        raise ValueError('Só um rascunho pode ser ativado.')
    missing = operation_missing(op)
    if missing:
        raise ValueError('Para ativar, preencha: ' + ', '.join(missing) + '.')
    for old in db.query(NfeFiscalOperation).filter_by(root_id=op.root_id, organization_id=organization_id,
                                                       farm_id=farm_id, status='ativa').all():
        old.status = 'substituida'
    op.status = 'ativa'
    op.activated_by = user_id
    op.activated_at = _now()
    db.commit()
    return op


def list_operations(db, *, organization_id: int, farm_id: int) -> list[dict]:
    """Uma linha por operação: a versão ativa (ou o rascunho, se ainda não houver ativa) e o rascunho pendente."""
    rows = (db.query(NfeFiscalOperation).filter_by(organization_id=organization_id, farm_id=farm_id)
            .order_by(NfeFiscalOperation.created_at.asc(), NfeFiscalOperation.version.asc()).all())
    groups: dict[str, dict] = {}
    for op in rows:
        g = groups.setdefault(op.root_id, {'active': None, 'draft': None})
        if op.status == 'ativa':
            g['active'] = op
        elif op.status == 'rascunho':
            g['draft'] = op
    result = []
    for g in groups.values():
        main = g['active'] or g['draft']
        if main:
            result.append({'main': main, 'active': g['active'], 'draft': g['draft'] if g['active'] else None})
    return result


def active_operations(db, *, organization_id: int, farm_id: int) -> list[NfeFiscalOperation]:
    return (db.query(NfeFiscalOperation).filter_by(organization_id=organization_id, farm_id=farm_id, status='ativa')
            .order_by(NfeFiscalOperation.created_at.asc()).all())


def suggest_operation(db, *, organization_id: int, farm_id: int):
    ops = active_operations(db, organization_id=organization_id, farm_id=farm_id)
    return next((o for o in ops if o.cfop_in.startswith('51')), ops[0] if ops else None)


def choose_cfop(op, *, buyer_state: str, issuer_state: str) -> str:
    if buyer_state and issuer_state and buyer_state != issuer_state and op.cfop_out:
        return op.cfop_out
    return op.cfop_in


# ---------------------------------------------------------------- aplicar no rascunho

def apply_setup_to_draft(db, *, draft, product, operation, buyer, issuer_state: str, user_id: int) -> None:
    """Preenche o rascunho a partir do produto fiscal e da operação ativa (só pré-preenchimento)."""
    from app.services.nfe_draft import apply_tax_profile_to_draft
    from app.services.nfe_snapshots import record_draft_event
    from app.services.nfe_tax_profiles import clean_extra_data

    if operation.status != 'ativa':
        raise ValueError('Escolha uma operação fiscal ativa.')
    buyer_state = (getattr(buyer, 'state', '') or '').upper() if buyer else ''
    cfop = choose_cfop(operation, buyer_state=buyer_state, issuer_state=issuer_state)
    rules = {'operation_nature': operation.nature, 'operation_cfop': cfop, 'icms_cst': operation.icms_cst,
             'ibs_cbs_cst': operation.ibs_cbs_cst, 'ibs_cbs_class': operation.ibs_cbs_class}
    if product:
        rules.update({'product_description': product.description, 'product_ncm': product.ncm, 'origin': product.origin,
                      'unit': product.commercial_unit, 'tax_unit': product.tax_unit})
        if product.commercial_unit == 'SC' and product.tax_unit == 'KG':
            rules['tax_factor'] = product.kg_per_unit
        elif product.commercial_unit == 'KG' and product.tax_unit == 'SC':
            rules['tax_factor'] = str(Decimal('1') / Decimal(product.kg_per_unit))
    holder = SimpleNamespace(id=operation.id, name=operation.name, version=operation.version, rules_json=json.dumps(rules))
    apply_tax_profile_to_draft(db, draft=draft, profile=holder, buyer=buyer, user_id=user_id,
                               summary=f'Operação fiscal “{operation.name}” (v{operation.version}) aplicada.')

    extra_row = db.query(NfeDraftExtraData).filter_by(draft_id=draft.id, organization_id=draft.organization_id,
                                                      farm_id=draft.farm_id).first()
    extra = json.loads(extra_row.data_json) if extra_row else {}
    if not str(extra.get('additional_info') or '').strip():
        prefs = get_prefs(db, organization_id=draft.organization_id, farm_id=draft.farm_id)
        parts = [operation.info.strip(), (prefs.default_info if prefs else '').strip()]
        funrural = {'retido_comprador': 'Contribuição ao Funrural retida pelo adquirente.',
                    'recolhido_produtor': 'Contribuição ao Funrural recolhida pelo produtor.'}.get(operation.funrural, '')
        if funrural and funrural.lower() not in operation.info.lower():
            parts.insert(1, funrural)
        extra['additional_info'] = ' '.join(p for p in parts if p)[:2000]
        cleaned = clean_extra_data(extra)
        if not extra_row:
            extra_row = NfeDraftExtraData(draft_id=draft.id, organization_id=draft.organization_id,
                                          farm_id=draft.farm_id, updated_by=user_id)
            db.add(extra_row)
        extra_row.data_json = json.dumps(cleaned, ensure_ascii=False)
        extra_row.updated_by = user_id
    record_draft_event(
        db, draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id,
        event_type='detalhes_salvos',
        summary=(f'Produto fiscal “{product.name}” aplicado.' if product else 'Rascunho sem produto fiscal ligado.'),
        user_id=user_id,
        metadata={'fiscal_product_id': product.id if product else '', 'fiscal_operation_id': operation.id,
                  'fiscal_operation_version': operation.version, 'cfop': cfop},
        commit=False,
    )
    db.commit()


def draft_sources(db, *, draft) -> dict:
    """Produto e operação que preencheram o rascunho (para os selos “de onde veio”)."""
    events = (db.query(NfeDraftEvent).filter_by(draft_id=draft.id, organization_id=draft.organization_id,
                                                 farm_id=draft.farm_id, event_type='detalhes_salvos')
              .order_by(NfeDraftEvent.created_at.desc()).limit(20).all())
    for ev in events:
        meta = json.loads(ev.metadata_json or '{}')
        if meta.get('fiscal_operation_id'):
            op = db.query(NfeFiscalOperation).filter_by(id=meta['fiscal_operation_id'], organization_id=draft.organization_id,
                                                        farm_id=draft.farm_id).first()
            product = None
            if meta.get('fiscal_product_id'):
                product = db.query(NfeFiscalProduct).filter_by(id=meta['fiscal_product_id'], organization_id=draft.organization_id,
                                                               farm_id=draft.farm_id).first()
            return {'operation': op, 'product': product, 'cfop': meta.get('cfop', '')}
    return {'operation': None, 'product': None, 'cfop': ''}


# ---------------------------------------------------------------- preferências e logotipo

def get_prefs(db, *, organization_id: int, farm_id: int):
    return db.query(NfeFarmFiscalPrefs).filter_by(organization_id=organization_id, farm_id=farm_id).first()


def save_prefs(db, *, organization_id: int, farm_id: int, user_id: int, data) -> NfeFarmFiscalPrefs:
    text = str(data.get('default_info') or '').strip()
    if len(text) > 1000:
        raise ValueError('O texto padrão pode ter no máximo 1000 caracteres.')
    if _UNSAFE_TEXT.search(text):
        raise ValueError('O texto padrão tem caracteres não permitidos (< > &).')
    prefs = get_prefs(db, organization_id=organization_id, farm_id=farm_id)
    if not prefs:
        prefs = NfeFarmFiscalPrefs(id=uuid4().hex, organization_id=organization_id, farm_id=farm_id, updated_by=user_id)
        db.add(prefs)
    prefs.default_info = text
    prefs.updated_by = user_id
    db.commit()
    return prefs


def save_logo(db, *, organization_id: int, farm_id: int, user_id: int, raw: bytes) -> NfeIssuerLogo:
    """Aceita PNG ou JPG do produtor, reprocessa (remove metadados) e guarda em PNG pequeno."""
    if not raw:
        raise ValueError('Escolha um arquivo de imagem.')
    if len(raw) > LOGO_MAX_BYTES:
        raise ValueError('A imagem pode ter no máximo 600 KB.')
    if not (raw.startswith(b'\x89PNG\r\n\x1a\n') or raw.startswith(b'\xff\xd8\xff')):
        raise ValueError('Envie o logotipo em PNG ou JPG.')
    try:
        from PIL import Image
        with Image.open(BytesIO(raw)) as img:
            img.load()
            if img.width * img.height > 25_000_000:
                raise ValueError('Imagem grande demais.')
            img = img.convert('RGBA')
            img.thumbnail((900, 450))
            out = BytesIO()
            img.save(out, format='PNG', optimize=True)
    except ValueError:
        raise
    except Exception:
        raise ValueError('Não foi possível ler a imagem. Envie um PNG ou JPG válido.') from None
    data = out.getvalue()
    logo = db.query(NfeIssuerLogo).filter_by(organization_id=organization_id, farm_id=farm_id).first()
    if not logo:
        logo = NfeIssuerLogo(id=uuid4().hex, organization_id=organization_id, farm_id=farm_id, content_type='image/png',
                             data=data, sha256='', updated_by=user_id)
        db.add(logo)
    logo.content_type = 'image/png'
    logo.data = data
    logo.sha256 = hashlib.sha256(data).hexdigest()
    logo.updated_by = user_id
    db.commit()
    return logo


def get_logo(db, *, organization_id: int, farm_id: int):
    return db.query(NfeIssuerLogo).filter_by(organization_id=organization_id, farm_id=farm_id).first()


def delete_logo(db, *, organization_id: int, farm_id: int) -> None:
    logo = get_logo(db, organization_id=organization_id, farm_id=farm_id)
    if logo:
        db.delete(logo)
        db.commit()


# ---------------------------------------------------------------- perfis antigos

def convert_legacy_profiles(db, *, organization_id: int, farm_id: int, user_id: int) -> int:
    """Converte uma vez os perfis fiscais antigos em produto + operação (sem perder dados)."""
    has_any = (db.query(NfeFiscalProduct.id).filter_by(organization_id=organization_id, farm_id=farm_id).first()
               or db.query(NfeFiscalOperation.id).filter_by(organization_id=organization_id, farm_id=farm_id).first())
    if has_any:
        return 0
    profiles = (db.query(NfeTaxProfile).filter_by(organization_id=organization_id)
                .filter(NfeTaxProfile.status.in_(('rascunho', 'aprovado_teste'))).order_by(NfeTaxProfile.created_at.asc()).all())
    created = 0
    seen_products = {}
    for prof in profiles:
        rules = json.loads(prof.rules_json or '{}')
        key = (rules.get('product_description') or '', rules.get('product_ncm') or '')
        if key[0] and key not in seen_products:
            unit = str(rules.get('unit') or 'SC').upper()
            product = NfeFiscalProduct(
                id=uuid4().hex, organization_id=organization_id, farm_id=farm_id, name=key[0][:120].title(),
                description=key[0][:120].upper(), ncm=re.sub(r'\D', '', key[1])[:8], origin=str(rules.get('origin') or '0')[:1],
                commercial_unit=unit if unit in ('SC', 'KG') else 'SC', tax_unit=unit if unit in ('SC', 'KG') else 'SC',
                kg_per_unit='60', lot_stages_json='[]', is_default=not seen_products, created_by=user_id)
            db.add(product)
            seen_products[key] = product
        cfop = re.sub(r'\D', '', str(rules.get('operation_cfop') or ''))[:4]
        op_id = uuid4().hex
        op = NfeFiscalOperation(
            id=op_id, root_id=op_id, organization_id=organization_id, farm_id=farm_id, version=1, status='rascunho',
            name=prof.name[:120], nature=str(rules.get('operation_nature') or '')[:60],
            cfop_in=cfop if cfop.startswith('5') else '', cfop_out=cfop if cfop.startswith('6') else '',
            icms_cst=re.sub(r'\D', '', str(rules.get('icms_cst') or ''))[:2],
            ibs_cbs_cst=re.sub(r'\D', '', str(rules.get('ibs_cbs_cst') or ''))[:3],
            ibs_cbs_class=re.sub(r'\D', '', str(rules.get('ibs_cbs_class') or ''))[:6],
            info=(prof.reference_notes or '')[:1000] if not _UNSAFE_TEXT.search(prof.reference_notes or '') else '',
            created_by=user_id)
        db.add(op)
        created += 1
    if created:
        db.commit()
    return created
