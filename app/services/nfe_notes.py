"""Gerenciamento das notas da fazenda: situação de cada rascunho/nota, emissão e cancelamento.

Funciona só no ambiente de teste fictício: a "emissão" usa o ciclo offline já existente
(sem rede, sem SEFAZ) e o cancelamento gera o evento offline e o guarda como simulação.
"""
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from app.models.nfe_draft import (
    NfeClientFiscalConfig,
    NfeDraft,
    NfeDraftDetails,
    NfeDraftEvent,
    NfeDraftExtraData,
    NfeDraftIdentification,
    NfeDraftTechnicalData,
    NfeIssuerProfile,
    NfeStoredDocument,
    NfeTransmissionAttempt,
)

CANCEL_WINDOW = timedelta(hours=24)
STATUS_META = {
    'rascunho': {'label': 'Rascunho', 'tone': 'neutral'},
    'pendente': {'label': 'Pendente', 'tone': 'amber'},
    'enviando': {'label': 'Aguardando SEFAZ', 'tone': 'blue'},
    'autorizada': {'label': 'Autorizada', 'tone': 'green'},
    'rejeitada': {'label': 'Rejeitada', 'tone': 'red'},
    'cancelada': {'label': 'Cancelada', 'tone': 'muted'},
}
STATUS_ORDER = ('rascunho', 'pendente', 'enviando', 'autorizada', 'rejeitada', 'cancelada')


def _aware(dt):
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _money(value) -> str:
    try:
        d = Decimal(str(value or '0'))
    except InvalidOperation:
        d = Decimal('0')
    s = f'{d:,.2f}'
    return 'R$ ' + s.replace(',', 'X').replace('.', ',').replace('X', '.')


def _num_br(value, places: int = 0) -> str:
    try:
        d = Decimal(str(value or '0'))
    except InvalidOperation:
        return str(value or '')
    s = f'{d:,.{places}f}'
    return s.replace(',', 'X').replace('.', ',').replace('X', '.')


def _fmt_doc(digits: str) -> str:
    d = ''.join(ch for ch in str(digits or '') if ch.isdigit())
    if len(d) == 11:
        return f'CPF {d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}'
    if len(d) == 14:
        return f'CNPJ {d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}'
    return ''


def _row(db, model, draft):
    return db.query(model).filter_by(draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id).first()


def _json(row) -> dict:
    return json.loads(row.data_json) if row else {}


def load_note(db, draft: NfeDraft, *, issuer=None, now: datetime | None = None) -> dict:
    """Junta tudo o que a lista e o detalhe precisam e calcula a situação real da nota."""
    from app.services.nfe_draft import review_preparation
    now = now or datetime.now(timezone.utc)
    snapshot = json.loads(draft.snapshot_json)
    details_row = _row(db, NfeDraftDetails, draft)
    details = _json(details_row)
    technical = _json(_row(db, NfeDraftTechnicalData, draft))
    extra = _json(_row(db, NfeDraftExtraData, draft))
    ident = _row(db, NfeDraftIdentification, draft)
    attempt = (db.query(NfeTransmissionAttempt)
               .filter_by(draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id)
               .order_by(NfeTransmissionAttempt.created_at.desc()).first())
    proc = (db.query(NfeStoredDocument)
            .filter_by(draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id,
                       document_type='nfe_proc_simulacao')
            .order_by(NfeStoredDocument.created_at.desc()).first())
    cancel_ev = (db.query(NfeDraftEvent)
                 .filter_by(draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id,
                            event_type='cancelamento_simulado')
                 .order_by(NfeDraftEvent.created_at.desc()).first())
    review = review_preparation(snapshot, issuer, details, details_saved=bool(details_row))
    issues = [issue for group in review['groups'] for issue in group['issues']]

    authorized_at = None
    if cancel_ev:
        status = 'cancelada'
    elif proc:
        status = 'autorizada'
    elif attempt and attempt.status_category in ('rejeitada', 'denegada'):
        status = 'rejeitada'
    elif attempt:
        status = 'enviando'
    elif issues:
        status = 'pendente'
    else:
        status = 'rascunho'
    if proc:
        authorized_at = _aware(proc.created_at)

    numbered = status in ('enviando', 'autorizada', 'rejeitada', 'cancelada') and ident is not None
    sacks = sum((Decimal(str(i.get('quantidade_sacas') or '0')) for i in snapshot.get('itens') or []), Decimal('0'))
    product = (details.get('product_description') or 'Café').strip().capitalize()
    try:
        from app.services.nfe_fiscal_setup import draft_sources
        fiscal_product = draft_sources(db, draft=draft).get('product')
        if fiscal_product is not None:
            product = fiscal_product.name
    except Exception:  # noqa: BLE001 - nome curto é só apresentação
        pass
    if len(product) > 30:
        product = product[:29].rstrip() + '…'
    emission = technical.get('emission_date') or (snapshot.get('data_venda') or '')
    try:
        emission_br = datetime.fromisoformat(emission).strftime('%d/%m/%Y') if emission else ''
        emission_day = datetime.fromisoformat(emission).date().isoformat() if emission else ''
    except ValueError:
        emission_br, emission_day = emission, ''
    deadline = authorized_at + CANCEL_WINDOW if authorized_at else None
    can_cancel = status == 'autorizada' and deadline is not None and now <= deadline
    return {
        'draft': draft, 'id': draft.id, 'status': status, 'meta': STATUS_META[status],
        'number': f'{ident.number:06d}' if numbered else '', 'series': ident.series if numbered else None,
        'title': f'{ident.number:06d}' if numbered else 'Rascunho',
        'subtitle': f'Série {ident.series}' if numbered else 'sem número',
        'emission_br': emission_br, 'emission_day': emission_day,
        'buyer': details.get('recipient_name') or snapshot.get('comprador') or 'Comprador não informado',
        'buyer_doc': _fmt_doc(details.get('recipient_cpf')),
        'product_line': f'{product} · {_num_br(sacks, 0 if sacks == sacks.to_integral() else 2)} sc',
        'sacks': sacks, 'sale_code': draft.sale_code,
        'value': Decimal(str(snapshot.get('valor_bruto') or '0')), 'value_br': _money(snapshot.get('valor_bruto')),
        'issues': issues, 'review': review,
        'attempt': attempt, 'proc': proc, 'cancel_event': cancel_ev,
        'access_key': (proc.access_key if proc else (attempt.access_key if attempt else '')),
        'protocol': (proc.protocol_number if proc else ''),
        'authorized_at': authorized_at, 'cancel_deadline': deadline, 'can_cancel': can_cancel,
        'cancel_late': status == 'autorizada' and not can_cancel,
        'reject_reason': (f'Rejeição {attempt.c_stat} — {attempt.x_motivo}' if status == 'rejeitada' and attempt else ''),
        'cancel_reason': json.loads(cancel_ev.metadata_json or '{}').get('reason', '') if cancel_ev else '',
        'snapshot': snapshot, 'details': details, 'technical': technical, 'extra': extra, 'identification': ident,
        'can_delete': status in ('rascunho', 'pendente') and ident is None and attempt is None,
    }


def list_notes(db, *, organization_id: int, farm_id: int, now: datetime | None = None) -> list[dict]:
    issuer = db.query(NfeIssuerProfile).filter_by(organization_id=organization_id, farm_id=farm_id).first()
    drafts = (db.query(NfeDraft).filter_by(organization_id=organization_id, farm_id=farm_id)
              .order_by(NfeDraft.created_at.desc()).limit(500).all())
    return [load_note(db, d, issuer=issuer, now=now) for d in drafts]


def latest_note_by_sale(notes: list[dict]) -> dict:
    """Nota mais recente de cada venda (para o botão Emitir NF / Continuar NF / Ver nota)."""
    result = {}
    for note in notes:
        if note['sale_code'] not in result:
            result[note['sale_code']] = note
    return result


def summarize(notes: list[dict], *, month_prefix: str) -> dict:
    month = [n for n in notes if (n['emission_day'] or '').startswith(month_prefix)]
    count = {code: sum(1 for n in notes if n['status'] == code) for code in STATUS_ORDER}
    authorized = [n for n in month if n['status'] == 'autorizada']
    cancelled = [n for n in month if n['status'] == 'cancelada']
    return {
        'count': count, 'total': len(notes),
        'authorized_month': len(authorized), 'authorized_value': _money(sum((n['value'] for n in authorized), Decimal('0'))),
        'cancelled_month': len(cancelled), 'cancelled_value': _money(sum((n['value'] for n in cancelled), Decimal('0'))),
    }


def _get_cfg(db, draft):
    return db.query(NfeClientFiscalConfig).filter_by(organization_id=draft.organization_id, farm_id=draft.farm_id).first()


def ensure_test_environment(cfg) -> None:
    if cfg is not None and cfg.environment != 'teste_ficticio':
        raise ValueError('A emissão neste ambiente ainda não está liberada. Use o ambiente de teste.')


def confirm_emission(db, *, draft: NfeDraft, user_id: int) -> dict:
    """Confirma a nota no ambiente de teste: reserva número, congela versão e roda o ciclo offline."""
    from app.services.nfe_cycle_simulator import run_offline_cycle_simulation
    from app.services.nfe_draft import map_sale_payment_and_freight, review_preparation
    from app.services.nfe_numbering import reserve_or_update_test_identification
    from app.services.nfe_snapshots import freeze_draft_snapshot

    cfg = _get_cfg(db, draft)
    ensure_test_environment(cfg)
    issuer = db.query(NfeIssuerProfile).filter_by(organization_id=draft.organization_id, farm_id=draft.farm_id).first()
    note = load_note(db, draft, issuer=issuer)
    if note['status'] not in ('rascunho', 'rejeitada'):
        if note['status'] == 'pendente':
            raise ValueError('Resolva as pendências antes de emitir: ' + '; '.join(note['issues'][:4]) + '.')
        raise ValueError('Esta nota já foi enviada.')
    snapshot = note['snapshot']
    details_row = _row(db, NfeDraftDetails, draft)
    review = review_preparation(snapshot, issuer, note['details'], details_saved=bool(details_row))
    if review['pending_count']:
        raise ValueError('Resolva as pendências antes de emitir.')
    ident = note['identification']
    if ident is None:
        sale = map_sale_payment_and_freight(snapshot)
        series = cfg.default_series if cfg and 920 <= int(cfg.default_series or 0) <= 969 else 920
        ident = reserve_or_update_test_identification(
            db, organization_id=draft.organization_id, farm_id=draft.farm_id, draft_id=draft.id, user_id=user_id,
            data={'series': str(series),
                  'consumer_indicator': '0' if note['details'].get('recipient_ie_indicator') == 'contribuinte' else '1',
                  'presence_indicator': '9', 'payment_method': sale['t_pag'] or '99'})
    freeze_draft_snapshot(db, draft=draft, issuer=issuer, details=note['details'], details_saved=True,
                          technical=note['technical'], identification=ident, user_id=user_id,
                          note='Emissão confirmada (ambiente de teste)', extra_data=note['extra'])
    result = run_offline_cycle_simulation(
        db, draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id, user_id=user_id,
        snapshot=snapshot, issuer=issuer, details=note['details'], technical=note['technical'], identification=ident,
        issuer_crt=(cfg.crt if cfg else ''), extra_data=note['extra'],
        simulated_protocol=f'1292600{ident.number:08d}'[-15:],
    )
    if not result['ok'] and result.get('missing_fields'):
        raise ValueError('Faltam dados para emitir: ' + ', '.join(result['missing_fields'][:6]) + '.')
    if load_note(db, draft, issuer=issuer)['status'] == 'autorizada':
        _sync_sale(db, draft, authorized=True)
    return result


def _sync_sale(db, draft: NfeDraft, *, authorized: bool) -> None:
    from app.services.coffee_sales import sync_status_with_invoice
    if draft.sale_code:
        sync_status_with_invoice(db, farm_id=draft.farm_id, sale_code=draft.sale_code, authorized=authorized)


def cancel_note(db, *, draft: NfeDraft, user_id: int, reason: str, now: datetime | None = None) -> None:
    """Cancelamento simulado dentro de 24 h, com o evento offline guardado."""
    from app.services.nfe_document_store import store_simulated_document
    from app.services.nfe_events import build_cancellation_event
    from app.services.nfe_snapshots import record_draft_event

    now = now or datetime.now(timezone.utc)
    ensure_test_environment(_get_cfg(db, draft))
    issuer = db.query(NfeIssuerProfile).filter_by(organization_id=draft.organization_id, farm_id=draft.farm_id).first()
    note = load_note(db, draft, issuer=issuer, now=now)
    if note['status'] != 'autorizada':
        raise ValueError('Só uma nota autorizada pode ser cancelada.')
    if not note['can_cancel']:
        raise ValueError('O prazo de 24 horas para cancelar terminou. Use carta de correção ou nota de devolução.')
    reason = ' '.join(str(reason or '').split())
    if not 15 <= len(reason) <= 255:
        raise ValueError('Escreva o motivo do cancelamento com 15 a 255 caracteres.')
    built = build_cancellation_event(
        state_uf=issuer.state, author_document=issuer.cpf, author_type='cpf', access_key=note['access_key'],
        protocol_number=note['protocol'], justification=reason,
        event_datetime=now.astimezone(timezone(timedelta(hours=-3))).replace(microsecond=0).isoformat())
    store_simulated_document(db, draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id,
                             document_type='evento_simulacao', access_key=note['access_key'],
                             xml_bytes=built['xml_bytes'], user_id=user_id, protocol_number=note['protocol'])
    record_draft_event(db, draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id,
                       event_type='cancelamento_simulado', summary='Nota cancelada (ambiente de teste).',
                       user_id=user_id, metadata={'reason': reason}, commit=True)
    _sync_sale(db, draft, authorized=False)


def delete_draft(db, *, draft: NfeDraft) -> None:
    """Exclui só rascunho sem número reservado e sem envio."""
    from app.models.nfe_draft import NfeDraftSnapshot
    note = load_note(db, draft)
    if not note['can_delete']:
        raise ValueError('Esta nota já tem número ou envio e não pode ser excluída.')
    for model in (NfeDraftDetails, NfeDraftTechnicalData, NfeDraftExtraData):
        row = _row(db, model, draft)
        if row:
            db.delete(row)
    for model in (NfeDraftEvent, NfeDraftSnapshot):
        db.query(model).filter_by(draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id).delete()
    db.delete(draft)
    db.commit()


# Ações que se repetem em sequência viram um único item na linha do tempo (os registros continuam no banco).
_HISTORY_GROUPS = {'complementos_salvos': 'edit', 'dados_tecnicos_salvos': 'edit', 'emitente_salvo': 'edit',
                   'detalhes_salvos': 'fisc'}
_HISTORY_ICONS = {'rascunho_criado': 'plus', 'snapshot_congelado': 'cce', 'simulacao_ciclo_offline': 'ok',
                  'cancelamento_simulado': 'ban', 'identificacao_teste_salva': 'list', 'detalhes_salvos': 'list'}


def history(db, *, draft: NfeDraft) -> list[dict]:
    """Linha do tempo amigável (mais recente primeiro), com ações repetidas em sequência agrupadas."""
    tone = {'rascunho_criado': 'base', 'detalhes_salvos': 'base', 'snapshot_congelado': 'info',
            'simulacao_ciclo_offline': 'ok', 'cancelamento_simulado': 'bad', 'complementos_salvos': 'base',
            'dados_tecnicos_salvos': 'base', 'identificacao_teste_salva': 'base', 'emitente_salvo': 'base'}
    titles = {'rascunho_criado': 'Rascunho criado', 'detalhes_salvos': 'Dados fiscais aplicados',
              'snapshot_congelado': 'Conferida e confirmada', 'simulacao_ciclo_offline': 'Enviada e autorizada (teste)',
              'cancelamento_simulado': 'Nota cancelada', 'complementos_salvos': 'Transporte e pagamento salvos',
              'dados_tecnicos_salvos': 'Detalhes fiscais do item salvos', 'identificacao_teste_salva': 'Número reservado',
              'emitente_salvo': 'Dados do produtor salvos'}
    events = (db.query(NfeDraftEvent).filter_by(draft_id=draft.id, organization_id=draft.organization_id, farm_id=draft.farm_id)
              .order_by(NfeDraftEvent.created_at.desc()).limit(300).all())
    rank = {'rascunho_criado': 0, 'snapshot_congelado': 3, 'simulacao_ciclo_offline': 4, 'cancelamento_simulado': 5}
    events.sort(key=lambda ev: (_aware(ev.created_at) or datetime.min.replace(tzinfo=timezone.utc), rank.get(ev.event_type, 1)),
                reverse=True)
    local = timezone(timedelta(hours=-3))

    def fmt(when, short=False):
        return when.astimezone(local).strftime('%H:%M' if short else '%d/%m/%Y %H:%M') if when else ''

    out = []
    for ev in events:
        meta = json.loads(ev.metadata_json or '{}')
        text = ev.summary
        if ev.event_type == 'cancelamento_simulado' and meta.get('reason'):
            text = 'Motivo: ' + meta['reason']
        elif ev.event_type == 'snapshot_congelado':
            text = f"Dados travados para o envio (versão {meta.get('version', '')})."
        elif ev.event_type == 'simulacao_ciclo_offline':
            text = 'Autorizada no ambiente de teste, sem envio à SEFAZ.' if meta.get('c_stat') == '100' else f"Retorno {meta.get('c_stat', '')} no ambiente de teste."
        elif ev.event_type == 'complementos_salvos' and text.startswith('Rascunho salvo'):
            text = 'Dados salvos na tela de emissão.'
        when = _aware(ev.created_at)
        group = _HISTORY_GROUPS.get(ev.event_type)
        entry = {'title': titles.get(ev.event_type, 'Atualização'), 'text': text, 'when': fmt(when),
                 'when_short': fmt(when, short=True), 'day': fmt(when)[:10]}
        last = out[-1] if out else None
        if group and last and last.get('group') == group:
            last['items'].append(entry)
            last['count'] += 1
            last['first'] = entry['when_short'] if entry['day'] == last['day'] else entry['when']
            if group == 'fisc' and text not in last['texts']:
                last['texts'].append(text)
            continue
        out.append({**entry, 'tone': tone.get(ev.event_type, 'base'), 'group': group, 'count': 1, 'items': [entry],
                    'icon': _HISTORY_ICONS.get(ev.event_type, 'edit'), 'texts': [text], 'first': ''})
    for item in out:
        if item['count'] > 1:
            if item['group'] == 'edit':
                item['title'] = 'Rascunho editado'
                item['text'] = 'Última alteração: ' + item['items'][0]['title'][:1].lower() + item['items'][0]['title'][1:] + '.'
            else:
                item['text'] = ' '.join(item['texts'][:3])
    return out
