"""DANFE no padrão retrato (A4), gerado a partir dos dados salvos da nota.

Enquanto o ambiente for de teste, todas as páginas levam a marca d'água
"SEM VALOR FISCAL · AMBIENTE DE TESTE". O logotipo é sempre o do produtor
(enviado em Configurações fiscais); o SiSFarm nunca coloca a própria marca.
"""
from datetime import date
from decimal import Decimal, InvalidOperation
from io import BytesIO

from reportlab.graphics.barcode import code128
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas

from app.services.nfe_numbering import build_simulated_access_key_summary

FREIGHT_LABELS = {
    '0': '0 - Contratação do Frete por conta do Remetente (CIF)',
    '1': '1 - Contratação do Frete por conta do Destinatário (FOB)',
    '2': '2 - Contratação do Frete por conta de Terceiros',
    '3': '3 - Transporte Próprio por conta do Remetente',
    '4': '4 - Transporte Próprio por conta do Destinatário',
    '9': '9 - Sem Ocorrência de Transporte',
}
LABEL_FONT, LABEL_SIZE = 'Helvetica', 5.6
VALUE_FONT = 'Helvetica'
BLACK = colors.black


def _fmt_cpf(digits: str) -> str:
    clean = ''.join(c for c in str(digits or '') if c.isdigit())
    if len(clean) == 11:
        return f'{clean[:3]}.{clean[3:6]}.{clean[6:9]}-{clean[9:]}'
    if len(clean) == 14:
        return f'{clean[:2]}.{clean[2:5]}.{clean[5:8]}/{clean[8:12]}-{clean[12:]}'
    return clean


def _fmt_access_key(key: str) -> str:
    clean = ''.join(c for c in str(key or '') if c.isdigit())
    if len(clean) != 44:
        return ''
    return ' '.join(clean[i:i + 4] for i in range(0, 44, 4))


def _fmt_cep(value: str) -> str:
    d = ''.join(c for c in str(value or '') if c.isdigit())
    return f'{d[:2]}.{d[2:5]}-{d[5:]}' if len(d) == 8 else d


def _br(value, places: int = 2) -> str:
    try:
        d = Decimal(str(value if value not in (None, '') else '0'))
    except InvalidOperation:
        return str(value)
    s = f'{d:,.{places}f}'
    return s.replace(',', 'X').replace('.', ',').replace('X', '.')


def _date_br(iso: str) -> str:
    try:
        return date.fromisoformat(str(iso)[:10]).strftime('%d/%m/%Y')
    except ValueError:
        return str(iso or '')


def _fit(text: str, font: str, size: float, width: float) -> str:
    text = str(text or '')
    if stringWidth(text, font, size) <= width:
        return text
    while text and stringWidth(text + '…', font, size) > width:
        text = text[:-1]
    return text + '…'


def _wrap(text: str, font: str, size: float, width: float, max_lines: int) -> list[str]:
    lines = []
    for paragraph in str(text or '').splitlines() or ['']:
        words, line = paragraph.split(), ''
        for word in words:
            probe = (line + ' ' + word).strip()
            if stringWidth(probe, font, size) <= width:
                line = probe
            else:
                if line:
                    lines.append(line)
                line = word
        lines.append(line)
    if len(lines) > max_lines:
        lines = lines[:max_lines]
        lines[-1] = _fit(lines[-1] + ' …', font, size, width)
    return lines


def _box(c, x, top, w, h, label='', value='', *, bold=False, size=8.2, align='left'):
    c.setStrokeColor(BLACK)
    c.setLineWidth(0.6)
    c.rect(x, top - h, w, h, stroke=1, fill=0)
    if label:
        c.setFillColor(BLACK)
        c.setFont(LABEL_FONT, LABEL_SIZE)
        c.drawString(x + 1 * mm, top - 2.4 * mm, _fit(label, LABEL_FONT, LABEL_SIZE, w - 2 * mm))
    if value not in (None, ''):
        font = 'Helvetica-Bold' if bold else VALUE_FONT
        c.setFont(font, size)
        text = _fit(value, font, size, w - 2 * mm)
        y = top - h + 1.6 * mm
        if align == 'right':
            c.drawRightString(x + w - 1 * mm, y, text)
        elif align == 'center':
            c.drawCentredString(x + w / 2, y, text)
        else:
            c.drawString(x + 1 * mm, y, text)


def _title(c, x, top, text):
    c.setFillColor(BLACK)
    c.setFont('Helvetica-Bold', 7.6)
    c.drawString(x, top - 2.8 * mm, text)
    return top - 3.6 * mm


def _row(c, x, top, h, cells):
    """cells: lista de (largura, rótulo, valor, opções)."""
    cx = x
    for cell in cells:
        w, label, value = cell[0], cell[1], cell[2]
        opts = cell[3] if len(cell) > 3 else {}
        _box(c, cx, top, w, h, label, value, **opts)
        cx += w
    return top - h


def _watermark(c, width, height):
    c.saveState()
    c.translate(width / 2, height / 2)
    c.rotate(32)
    c.setFillColor(colors.Color(0.71, 0.14, 0.09, alpha=0.14))
    c.setFont('Helvetica-Bold', 40)
    c.drawCentredString(0, 8 * mm, 'SEM VALOR FISCAL')
    c.setFont('Helvetica-Bold', 22)
    c.drawCentredString(0, -6 * mm, 'AMBIENTE DE TESTE')
    c.restoreState()


def build_danfe_preview_pdf(
    *,
    snapshot: dict,
    issuer,
    details: dict,
    technical: dict,
    identification=None,
    extra_data: dict | None = None,
    logo_bytes: bytes | None = None,
    access_key: str = '',
    protocol: str = '',
    protocol_when: str = '',
    issuer_phone: str = '',
    cancelled: bool = False,
) -> bytes:
    """Gera o DANFE retrato com os dados salvos da nota (sempre com marca de teste)."""
    if not isinstance(snapshot, dict) or not isinstance(snapshot.get('itens'), list):
        raise ValueError('Dados comerciais da nota indisponíveis para o DANFE.')
    det = details or {}
    tech = technical or {}
    extra = extra_data or {}
    width, height = A4
    m = 9 * mm
    cw = width - 2 * m
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    issuer_name = ((getattr(issuer, 'name', '') if issuer else '') or 'PRODUTOR NÃO INFORMADO').upper()
    number = f'{identification.number:06d}' if identification else '—'
    series = str(identification.series) if identification else '—'
    c.setTitle(f'DANFE NF-e {number} - {issuer_name.title()}')
    key = access_key or build_simulated_access_key_summary(issuer, tech, identification).get('key', '')

    def new_page(page_no: int, total_pages: int) -> float:
        _watermark(c, width, height)
        if cancelled:
            c.saveState()
            c.setFillColor(colors.Color(0.71, 0.14, 0.09, alpha=0.5))
            c.setFont('Helvetica-Bold', 30)
            c.drawCentredString(width / 2, height - 150 * mm, 'NF-e CANCELADA')
            c.restoreState()
        top = height - m
        # Canhoto
        right_w = 30 * mm
        left_w = cw - right_w
        _box(c, m, top, left_w, 7 * mm, '', '')
        c.setFont('Helvetica', 6.6)
        c.drawString(m + 1.2 * mm, top - 4.6 * mm, _fit(
            f'RECEBEMOS DE {issuer_name} OS PRODUTOS CONSTANTES DA NOTA FISCAL INDICADA AO LADO',
            'Helvetica', 6.6, left_w - 2.4 * mm))
        _box(c, m, top - 7 * mm, left_w * 0.3, 9 * mm, 'Data de recebimento')
        _box(c, m + left_w * 0.3, top - 7 * mm, left_w * 0.7, 9 * mm, 'Identificação e assinatura do recebedor')
        _box(c, m + left_w, top, right_w, 16 * mm)
        c.setFont('Helvetica-Bold', 7.5)
        c.drawCentredString(m + left_w + right_w / 2, top - 4.8 * mm, 'NF-e')
        c.setFont('Helvetica-Bold', 10)
        c.drawCentredString(m + left_w + right_w / 2, top - 9.4 * mm, f'Nº {number}')
        c.setFont('Helvetica-Bold', 7.5)
        c.drawCentredString(m + left_w + right_w / 2, top - 13.4 * mm, f'Série {series}')
        top -= 19 * mm
        c.setDash(2, 2)
        c.line(m, top + 1.5 * mm, m + cw, top + 1.5 * mm)
        c.setDash()
        top -= 1.5 * mm

        # Cabeçalho: emitente | DANFE | controle do fisco
        hh = 34 * mm
        w1, w2 = cw * 0.42, cw * 0.19
        w3 = cw - w1 - w2
        _box(c, m, top, w1, hh)
        tx = m + 2.5 * mm
        if logo_bytes:
            try:
                img = ImageReader(BytesIO(logo_bytes))
                iw, ih = img.getSize()
                box_w, box_h = 26 * mm, 20 * mm
                scale = min(box_w / iw, box_h / ih)
                dw, dh = iw * scale, ih * scale
                c.drawImage(img, m + 2.5 * mm + (box_w - dw) / 2, top - hh / 2 - dh / 2, dw, dh, mask='auto')
                tx = m + 31 * mm
            except Exception:
                tx = m + 2.5 * mm
        text_w = m + w1 - tx - 2 * mm
        lines = [(issuer_name, 'Helvetica-Bold', 8.2)]
        if issuer:
            doc = _fmt_cpf(getattr(issuer, 'cpf', ''))
            addr = f"{getattr(issuer, 'street', '')}, {getattr(issuer, 'number', '') or 'S/N'}".strip(', ')
            if getattr(issuer, 'district', ''):
                addr += f" - {issuer.district}"
            lines += [
                (f'Produtor rural · CPF {doc}' if doc else 'Produtor rural', 'Helvetica', 7.2),
                (addr, 'Helvetica', 7.2),
                (f"{_fmt_cep(getattr(issuer, 'postal_code', ''))} · {getattr(issuer, 'city', '')} - {getattr(issuer, 'state', '')}", 'Helvetica', 7.2),
            ]
            if issuer_phone:
                lines.append((f'Fone {issuer_phone}', 'Helvetica', 7.2))
        ly = top - hh / 2 + (len(lines) * 3.6 * mm) / 2 - 2.6 * mm
        for text, font, size in lines:
            c.setFont(font, size)
            c.drawString(tx, ly, _fit(text, font, size, text_w))
            ly -= 3.6 * mm

        x2 = m + w1
        _box(c, x2, top, w2, hh)
        cx = x2 + w2 / 2
        c.setFont('Helvetica-Bold', 12)
        c.drawCentredString(cx, top - 6 * mm, 'DANFE')
        c.setFont('Helvetica', 6.8)
        for i, t in enumerate(('Documento Auxiliar', 'da Nota Fiscal', 'Eletrônica')):
            c.drawCentredString(cx, top - 9.6 * mm - i * 2.8 * mm, t)
        c.setFont('Helvetica', 6.6)
        c.drawString(cx - 11 * mm, top - 19.4 * mm, '0-Entrada')
        c.drawString(cx - 11 * mm, top - 22.2 * mm, '1-Saída')
        c.rect(cx + 3.5 * mm, top - 23.2 * mm, 5 * mm, 5 * mm)
        c.setFont('Helvetica-Bold', 8)
        c.drawCentredString(cx + 6 * mm, top - 21.6 * mm, '1')
        c.setFont('Helvetica-Bold', 9)
        c.drawCentredString(cx, top - 27 * mm, f'Nº {number}')
        c.setFont('Helvetica', 6.8)
        c.drawCentredString(cx, top - 30 * mm, f'SÉRIE: {series} · Página {page_no} de {total_pages}')

        x3 = x2 + w2
        _box(c, x3, top, w3, 15 * mm, 'Controle do Fisco')
        if key and len(''.join(ch for ch in key if ch.isdigit())) == 44:
            bar = code128.Code128(key, barHeight=9.5 * mm, barWidth=0.235 * mm, quiet=False)
            bx = x3 + (w3 - bar.width) / 2
            bar.drawOn(c, max(bx, x3 + 1 * mm), top - 13.6 * mm)
        _box(c, x3, top - 15 * mm, w3, 7 * mm, 'Chave de acesso', _fmt_access_key(key) or 'Gerada ao confirmar a emissão',
             size=6.9, align='center')
        _box(c, x3, top - 22 * mm, w3, 12 * mm)
        c.setFont('Helvetica', 6.8)
        for i, t in enumerate(('Consulta de autenticidade no portal nacional da NF-e',
                               'www.nfe.fazenda.gov.br/portal', 'ou no site da Sefaz autorizadora')):
            c.drawString(x3 + 1.5 * mm, top - 25.6 * mm - i * 3 * mm, t)
        top -= hh + 1.2 * mm

        # Natureza / protocolo / IE / CPF
        top = _row(c, m, top, 7 * mm, [
            (cw * 0.5, 'Natureza da operação', det.get('operation_nature') or ''),
            (cw * 0.5, 'Protocolo de autorização de uso', f'{protocol} {protocol_when}'.strip() or 'Gerado ao autorizar'),
        ])
        top = _row(c, m, top, 7 * mm, [
            (cw * 0.34, 'Inscrição Estadual', getattr(issuer, 'state_registration', '') if issuer else ''),
            (cw * 0.33, 'Inscr. est. do subst. trib.', ''),
            (cw * 0.33, 'CNPJ/CPF', _fmt_cpf(getattr(issuer, 'cpf', '')) if issuer else ''),
        ])
        return top

    items = snapshot['itens']
    tech_items = {row.get('item_index'): row for row in tech.get('items', []) if isinstance(row, dict)}
    first_page_rows = 13
    other_page_rows = 52
    remaining = max(0, len(items) - first_page_rows)
    total_pages = 1 + (remaining + other_page_rows - 1) // other_page_rows if remaining else 1

    top = new_page(1, total_pages)

    # Destinatário
    top = _title(c, m, top - 0.8 * mm, 'DESTINATÁRIO/REMETENTE')
    side = 34 * mm
    main = cw - side - 1.5 * mm
    emission = _date_br(tech.get('emission_date') or snapshot.get('data_venda') or '')
    addr = f"{det.get('recipient_street', '')}, {det.get('recipient_number', '') or 'S/N'}".strip(', ')
    rtop = top
    rtop = _row(c, m, rtop, 7 * mm, [(main * 0.5, 'Nome / Razão Social', (det.get('recipient_name') or snapshot.get('comprador') or '').upper()),
                                      (main * 0.27, 'CNPJ/CPF', _fmt_cpf(det.get('recipient_cpf'))),
                                      (main * 0.23, 'Inscrição Estadual', det.get('recipient_ie') or '')])
    rtop = _row(c, m, rtop, 7 * mm, [(main * 0.5, 'Endereço', addr),
                                      (main * 0.27, 'Bairro', det.get('recipient_district') or ''),
                                      (main * 0.23, 'CEP', _fmt_cep(det.get('recipient_postal_code')))])
    rtop = _row(c, m, rtop, 7 * mm, [(main * 0.5, 'Município', det.get('recipient_city') or ''),
                                      (main * 0.27, 'UF', det.get('recipient_state') or ''),
                                      (main * 0.23, 'Fone/Fax', '')])
    sx = m + main + 1.5 * mm
    _box(c, sx, top, side, 7 * mm, 'Data emissão', emission)
    _box(c, sx, top - 7 * mm, side, 7 * mm, 'Data saída', emission)
    _box(c, sx, top - 14 * mm, side, 7 * mm, 'Hora saída', '')
    top = rtop

    # Faturas
    top = _title(c, m, top - 0.8 * mm, 'FATURAS')
    total_prod = Decimal(str(snapshot.get('valor_bruto') or '0'))
    pay_value = extra.get('payment_amount') or total_prod
    cell = cw / 9
    cells = []
    for i in range(3):
        cells += [(cell, 'Número', '001' if i == 0 else '', {'size': 7.6}),
                  (cell, 'Vencimento', emission if i == 0 else '', {'size': 7.6}),
                  (cell, 'Valor', _br(pay_value) if i == 0 else '', {'size': 7.6})]
    top = _row(c, m, top, 6.5 * mm, cells)

    # Cálculo do imposto
    top = _title(c, m, top - 0.8 * mm, 'CÁLCULO DO IMPOSTO')
    icms_base = sum((Decimal(str(t.get('icms_base') or '0')) for t in tech_items.values()), Decimal('0'))
    icms_value = sum((Decimal(str(t.get('icms_value') or '0')) for t in tech_items.values()), Decimal('0'))
    w = cw / 7
    top = _row(c, m, top, 7 * mm, [(w, 'Base de cálculo do ICMS', _br(icms_base), {'align': 'right'}),
                                    (w, 'Valor do ICMS', _br(icms_value), {'align': 'right'}),
                                    (w, 'Base cálc. ICMS Subst.', '0,00', {'align': 'right'}),
                                    (w, 'Valor do ICMS Subst.', '0,00', {'align': 'right'}),
                                    (w, 'Valor do FCP ST', '0,00', {'align': 'right'}),
                                    (w * 2, 'Valor total dos produtos', _br(total_prod), {'align': 'right'})])
    top = _row(c, m, top, 7 * mm, [(w, 'Valor do frete', '0,00', {'align': 'right'}),
                                    (w, 'Valor do seguro', '0,00', {'align': 'right'}),
                                    (w, 'Desconto', '0,00', {'align': 'right'}),
                                    (w, 'Outras despesas', '0,00', {'align': 'right'}),
                                    (w, 'Valor do IPI', '0,00', {'align': 'right'}),
                                    (w * 2, 'Valor total da nota', _br(total_prod), {'align': 'right', 'bold': True})])

    # Transportador
    top = _title(c, m, top - 0.8 * mm, 'TRANSPORTADOR/VOLUMES TRANSPORTADOS')
    top = _row(c, m, top, 7 * mm, [(cw * 0.32, 'Nome', (extra.get('carrier_name') or '').upper()),
                                    (cw * 0.26, 'Frete por conta', FREIGHT_LABELS.get(str(tech.get('freight_mode') or ''), ''), {'size': 6.2}),
                                    (cw * 0.12, 'Código ANTT', ''),
                                    (cw * 0.13, 'Placa do veículo', extra.get('vehicle_plate') or ''),
                                    (cw * 0.05, 'UF', extra.get('vehicle_uf') or ''),
                                    (cw * 0.12, 'CNPJ/CPF', '')])
    gross = extra.get('gross_weight_kg')
    net = extra.get('net_weight_kg')
    top = _row(c, m, top, 7 * mm, [(cw * 0.14, 'Quantidade', extra.get('volume_quantity') or ''),
                                    (cw * 0.2, 'Espécie', extra.get('volume_species') or ''),
                                    (cw * 0.2, 'Marca', ''),
                                    (cw * 0.14, 'Numeração', ''),
                                    (cw * 0.16, 'Peso bruto', _br(gross, 3) if gross else ''),
                                    (cw * 0.16, 'Peso líquido', _br(net, 3) if net else '')])

    # Itens
    top = _title(c, m, top - 0.8 * mm, 'ITENS DA NOTA FISCAL')
    cols = [('Código', 15), ('Descrição do produto/serviço', 0), ('NCM/SH', 13), ('CST', 8), ('CFOP', 9), ('UN', 7),
            ('Qtde', 14), ('Preço un', 15), ('Preço total', 17), ('BC ICMS', 12), ('Vlr.ICMS', 12), ('Vlr.IPI', 10),
            ('%ICMS', 10), ('%IPI', 8)]
    fixed = sum(cw_ for _, cw_ in cols) * mm
    widths = [(cw - fixed) if cw_ == 0 else cw_ * mm for _, cw_ in cols]
    header_h = 4.6 * mm
    row_h = 4.2 * mm

    def items_header(t):
        x = m
        c.setFont('Helvetica-Bold', 6)
        for (label, _), wd in zip(cols, widths):
            c.rect(x, t - header_h, wd, header_h)
            c.drawString(x + 0.8 * mm, t - 3.2 * mm, _fit(label, 'Helvetica-Bold', 6, wd - 1.2 * mm))
            x += wd
        return t - header_h

    def item_row(t, idx, item):
        row = tech_items.get(idx, {})
        desc = f"{(det.get('product_description') or 'CAFÉ').upper()} - LOTE {str(item.get('lote') or '-').upper()}"
        cst = f"{row.get('origin') or ''}{row.get('icms_cst') or ''}"
        values = [f'LOTE-{idx}', desc, det.get('product_ncm') or '', cst,
                  det.get('operation_cfop') or '', row.get('commercial_unit') or 'SC',
                  _br(row.get('commercial_quantity') or item.get('quantidade_sacas'), 4),
                  _br(row.get('commercial_unit_price') or item.get('preco_unitario') or '0', 2),
                  _br(item.get('valor_bruto')), _br(row.get('icms_base') or 0), _br(row.get('icms_value') or 0),
                  '0,00', _br(row.get('icms_rate') or 0), '0,00']
        x = m
        c.setFont('Helvetica', 6.4)
        for i, (val, wd) in enumerate(zip(values, widths)):
            text = _fit(val, 'Helvetica', 6.4, wd - 1.2 * mm)
            if i >= 6:
                c.drawRightString(x + wd - 0.8 * mm, t - 3 * mm, text)
            else:
                c.drawString(x + 0.8 * mm, t - 3 * mm, text)
            x += wd
        return t - row_h

    def items_frame(t_start, t_end):
        x = m
        for wd in widths:
            c.rect(x, t_end, wd, t_start - t_end)
            x += wd

    top = items_header(top)
    body_top = top
    shown = items[:first_page_rows]
    for idx, item in enumerate(shown, 1):
        top = item_row(top, idx, item)
    body_end = min(top, body_top - 40 * mm)
    items_frame(body_top, body_end)
    top = body_end

    # ISSQN
    top = _title(c, m, top - 0.8 * mm, 'CÁLCULO DO ISSQN')
    top = _row(c, m, top, 6.5 * mm, [(cw * 0.25, 'Inscrição Municipal', ''),
                                      (cw * 0.25, 'Valor total dos serviços', '0,00', {'align': 'right'}),
                                      (cw * 0.25, 'Base de cálculo do ISSQN', '0,00', {'align': 'right'}),
                                      (cw * 0.25, 'Valor do ISSQN', '0,00', {'align': 'right'})])

    # Dados adicionais
    top = _title(c, m, top - 0.8 * mm, 'DADOS ADICIONAIS')
    dh = min(34 * mm, max(26 * mm, top - (m + 6 * mm)))
    _box(c, m, top, cw * 0.6, dh, 'Informações complementares')
    _box(c, m + cw * 0.6, top, cw * 0.4, dh, 'Reservado ao fisco')
    info = (extra.get('additional_info') or '').strip()
    venda = snapshot.get('venda')
    if venda and venda not in info:
        info = (info + f' Referente à venda {venda}.').strip()
    c.setFont('Helvetica', 7)
    ly = top - 5.4 * mm
    for line in _wrap(info.upper(), 'Helvetica', 7, cw * 0.6 - 3 * mm, int((dh - 6 * mm) / (3.2 * mm))):
        c.drawString(m + 1.2 * mm, ly, line)
        ly -= 3.2 * mm
    c.setFont('Helvetica', 6.6)
    c.drawString(m, m - 1 * mm, 'Emitido pelo SiSFarm · ambiente de teste, sem valor fiscal')
    c.drawRightString(m + cw, m - 1 * mm, _date_br(tech.get('emission_date') or snapshot.get('data_venda') or ''))

    # Itens que não couberam
    rest = items[first_page_rows:]
    page = 1
    while rest:
        c.showPage()
        page += 1
        top = new_page(page, total_pages)
        top = _title(c, m, top - 0.8 * mm, 'ITENS DA NOTA FISCAL (CONTINUAÇÃO)')
        top = items_header(top)
        body_top = top
        chunk, rest = rest[:other_page_rows], rest[other_page_rows:]
        start = len(items) - len(rest) - len(chunk) + 1
        for offset, item in enumerate(chunk):
            top = item_row(top, start + offset, item)
        items_frame(body_top, top)

    c.save()
    return buf.getvalue()
