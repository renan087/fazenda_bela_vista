"""Geração de PDF da Carta de Correção Eletrônica (DACCE) em ReportLab no padrão visual SiSFarm."""
from io import BytesIO
import re
from xml.sax.saxutils import escape

from reportlab.graphics.barcode import code128
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph

# Paleta oficial SiSFarm
BRAND_DARK = colors.HexColor('#294127')
BRAND_GREEN = colors.HexColor('#446A36')
BRAND_SOFT = colors.HexColor('#EEF3EB')
SURFACE_SOFT = colors.HexColor('#F6F8F5')
BORDER_GREEN = colors.HexColor('#C9D6C3')
BORDER_DARK = colors.HexColor('#294127')
INK = colors.HexColor('#16201A')
MUTED = colors.HexColor('#5B6660')

CCE_COND_USO_PT = (
    'A Carta de Correção é disciplinada pelo § 1º-A do art. 7º do Convênio S/N, de 15 de dezembro de 1970 e pode '
    'ser utilizada para regularização de erro ocorrido na emissão de documento fiscal, desde que o erro não esteja '
    'relacionado com: I - as variáveis que determinam o valor do imposto tais como: base de cálculo, alíquota, '
    'diferença de preço, quantidade, valor da operação ou da prestação; II - a correção de dados cadastrais que '
    'implique mudança do remetente ou do destinatário; III - a data de emissão ou de saída.'
)


def _digits(value: str) -> str:
    return re.sub(r'\D', '', str(value or ''))


def _fmt_doc(doc: str) -> str:
    d = _digits(doc)
    if len(d) == 11:
        return f'{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}'
    if len(d) == 14:
        return f'{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}'
    return str(doc or '').strip()


def _fmt_cep(cep: str) -> str:
    d = _digits(cep)
    return f'{d[:5]}-{d[5:]}' if len(d) == 8 else str(cep or '').strip()


def _draw_cell(
    c,
    x: float,
    y: float,
    w: float,
    h: float,
    label: str,
    value: str,
    *,
    align: str = 'left',
    bold_value: bool = False,
    value_size: float = 8.8,
    fill_color=None,
    stroke_color=BORDER_GREEN,
):
    c.setStrokeColor(stroke_color)
    c.setLineWidth(0.7)
    if fill_color is not None:
        c.setFillColor(fill_color)
        c.rect(x, y, w, h, stroke=1, fill=1)
    else:
        c.rect(x, y, w, h, stroke=1, fill=0)

    c.setFillColor(BRAND_GREEN)
    c.setFont('Helvetica-Bold', 7.2)
    if align == 'center':
        c.drawCentredString(x + w / 2, y + h - 3.6 * mm, label)
    else:
        c.drawString(x + 2.2 * mm, y + h - 3.6 * mm, label)

    c.setFillColor(INK)
    c.setFont('Helvetica-Bold' if bold_value else 'Helvetica', value_size)
    text = str(value or '').strip()
    if align == 'center':
        c.drawCentredString(x + w / 2, y + 2.0 * mm, text[:95])
    else:
        c.drawString(x + 2.2 * mm, y + 2.0 * mm, text[:95])


def build_cce_pdf(
    *,
    issuer,
    note: dict,
    cce: dict,
    logo_bytes: bytes | None = None,
    buyer_phone: str = '',
    test_mode: bool = True,
) -> bytes:
    """Monta a representação gráfica em PDF (A4) de uma Carta de Correção Eletrônica (CC-e)."""
    buf = BytesIO()
    w, h = A4
    m = 10 * mm
    cw = w - 2 * m
    c = canvas.Canvas(buf, pagesize=A4)
    seq_int = int(cce.get('seq') or 1)
    seq_str = f'{seq_int:02d}'
    c.setTitle(f"Carta de Correcao {seq_str} - NF-e {note.get('title', '')}")

    # Marca d'água sutil em ambiente de teste
    if test_mode:
        c.saveState()
        c.setFillColor(colors.HexColor('#F1F4F0'))
        c.setFont('Helvetica-Bold', 28)
        c.translate(w / 2, h / 2 - 20 * mm)
        c.rotate(32)
        c.drawCentredString(0, 12, 'AMBIENTE DE TESTE')
        c.setFont('Helvetica-Bold', 14)
        c.drawCentredString(0, -12, 'SEM VALIDADE FISCAL — NÃO TRANSMITIDO À SEFAZ')
        c.restoreState()

    top = h - m

    # 1. Barra de título principal: "Carta de Correção - CC-e"
    title_h = 9.5 * mm
    c.setFillColor(BRAND_DARK)
    c.setStrokeColor(BRAND_DARK)
    c.setLineWidth(0.9)
    c.rect(m, top - title_h, cw, title_h, stroke=1, fill=1)
    c.setFillColor(colors.white)
    c.setFont('Helvetica-Bold', 13)
    c.drawCentredString(m + cw / 2, top - title_h + 2.7 * mm, 'Carta de Correção - CC-e')
    top -= title_h + 2.5 * mm

    # 2. Cabeçalho em 2 colunas: Identificação do Emitente (esq) | IE, CPF/CNPJ, Chave e Código de Barras (dir)
    hdr_h = 46 * mm
    left_w = cw * 0.47
    right_w = cw - left_w
    right_x = m + left_w

    # 2a. Coluna esquerda: Identificação do Emitente
    c.setStrokeColor(BORDER_GREEN)
    c.setLineWidth(0.8)
    c.rect(m, top - hdr_h, left_w, hdr_h, stroke=1, fill=0)
    # Faixa superior sutil do título do emitente
    c.setFillColor(BRAND_SOFT)
    c.rect(m, top - 6 * mm, left_w, 6 * mm, stroke=1, fill=1)
    c.setFillColor(BRAND_DARK)
    c.setFont('Helvetica-Bold', 8)
    c.drawCentredString(m + left_w / 2, top - 4.3 * mm, 'Identificação do Emitente')

    cursor_y = top - 8 * mm
    if logo_bytes:
        try:
            img = ImageReader(BytesIO(logo_bytes))
            iw, ih = img.getSize()
            max_w, max_h = left_w - 14 * mm, 16 * mm
            scale = min(max_w / max(iw, 1), max_h / max(ih, 1))
            dw, dh = iw * scale, ih * scale
            c.drawImage(
                img,
                m + (left_w - dw) / 2,
                cursor_y - dh,
                width=dw,
                height=dh,
                preserveAspectRatio=True,
                mask='auto',
            )
            cursor_y -= dh + 2.5 * mm
        except Exception:
            cursor_y -= 4 * mm
    else:
        cursor_y -= 7 * mm

    issuer_name = (getattr(issuer, 'name', '') or 'Produtor rural').strip().upper()
    issuer_street = ', '.join(
        p for p in (
            (getattr(issuer, 'street', '') or '').strip(),
            (getattr(issuer, 'number', '') or '').strip(),
        ) if p
    )
    issuer_district = (getattr(issuer, 'district', '') or '').strip()
    if issuer_district:
        issuer_addr1 = f'{issuer_street} - {issuer_district}' if issuer_street else issuer_district
    else:
        issuer_addr1 = issuer_street
    issuer_cep = _fmt_cep(getattr(issuer, 'postal_code', ''))
    issuer_city = (getattr(issuer, 'city', '') or '').strip()
    issuer_uf = (getattr(issuer, 'state', '') or '').strip().upper()
    issuer_loc = ' - '.join(p for p in (issuer_city, issuer_uf) if p)
    issuer_addr2 = ' '.join(p for p in (issuer_cep, issuer_loc) if p)

    c.setFillColor(INK)
    c.setFont('Helvetica-Bold', 9)
    c.drawCentredString(m + left_w / 2, cursor_y - 3 * mm, issuer_name[:44])
    c.setFont('Helvetica', 8)
    c.setFillColor(MUTED)
    if issuer_addr1:
        c.drawCentredString(m + left_w / 2, cursor_y - 7.5 * mm, issuer_addr1[:52])
    if issuer_addr2:
        c.drawCentredString(m + left_w / 2, cursor_y - 11.5 * mm, issuer_addr2[:52])

    # 2b. Coluna direita: IE | CPF/CNPJ, Chave de Acesso, Código de Barras
    r1_h = 9.5 * mm
    r2_h = 9.5 * mm
    r3_h = hdr_h - r1_h - r2_h

    issuer_ie = (getattr(issuer, 'state_registration', '') or 'ISENTO').strip()
    issuer_doc_raw = _digits(getattr(issuer, 'cpf', ''))
    issuer_doc_label = 'CNPJ' if len(issuer_doc_raw) == 14 else 'CPF'
    issuer_doc_fmt = _fmt_doc(issuer_doc_raw) or '—'

    _draw_cell(c, right_x, top - r1_h, right_w * 0.50, r1_h, 'Inscrição Estadual', issuer_ie, value_size=8.6)
    _draw_cell(c, right_x + right_w * 0.50, top - r1_h, right_w * 0.50, r1_h, issuer_doc_label, issuer_doc_fmt, value_size=8.6)

    access_key = _digits(note.get('access_key', ''))
    _draw_cell(
        c, right_x, top - r1_h - r2_h, right_w, r2_h, 'Chave de Acesso', access_key or '—',
        bold_value=True, value_size=8.2,
    )

    # Caixa do Código de Barras
    bar_y = top - hdr_h
    c.setStrokeColor(BORDER_GREEN)
    c.setLineWidth(0.7)
    c.rect(right_x, bar_y, right_w, r3_h, stroke=1, fill=0)
    c.setFillColor(BRAND_GREEN)
    c.setFont('Helvetica-Bold', 7.2)
    c.drawString(right_x + 2.2 * mm, bar_y + r3_h - 3.6 * mm, 'Código de Barras')

    if len(access_key) == 44:
        try:
            bar = code128.Code128(access_key, barHeight=15 * mm, barWidth=0.255 * mm, quiet=False)
            bar_x = right_x + max(2 * mm, (right_w - bar.width) / 2)
            bar.drawOn(c, bar_x, bar_y + 5.8 * mm)
        except Exception:
            pass
        c.setFillColor(INK)
        c.setFont('Helvetica', 7.5)
        c.drawCentredString(right_x + right_w / 2, bar_y + 2.0 * mm, access_key)

    top -= hdr_h + 2.5 * mm

    # 3. Faixa da NF-e: Número da NF-e | Série | Modelo | Data de Emissão | Página
    strip_h = 9.5 * mm
    nfe_num = str(int(note['number'])) if str(note.get('number') or '').isdigit() else (note.get('number') or '—')
    nfe_series = str(note.get('series') or '—')
    nfe_model = str(getattr(note.get('identification'), 'model', None) or '55')
    nfe_emission = note.get('emission_br') or '—'

    cols = [
        (0.20, 'Número da NF-e', nfe_num, True),
        (0.20, 'Série', nfe_series, False),
        (0.20, 'Modelo', nfe_model, False),
        (0.20, 'Data de Emissão', nfe_emission, False),
        (0.20, 'Página', '1 / 1', False),
    ]
    cx = m
    for ratio, label, val, is_bold in cols:
        col_w = cw * ratio
        _draw_cell(
            c, cx, top - strip_h, col_w, strip_h, label, val,
            align='center', bold_value=is_bold, value_size=8.6, fill_color=SURFACE_SOFT,
        )
        cx += col_w
    top -= strip_h + 2.5 * mm

    # 4. Bloco do Destinatário (3 linhas)
    det = note.get('details') or {}
    snap = note.get('snapshot') or {}
    dest_h = 9.5 * mm

    dest_name = (det.get('recipient_name') or snap.get('comprador') or note.get('buyer') or '—').strip().upper()
    dest_doc = _fmt_doc(det.get('recipient_cpf') or note.get('buyer_doc') or '') or '—'
    dest_addr = ', '.join(
        p for p in (
            (det.get('recipient_street') or '').strip(),
            (det.get('recipient_number') or '').strip(),
        ) if p
    ) or '—'
    dest_district = (det.get('recipient_district') or '').strip() or '—'
    dest_city = (det.get('recipient_city') or '').strip() or '—'
    dest_uf = (det.get('recipient_state') or '').strip().upper() or '—'
    dest_phone = (buyer_phone or det.get('recipient_phone') or '').strip() or '—'
    dest_ie = (det.get('recipient_ie') or '').strip() or '—'

    # Linha 1
    _draw_cell(c, m, top - dest_h, cw * 0.67, dest_h, 'Nome / Razão Social', dest_name, bold_value=True, value_size=8.5)
    _draw_cell(c, m + cw * 0.67, top - dest_h, cw * 0.33, dest_h, 'CNPJ / CPF', dest_doc, value_size=8.5)
    top -= dest_h

    # Linha 2
    _draw_cell(c, m, top - dest_h, cw * 0.50, dest_h, 'Endereço', dest_addr, value_size=8.5)
    _draw_cell(c, m + cw * 0.50, top - dest_h, cw * 0.50, dest_h, 'Bairro / Distrito', dest_district, value_size=8.5)
    top -= dest_h

    # Linha 3
    _draw_cell(c, m, top - dest_h, cw * 0.50, dest_h, 'Município', dest_city, value_size=8.5)
    _draw_cell(c, m + cw * 0.50, top - dest_h, cw * 0.08, dest_h, 'UF', dest_uf, align='center', value_size=8.5)
    _draw_cell(c, m + cw * 0.58, top - dest_h, cw * 0.17, dest_h, 'Telefone / Fax', dest_phone, value_size=8.3)
    _draw_cell(c, m + cw * 0.75, top - dest_h, cw * 0.25, dest_h, 'Inscrição Estadual', dest_ie, value_size=8.5)
    top -= dest_h + 4.5 * mm

    # 5. Condição de Uso
    c.setFillColor(BRAND_DARK)
    c.setFont('Helvetica-Bold', 8.5)
    c.drawString(m, top, 'Condição de Uso')
    top -= 2.2 * mm

    cond_style = ParagraphStyle(
        'cce_cond',
        fontName='Helvetica-Oblique',
        fontSize=8.0,
        leading=11.0,
        textColor=INK,
    )
    cond_para = Paragraph(escape(CCE_COND_USO_PT), cond_style)
    _, cond_h = cond_para.wrap(cw - 5 * mm, 45 * mm)
    cond_box_h = cond_h + 5 * mm
    c.setStrokeColor(BORDER_GREEN)
    c.setFillColor(SURFACE_SOFT)
    c.setLineWidth(0.7)
    c.rect(m, top - cond_box_h, cw, cond_box_h, stroke=1, fill=1)
    cond_para.drawOn(c, m + 2.5 * mm, top - 2.5 * mm - cond_h)
    top -= cond_box_h + 4.5 * mm

    # 6. Eventos / Correções (ocupa o restante da folha até a margem inferior)
    c.setFillColor(BRAND_DARK)
    c.setFont('Helvetica-Bold', 8.5)
    c.drawString(m, top, 'Eventos / Correções')
    top -= 2.2 * mm

    bottom_y = m + 5.5 * mm
    ev_box_h = max(48 * mm, top - bottom_y)
    ev_head_h = 10.5 * mm

    # Caixa externa da seção Eventos / Correções
    c.setStrokeColor(BORDER_DARK)
    c.setLineWidth(0.8)
    c.rect(m, bottom_y, cw, ev_box_h, stroke=1, fill=0)

    # Cabeçalho da tabela de evento (faixa verde suave SiSFarm)
    c.setFillColor(BRAND_SOFT)
    c.setStrokeColor(BORDER_GREEN)
    c.setLineWidth(0.7)
    c.rect(m, top - ev_head_h, cw, ev_head_h, stroke=1, fill=1)

    # Colunas: Sequência (sem total!) | Status | Data de Registro | Número do Protocolo
    ev_cols = [
        (m + 2.5 * mm, 'Sequência', seq_str),
        (m + cw * 0.14, 'Status', '135'),
        (m + cw * 0.26, 'Data de Registro', cce.get('when') or '—'),
        (m + cw * 0.46, 'Número do Protocolo', cce.get('protocol') or '—'),
    ]
    for ex, elabel, eval_str in ev_cols:
        c.setFillColor(BRAND_GREEN)
        c.setFont('Helvetica-Bold', 7.5)
        c.drawString(ex, top - 3.8 * mm, elabel)
        c.setFillColor(INK)
        c.setFont('Helvetica-Bold' if elabel == 'Sequência' else 'Helvetica', 8.8)
        c.drawString(ex, top - 8.4 * mm, str(eval_str))

    # Corpo da correção
    body_style = ParagraphStyle(
        'cce_body',
        fontName='Helvetica',
        fontSize=9.2,
        leading=13.5,
        textColor=INK,
    )
    corr_para = Paragraph(escape(str(cce.get('text') or '')), body_style)
    avail_h = ev_box_h - ev_head_h - 6 * mm
    _, corr_h = corr_para.wrap(cw - 6 * mm, avail_h)
    corr_para.drawOn(c, m + 3 * mm, top - ev_head_h - 3.5 * mm - corr_h)

    # 7. Rodapé discreto
    c.setFillColor(MUTED)
    c.setFont('Helvetica', 7.2)
    c.drawString(m, m + 1 * mm, 'SiSFarm · Documento auxiliar de Carta de Correção Eletrônica (CC-e)')
    if test_mode:
        c.setFillColor(BRAND_GREEN)
        c.setFont('Helvetica-Bold', 7.2)
        c.drawRightString(m + cw, m + 1 * mm, 'AMBIENTE DE TESTE — SEM VALIDADE FISCAL')

    c.showPage()
    c.save()
    return buf.getvalue()
