"""Geração de PDF da Carta de Correção Eletrônica (DACCE) em ReportLab."""
from io import BytesIO
import re
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph

from app.services.nfe_events import CCE_COND_USO


def _fmt_key(key: str) -> str:
    digits = re.sub(r'\D', '', str(key or ''))
    return ' '.join(digits[i:i + 4] for i in range(0, len(digits), 4)) if digits else '—'


def _fmt_doc(doc: str) -> str:
    d = re.sub(r'\D', '', str(doc or ''))
    if len(d) == 11:
        return f'{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}'
    if len(d) == 14:
        return f'{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}'
    return doc or '—'


def _draw_box(c, x, y, w, h, label: str, value: str, *, bold: bool = False, size: float = 9.0):
    c.setStrokeColor(colors.HexColor('#D6DDD5'))
    c.setLineWidth(0.6)
    c.rect(x, y, w, h, stroke=1, fill=0)
    c.setFillColor(colors.HexColor('#5B6660'))
    c.setFont('Helvetica-Bold', 6.5)
    c.drawString(x + 2.5 * mm, y + h - 3.3 * mm, label.upper())
    c.setFillColor(colors.HexColor('#16201A'))
    c.setFont('Helvetica-Bold' if bold else 'Helvetica', size)
    c.drawString(x + 2.5 * mm, y + 2.3 * mm, str(value or '—')[:95])


def build_cce_pdf(
    *,
    issuer,
    note: dict,
    cce: dict,
    test_mode: bool = True,
) -> bytes:
    """Monta a representação gráfica em PDF (A4) de uma Carta de Correção Eletrônica (CC-e)."""
    buf = BytesIO()
    w, h = A4
    m = 14 * mm
    cw = w - 2 * m
    c = canvas.Canvas(buf, pagesize=A4)
    c.setTitle(f"Carta de Correcao #{cce.get('seq', 1)} - NF-e {note.get('title', '')}")

    # Marca d'água discreta em ambiente de teste
    if test_mode:
        c.saveState()
        c.setFillColor(colors.HexColor('#EEF1ED'))
        c.setFont('Helvetica-Bold', 30)
        c.translate(w / 2, h / 2)
        c.rotate(35)
        c.drawCentredString(0, 12, 'AMBIENTE DE TESTE')
        c.setFont('Helvetica-Bold', 16)
        c.drawCentredString(0, -14, 'SEM VALIDADE FISCAL — NÃO TRANSMITIDO À SEFAZ')
        c.restoreState()

    top = h - m

    # Cabeçalho verde escuro da marca
    hdr_h = 20 * mm
    c.setFillColor(colors.HexColor('#294127'))
    c.roundRect(m, top - hdr_h, cw, hdr_h, 3 * mm, stroke=0, fill=1)
    c.setFillColor(colors.white)
    c.setFont('Helvetica-Bold', 13)
    c.drawString(m + 5 * mm, top - 8.5 * mm, 'CARTA DE CORREÇÃO ELETRÔNICA (CC-e)')
    c.setFont('Helvetica', 8.5)
    c.drawString(m + 5 * mm, top - 14.2 * mm, 'Evento fiscal 110110 · Representação gráfica auxiliar')
    c.setFont('Helvetica-Bold', 11)
    c.drawRightString(m + cw - 5 * mm, top - 8.5 * mm, f"Sequência #{cce.get('seq', 1)} de 20")
    c.setFont('Helvetica', 8)
    c.drawRightString(
        m + cw - 5 * mm,
        top - 14.2 * mm,
        'AMBIENTE DE TESTE' if test_mode else 'PRODUÇÃO',
    )
    top -= hdr_h + 4 * mm

    # Dados do emitente
    row_h = 11.5 * mm
    issuer_name = getattr(issuer, 'name', '') or 'Produtor rural'
    issuer_doc = _fmt_doc(getattr(issuer, 'cpf', ''))
    issuer_ie = getattr(issuer, 'state_registration', '') or '—'
    issuer_loc = f"{getattr(issuer, 'city', '') or ''}/{getattr(issuer, 'state', '') or ''}".strip('/')
    _draw_box(c, m, top - row_h, cw * 0.52, row_h, 'Emitente', issuer_name, bold=True)
    _draw_box(c, m + cw * 0.52, top - row_h, cw * 0.24, row_h, 'CPF / CNPJ', issuer_doc)
    _draw_box(c, m + cw * 0.76, top - row_h, cw * 0.24, row_h, 'Inscrição Estadual', issuer_ie)
    top -= row_h

    # Dados da NF-e vinculada
    nfe_num = note.get('number') or '—'
    nfe_series = str(note.get('series') or '—')
    _draw_box(c, m, top - row_h, cw * 0.20, row_h, 'NF-e Número', nfe_num, bold=True)
    _draw_box(c, m + cw * 0.20, top - row_h, cw * 0.14, row_h, 'Série', nfe_series)
    _draw_box(c, m + cw * 0.34, top - row_h, cw * 0.36, row_h, 'Município / UF do emitente', issuer_loc or '—')
    _draw_box(c, m + cw * 0.70, top - row_h, cw * 0.30, row_h, 'Data / Hora do evento', cce.get('when') or '—')
    top -= row_h

    # Chave de acesso e protocolo
    _draw_box(c, m, top - row_h, cw * 0.68, row_h, 'Chave de acesso da NF-e', _fmt_key(note.get('access_key', '')), bold=True, size=8.3)
    _draw_box(c, m + cw * 0.68, top - row_h, cw * 0.32, row_h, 'Protocolo do evento', cce.get('protocol') or '—', size=8.5)
    top -= row_h

    # Destinatário
    _draw_box(c, m, top - row_h, cw * 0.68, row_h, 'Destinatário / Comprador', note.get('buyer') or '—')
    _draw_box(c, m + cw * 0.68, top - row_h, cw * 0.32, row_h, 'CPF / CNPJ do destinatário', note.get('buyer_doc') or '—')
    top -= row_h + 6 * mm

    # Seção: Texto da correção
    c.setFillColor(colors.HexColor('#294127'))
    c.setFont('Helvetica-Bold', 9.5)
    c.drawString(m, top, 'CORREÇÃO A SER CONSIDERADA')
    top -= 3.5 * mm

    text_style = ParagraphStyle(
        'cce_body',
        fontName='Helvetica',
        fontSize=10,
        leading=14.5,
        textColor=colors.HexColor('#16201A'),
    )
    corr_para = Paragraph(escape(str(cce.get('text') or '')), text_style)
    _, corr_h = corr_para.wrap(cw - 8 * mm, 120 * mm)
    box_h = max(32 * mm, corr_h + 8 * mm)
    c.setFillColor(colors.HexColor('#FAFBF9'))
    c.setStrokeColor(colors.HexColor('#C9D6C3'))
    c.setLineWidth(0.9)
    c.roundRect(m, top - box_h, cw, box_h, 2 * mm, stroke=1, fill=1)
    corr_para.drawOn(c, m + 4 * mm, top - 4 * mm - corr_h)
    top -= box_h + 5 * mm

    if int(cce.get('seq') or 1) > 1:
        c.setFillColor(colors.HexColor('#7A4F00'))
        c.setFont('Helvetica-Bold', 8.2)
        c.drawString(
            m,
            top,
            f"Atenção: esta Carta de Correção (#{cce.get('seq')}) substitui todas as cartas de correção anteriores vinculadas a esta NF-e.",
        )
        top -= 6 * mm

    # Seção: Condições de uso legal (obrigatória na CC-e)
    c.setFillColor(colors.HexColor('#5B6660'))
    c.setFont('Helvetica-Bold', 8)
    c.drawString(m, top, 'CONDIÇÕES DE USO (CONVÊNIO S/N, DE 15 DE DEZEMBRO DE 1970)')
    top -= 3 * mm

    cond_style = ParagraphStyle(
        'cce_cond',
        fontName='Helvetica',
        fontSize=8.2,
        leading=11.8,
        textColor=colors.HexColor('#46514A'),
    )
    cond_para = Paragraph(escape(CCE_COND_USO), cond_style)
    _, cond_h = cond_para.wrap(cw - 6 * mm, 60 * mm)
    cond_box_h = cond_h + 6 * mm
    c.setStrokeColor(colors.HexColor('#E3E8E2'))
    c.setLineWidth(0.6)
    c.rect(m, top - cond_box_h, cw, cond_box_h, stroke=1, fill=0)
    cond_para.drawOn(c, m + 3 * mm, top - 3 * mm - cond_h)

    # Rodapé
    c.setFillColor(colors.HexColor('#7A857E'))
    c.setFont('Helvetica', 7.5)
    c.drawString(m, m, 'Documento gerado pelo SiSFarm · A CC-e não substitui o DANFE e deve acompanhar a NF-e.')
    c.drawRightString(m + cw, m, f"ID Evento: {cce.get('event_id') or '—'}")

    c.showPage()
    c.save()
    return buf.getvalue()
