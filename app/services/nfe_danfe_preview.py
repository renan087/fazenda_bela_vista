"""Gerador de prévia de DANFE em PDF (exclusivamente para conferência local).

Sempre aplica marca d'água diagonal 'SEM VALOR FISCAL · SIMULAÇÃO' e faixa de aviso.
Não substitui o DANFE oficial de uma NF-e autorizada pela SEFAZ.
"""
from io import BytesIO
from reportlab.graphics.barcode import code128
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas

from app.services.nfe_numbering import build_simulated_access_key_summary


def _fmt_cpf(digits: str) -> str:
    clean = ''.join(c for c in str(digits or '') if c.isdigit())
    if len(clean) == 11:
        return f'{clean[:3]}.{clean[3:6]}.{clean[6:9]}-{clean[9:]}'
    return clean or 'Não informado'


def _fmt_access_key(key: str) -> str:
    clean = ''.join(c for c in str(key or '') if c.isdigit())
    if len(clean) != 44:
        return 'Chave simulada indisponível (informe CPF, UF, data de emissão e reserve numeração)'
    return ' '.join(clean[i:i + 4] for i in range(0, 44, 4))


def _draw_watermark(c: canvas.Canvas, width: float, height: float) -> None:
    c.saveState()
    c.translate(width / 2, height / 2)
    c.rotate(38)
    c.setFillColor(colors.Color(0.75, 0.78, 0.76, alpha=0.28))
    c.setFont('Helvetica-Bold', 34)
    c.drawCentredString(0, 0, 'SEM VALOR FISCAL · SIMULAÇÃO')
    c.restoreState()


def _draw_header_banner(c: canvas.Canvas, width: float, y: float) -> float:
    margin = 14 * mm
    banner_h = 11 * mm
    c.setFillColor(colors.HexColor('#FFF6E5'))
    c.setStrokeColor(colors.HexColor('#D18A00'))
    c.rect(margin, y - banner_h, width - 2 * margin, banner_h, fill=1, stroke=1)
    c.setFillColor(colors.HexColor('#7A4F00'))
    c.setFont('Helvetica-Bold', 8.5)
    c.drawCentredString(
        width / 2,
        y - 7 * mm,
        'PRÉVIA DE CONFERÊNCIA (DANFE SIMULADO) — SEM VALOR FISCAL — NÃO TRANSMITIDO À SEFAZ',
    )
    return y - banner_h - 4 * mm


def build_danfe_preview_pdf(
    *,
    snapshot: dict,
    issuer,
    details: dict,
    technical: dict,
    identification=None,
    extra_data: dict | None = None,
) -> bytes:
    """Gera PDF A4 de conferência dos dados salvos no rascunho com marca d'água obrigatória."""
    if not isinstance(snapshot, dict) or not isinstance(snapshot.get('itens'), list):
        raise ValueError('Dados comerciais do rascunho indisponíveis para prévia PDF.')

    buf = BytesIO()
    width, height = A4
    margin = 14 * mm
    content_w = width - 2 * margin
    c = canvas.Canvas(buf, pagesize=A4)
    c.setTitle('Prévia DANFE - Sem Valor Fiscal - SiSFarm')

    _draw_watermark(c, width, height)
    y = _draw_header_banner(c, width, height - margin)

    key_summary = build_simulated_access_key_summary(issuer, technical or {}, identification)
    sim_key = key_summary.get('key', '')

    # Bloco 1: Emitente e Identificação
    box_h = 34 * mm
    c.setStrokeColor(colors.HexColor('#D6DDD5'))
    c.setFillColor(colors.white)
    c.rect(margin, y - box_h, content_w, box_h, fill=0, stroke=1)

    c.setFillColor(colors.HexColor('#16201A'))
    c.setFont('Helvetica-Bold', 10)
    issuer_name = (getattr(issuer, 'name', '') if issuer else '') or 'Produtor não informado'
    c.drawString(margin + 3 * mm, y - 6 * mm, issuer_name[:58])
    c.setFont('Helvetica', 8)
    c.setFillColor(colors.HexColor('#5B6660'))
    cpf_text = _fmt_cpf(getattr(issuer, 'cpf', '') if issuer else '')
    ie_text = (getattr(issuer, 'state_registration', '') if issuer else '') or 'Não informada'
    c.drawString(margin + 3 * mm, y - 11 * mm, f'CPF: {cpf_text}   ·   IE: {ie_text}')
    if issuer:
        addr = f"{getattr(issuer, 'street', '')} {getattr(issuer, 'number', '')} - {getattr(issuer, 'district', '')}".strip(' -')
        city_uf = f"{getattr(issuer, 'city', '')}/{getattr(issuer, 'state', '')} · CEP {getattr(issuer, 'postal_code', '')}".strip(' /·')
    else:
        addr, city_uf = 'Endereço não informado', ''
    c.drawString(margin + 3 * mm, y - 16 * mm, addr[:70])
    if city_uf:
        c.drawString(margin + 3 * mm, y - 20.5 * mm, city_uf[:70])

    # Coluna direita: Modelo/Série/Número fictício
    right_x = margin + content_w * 0.62
    c.line(right_x, y, right_x, y - box_h)
    c.setFillColor(colors.HexColor('#16201A'))
    c.setFont('Helvetica-Bold', 9)
    c.drawString(right_x + 3 * mm, y - 6 * mm, 'DANFE SIMULADO · MODELO 55')
    c.setFont('Helvetica', 8)
    if identification:
        c.drawString(right_x + 3 * mm, y - 11.5 * mm, f'Série (teste): {identification.series}   ·   Nº fictício: {identification.number}')
        c.drawString(right_x + 3 * mm, y - 16.5 * mm, f'Ambiente: {identification.environment.upper()}')
    else:
        c.drawString(right_x + 3 * mm, y - 11.5 * mm, 'Numeração fictícia: não reservada')
        c.drawString(right_x + 3 * mm, y - 16.5 * mm, 'Ambiente: PREPARAÇÃO LOCAL')
    c.drawString(right_x + 3 * mm, y - 21.5 * mm, f"Venda origem: {snapshot.get('venda', '-')}")
    c.drawString(right_x + 3 * mm, y - 26.5 * mm, f"Emissão teste: {(technical or {}).get('emission_date') or 'Não informada'}")

    y -= box_h + 3 * mm

    # Bloco 2: Chave simulada e código de barras (quando disponível)
    key_h = 20 * mm if sim_key else 11 * mm
    c.setStrokeColor(colors.HexColor('#D6DDD5'))
    c.rect(margin, y - key_h, content_w, key_h, fill=0, stroke=1)
    c.setFillColor(colors.HexColor('#5B6660'))
    c.setFont('Helvetica-Bold', 7.5)
    c.drawString(margin + 3 * mm, y - 4.5 * mm, 'CHAVE DE ACESSO SIMULADA (SEM VALIDADE FISCAL)')
    c.setFillColor(colors.HexColor('#16201A'))
    c.setFont('Courier-Bold', 8)
    c.drawString(margin + 3 * mm, y - 9 * mm, _fmt_access_key(sim_key))
    if sim_key:
        barcode = code128.Code128(sim_key, barHeight=7 * mm, barWidth=0.72)
        barcode.drawOn(c, margin + 3 * mm, y - 18.5 * mm)
    y -= key_h + 4 * mm

    # Bloco 3: Destinatário e Operação
    dest_h = 24 * mm
    c.setStrokeColor(colors.HexColor('#D6DDD5'))
    c.rect(margin, y - dest_h, content_w, dest_h, fill=0, stroke=1)
    c.setFillColor(colors.HexColor('#5B6660'))
    c.setFont('Helvetica-Bold', 7.5)
    c.drawString(margin + 3 * mm, y - 4.5 * mm, 'DESTINATÁRIO / OPERAÇÃO (DADOS SALVOS PARA CONFERÊNCIA)')
    c.setFillColor(colors.HexColor('#16201A'))
    c.setFont('Helvetica-Bold', 8.5)
    det = details or {}
    c.drawString(margin + 3 * mm, y - 9.5 * mm, (det.get('recipient_name') or snapshot.get('comprador') or 'Não informado')[:70])
    c.setFont('Helvetica', 8)
    c.drawString(
        margin + 3 * mm,
        y - 14.5 * mm,
        f"CPF: {_fmt_cpf(det.get('recipient_cpf', ''))}   ·   UF: {det.get('recipient_state') or '-'}   ·   Ind. IE: {det.get('recipient_ie_indicator') or 'Não informado'}",
    )
    c.drawString(
        margin + 3 * mm,
        y - 19.5 * mm,
        f"Natureza: {(det.get('operation_nature') or 'Não informada')[:48]}   ·   CFOP: {det.get('operation_cfop') or '-'}   ·   NCM: {det.get('product_ncm') or '-'}",
    )
    y -= dest_h + 4 * mm

    # Bloco 4: Itens / Lotes
    c.setFillColor(colors.HexColor('#16201A'))
    c.setFont('Helvetica-Bold', 8.5)
    c.drawString(margin, y - 3 * mm, 'ITENS / LOTES DA VENDA (VALORES COMERCIAIS DE CONFERÊNCIA)')
    y -= 6 * mm

    c.setFillColor(colors.HexColor('#F6F8F5'))
    c.setStrokeColor(colors.HexColor('#D6DDD5'))
    c.rect(margin, y - 6.5 * mm, content_w, 6.5 * mm, fill=1, stroke=1)
    c.setFillColor(colors.HexColor('#16201A'))
    c.setFont('Helvetica-Bold', 7.5)
    c.drawString(margin + 2 * mm, y - 4.5 * mm, '#')
    c.drawString(margin + 10 * mm, y - 4.5 * mm, 'Descrição / Lote')
    c.drawString(margin + 96 * mm, y - 4.5 * mm, 'Unid.')
    c.drawString(margin + 112 * mm, y - 4.5 * mm, 'Qtd.')
    c.drawString(margin + 132 * mm, y - 4.5 * mm, 'Vlr. Unit.')
    c.drawRightString(margin + content_w - 2 * mm, y - 4.5 * mm, 'Vlr. Bruto (R$)')
    y -= 6.5 * mm

    tech_items = {row.get('item_index'): row for row in (technical or {}).get('items', []) if isinstance(row, dict)}
    prod_desc = (det.get('product_description') or 'Café').strip()
    c.setFont('Helvetica', 8)
    for idx, item in enumerate(snapshot['itens'], 1):
        if y < 38 * mm:
            c.showPage()
            _draw_watermark(c, width, height)
            y = _draw_header_banner(c, width, height - margin)
            c.setFont('Helvetica', 8)
        row_h = 6.5 * mm
        c.setStrokeColor(colors.HexColor('#EEF1ED'))
        c.rect(margin, y - row_h, content_w, row_h, fill=0, stroke=1)
        t = tech_items.get(idx, {})
        c.setFillColor(colors.HexColor('#16201A'))
        c.drawString(margin + 2 * mm, y - 4.5 * mm, str(idx))
        label = f"{prod_desc[:32]} · Lote {item.get('lote', '-')}"
        c.drawString(margin + 10 * mm, y - 4.5 * mm, label[:48])
        c.drawString(margin + 96 * mm, y - 4.5 * mm, str(t.get('commercial_unit') or 'SC')[:6])
        c.drawString(margin + 112 * mm, y - 4.5 * mm, str(t.get('commercial_quantity') or item.get('quantidade_sacas') or '-')[:12])
        c.drawString(margin + 132 * mm, y - 4.5 * mm, str(t.get('commercial_unit_price') or '-')[:14])
        c.drawRightString(margin + content_w - 2 * mm, y - 4.5 * mm, str(item.get('valor_bruto') or '0.00'))
        y -= row_h

    y -= 4 * mm
    c.setFont('Helvetica-Bold', 9)
    c.setFillColor(colors.HexColor('#16201A'))
    c.drawRightString(margin + content_w, y - 3 * mm, f"Total comercial bruto: R$ {snapshot.get('valor_bruto', '0.00')}")
    y -= 9 * mm

    # Bloco 5: Informações complementares (quando houver)
    extra = extra_data or {}
    if extra.get('additional_info'):
        c.setFont('Helvetica-Bold', 7.5)
        c.setFillColor(colors.HexColor('#5B6660'))
        c.drawString(margin, y, 'INFORMAÇÕES COMPLEMENTARES (TESTE)')
        y -= 4.5 * mm
        c.setFont('Helvetica', 8)
        c.setFillColor(colors.HexColor('#16201A'))
        for line in str(extra['additional_info'])[:450].splitlines()[:4]:
            c.drawString(margin, y, line[:105])
            y -= 4 * mm

    # Rodapé de segurança
    c.setFont('Helvetica', 7)
    c.setFillColor(colors.HexColor('#5B6660'))
    c.drawString(
        margin,
        12 * mm,
        'SiSFarm · Documento gerado exclusivamente para revisão local. Não substitui NF-e/DANFE oficial e não possui validade jurídica ou fiscal.',
    )

    c.save()
    return buf.getvalue()
