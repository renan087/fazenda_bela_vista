"""Gerador de Comprovante Oficial de Validação em PDF (A4) com selo vetorial SiSFarm."""
from __future__ import annotations

import io
import math
from datetime import datetime

from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas as rl_canvas

from app.services.document_signing.electronic import (
    OFFICIAL_PUBLIC_BASE_URL,
    SP_TZ,
    _draw_vector_qr,
    _fit_font,
)

# Paleta semântica alinhada ao sistema de design oficial do SiSFarm
_TONE_STYLES = {
    "valid": {
        "bg": "#F1F9EF",
        "border": "#C4E3C9",
        "dark": "#294127",
        "main": "#1F7A3A",
        "seal_word": "AUTÊNTICO",
        "badge": "DOCUMENTO AUTÊNTICO E ÍNTEGRO",
    },
    "partial": {
        "bg": "#FFF9EC",
        "border": "#F2DFB4",
        "dark": "#7A4F00",
        "main": "#C98200",
        "seal_word": "PARCIAL",
        "badge": "ASSINATURA EM ANDAMENTO",
    },
    "invalid": {
        "bg": "#FEF2F1",
        "border": "#F5CBC7",
        "dark": "#912018",
        "main": "#B42318",
        "seal_word": "INVÁLIDO",
        "badge": "DOCUMENTO INVÁLIDO / ALTERADO",
    },
    "indeterminate": {
        "bg": "#FFF8EA",
        "border": "#F2DFB4",
        "dark": "#7A4F00",
        "main": "#C98200",
        "seal_word": "PENDENTE",
        "badge": "VALIDADE NÃO CONFIRMADA",
    },
    "unsigned": {
        "bg": "#F6F8F5",
        "border": "#D6DDD5",
        "dark": "#47524C",
        "main": "#5B6660",
        "seal_word": "SEM ASSINATURA",
        "badge": "SEM ASSINATURA DIGITAL",
    },
}


def _wrap_text(text: str, font_name: str, font_size: float, max_width: float) -> list[str]:
    words = str(text or "").split()
    if not words:
        return []
    lines: list[str] = []
    cur = ""
    for word in words:
        candidate = f"{cur} {word}".strip()
        if not cur or stringWidth(candidate, font_name, font_size) <= max_width:
            cur = candidate
        else:
            lines.append(cur)
            cur = word
    if cur:
        lines.append(cur)
    return lines


def _draw_authenticity_seal(c: rl_canvas.Canvas, cx: float, cy: float, tone: str) -> None:
    """Desenha selo circular vetorial estilizado (chancela de autenticidade SiSFarm)."""
    st = _TONE_STYLES.get(tone, _TONE_STYLES["valid"])
    c.saveState()

    # Círculo externo preenchido + borda dupla
    c.setFillColor(HexColor(st["bg"]))
    c.setStrokeColor(HexColor(st["dark"]))
    c.setLineWidth(1.8)
    c.circle(cx, cy, 42, stroke=1, fill=1)

    # Anel tracejado interno
    c.setStrokeColor(HexColor(st["main"]))
    c.setLineWidth(0.85)
    c.setDash(3, 2)
    c.circle(cx, cy, 36.5, stroke=1, fill=0)
    c.setDash()

    # Miolo branco
    c.setFillColor(HexColor("#FFFFFF"))
    c.setStrokeColor(HexColor(st["dark"]))
    c.setLineWidth(1.0)
    c.circle(cx, cy, 27.5, stroke=1, fill=1)

    # Pequenas estrelas/pontos decorativos laterais na coroa do selo
    c.setFillColor(HexColor(st["dark"]))
    for angle_deg in (160, 20, 200, 340):
        rad = math.radians(angle_deg)
        c.circle(cx + 32 * math.cos(rad), cy + 32 * math.sin(rad), 1.15, stroke=0, fill=1)

    # Texto superior discreto no anel
    c.setFillColor(HexColor(st["dark"]))
    c.setFont("Helvetica-Bold", 6.2)
    c.drawCentredString(cx, cy + 29.8, "SISFARM")

    # Símbolo central vetorial
    c.setStrokeColor(HexColor(st["main"]))
    c.setLineWidth(2.6)
    c.setLineCap(1)
    c.setLineJoin(1)
    if tone == "valid":
        p = c.beginPath()
        p.moveTo(cx - 10, cy + 3.5)
        p.lineTo(cx - 3, cy - 3.5)
        p.lineTo(cx + 11, cy + 11)
        c.drawPath(p, stroke=1, fill=0)
    elif tone == "invalid":
        c.line(cx - 8, cy - 3, cx + 8, cy + 13)
        c.line(cx + 8, cy - 3, cx - 8, cy + 13)
    elif tone in ("partial", "indeterminate"):
        c.line(cx, cy + 12, cx, cy + 1)
        c.setFillColor(HexColor(st["main"]))
        c.circle(cx, cy - 3.5, 1.7, stroke=0, fill=1)
    else:
        c.line(cx - 9, cy + 4, cx + 9, cy + 4)

    # Faixa horizontal de destaque sobre a base do selo
    pill_w = 74
    pill_h = 15
    c.setFillColor(HexColor(st["dark"]))
    c.roundRect(cx - pill_w / 2, cy - 26, pill_w, pill_h, 7.5, stroke=0, fill=1)
    c.setFillColor(HexColor("#FFFFFF"))
    seal_word = st["seal_word"]
    fsize = _fit_font(seal_word, "Helvetica-Bold", 7.4, pill_w - 10, min_size=5.4)
    c.setFont("Helvetica-Bold", fsize)
    c.drawCentredString(cx, cy - 21.3, seal_word)

    c.restoreState()


def build_validation_receipt_pdf(payload: dict) -> bytes:
    """Gera o PDF A4 estilizado do Comprovante de Validação de Documento."""
    tone = str(payload.get("tone") or "valid")
    if tone not in _TONE_STYLES:
        tone = "valid"
    st = _TONE_STYLES[tone]

    headline = str(payload.get("headline") or "Documento autêntico")[:120]
    summary = str(payload.get("summary") or "")[:360]
    title = str(payload.get("title") or "Documento analisado")[:160]
    origin = str(payload.get("origin") or "SiSFarm")[:90]
    code = str(payload.get("code") or "").strip()[:32]
    integrity = str(payload.get("integrity") or "Confirmada")[:64]
    integrity_bad = bool(payload.get("integrity_bad", False))
    sha256_hash = str(payload.get("sha256") or "").strip()[:128]
    policy = str(payload.get("policy") or "Verificação pública SiSFarm")[:140]
    seal_note = str(payload.get("seal_note") or "")[:360]
    checked_at = str(payload.get("checked_at") or datetime.now(SP_TZ).strftime("%d/%m/%Y às %H:%M"))[:48]
    protocol = str(payload.get("protocol") or f"VAL-{datetime.now(SP_TZ).strftime('%Y%m%d-%H%M')}")[:32]
    verify_url = (
        f"{OFFICIAL_PUBLIC_BASE_URL}/validar/{code}"
        if code
        else f"{OFFICIAL_PUBLIC_BASE_URL}/validar"
    )
    signers = list(payload.get("signers") or [])[:12]

    buf = io.BytesIO()
    W, H = A4  # 595.28 x 841.89 pt
    M = 42.0
    CW = W - M * 2

    c = rl_canvas.Canvas(buf, pagesize=A4)
    c.setTitle(f"Comprovante de Validação — {code or title}")
    c.setAuthor("SiSFarm")
    c.setSubject("Comprovante Oficial de Autenticidade de Documento")

    # 1. Cabeçalho institucional verde escuro SiSFarm
    c.setFillColor(HexColor("#294127"))
    c.rect(0, H - 90, W, 90, stroke=0, fill=1)
    c.setFillColor(HexColor("#9BCB6B"))
    c.rect(0, H - 94, W, 4, stroke=0, fill=1)

    # Emblema escudo à esquerda
    c.setFillColor(HexColor("#446A36"))
    c.setStrokeColor(HexColor("#9BCB6B"))
    c.setLineWidth(1.0)
    c.roundRect(M, H - 64, 34, 34, 8, stroke=1, fill=1)
    c.setStrokeColor(HexColor("#FFFFFF"))
    c.setLineWidth(2.1)
    c.setLineCap(1)
    c.setLineJoin(1)
    p = c.beginPath()
    p.moveTo(M + 10, H - 47)
    p.lineTo(M + 15, H - 52)
    p.lineTo(M + 25, H - 41)
    c.drawPath(p, stroke=1, fill=0)

    c.setFillColor(HexColor("#C8DFBD"))
    c.setFont("Helvetica-Bold", 8.2)
    c.drawString(M + 46, H - 39, "SISFARM  ·  VALIDAÇÃO PÚBLICA DE DOCUMENTOS")
    c.setFillColor(HexColor("#FFFFFF"))
    c.setFont("Helvetica-Bold", 16.5)
    c.drawString(M + 46, H - 58, "Comprovante Oficial de Autenticidade")

    c.setFillColor(HexColor("#FFFFFF"))
    c.setFont("Helvetica-Bold", 8.5)
    c.drawRightString(W - M, H - 41, f"Protocolo: {protocol}")
    c.setFillColor(HexColor("#DCE9D5"))
    c.setFont("Helvetica", 8.0)
    c.drawRightString(W - M, H - 55, f"Emitido em {checked_at}")

    # 2. Cartão de Veredito + Selo Circular Vetorial de Autenticidade
    y = H - 118
    hero_h = 114
    c.setFillColor(HexColor(st["bg"]))
    c.setStrokeColor(HexColor(st["border"]))
    c.setLineWidth(1.2)
    c.roundRect(M, y - hero_h, CW, hero_h, 12, stroke=1, fill=1)

    badge_txt = st["badge"]
    badge_w = min(240, stringWidth(badge_txt, "Helvetica-Bold", 7.8) + 20)
    c.setFillColor(HexColor(st["main"]))
    c.roundRect(M + 18, y - 31, badge_w, 16, 8, stroke=0, fill=1)
    c.setFillColor(HexColor("#FFFFFF"))
    c.setFont("Helvetica-Bold", 7.8)
    c.drawString(M + 28, y - 25.8, badge_txt)

    text_max_w = CW - 135
    c.setFillColor(HexColor("#16201A"))
    h_size = _fit_font(headline, "Helvetica-Bold", 16.5, text_max_w, min_size=12.0)
    c.setFont("Helvetica-Bold", h_size)
    c.drawString(M + 18, y - 54, headline)

    c.setFillColor(HexColor("#3E4942"))
    c.setFont("Helvetica", 9.6)
    for idx, ln in enumerate(_wrap_text(summary, "Helvetica", 9.6, text_max_w)[:3]):
        c.drawString(M + 18, y - 72 - idx * 13.5, ln)

    _draw_authenticity_seal(c, W - M - 58, y - hero_h / 2, tone)

    # 3. Seção 1 — Identificação do Documento
    y = y - hero_h - 24
    c.setFillColor(HexColor("#5B6660"))
    c.setFont("Helvetica-Bold", 8.4)
    c.drawString(M, y, "1. IDENTIFICAÇÃO DO DOCUMENTO")
    y -= 9

    doc_h = 86
    c.setFillColor(HexColor("#FFFFFF"))
    c.setStrokeColor(HexColor("#E3E8E2"))
    c.setLineWidth(0.9)
    c.roundRect(M, y - doc_h, CW, doc_h, 10, stroke=1, fill=1)

    c.setFillColor(HexColor("#F6F8F5"))
    c.rect(M + 0.5, y - 38, CW - 1, 37.5, stroke=0, fill=1)
    c.setStrokeColor(HexColor("#E3E8E2"))
    c.line(M, y - 38, M + CW, y - 38)

    c.setFillColor(HexColor("#7A857E"))
    c.setFont("Helvetica", 7.8)
    c.drawString(M + 14, y - 14, "Título do documento")
    c.setFillColor(HexColor("#16201A"))
    t_size = _fit_font(title, "Helvetica-Bold", 11.2, CW - 28, min_size=8.5)
    c.setFont("Helvetica-Bold", t_size)
    c.drawString(M + 14, y - 28.5, title)

    col_w = CW / 3
    c.setStrokeColor(HexColor("#EEF1ED"))
    c.line(M + col_w, y - 38, M + col_w, y - doc_h)
    c.line(M + col_w * 2, y - 38, M + col_w * 2, y - doc_h)

    cols = [
        ("Origem", origin, "#16201A"),
        ("Código de verificação", code or "Documento externo", "#16201A"),
        ("Integridade do arquivo", integrity, "#B42318" if integrity_bad else "#1F7A3A"),
    ]
    for i, (label, val, color) in enumerate(cols):
        cx = M + col_w * i + 14
        c.setFillColor(HexColor("#7A857E"))
        c.setFont("Helvetica", 7.8)
        c.drawString(cx, y - 53, label)
        c.setFillColor(HexColor(color))
        v_size = _fit_font(val, "Helvetica-Bold", 9.4, col_w - 22, min_size=7.4)
        c.setFont("Helvetica-Bold", v_size)
        c.drawString(cx, y - 69, val)

    # 4. Seção 2 — Signatários Registrados
    y = y - doc_h - 24
    c.setFillColor(HexColor("#5B6660"))
    c.setFont("Helvetica-Bold", 8.4)
    c.drawString(M, y, f"2. SIGNATÁRIOS REGISTRADOS ({len(signers)})")
    y -= 9

    head_h = 24
    row_h = 33
    visible_signers = signers[:7]
    table_h = head_h + max(1, len(visible_signers)) * row_h

    c.setFillColor(HexColor("#FFFFFF"))
    c.setStrokeColor(HexColor("#E3E8E2"))
    c.setLineWidth(0.9)
    c.roundRect(M, y - table_h, CW, table_h, 10, stroke=1, fill=1)

    c.setFillColor(HexColor("#F6F8F5"))
    c.rect(M + 0.5, y - head_h, CW - 1, head_h - 0.5, stroke=0, fill=1)
    c.setStrokeColor(HexColor("#E3E8E2"))
    c.line(M, y - head_h, M + CW, y - head_h)

    c.setFillColor(HexColor("#5B6660"))
    c.setFont("Helvetica-Bold", 8.2)
    c.drawString(M + 12, y - 15.5, "Signatário")
    c.drawString(M + 196, y - 15.5, "CPF (LGPD)")
    c.drawString(M + 286, y - 15.5, "Data e hora")
    c.drawString(M + 392, y - 15.5, "Modalidade")

    if not visible_signers:
        c.setFillColor(HexColor("#5B6660"))
        c.setFont("Helvetica", 9.2)
        c.drawString(M + 12, y - head_h - 20, "Nenhuma assinatura digital encontrada neste arquivo.")
    else:
        for idx, s in enumerate(visible_signers):
            ry = y - head_h - idx * row_h
            if idx > 0:
                c.setStrokeColor(HexColor("#EEF1ED"))
                c.line(M, ry, M + CW, ry)
            s_name = str(s.get("name") or "Signatário")
            s_ev = str(s.get("evidence") or "")
            s_cpf = str(s.get("cpf") or "Conforme certificado")
            s_date = str(s.get("signed_at") or "Pendente")
            s_type = str(s.get("type") or "Confirmada")
            s_status = str(s.get("status") or "valid")

            c.setFillColor(HexColor("#16201A"))
            ns = _fit_font(s_name, "Helvetica-Bold", 9.3, 176, min_size=7.4)
            c.setFont("Helvetica-Bold", ns)
            c.drawString(M + 12, ry - 13.5, s_name)

            if s_ev:
                c.setFillColor(HexColor("#7A857E"))
                c.setFont("Helvetica", 7.2)
                c.drawString(M + 12, ry - 24.5, f"Evidência: {s_ev[:28]}")

            c.setFillColor(HexColor("#3E4942"))
            c.setFont("Helvetica", 8.6)
            c.drawString(M + 196, ry - 19, s_cpf[:22])
            c.drawString(M + 286, ry - 19, s_date[:22])

            type_color = "#294127" if s_status == "valid" else "#B42318" if s_status == "invalid" else "#7A4F00"
            c.setFillColor(HexColor(type_color))
            ts = _fit_font(s_type, "Helvetica-Bold", 8.4, CW - 402, min_size=6.8)
            c.setFont("Helvetica-Bold", ts)
            c.drawString(M + 392, ry - 19, s_type)

    # 5. Seção 3 — Registro de Integridade, QR Code e Base Legal
    y = y - table_h - 24
    c.setFillColor(HexColor("#5B6660"))
    c.setFont("Helvetica-Bold", 8.4)
    c.drawString(M, y, "3. REGISTRO DE INTEGRIDADE E BASE LEGAL")
    y -= 9

    tech_h = 112
    c.setFillColor(HexColor("#F6F8F5"))
    c.setStrokeColor(HexColor("#E3E8E2"))
    c.setLineWidth(0.9)
    c.roundRect(M, y - tech_h, CW, tech_h, 10, stroke=1, fill=1)

    _draw_vector_qr(c, verify_url, M + 14, y - tech_h + 20, 72)

    tx = M + 100
    tw = CW - 114
    c.setFillColor(HexColor("#446A36"))
    u_size = _fit_font(f"Conferência online: {verify_url}", "Helvetica-Bold", 8.8, tw, min_size=7.0)
    c.setFont("Helvetica-Bold", u_size)
    c.drawString(tx, y - 20, f"Conferência online: {verify_url}")

    if sha256_hash:
        c.setFillColor(HexColor("#3E4942"))
        h_font = _fit_font(f"SHA-256: {sha256_hash}", "Courier", 7.4, tw, min_size=5.8)
        c.setFont("Courier", h_font)
        c.drawString(tx, y - 34, f"SHA-256: {sha256_hash}")

    c.setFillColor(HexColor("#5B6660"))
    c.setFont("Helvetica", 7.9)
    p_size = _fit_font(f"Verificação: {policy}", "Helvetica", 7.9, tw, min_size=6.8)
    c.setFont("Helvetica", p_size)
    c.drawString(tx, y - 48, f"Verificação: {policy}")

    seal_lines = _wrap_text(seal_note, "Helvetica", 7.6, tw)[:2]
    c.setFont("Helvetica", 7.6)
    for i, ln in enumerate(seal_lines):
        c.drawString(tx, y - 62 - i * 11, ln)

    c.setFillColor(HexColor("#5B6660"))
    c.setFont("Helvetica-Bold", 7.6)
    c.drawString(tx, y - 96, "Base legal: Lei 14.063/2020 · MP 2.200-2/2001 · LGPD (Lei 13.709/2018)")

    # Rodapé da página
    c.setStrokeColor(HexColor("#E3E8E2"))
    c.setLineWidth(0.7)
    c.line(M, 44, W - M, 44)
    c.setFillColor(HexColor("#7A857E"))
    c.setFont("Helvetica", 7.8)
    c.drawString(M, 29, "SiSFarm · Comprovante gerado eletronicamente · Válido para conferência de autenticidade")
    c.drawRightString(W - M, 29, "Página 1/1")

    c.showPage()
    c.save()
    return buf.getvalue()
