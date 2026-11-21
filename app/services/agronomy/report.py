"""PDF da recomendação agronômica (motor v3), com ilustrações de apoio e referências."""
from __future__ import annotations

import os
from datetime import date
from io import BytesIO
from xml.sax.saxutils import escape

import reportlab
from reportlab.graphics.shapes import Drawing, Ellipse, Line, Polygon, Rect, String
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    CondPageBreak, Image, KeepTogether, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

from app.services.agronomy import references as R

INK, MUTED, FAINT = colors.HexColor("#16201A"), colors.HexColor("#5B6660"), colors.HexColor("#7A857E")
LINE, LINE_SOFT, SOFT, PAGE = colors.HexColor("#E3E8E2"), colors.HexColor("#EEF1ED"), colors.HexColor("#F6F8F5"), colors.HexColor("#F3F5F2")
GREEN, GREEN_DARK, GREEN_LIGHT, GREEN_ACCENT = (colors.HexColor(c) for c in ("#446A36", "#294127", "#EEF3EB", "#9BCB6B"))
AMBER, AMBER_BG = colors.HexColor("#8A5A00"), colors.HexColor("#FFF6E5")
RED, RED_BG = colors.HexColor("#B42318"), colors.HexColor("#FDECEA")
OK, OK_BG = colors.HexColor("#1F7A3A"), colors.HexColor("#E6F4E2")
BLUE, BLUE_BG = colors.HexColor("#1F5FA8"), colors.HexColor("#E8F1FB")
SOIL_TOP, SOIL_SUB = colors.HexColor("#B98B5E"), colors.HexColor("#D9B98F")

STATUS = {"critico": (RED, RED_BG, "Crítico"), "baixo": (AMBER, AMBER_BG, "Baixo"), "atencao": (AMBER, AMBER_BG, "Atenção"),
          "adequado": (OK, OK_BG, "Adequado"), "alto": (BLUE, BLUE_BG, "Alto"), "neutro": (MUTED, PAGE, "—"),
          "ok": (OK, OK_BG, "Em ordem")}
MONTHS = ("jul", "ago", "set", "out", "nov", "dez", "jan", "fev", "mar", "abr", "mai", "jun")
CONTENT_W = A4[0] - 36 * mm
LOGO_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "static", "images", "logo.png")

_FONTS = False
SUBSCRIPTS = {str(d): chr(0x2080 + d) for d in range(10)}


def _fonts():
    global _FONTS
    if _FONTS:
        return
    folder = os.path.join(os.path.dirname(reportlab.__file__), "fonts")
    pdfmetrics.registerFont(TTFont("Sans", os.path.join(folder, "Vera.ttf")))
    pdfmetrics.registerFont(TTFont("Sans-Bold", os.path.join(folder, "VeraBd.ttf")))
    pdfmetrics.registerFont(TTFont("Sans-Italic", os.path.join(folder, "VeraIt.ttf")))
    pdfmetrics.registerFont(TTFont("Sans-BoldItalic", os.path.join(folder, "VeraBI.ttf")))
    pdfmetrics.registerFontFamily("Sans", normal="Sans", bold="Sans-Bold", italic="Sans-Italic", boldItalic="Sans-BoldItalic")
    _FONTS = True


def t(text) -> str:
    """Texto seguro para Paragraph: escapa e troca glifos ausentes na fonte."""
    value = escape(str(text if text is not None else ""))
    for digit, sub in SUBSCRIPTS.items():
        value = value.replace(sub, f"<sub>{digit}</sub>")
    return value.replace("→", "»")


def plain(text) -> str:
    value = str(text or "")
    for digit, sub in SUBSCRIPTS.items():
        value = value.replace(sub, digit)
    return value.replace("→", "»")


def file_slug(text: str) -> str:
    import re
    import unicodedata
    value = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", value).strip("-") or "setor"


def br(value, digits=0) -> str:
    if value is None:
        return "—"
    text = f"{value:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return text


STYLES = {}


def _styles():
    if STYLES:
        return STYLES
    base = dict(fontName="Sans", textColor=INK, leading=13.5, fontSize=9.5, alignment=TA_LEFT)
    STYLES.update({
        "body": ParagraphStyle("body", **base),
        "muted": ParagraphStyle("muted", **{**base, "textColor": MUTED, "fontSize": 8.5, "leading": 12}),
        "small": ParagraphStyle("small", **{**base, "textColor": MUTED, "fontSize": 7.5, "leading": 10}),
        "h1": ParagraphStyle("h1", **{**base, "fontName": "Sans-Bold", "fontSize": 20, "leading": 24, "textColor": colors.white}),
        "kicker": ParagraphStyle("kicker", **{**base, "fontName": "Sans-Bold", "fontSize": 7.5, "textColor": GREEN_ACCENT, "leading": 10}),
        "cover": ParagraphStyle("cover", **{**base, "textColor": colors.HexColor("#DCE7D6"), "fontSize": 9}),
        "h2": ParagraphStyle("h2", **{**base, "fontName": "Sans-Bold", "fontSize": 13.5, "leading": 17, "spaceBefore": 6, "spaceAfter": 2}),
        "h3": ParagraphStyle("h3", **{**base, "fontName": "Sans-Bold", "fontSize": 10.5, "leading": 14}),
        "cell": ParagraphStyle("cell", **{**base, "fontSize": 8.5, "leading": 11.5}),
        "cellb": ParagraphStyle("cellb", **{**base, "fontName": "Sans-Bold", "fontSize": 8.5, "leading": 11.5}),
        "th": ParagraphStyle("th", **{**base, "fontName": "Sans-Bold", "fontSize": 7, "leading": 9, "textColor": FAINT}),
        "big": ParagraphStyle("big", **{**base, "fontName": "Sans-Bold", "fontSize": 17, "leading": 20}),
        "ref": ParagraphStyle("ref", **{**base, "fontSize": 8.5, "leading": 12, "leftIndent": 10, "firstLineIndent": -10, "spaceAfter": 5}),
    })
    return STYLES


def P(text, style="body"):
    return Paragraph(text, _styles()[style])


def section(number, title, subtitle=""):
    items = [CondPageBreak(60 * mm), Spacer(1, 6),
             Table([[P(f'<font color="#446A36">{number}</font>', "h2"), P(t(title), "h2")]],
                   colWidths=[9 * mm, CONTENT_W - 9 * mm],
                   style=[("LEFTPADDING", (0, 0), (-1, -1), 0), ("VALIGN", (0, 0), (-1, -1), "TOP"),
                          ("BOTTOMPADDING", (0, 0), (-1, -1), 0)])]
    if subtitle:
        items.append(P(t(subtitle), "muted"))
    items.append(Spacer(1, 6))
    return items


def card(rows, background=SOFT, border=None, padding=8, widths=None):
    table = Table(rows, colWidths=widths or [CONTENT_W])
    style = [("BACKGROUND", (0, 0), (-1, -1), background), ("LEFTPADDING", (0, 0), (-1, -1), padding),
             ("RIGHTPADDING", (0, 0), (-1, -1), padding), ("TOPPADDING", (0, 0), (-1, -1), padding * .75),
             ("BOTTOMPADDING", (0, 0), (-1, -1), padding * .75), ("VALIGN", (0, 0), (-1, -1), "TOP")]
    if border:
        style.append(("BOX", (0, 0), (-1, -1), .8, border))
    table.setStyle(TableStyle(style))
    return table


def grid(header, rows, widths, zebra=False):
    data = [[P(t(h).upper(), "th") for h in header]] + rows
    table = Table(data, colWidths=widths, repeatRows=1)
    style = [("LINEBELOW", (0, 0), (-1, 0), .8, LINE), ("LINEBELOW", (0, 1), (-1, -2), .4, LINE_SOFT),
             ("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 5),
             ("RIGHTPADDING", (0, 0), (-1, -1), 5), ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5)]
    if zebra:
        style += [("BACKGROUND", (0, i), (-1, i), SOFT) for i in range(2, len(data), 2)]
    table.setStyle(TableStyle(style))
    return table


def badge(status):
    fg, bg, label = STATUS.get(status, STATUS["neutro"])
    return f'<font color="{fg.hexval().replace("0x", "#")}"><b>{label}</b></font>'


# ---------------------------------------------------------------- ilustrações

def illustration_soil_profile(lime, gyp) -> Drawing:
    """Corte do solo: onde o calcário e o gesso agem e a faixa sob a saia."""
    w, h = CONTENT_W, 62 * mm
    d = Drawing(w, h)
    ground = h - 18 * mm
    d.add(Rect(0, ground - 20 * mm, w * .62, 20 * mm, fillColor=SOIL_TOP, strokeColor=None))
    d.add(Rect(0, ground - 40 * mm, w * .62, 20 * mm, fillColor=SOIL_SUB, strokeColor=None))
    d.add(String(4, ground - 6 * mm, "0–20 cm", fontName="Sans-Bold", fontSize=7.5, fillColor=colors.white))
    d.add(String(4, ground - 26 * mm, "20–40 cm", fontName="Sans-Bold", fontSize=7.5, fillColor=colors.HexColor("#6B4F2E")))
    for cx in (w * .16, w * .46):
        d.add(Rect(cx - 1.2, ground, 2.4, 6 * mm, fillColor=colors.HexColor("#6B4F2E"), strokeColor=None))
        d.add(Ellipse(cx, ground + 9 * mm, 15 * mm, 8 * mm, fillColor=GREEN, strokeColor=None))
        d.add(Ellipse(cx - 4 * mm, ground + 11 * mm, 6 * mm, 4 * mm, fillColor=colors.HexColor("#5C8A49"), strokeColor=None))
        d.add(Rect(cx - 15 * mm, ground - 1.6 * mm, 30 * mm, 1.6 * mm, fillColor=colors.white, strokeColor=None))
        for x in range(int(cx - 14 * mm), int(cx + 14 * mm), 9):
            d.add(Line(x, ground - .8 * mm, x + 3, ground - 4 * mm, strokeColor=colors.white, strokeWidth=.8))
        if gyp:
            for x in (cx - 8 * mm, cx, cx + 8 * mm):
                d.add(Line(x, ground - 5 * mm, x, ground - 30 * mm, strokeColor=BLUE, strokeWidth=1.2, strokeDashArray=[2, 2]))
                d.add(Polygon([x - 2, ground - 29 * mm, x + 2, ground - 29 * mm, x, ground - 32 * mm], fillColor=BLUE, strokeColor=None))
    x0 = w * .66
    d.add(String(x0, h - 8 * mm, "Como ler o desenho", fontName="Sans-Bold", fontSize=9, fillColor=INK))
    legend = [(colors.white, LINE, "Faixa sob a saia: onde o calcário é espalhado"),
              (SOIL_TOP, None, "Camada de 0–20 cm: corrigida pelo calcário")]
    if gyp:
        legend += [(SOIL_SUB, None, "Camada de 20–40 cm: o gesso leva cálcio"), (BLUE, None, "O gesso desce com a água da chuva")]
    for i, (fill, stroke, text) in enumerate(legend):
        y = h - 16 * mm - i * 7.5 * mm
        d.add(Rect(x0, y, 4 * mm, 4 * mm, fillColor=fill, strokeColor=stroke or fill, strokeWidth=.6))
        d.add(String(x0 + 6 * mm, y + 1, text, fontName="Sans", fontSize=7.5, fillColor=INK))
    if lime and lime.get("g_m2"):
        d.add(String(x0, 6 * mm, f"Calcário: {br(lime['g_m2'])} g por m² na faixa", fontName="Sans-Bold", fontSize=8, fillColor=GREEN_DARK))
    if gyp and gyp.get("g_m2"):
        d.add(String(x0, 1 * mm, f"Gesso: {br(gyp['g_m2'])} g por m² na faixa", fontName="Sans-Bold", fontSize=8, fillColor=BLUE))
    return d


def illustration_indicators(rows) -> Drawing:
    """Régua de classes por indicador: onde o valor do laudo está."""
    rows = [row for row in rows if row["status"] != "neutro"][:10]
    row_h = 9.5 * mm
    w, h = CONTENT_W, row_h * len(rows) + 10 * mm
    d = Drawing(w, h)
    label_w, bar_x = 52 * mm, 56 * mm
    bar_w = w - bar_x - 34 * mm
    bands = [(RED_BG, "Muito baixo"), (AMBER_BG, "Baixo"), (colors.HexColor("#FBF3D9"), "Médio"), (OK_BG, "Adequado"), (BLUE_BG, "Alto")]
    for i, (fill, label) in enumerate(bands):
        d.add(String(bar_x + bar_w * (i + .5) / 5, h - 5 * mm, label, fontName="Sans", fontSize=6.5, fillColor=FAINT, textAnchor="middle"))
    position = {"critico": .5, "baixo": 1.5, "atencao": 2.5, "adequado": 3.5, "alto": 4.5}
    for index, row in enumerate(rows):
        y = h - 12 * mm - index * row_h
        d.add(String(0, y + 4, plain(row["nome"]), fontName="Sans-Bold", fontSize=8, fillColor=INK))
        d.add(String(0, y - 4, plain(f"Ideal: {row['faixa_ideal']}")[:48], fontName="Sans", fontSize=6.5, fillColor=FAINT))
        for i, (fill, _) in enumerate(bands):
            d.add(Rect(bar_x + bar_w * i / 5, y - 1, bar_w / 5 - 1, 5 * mm - 2, fillColor=fill, strokeColor=None))
        slot = position.get(row["status"], 2.5)
        if row["id"] in ("aluminum", "m", "h_al"):  # acidez: quanto menor, melhor
            slot = {"adequado": 3.5, "atencao": 2.5, "critico": .5}.get(row["status"], 2.5)
        mx = bar_x + bar_w * slot / 5
        d.add(Rect(mx - 1.4, y - 2.5, 2.8, 5 * mm + 1, fillColor=INK, strokeColor=None))
        value = f"{br(row['valor'], 2 if row['valor'] < 10 else 1 if row['valor'] < 100 else 0)} {row['unidade']}".strip()
        d.add(String(w, y + 1, plain(value), fontName="Sans-Bold", fontSize=8, fillColor=INK, textAnchor="end"))
    return d


def illustration_calendar(events, cycle_start: date) -> Drawing:
    """Linha do ano agrícola (jul–jun) com o período chuvoso e as aplicações."""
    w, h = CONTENT_W, 40 * mm
    d = Drawing(w, h)
    left, right, axis = 2 * mm, w - 2 * mm, 16 * mm
    span = right - left
    month_w = span / 12
    d.add(Rect(left + month_w * 3, axis - 3 * mm, month_w * 6, 26 * mm, fillColor=BLUE_BG, strokeColor=None))
    d.add(String(left + month_w * 6, axis + 20 * mm, "Período chuvoso: adubação de N e K", fontName="Sans-Bold", fontSize=7.5,
                 fillColor=BLUE, textAnchor="middle"))
    d.add(Line(left, axis, right, axis, strokeColor=LINE, strokeWidth=1.2))
    for i, name in enumerate(MONTHS):
        x = left + month_w * i
        d.add(Line(x, axis - 1.5 * mm, x, axis + 1.5 * mm, strokeColor=FAINT, strokeWidth=.5))
        d.add(String(x + month_w / 2, axis - 6 * mm, name, fontName="Sans", fontSize=7, fillColor=MUTED, textAnchor="middle"))
    kinds = {"calagem": (INK, "Calagem"), "gessagem": (colors.HexColor("#6B4F2E"), "Gesso"), "adubacao": (GREEN, "Adubação"),
             "foliar": (BLUE, "Foliar"), "agendado": (FAINT, "Já agendado")}
    used, stacks = [], {}
    for event in events:
        day = date.fromisoformat(event["data"])
        months = (day.year - cycle_start.year) * 12 + day.month - cycle_start.month
        offset = (months + (day.day - 1) / 30) % 12
        x = left + month_w * offset
        color, label = kinds.get(event["chave"], (MUTED, ""))
        level = stacks.get(round(offset), 0)
        stacks[round(offset)] = level + 1
        d.add(Ellipse(x, axis + 4 * mm + level * 4.2 * mm, 1.7 * mm, 1.7 * mm, fillColor=color, strokeColor=colors.white, strokeWidth=.8))
        if label and label not in used:
            used.append(label)
    for i, label in enumerate(used):
        color = next(c for c, name in kinds.values() if name == label)
        x = left + i * 30 * mm
        d.add(Ellipse(x + 1.5 * mm, 2.2 * mm, 1.5 * mm, 1.5 * mm, fillColor=color, strokeColor=None))
        d.add(String(x + 4.5 * mm, 1.2 * mm, label, fontName="Sans", fontSize=7.5, fillColor=INK))
    return d


def illustration_nutrients(needs) -> Drawing | None:
    rows = [n for n in needs if n["nutriente"] in ("N", "P2O5", "K2O", "S")]
    if not rows:
        return None
    w, row_h = CONTENT_W, 9 * mm
    h = row_h * len(rows) + 8 * mm
    d = Drawing(w, h)
    top = max(n["dose_kg_ha"] for n in rows) or 1
    bar_x, bar_w = 18 * mm, w - 18 * mm - 40 * mm
    for i, need in enumerate(rows):
        y = h - 8 * mm - i * row_h
        d.add(String(0, y + 1.5, plain(need["label"]), fontName="Sans-Bold", fontSize=9, fillColor=INK))
        full = bar_w * need["dose_kg_ha"] / top
        done = bar_w * (need.get("aplicado_kg_ha", 0) + need.get("agendado_kg_ha", 0)) / top
        d.add(Rect(bar_x, y - 1, full, 5 * mm, fillColor=GREEN, strokeColor=None))
        if done:
            d.add(Rect(bar_x, y - 1, min(done, full), 5 * mm, fillColor=colors.HexColor("#B9CDB0"), strokeColor=None))
        d.add(String(w, y + 1.5, f"falta {br(need['saldo_kg_ha'])} de {br(need['dose_kg_ha'])} kg/ha",
                     fontName="Sans", fontSize=8, fillColor=INK, textAnchor="end"))
    d.add(Rect(bar_x, 0, 3 * mm, 3 * mm, fillColor=GREEN, strokeColor=None))
    d.add(String(bar_x + 4.5 * mm, .5, "falta aplicar no ciclo", fontName="Sans", fontSize=7, fillColor=MUTED))
    d.add(Rect(bar_x + 42 * mm, 0, 3 * mm, 3 * mm, fillColor=colors.HexColor("#B9CDB0"), strokeColor=None))
    d.add(String(bar_x + 46.5 * mm, .5, "já aplicado ou agendado", fontName="Sans", fontSize=7, fillColor=MUTED))
    return d


# ---------------------------------------------------------------- documento

def _qty(item) -> str:
    if item.get("g_100l"):
        return f"{br(item['g_100l'])} g por 100 L de calda"
    if item.get("kg_ha") is not None:
        text = f"{br(item['kg_ha'], 1 if item['kg_ha'] < 100 else 0)} kg/ha"
        if item.get("g_m2"):
            return text + f" · {br(item['g_m2'])} g/m²"
        if item.get("g_planta"):
            text += f" · {br(item['g_planta'])} g/planta"
        if item.get("total_kg"):
            text += f" · {br(item['total_kg'])} kg no setor"
        return text
    if item.get("quantidade") is not None:
        return f"{br(item['quantidade'], 1)} {item.get('unidade') or ''}"
    return ""


def _date_label(value: str) -> str:
    return value.rsplit("/", 1)[0] if value else ""


def build_pdf(plan: dict, *, farm: str, plot: str, analysis_date: date | None, laboratory: str,
              generated_at: str | None = None) -> bytes:
    _fonts()
    s = _styles()
    scope = plan["escopo"]
    buffer = BytesIO()
    title = f"Recomendação agronômica · {plot}"

    def decorate(canvas, doc):
        canvas.saveState()
        canvas.setFillColor(GREEN)
        canvas.rect(0, A4[1] - 4 * mm, A4[0], 4 * mm, stroke=0, fill=1)
        canvas.setFont("Sans", 7.5)
        canvas.setFillColor(FAINT)
        if doc.page > 1:
            canvas.drawString(18 * mm, A4[1] - 12 * mm, plain(f"SiSFarm · {title} · {farm}"))
        canvas.drawString(18 * mm, 10 * mm, "Plano para planejamento. Confira o laudo e os rótulos dos produtos com o responsável técnico antes de aplicar.")
        canvas.drawRightString(A4[0] - 18 * mm, 10 * mm, f"Página {doc.page}")
        canvas.restoreState()

    doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=18 * mm, rightMargin=18 * mm, topMargin=18 * mm,
                            bottomMargin=18 * mm, title=plain(title), author="SiSFarm")
    story = []

    # Capa
    chips = [scope.get("cultura"), scope.get("fase_label"),
             f"{br(scope['produtividade_sc_ha'])} sc/ha" if scope.get("produtividade_sc_ha") else None,
             scope.get("irrigacao_label"), f"{br(scope['plantas_ha'])} plantas/ha" if scope.get("plantas_ha") else None,
             f"{br(scope['area_ha'], 2)} ha" if scope.get("area_ha") else None]
    sample = scope.get("amostra", {})
    cover = [
        [P("RECOMENDAÇÃO AGRONÔMICA", "kicker")],
        [P(t(f"{plot} · {farm}"), "h1")],
        [P(t(f"Laudo de {analysis_date.strftime('%d/%m/%Y') if analysis_date else '—'} · {laboratory} · amostra "
             f"{sample.get('profundidade', '')}{' + 20–40 cm' if scope.get('subsolo') else ''}"
             + (f" · gerado em {generated_at}" if generated_at else "")), "cover")],
        [P("  ·  ".join(t(c) for c in chips if c), "cover")],
    ]
    logo_w = 30 * mm
    if os.path.exists(LOGO_PATH):
        badge_box = Table([[Image(LOGO_PATH, width=logo_w, height=logo_w * 312 / 392)]], colWidths=[logo_w + 8 * mm])
        badge_box.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), colors.white), ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                                       ("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("TOPPADDING", (0, 0), (-1, -1), 4 * mm),
                                       ("BOTTOMPADDING", (0, 0), (-1, -1), 4 * mm), ("ROUNDEDCORNERS", [10, 10, 10, 10])]))
        text_w = CONTENT_W - logo_w - 8 * mm - 28
        cover_text = Table(cover, colWidths=[text_w - 12])
        cover_text.setStyle(TableStyle([("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 0)]))
        story.append(card([[cover_text, badge_box]], background=GREEN_DARK, padding=14,
                          widths=[text_w, logo_w + 8 * mm + 28]))
    else:
        story.append(card(cover, background=GREEN_DARK, padding=14))
    story.append(Spacer(1, 10))
    if plan.get("resumo"):
        origin = "Texto escrito por IA sobre o plano calculado pelo sistema" if plan.get("ia", {}).get("usada") else "Resumo gerado pelo sistema"
        story.append(card([[P("Leitura do agrônomo", "h3")], [P(t(plan["resumo"]))], [P(t(origin), "small")]],
                          background=colors.white, border=LINE, padding=10))
        story.append(Spacer(1, 10))

    # 1. Prioridades
    story += section(1, "O que fazer, em ordem", "Corrija o solo antes de adubar: sem a correção, parte do adubo não é aproveitada.")
    rows = []
    for index, row in enumerate(plan.get("prioridades", []), start=1):
        number = Table([[P(f'<para alignment="center"><font color="white"><b>{index}</b></font></para>', "h3")]], colWidths=[8 * mm], rowHeights=[8 * mm],
                       style=[("BACKGROUND", (0, 0), (-1, -1), GREEN), ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                              ("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("LEFTPADDING", (0, 0), (-1, -1), 0),
                              ("RIGHTPADDING", (0, 0), (-1, -1), 0), ("TOPPADDING", (0, 0), (-1, -1), 0)])
        rows.append([number, [P(t(row["titulo"]), "h3"), P(t(row["detalhe"]), "muted")],
                     P(f'<font color="#294127"><b>{t(row.get("quando") or "")}</b></font>', "cell")])
    if rows:
        table = Table(rows, colWidths=[12 * mm, CONTENT_W - 36 * mm, 24 * mm])
        table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("BACKGROUND", (0, 0), (-1, -1), SOFT),
                                   ("LINEBELOW", (0, 0), (-1, -2), 3, colors.white), ("TOPPADDING", (0, 0), (-1, -1), 7),
                                   ("BOTTOMPADDING", (0, 0), (-1, -1), 7), ("ALIGN", (2, 0), (2, -1), "RIGHT")]))
        story.append(table)
    if plan.get("pendencias"):
        story.append(Spacer(1, 8))
        items = [[P(f'<font color="#6B4700"><b>Dados que faltam para fechar o plano</b></font>', "h3")]]
        items += [[P(f'<font color="#6B4700">• <b>{t(p["dado"])}.</b> {t(p["motivo"])} {t(p["impacto"])}</font>', "cell")]
                  for p in plan["pendencias"]]
        story.append(card(items, background=AMBER_BG, padding=9))

    # 2. Diagnóstico
    header = section(2, "Diagnóstico do solo", "Cada indicador do laudo comparado às classes das tabelas oficiais. "
                     "A barra preta mostra onde o seu valor está.")
    story += [header[0], KeepTogether(header[1:] + [illustration_indicators(plan.get("indicadores", []))])]
    story.append(Spacer(1, 8))
    rows = [[P(badge(f["nivel"]), "cell"), [P(t(f["titulo"]), "cellb"), P(t(f["texto"]), "muted")]] for f in plan.get("diagnostico", [])]
    if rows:
        story.append(grid(["Situação", "O que o laudo mostra"], rows, [22 * mm, CONTENT_W - 22 * mm]))

    # 3. Correção do solo
    corrections = {c["tipo"]: c for c in plan.get("corretivos", [])}
    lime, gyp = corrections.get("calagem") or {}, corrections.get("gessagem") or {}
    events = plan.get("cronograma", [])
    if lime or gyp:
        story += section(3, "Correção do solo: calagem e gessagem")
        cells = []
        for item in (lime, gyp):
            if not item:
                continue
            when = next((e["quando"] for e in events if e["chave"] == item["tipo"]), "")
            if item.get("necessario"):
                dose = item.get("dose_t_ha") or item.get("dose_prnt100_t_ha")
                prnt = "" if item["tipo"] != "calagem" else (" (PRNT do lote)" if item.get("dose_t_ha") else " (PRNT 100 %)")
                total = f" · {br(dose * scope['area_ha'], 1)} t no setor" if scope.get("area_ha") and dose else ""
                block = [P(t(item["titulo"]), "h3"), P(f"{br(dose, 2)} t/ha", "big"),
                         P(t(f"{br(item.get('g_m2'))} g/m² na faixa{total}{prnt}"), "muted"), Spacer(1, 4),
                         P(f"<b>Produto:</b> {t(item.get('produto'))}", "cell"),
                         P(f"<b>Como:</b> {t(item.get('metodo'))}", "cell"),
                         P(f"<b>Quando:</b> {t(when)}" + (" · em 2 parcelas, com 6 meses ou mais entre elas" if item.get("parcelas", 1) > 1 else ""), "cell"),
                         P(f"<b>Por quê:</b> {t(item.get('justificativa'))}", "cell")]
            else:
                state = "Não necessária" if item.get("necessario") is False else "Dados incompletos"
                block = [P(t(item["titulo"]), "h3"), P(t(state), "big"), P(t(item.get("justificativa") or "Veja os dados que faltam."), "muted")]
            cells.append(block)
        widths = [CONTENT_W / len(cells) - 2 * mm] * len(cells)
        row = Table([cells], colWidths=widths)
        row.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), SOFT), ("VALIGN", (0, 0), (-1, -1), "TOP"),
                                 ("LEFTPADDING", (0, 0), (-1, -1), 9), ("RIGHTPADDING", (0, 0), (-1, -1), 9),
                                 ("TOPPADDING", (0, 0), (-1, -1), 8), ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
                                 ("LINEAFTER", (0, 0), (-2, -1), 6, colors.white)]))
        story.append(row)
        if lime.get("necessario") or gyp.get("necessario"):
            story.append(Spacer(1, 10))
            story.append(KeepTogether([P("Onde e como os corretivos agem", "h3"), Spacer(1, 4),
                                       illustration_soil_profile(lime if lime.get("necessario") else None,
                                                                 gyp if gyp.get("necessario") else None)]))

    # 4. Adubação
    needs = plan.get("necessidades", []) + plan.get("micronutrientes", [])
    if needs or plan.get("produtos"):
        story += section(4, "Adubação do ciclo", f"Desconta o que já foi aplicado ou agendado desde "
                         f"{'/'.join(reversed(plan['historico']['desde'].split('-')))}.")
        chart = illustration_nutrients(plan.get("necessidades", []))
        if chart:
            story += [chart, Spacer(1, 8)]
        rows = [[P(t(n["label"]), "cellb"), P(br(n["dose_kg_ha"], 1 if n["dose_kg_ha"] < 20 else 0), "cell"),
                 P(br((n.get("aplicado_kg_ha") or 0) + (n.get("agendado_kg_ha") or 0)), "cell"),
                 P(f"<b>{br(n['saldo_kg_ha'], 1 if n['saldo_kg_ha'] < 20 else 0)}</b>", "cell"),
                 P(t(n["calculo"]["referencia"]["criterio"] + (" · via foliar" if n.get("via") == "foliar" else "")), "muted")]
                for n in needs]
        if rows:
            story.append(grid(["Nutriente", "Necessidade kg/ha", "Já feito", "Falta", "Critério"], rows,
                              [24 * mm, 30 * mm, 22 * mm, 20 * mm, CONTENT_W - 96 * mm], zebra=True))
            story.append(Spacer(1, 10))
        rows, spans = [], []
        for product in plan.get("produtos", []):
            guarantee = " · ".join(f"{br(v)} % {k}" for k, v in (product.get("garantia") or {}).items())
            stock = product.get("estoque")
            status = (stock or {}).get("situacao") or ("Em estoque" if stock else "Comprar")
            color = "#1F7A3A" if status.startswith(("Saldo", "Em estoque")) else "#B42318" if status.startswith("Faltam") else "#8A5A00"
            corrective = product["produto"].lower().startswith(("calcário", "gesso"))
            if product.get("dose_texto"):
                spans.append(len(rows) + 1)
                dose_cells = [P(t(product["dose_texto"]), "cell"), P("", "cell"), P("", "cell")]
            else:
                dose_cells = [P(br(product["kg_ha"], 1 if product["kg_ha"] < 100 else 0), "cell"),
                              P("—" if corrective else br(product.get("g_planta")), "cell"),
                              P(f"<b>{br(product.get('total_kg'))} kg</b>" if product.get("total_kg") is not None else "—", "cell")]
            rows.append([[P(t(product["produto"]), "cellb"), P(t(guarantee), "small")], *dose_cells,
                         [P(t(product.get("finalidade")), "cell"), P(f'<font color="{color}">{t(status)}</font>', "small")]])
        if rows:
            table = grid(["Produto", "kg/ha no ciclo", "g/planta", "Total no setor", "Para quê · estoque"], rows,
                         [44 * mm, 24 * mm, 18 * mm, 26 * mm, CONTENT_W - 112 * mm], zebra=True)
            table.setStyle(TableStyle([("SPAN", (1, i), (3, i)) for i in spans]))
            story.append(KeepTogether([P("Produtos e quantidades", "h3"), Spacer(1, 3), table,
                                       P("Calcário e gesso são espalhados na faixa: use a dose em g/m², não por planta.", "small")]))

    # 5. Cronograma
    if events:
        story += section(5, "Cronograma de aplicações", "Ano agrícola de julho a junho. Siga a ordem: corretivos na seca, "
                         "adubação parcelada nas chuvas.")
        start = date.fromisoformat(plan["historico"]["desde"])
        story += [illustration_calendar(events, start), Spacer(1, 8)]
        rows = []
        for event in events:
            items = "<br/>".join(f"<b>{t(i['produto'])}</b> — {t(_qty(i))}" for i in event.get("itens", []))
            note = t(event.get("finalidade") or "") + (f" {t(event['observacao'])}" if event.get("observacao") else "")
            rows.append([P(f"<b>{t(_date_label(event['quando']))}</b><br/>{t(event['quando'].rsplit('/', 1)[-1])}", "cell"),
                         [P(t(event["operacao"]), "cellb"), P(t(event.get("metodo")), "small")],
                         [P(items, "cell"), P(note, "small")]])
        story.append(grid(["Data", "Operação", "Produtos e doses"], rows, [16 * mm, 50 * mm, CONTENT_W - 66 * mm]))

    # 6. Cuidados
    cares = plan.get("compatibilidade", []) + [{"texto": text} for text in plan.get("orientacoes", [])]
    if cares or plan.get("alertas"):
        story += section(6, "Cuidados na aplicação")
        story += [P(f"• {t(c['texto'])}", "body") for c in cares]
        if plan.get("alertas"):
            story.append(Spacer(1, 6))
            story.append(card([[P('<font color="#6B4700"><b>Confira no laudo</b></font>', "h3")]] +
                              [[P(f'<font color="#6B4700">• {t(a)}</font>', "cell")] for a in plan["alertas"]],
                              background=AMBER_BG, padding=9))

    # 7. Memória de cálculo
    traces = [c for c in plan.get("corretivos", []) + plan.get("necessidades", []) + plan.get("micronutrientes", []) if c.get("calculo")]
    if traces:
        story += section(7, "Memória de cálculo", "Como cada dose foi obtida, para conferência do responsável técnico.")
        for item in traces:
            calc = item["calculo"]
            block = [P(t(item.get("titulo") or item.get("label")), "cellb"),
                     P(f'<font face="Courier" size="8">{t(calc["formula"])}</font>', "cell")]
            block += [P(f"{i}. {t(step)}", "small") for i, step in enumerate(calc["passos"], start=1)]
            block.append(P(t(f"Fonte: {calc['referencia']['criterio']}"), "small"))
            story.append(KeepTogether([card([[block]], background=SOFT, padding=8), Spacer(1, 5)]))
    if plan.get("premissas"):
        story.append(Spacer(1, 4))
        story.append(P("Premissas usadas", "h3"))
        story += [P(f"• {t(p)}", "muted") for p in plan["premissas"]]

    # Referências
    codes = [ref.get("id") for ref in plan.get("referencias", []) if ref.get("id") in R.REFERENCES]
    if "manual_cafe_mg" in codes:  # as tabelas do Manual são transcritas da 5ª Aproximação
        codes.append("cfsemg_5a")
    refs = [R.REFERENCES[code] for code in dict.fromkeys(codes)]
    if refs:
        story += [CondPageBreak(50 * mm), Spacer(1, 10), P("Referências bibliográficas", "h2"),
                  P("Publicações técnicas usadas nas tabelas e fórmulas deste plano.", "muted"), Spacer(1, 6)]
        for ref in sorted(refs, key=lambda item: item["abnt"]):
            link = f' Disponível em: <font color="#446A36">{escape(ref["url"])}</font>.' if ref.get("url") else ""
            story.append(P(ref["abnt"] + link, "ref"))
    story.append(Spacer(1, 10))
    story.append(P(t(f"{plan.get('motor', 'SiSFarm')}. As doses vêm de cálculo determinístico sobre tabelas oficiais; "
                     "a IA apenas redige textos explicativos."), "small"))
    doc.build(story, onFirstPage=decorate, onLaterPages=decorate)
    return buffer.getvalue()
