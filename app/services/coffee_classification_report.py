"""Relatório de dados informados pelo avaliador, sem inferir classificação."""
import math
from io import BytesIO
from xml.sax.saxutils import escape

from reportlab.graphics.shapes import Circle, Drawing, Line, Polygon, String
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def build_classification_pdf(analysis, farm, season):
    out = BytesIO()
    doc = SimpleDocTemplate(out, pagesize=A4, leftMargin=42, rightMargin=42, topMargin=36, bottomMargin=36)
    styles = getSampleStyleSheet()
    def p(text):
        return Paragraph(escape(str("Não informado" if text is None or text == "" else text)), styles["BodyText"])
    harvest = analysis.harvest
    rows = [[p("Fazenda"), p(farm.name), p("Safra"), p(season.name)],
            [p("Lote"), p(harvest.lot_code or f"Lote {harvest.id}"), p("Setor"), p(harvest.plot.name)],
            [p("Data"), p(analysis.analysis_date.strftime("%d/%m/%Y")), p("Origem"), p("Informado com laudo" if analysis.source == "laudo" else "Informado pelo usuário")],
            [p("Avaliador"), p(analysis.evaluator), p("Método"), p(analysis.method)],
            [p("Tipo"), p(analysis.coffee_type), p("Peneira"), p(analysis.sieve)],
            [p("Umidade"), p(f"{analysis.moisture}%" if analysis.moisture is not None else None), p("Defeitos"), p(analysis.defects)],
            [p("Bebida"), p(analysis.beverage), p("Nota informada"), p(analysis.sensory_score if analysis.sensory_score is not None else None)]]
    table = Table(rows, colWidths=[68, 170, 70, 170], hAlign="LEFT")
    table.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f5f8f3")),
                               ("GRID", (0, 0), (-1, -1), .4, colors.HexColor("#dce8d8")),
                               ("VALIGN", (0, 0), (-1, -1), "TOP"), ("BOTTOMPADDING", (0, 0), (-1, -1), 8)]))
    story = [Paragraph("SiSFarm · Classificação do café", styles["Title"]), Spacer(1, 10), table, Spacer(1, 12)]
    profile = analysis.sensory_profile
    if profile and profile.attributes:
        pairs = profile.attribute_pairs
        story += [Paragraph("Atributos sensoriais informados", styles["Heading2"]),
                  p(f"Escala declarada: 0 a {profile.scale_max}. Valores não informados não aparecem no gráfico.")]
        if len(pairs) >= 3:
            graph = Drawing(440, 260); cx, cy, radius, n = 220, 130, 88, len(pairs)
            for ratio in (.25, .5, .75, 1):
                points = []
                for i in range(n):
                    angle = math.pi / 2 - 2 * math.pi * i / n
                    points.extend([cx + radius * ratio * math.cos(angle), cy + radius * ratio * math.sin(angle)])
                graph.add(Polygon(points, fillColor=None, strokeColor=colors.HexColor("#dce8d8"), strokeWidth=.6))
            values = []
            for i, (label, value) in enumerate(pairs):
                angle = math.pi / 2 - 2 * math.pi * i / n
                x, y = cx + radius * math.cos(angle), cy + radius * math.sin(angle)
                graph.add(Line(cx, cy, x, y, strokeColor=colors.HexColor("#dce8d8"), strokeWidth=.5))
                graph.add(String(cx + (radius + 16) * math.cos(angle), cy + (radius + 16) * math.sin(angle),
                                 label, fontSize=7, textAnchor="middle", fillColor=colors.HexColor("#294127")))
                px = cx + radius * float(value) / profile.scale_max * math.cos(angle)
                py = cy + radius * float(value) / profile.scale_max * math.sin(angle)
                values.extend([px, py])
                graph.add(Circle(px, py, 2, fillColor=colors.HexColor("#446a36"), strokeColor=None))
            if len(values) == n * 2:
                graph.add(Polygon(values, fillColor=colors.HexColor("#d7ead0"), strokeColor=colors.HexColor("#446a36")))
            story.append(graph)
        story.append(p(" · ".join(f"{label}: {value:g}" for label, value in pairs)))
    if analysis.sensory_notes:
        story += [Spacer(1, 8), Paragraph("Descrição sensorial", styles["Heading2"]), p(analysis.sensory_notes)]
    if analysis.notes:
        story += [Spacer(1, 8), Paragraph("Observações", styles["Heading2"]), p(analysis.notes)]
    story += [Spacer(1, 12), p("Resultados informados pelo usuário. Este relatório não é um laudo oficial nem certifica o método ou a autenticidade dos dados.")]
    doc.build(story)
    return out.getvalue()
