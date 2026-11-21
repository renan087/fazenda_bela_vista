"""Referências técnicas e tabelas usadas pelo motor agronômico.

Todos os números deste arquivo foram transcritos das publicações abaixo (tabela e página
indicadas em cada constante). Nenhum outro módulo deve conter valores de tabela: os
cálculos importam daqui, o que mantém a rastreabilidade de cada dose.
"""

REFERENCES = {
    "manual_cafe_mg": {
        "titulo": "Manual do Café — Manejo de cafezais em produção (Emater-MG), tabelas transcritas da "
                  "5ª Aproximação (CFSEMG, 1999)",
        "url": "https://www.emater.mg.gov.br/download.do?id=17572",
        "escopo": "Café arábica em produção — Minas Gerais e regiões com recomendação semelhante.",
    },
    "cfsemg_5a": {
        "titulo": "Recomendações para o uso de corretivos e fertilizantes em Minas Gerais — 5ª Aproximação "
                  "(Ribeiro, Guimarães & Alvarez V., 1999)",
        "url": "https://www.infoteca.cnptia.embrapa.br/infoteca/handle/doc/1148365",
        "escopo": "Interpretação de análise de solo, calagem e gessagem.",
    },
    "incaper_conilon": {
        "titulo": "Incaper — Café Conilon, cap. 14: Calagem e adubação (Prezotti et al.; 2019)",
        "url": "https://biblioteca.incaper.es.gov.br/digital/bitstream/123456789/3531/1/chapter-14-liming-fertilization.pdf",
        "escopo": "Café conilon/robusta (Coffea canephora) — plantio, formação e produção.",
    },
    "embrapa_conilon_pa": {
        "titulo": "Embrapa — Recomendações de calagem e adubação para o Pará, cap. Café Conilon (Veloso & Augusto, 2020)",
        "url": "https://www.infoteca.cnptia.embrapa.br/infoteca/bitstream/doc/1127269/1/LV-RecomendacaoSolo-2020-269-274.pdf",
        "escopo": "Parcelamento (40/30/30) e calagem em faixa para café conilon.",
    },
    "embrapa_cafe_mg": {
        "titulo": "Embrapa Café — Boas práticas agrícolas aplicadas à lavoura cafeeira para o estado de Minas Gerais (2022)",
        "url": "https://www.infoteca.cnptia.embrapa.br/handle/doc/1148365",
        "escopo": "Café arábica em formação — Tabela 2 (g por cova).",
    },
}

CLASSES5 = ("muito_baixo", "baixo", "medio", "bom", "muito_bom")
CLASS_LABELS = {
    "muito_baixo": "Muito baixo", "baixo": "Baixo", "medio": "Médio", "bom": "Bom", "muito_bom": "Muito bom",
    "adequado": "Adequado", "alto": "Alto", "muito_alto": "Muito alto",
}

# Manual do Café, Tabela 01 (p. 24) — limites superiores de cada classe (muito baixo … bom).
SOIL_CLASSES_MG = {
    "organic_matter_dag": (0.70, 2.00, 4.00, 7.00),
    "calcium": (0.40, 1.20, 2.40, 4.00),
    "magnesium": (0.15, 0.45, 0.90, 1.50),
    "aluminum": (0.20, 0.50, 1.00, 2.00),
    "h_al": (1.00, 2.50, 5.00, 9.00),
    "sb": (0.60, 1.80, 3.60, 6.00),
    "t_effective": (0.80, 2.30, 4.60, 8.00),
    "ctc": (1.60, 4.30, 8.60, 15.00),
    "m": (15.0, 30.0, 50.0, 75.0),
    "base_saturation": (20.0, 40.0, 60.0, 80.0),
}
# Para acidez (Al, H+Al, m) as classes “bom/muito bom” significam alto/muito alto (nota da Tabela 01).
INVERTED_SOIL_FIELDS = {"aluminum", "h_al", "m"}

# Manual do Café, Tabela 05 (p. 26) — classificação agronômica do pH em água.
PH_WATER_LIMITS = (4.4, 5.4, 6.0, 7.0)  # muito baixo | baixo | bom | alto | muito alto

# Manual do Café, Tabela 03 (p. 25) — P Mehlich-1 (mg/dm³), limites superiores MB/B/M/Bom.
P_CLASSES_BY_CLAY = (  # (argila mín %, argila máx %, limites)
    (60, 100, (1.9, 4.0, 6.0, 9.0)),
    (35, 60, (3.0, 6.0, 9.0, 13.5)),
    (15, 35, (5.0, 9.0, 15.0, 22.5)),
    (0, 15, (7.5, 15.0, 22.5, 33.8)),
)
P_CLASSES_BY_PREM = (  # (P-rem mín, P-rem máx mg/L, limites)
    (0, 4, (2.3, 3.2, 4.5, 6.8)),
    (4, 10, (3.0, 4.5, 6.2, 9.4)),
    (10, 19, (4.5, 6.2, 8.3, 13.1)),
    (19, 30, (6.0, 8.5, 11.9, 18.0)),
    (30, 44, (8.3, 11.9, 16.4, 24.8)),
    (44, 60, (11.3, 16.4, 22.5, 33.8)),
)
# Manual do Café, Tabela 04 (p. 26) — S disponível (mg/dm³) por P-rem, limites superiores MB/B/M/Bom.
S_CLASSES_BY_PREM = (
    (0, 4, (1.7, 2.5, 3.6, 5.4)),
    (4, 10, (2.4, 3.6, 5.0, 7.5)),
    (10, 19, (3.3, 5.0, 6.9, 10.3)),
    (19, 30, (4.6, 6.9, 9.4, 14.2)),
    (30, 44, (6.4, 9.4, 13.0, 19.6)),
    (44, 60, (8.9, 13.0, 18.0, 27.0)),
)
# Incaper cap. 14, Tabela 3 — S (mg/dm³): baixo < 5, médio 5–10, alto > 10 (sem P-rem).
S_CLASSES_SIMPLE = (5.0, 10.0)

# Classes de K (mg/dm³, Mehlich-1) usadas nas tabelas de dose do cafeeiro (Manual Tab. 15; Incaper Tab. 7).
K_COFFEE_LIMITS = (60.0, 120.0, 200.0)  # baixo | médio | bom | muito bom
K_COFFEE_CLASSES = ("baixo", "medio", "bom", "muito_bom")

# ---------------------------------------------------------------- calagem e gessagem
LIMING_ARABICA = {
    "ref": "manual_cafe_mg", "pagina": "p. 34–40",
    "v_target": 60.0,          # V2 para café (p. 37)
    "x_factor": 3.0,           # X para cafeeiro (p. 34)
    "mt": 25.0,                # máxima saturação por Al tolerada (p. 36)
    "pf_established_cm": 7.0,  # incorporação natural sob a saia em lavoura formada (p. 39)
    "pf_with_gypsum_cm": 20.0,  # com gessagem, manter dose de 0–20 cm (p. 42)
    "pf_new_cm": 20.0,
}
# Manual do Café, Tabela 12 (p. 36) — Y por argila (interpolado) e Tabela 13 — Y por P-rem.
Y_BY_CLAY = ((0, 0.0), (15, 1.0), (35, 2.0), (60, 3.0), (100, 4.0))
Y_BY_PREM = ((0, 4.0), (4, 3.5), (10, 2.9), (19, 2.0), (30, 1.2), (44, 0.5), (60, 0.0))

LIMING_CONILON = {
    "ref": "incaper_conilon", "pagina": "p. 425–427",
    "v_target": 60.0, "v_target_high_yield": 70.0, "high_yield_sacks": 60.0,
    "p_surface": 0.5, "p_incorporated": 1.0,
    "split_above_t_ha": 1.5,   # Embrapa Pará: parcelar acima de 1,5 t/ha em áreas produtivas
}
LIMING_SPLIT_ABOVE_T_HA = 3.0  # Embrapa Pará (p. 269): acima de 3 t/ha, parcelar com 6 meses
LIMING_MIN_DAYS_BEFORE_FERTILIZATION = 60  # Embrapa Pará (p. 268)

# Gessagem — Manual do Café p. 32 (5ª Aproximação) e Incaper p. 428–429.
GYPSUM_CRITERIA = {"calcium_max": 0.4, "aluminum_min": 0.5, "m_min": 30.0}
GYPSUM_BY_PREM = ((0, 1.680), (4, 1.333), (10, 1.013), (19, 0.720), (30, 0.453), (44, 0.213), (60, 0.0))
GYPSUM_BY_CLAY = ((0, 0.0), (15, 0.4), (35, 0.8), (60, 1.2), (100, 1.6))
GYPSUM_FRACTION_OF_NC_ARABICA = 0.25
GYPSUM_FRACTION_OF_QC_CONILON = 0.30

# ---------------------------------------------------------------- adubação arábica em produção
YIELD_ROWS_ARABICA = (20, 30, 40, 50, 60, float("inf"))
# Manual do Café, Tabela 15 (p. 43) — N “sem análise foliar” (kg/ha/ano) por faixa de produtividade.
N_ARABICA_NO_LEAF = (200, 250, 300, 350, 400, 450)
# Mesma tabela — colunas por teor foliar de N (baixo < 2,5; adequado 2,6–3,0; alto 3,1–3,5 dag/kg).
N_ARABICA_BY_LEAF = {
    "baixo": (200, 250, 300, 350, 400, 450),
    "adequado": (140, 175, 220, 260, 300, 340),
    "alto": (80, 110, 140, 170, 200, 230),
}
# Mesma tabela — K2O por K no solo (baixo, médio, bom, muito bom).
K2O_ARABICA = (
    (200, 150, 100, 0), (250, 190, 125, 0), (300, 225, 150, 0),
    (350, 260, 175, 50), (400, 300, 200, 75), (450, 340, 225, 100),
)
# Manual do Café, Tabela 14 (p. 43) — P2O5 por classe de P (MB, B, M, Bom, MBom).
P2O5_ARABICA = (
    (30, 20, 10, 0, 0), (40, 30, 20, 0, 0), (50, 40, 25, 0, 0),
    (60, 50, 30, 15, 0), (70, 55, 35, 18, 0), (80, 60, 40, 20, 0),
)
# Manual do Café p. 47 — S: 1/8 da dose de N (teor baixo ou sem análise); 1/16 com teor médio.
S_FRACTION_OF_N = {"muito_baixo": 1 / 8, "baixo": 1 / 8, "sem_analise": 1 / 8, "medio": 1 / 16}

# Embrapa Café MG (2022), Tabela 2 — formação do arábica (g por cova). A coluna de K2O é anual;
# o intervalo da coluna de N não é explícito na fonte e fica para conferência técnica.
FORMATION_ARABICA = {
    1: {"N": 10, "K2O": (40, 20, 10, 0)},
    2: {"N": 20, "K2O": (60, 40, 20, 0)},
}

# ---------------------------------------------------------------- adubação conilon
YIELD_ROWS_CONILON = (20, 30, 50, 70, 100, 130, 170, float("inf"))
# Incaper cap. 14, Tabela 7 — N e K2O (kg/ha/ano) por produtividade e K no solo.
N_CONILON = (200, 260, 320, 380, 440, 500, 560, 620)
K2O_CONILON = (
    (170, 100, 30, 0), (230, 160, 90, 0), (290, 220, 150, 0), (350, 280, 210, 80),
    (410, 340, 270, 140), (470, 400, 330, 200), (530, 460, 390, 260), (600, 520, 450, 320),
)
# Incaper cap. 14, Tabela 8 — classes de P (mg/dm³) por P-rem/textura e doses de P2O5.
P_CLASSES_CONILON = {  # limites superiores muito baixo | baixo | médio → acima = alto
    "argilosa": (3.0, 6.0, 10.0),
    "media": (5.0, 10.0, 20.0),
    "arenosa": (10.0, 20.0, 30.0),
}
P2O5_CONILON = (
    (30, 20, 0, 0), (45, 35, 0, 0), (60, 45, 0, 0), (75, 60, 20, 0),
    (90, 75, 35, 0), (105, 90, 50, 20), (120, 105, 65, 40), (140, 120, 80, 60),
)
S_MIN_CONILON_LOW = 50.0  # Embrapa Pará p. 271: S baixo → mínimo de 50 kg/ha de S
# Incaper cap. 14, Tabela 6 — formação (g por planta por aplicação; 3 aplicações nas chuvas).
FORMATION_CONILON = {
    1: {"N": 15, "K2O": (30, 20, 10, 0)},
    2: {"N": 30, "K2O": (40, 30, 20, 0)},
}
FORMATION_CONILON_APPLICATIONS = 3
# Incaper p. 433 — após o pegamento: 3 aplicações de 7 g N + 15 g K2O por planta (5 g se K > 80).
POST_PLANTING_CONILON = {"N": 7, "K2O": 15, "K2O_high_k": 5, "k_limit": 80.0, "applications": 3}

# ---------------------------------------------------------------- micronutrientes
# Manual do Café, Tabelas 02 e 19 (p. 25 e 50) — limites superiores baixa | média | adequada e doses (kg/ha).
MICROS_ARABICA = {
    "boron": {"Água quente": (0.20, 0.40, 0.60), "Mehlich-1": (0.29, 0.70, 1.00),
              "doses": (3.0, 2.0, 1.0, 0.0), "nutriente": "B"},
    "copper": {"Mehlich-1": (0.49, 1.00, 1.50), "DTPA": (0.29, 0.60, 1.00),
               "doses": (3.0, 2.0, 1.0, 0.0), "nutriente": "Cu"},
    "manganese": {"Mehlich-1": (4.9, 10.0, 15.0), "DTPA": (1.0, 2.5, 5.0),
                  "doses": (15.0, 10.0, 5.0, 0.0), "nutriente": "Mn"},
    "zinc": {"Mehlich-1": (1.9, 4.0, 6.0), "DTPA": (0.69, 1.10, 1.50),
             "doses": (6.0, 4.0, 2.0, 0.0), "nutriente": "Zn"},
}
MICRO_CLASSES = ("baixo", "medio", "adequado", "alto")
# Incaper cap. 14, Tabela 9 — limites baixo | médio e doses (kg/ha) para conilon.
MICROS_CONILON = {
    "zinc": {"Mehlich-1": (1.99, 6.0), "doses": (3.0, 2.0, 0.0), "nutriente": "Zn"},
    "boron": {"Água quente": (0.19, 0.6), "doses": (2.0, 1.0, 0.0), "nutriente": "B"},
    "copper": {"Mehlich-1": (0.49, 1.5), "doses": (3.0, 2.0, 0.0), "nutriente": "Cu"},
    "manganese": {"Mehlich-1": (4.99, 15.0), "doses": (15.0, 10.0, 0.0), "nutriente": "Mn"},
}
# Incaper p. 435 — calda foliar: sulfato de zinco, ácido bórico e sulfato de cobre a 0,3 %; KCl 0,3 %.
FOLIAR_CONCENTRATION_PCT = 0.3
FOLIAR_SPRAYS = 3
FOLIAR_INTERVAL_DAYS = 60

# ---------------------------------------------------------------- parcelamento
# Manual do Café p. 47: N e K em 3 ou 4 parcelas no período chuvoso; P de uma vez, localizado.
SPLIT_ARABICA_RAINFED = {"parcelas_ate_250_n": 3, "parcelas_acima": 4}
# Embrapa Pará p. 270–271: 40 % no início das chuvas e 30 % + 30 % a cada 50–60 dias.
SPLIT_CONILON = (0.40, 0.30, 0.30)


def ref_info(code: str, criterio: str) -> dict:
    ref = REFERENCES[code]
    return {"id": code, "titulo": ref["titulo"], "url": ref["url"], "criterio": criterio}
