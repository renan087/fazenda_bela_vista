# Agro ERP Cafe

Sistema profissional de gestao agricola para fazenda de cafe, com backend FastAPI, PostgreSQL, autenticacao, dashboard analitico, mapa da fazenda e interface web estilo ERP.

## Principais modulos

- Dashboard executivo com KPIs, previsao de producao e graficos em Chart.js
- Cadastro completo de talhoes
- Cadastro de variedades de cafe
- Irrigacao por talhao
- Fertilizacao com produto, dose e custo
- Producao e colheita com produtividade por hectare
- Pragas e doencas da lavoura
- Mapa da fazenda com Leaflet e OpenStreetMap
- Interface mobile para registro rapido em campo

## Arquitetura

```text
app/
  core/            # configuracao, seguranca, csrf e dependencias
  db/              # sessao, bootstrap e sincronizacao basica do schema
  models/          # modelos SQLAlchemy do dominio agricola
  repositories/    # acesso a dados
  services/        # regras de negocio, dashboard e previsao
  routers/         # autenticacao e API JSON
  web/             # rotas HTML do frontend
  templates/       # interface ERP com Tailwind, Chart.js e Leaflet
  static/          # estilos complementares
scripts/
  init_db.py       # inicializacao do banco e seed inicial
```

## Funcionalidades do dashboard

- area total plantada
- numero de talhoes
- producao estimada
- producao total
- produtividade media por hectare
- previsao baseada em historico de colheitas
- irrigacao recente
- mapa dos talhoes

## Como executar localmente

1. Copie o ambiente:

```bash
cp .env.example .env
```

2. Instale dependencias:

```bash
pip install -r requirements.txt
```

3. Inicialize o banco:

```bash
python scripts/init_db.py
```

4. Rode a aplicacao:

```bash
uvicorn app.main:app --reload
```

## Credenciais iniciais

- Email: `admin@fazenda.local`
- Senha: `admin123`

## Publicacao

O projeto permanece preparado para deploy no Render com `render.yaml`.
