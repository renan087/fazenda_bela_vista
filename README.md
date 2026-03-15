# Sistema Web para Fazenda de Cafe

Projeto completo com FastAPI, PostgreSQL, autenticacao de usuarios, interface web responsiva e API REST para gerenciamento de uma fazenda de cafe.

## Funcionalidades

- Cadastro de talhoes
- Cadastro de variedades de cafe
- Registro de irrigacao por talhao
- Registro de adubacao com produto, dose e custo
- Registro de aplicacao de defensivos
- Registro de colheita com producao em sacas
- Login de usuarios
- Interface web simples e responsiva
- API REST protegida por JWT

## Estrutura

```text
app/
  api/               # reservado para expansoes futuras
  core/              # configuracao, seguranca e dependencias
  crud/              # regras de acesso a dados
  db/                # sessao, base e bootstrap do banco
  models/            # modelos SQLAlchemy
  routers/           # rotas web e API
  schemas/           # schemas Pydantic
  static/            # css e js
  templates/         # paginas HTML
scripts/
  init_db.py         # cria tabelas e usuario administrador
```

## Como executar com Docker

1. Copie o arquivo de ambiente:

```bash
cp .env.example .env
```

2. Suba os containers:

```bash
docker compose up --build
```

3. Acesse:

- Web: http://localhost:8000
- Docs da API: http://localhost:8000/docs

Credenciais iniciais:

- Email: `admin@fazenda.local`
- Senha: `admin123`

## Como publicar no Render

1. Envie este projeto para um repositorio GitHub
2. No Render, escolha a opcao de criar recurso a partir do arquivo `render.yaml`
3. O blueprint criara:

- um web service Python
- um banco PostgreSQL gerenciado
- variaveis de ambiente essenciais

4. Apos o deploy, acesse a URL publica gerada pelo Render

Observacoes:

- O Render fornece a porta por variavel `PORT`, que o projeto ja usa
- O banco e lido por `DATABASE_URL_OVERRIDE`, tambem ja suportado no projeto
- O usuario administrador inicial e criado automaticamente na primeira inicializacao

Se quiser automatizar via API do Render, gere uma API key na sua conta e use-a para criar o blueprint ou o servico programaticamente.

## Como executar localmente

1. Crie e ative um ambiente virtual
2. Instale dependencias:

```bash
pip install -r requirements.txt
```

3. Configure o PostgreSQL e copie `.env.example` para `.env`
4. Inicialize o banco:

```bash
python scripts/init_db.py
```

5. Rode a aplicacao:

```bash
uvicorn app.main:app --reload
```

## Endpoints principais

### Web

- `GET /login`
- `POST /login`
- `GET /dashboard`
- `GET /talhoes`
- `GET /variedades`
- `GET /operacoes`

### API

- `POST /api/v1/auth/token`
- `GET /api/v1/plots`
- `POST /api/v1/plots`
- `GET /api/v1/varieties`
- `POST /api/v1/varieties`
- `GET /api/v1/irrigations`
- `POST /api/v1/irrigations`
- `GET /api/v1/fertilizations`
- `POST /api/v1/fertilizations`
- `GET /api/v1/pesticides`
- `POST /api/v1/pesticides`
- `GET /api/v1/harvests`
- `POST /api/v1/harvests`

## Banco de dados

O projeto usa PostgreSQL com SQLAlchemy. As tabelas sao criadas automaticamente pelo script `scripts/init_db.py` e tambem na inicializacao da aplicacao, caso ainda nao existam.
