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

- Email: definido pelas variáveis `SUPER_ADMIN_EMAIL` e `ADMIN_EMAIL`
- Senha: enviada por email quando a conta de contingência precisar ser criada automaticamente

## Login com 2FA por email

O login web usa autenticacao em duas etapas por email:

1. usuario informa email e senha
2. o sistema envia um codigo numerico de 6 digitos
3. o acesso so e liberado apos a validacao do codigo

Para habilitar o envio do codigo, configure no `.env`:

- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USERNAME`
- `SMTP_PASSWORD`
- `SMTP_FROM_EMAIL`
- `SMTP_FROM_NAME`
- `SMTP_USE_TLS`

Parametros de seguranca do 2FA:

- `TWO_FACTOR_CODE_MINUTES`
- `TWO_FACTOR_MAX_ATTEMPTS`

## Alertas de backup por e-mail

Backups com falha ou concluídos parcialmente geram um alerta pelo SMTP do SiSFarm. Defina `BACKUP_FAILURE_ALERT_EMAIL` para escolher o destinatário. Sem essa configuração, o sistema usa `SUPER_ADMIN_EMAIL` ou, na ausência dele, `ADMIN_EMAIL`.

## Assinaturas

O Mercado Pago processa as assinaturas mensais e anuais com checkout hospedado. O SiSFarm gerencia o catálogo, o histórico e o acesso da organização. Acompanhe os contratos em **Assinaturas** e em **Minha assinatura**. [Homologação](docs/mercadopago-sandbox.md) · [Entrada em produção](docs/mercadopago-producao.md)

### Inadimplência e limites do plano

- Atraso de 1 a 10 dias (`SUBSCRIPTION_GRACE_DAYS`): aviso no topo de todas as páginas e e-mail ao responsável financeiro.
- A partir do 11º dia: modo somente leitura (consulta e exportação liberadas; cadastrar/editar bloqueado). Minha assinatura, perfil e pagamento continuam disponíveis.
- 2 dias antes da restrição, novo e-mail. Cada aviso é enviado uma única vez por fatura (requer SMTP configurado).
- Liberações manuais ativas preservam o acesso pelo prazo definido. A organização interna SiSFarm é isenta. Cada plano define seus próprios limites.
- Limites do plano: impede cadastrar fazendas, área (soma das fazendas) e usuários ativos (inclusive reativações) acima do contratado, sem apagar nada. A API JSON (`/api/v1`) segue os mesmos módulos e o modo somente leitura. Módulos fora do plano aparecem com cadeado e levam a "Planos disponíveis", onde o cliente pede a mudança (aviso por e-mail para `SUPPORT_EMAIL`).

## Assinatura de documentos

A área Documentos reúne a Central de Certificados, a assinatura de PDFs e a validação independente. A configuração e o procedimento de liberação estão no [guia de assinatura digital](docs/assinatura-digital.md).

## Testes automatizados

Instale dependências de desenvolvimento e execute a suíte:

```bash
pip install -r requirements-dev.txt
pytest
```

No painel web (super administrador): **Configuração → Testes automatizados** (`/qualidade/testes-automatizados`). Cada card representa um implemento (cotação, RBAC, segurança, rotas, dashboard). A execução roda pytest em background, grava o histórico no PostgreSQL (últimas 30 execuções, taxa de sucesso por implemento) e mantém cópia local em `.cache/automated_tests_state.json`. O commit do deploy aparece quando `RENDER_GIT_COMMIT`, `GITHUB_SHA` ou `GIT_COMMIT` estiver definido. **Não há correção automática de dados de negócio** — apenas relatório e histórico.

**CI (Nível 1):** em todo push/PR para `main`, o workflow `.github/workflows/ci.yml` executa `pytest`. Falhas na CI bloqueiam merge; o painel em produção é para execução manual de diagnóstico.

## Publicacao

O projeto permanece preparado para deploy no Render com `render.yaml`.

### Encerramento do atendimento pelo WhatsApp

A rotina verifica os prazos aproximadamente a cada minuto enquanto a aplicação está em execução. Os valores são configuráveis por variáveis de ambiente: `WHATSAPP_IDLE_MINUTES` (padrão 30), `WHATSAPP_CLOSING_MINUTES` (10) e `WHATSAPP_SURVEY_MINUTES` (30). A rotina respeita a janela de mensagens do WhatsApp; pesquisas que não puderem ser enviadas ficam registradas como indisponíveis. Em falhas de envio com resultado incerto, não repete automaticamente a mensagem para evitar duplicação. O histórico das conversas é preservado.

A especialização individual em doramas usa a API da Anthropic para a conversa e para pesquisas públicas na web. Configure `ANTHROPIC_API_KEY` no ambiente do servidor; o modelo segue `ANTHROPIC_ASSISTANT_MODEL` ou o padrão do adaptador. O provedor configurado para os demais usuários não é alterado. A pesquisa envia somente os termos fornecidos à ferramenta e depende da disponibilidade e das permissões da busca web na conta Anthropic.
