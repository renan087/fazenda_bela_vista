# SiSFarm — Diretrizes Permanentes de Desenvolvimento

Este documento estabelece as diretrizes permanentes para o desenvolvimento, manutenção e evolução do **SiSFarm** (ERP agrícola para fazendas de café). Ele consolida as regras do projeto, a arquitetura existente e os padrões visuais e técnicos adotados nas páginas mais recentes do sistema.

---

## 1. Idioma, Público e Comunicação

- **Comunicação sempre em português do Brasil (pt-BR):** toda interação com o usuário, textos de interface, mensagens de erro, tooltips, e-mails, comentários relevantes de negócio e mensagens de commit devem estar em **pt-BR**.
- **Público-alvo:** produtores rurais, gerentes e equipe de campo.
- **Linguagem clara e acessível:** use termos do dia a dia da fazenda (ex.: *lote*, *safra*, *talhão*, *sacas*, *bebida*, *peneira*, *saldo disponível*). Evite jargões técnicos, siglas de TI ou mensagens genéricas na interface.

---

## 2. Identidade Visual e Sistema de Design

As páginas mais recentes do módulo **Produção** — **Visão geral** (`production.html`), **Classificação do café** (`coffee_classification.html`, `classification_form.html`, `classification_detail.html`, `classification_compare.html`), **Ficha do lote** (`lot_detail.html`), **Registrar colheita** (`harvest_form.html`) e **Vendas / Compradores** (`commercialization.html`, `sale_form.html`, `partials/buyer_picker.html`) — são a **referência visual oficial** do SiSFarm.

### 2.1 Tipografia
- **Fonte principal:** `Inter` (com fallback sans-serif do sistema).
- **Hierarquia tipográfica:**
  - Título da página (`.sf-page-title`): `1.625rem` (`26px`), `font-weight: 700`, `line-height: 1.2`, `letter-spacing: -0.02em`.
  - Subtítulo da página (`.sf-page-subtitle`): `0.875rem` (`14px`), cor secundária (`#5B6660`).
  - Títulos de seção/cartão (`h2`): `1rem` (`16px`), `font-weight: 600` ou `700`.
  - Rótulos superiores (*eyebrow*, `.pd-eyebrow` / cabeçalhos de tabela): `0.75rem` (`12px`), `font-weight: 600`, `text-transform: uppercase`, `letter-spacing: 0.04em`–`0.06em`, cor secundária.
  - Valores de KPI / grandes números: `24px`–`32px`, `font-weight: 700`, `letter-spacing: -0.02em`, sempre com `font-variant-numeric: tabular-nums` para alinhamento numérico.
  - Corpo de texto e campos: `14px`–`15px`; textos auxiliares e metadados: `12px`–`13px`.

### 2.2 Paleta de Cores Oficial

| Papel | Cor (Hex) | Uso principal |
| :--- | :--- | :--- |
| **Tinta principal** | `#16201A` | Títulos, valores e texto principal |
| **Texto secundário** | `#5B6660` / `#7A857E` | Subtítulos, rótulos, metadados e dicas |
| **Linhas / Bordas** | `#E3E8E2` / `#EEF1ED` | Bordas de cartões, divisores e tabelas (forte: `#D6DDD5`) |
| **Fundo da página** | `#F3F5F2` | Fundo geral das telas |
| **Superfície** | `#FFFFFF` | Cartões, modais, inputs e botões secundários |
| **Superfície suave** | `#F6F8F5` | Blocos internos, cabeçalhos de tabela, hover sutil e áreas de apoio |
| **Verde da marca** | `#446A36` | Ações primárias, links de destaque, indicadores ativos |
| **Verde hover** | `#3A5C2E` | Estado hover de elementos da marca |
| **Verde escuro** | `#294127` | Hover primário, badges de destaque (ex.: cafés especiais 80+), barras entregues |
| **Verde claro** | `#EEF3EB` / `#EEF7ED` | Fundos suaves de seleção, ícones e chips positivos |
| **Verde destaque** | `#9BCB6B` | Anel de foco (`:focus-visible`), barras negociadas e radar sensorial |
| **Menu lateral escuro** | `#0F1513` | Barra lateral principal (`base.html`) |

**Estados semânticos:**
- **Sucesso / Concluído:** texto `#1F7A3A`, fundo `#E6F4E2`.
- **Alerta / Pendente:** texto/ícone `#D18A00` (ou `#7A4F00` para contraste em texto), fundo `#FFF6E5` (borda `#F0D9A6` em cartões de pendência).
- **Erro / Perigo:** texto/botão `#B42318` (hover `#912018`), fundo `#FDECEA`.
- **Informação / Chuva / A receber:** texto `#1F5FA8`, fundo `#E8F1FB`.

### 2.3 Tokens CSS e Convenção de Prefixos
- Os estilos globais vivem em `app/static/css/styles.css` (complementando Tailwind CSS via CDN).
- Reutilize os tokens e classes base de `.sf-page` (`--sf-ink`, `--sf-muted`, `--sf-line`, `--sf-line-strong`, `--sf-soft`, `--sf-brand`, `--sf-brand-dark`, `--sf-brand-soft`) e de `.pd-page` (`--pd-*`, `.pd-head`, `.pd-tabs`, `.pd-card`, `.pd-chip`, `.pd-alert`, `.pd-empty`).
- **Prefixo por página/módulo:** cada página ou fluxo novo deve adotar seu próprio prefixo curto de classes CSS para evitar colisões (ex.: `sf-`, `pd-`, `pv-`, `lt-`, `hf-`, `sl-`, `vw-`, `cl-`, `cf-`, `bp-`, `db-`, `fm-`, `as-`, `hd-`, `pzw-`).
- **Cache busting obrigatório:** sempre que alterar `app/static/css/styles.css` ou qualquer arquivo em `app/static/js/`, incremente o parâmetro `?v=` correspondente em `app/templates/base.html` ou no template que o carrega.

### 2.4 Padrões de Componentes Existentes
- **Cabeçalho de página (`.sf-page-header` / `.pd-head`):** título + subtítulo à esquerda, ações à direita (`.pd-head-actions`). Em fluxos internos/formulários, incluir breadcrumbs (`.pd-crumbs`).
- **Abas internas (`.pd-tabs`):** navegação horizontal entre subseções de um mesmo módulo (como em `partials/production_nav.html`), com `aria-current="page"` e borda inferior verde na aba ativa.
- **Cartões (`.sf-card` / `.pd-card`):** fundo branco `#FFFFFF`, borda `1px solid #E3E8E2`, raio `16px`–`18px` (`1rem`–`1.125rem`), padding `20px`. **Não usar sombras pesadas em cartões comuns** — reservar sombras elevadas apenas para painéis flutuantes, menus suspensos e modais.
- **Botões (`.sf-btn`):** altura entre `36px` e `44px` (`min-height: 2.75rem` para alvos principais), raio `10px`–`12px`, `font-weight: 600`.
  - Primário (`.sf-btn-primary`): verde `#446A36`, texto branco.
  - Secundário (`.sf-btn-secondary`): branco com borda `#D6DDD5`, texto `#16201A`.
  - Perigo (`.cl-btn-danger` / `.sf-menu-danger`): vermelho `#B42318`.
- **KPIs e Barras de Progresso:** grids responsivos (`repeat(auto-fit, minmax(200px, 1fr))` ou 2 → 4 colunas), com `eyebrow`, valor em destaque, unidade secundária e barra horizontal proporcional quando aplicável.
- **Busca e Filtros:** chips tipo pílula (`.sf-chip` / `.sl-chip`) com contador numérico e `aria-pressed="true"` (fundo `#16201A`, texto branco quando ativo) combinados com campo de busca (`.sf-search` / `.sl-search`).
- **Formulários e Wizards Multi-etapas (`vw-`, `hf-`, `cf-`):** layout em duas colunas no desktop (formulário principal + resumo lateral `position: sticky`) com indicador de passos (`1 → 2 → 3`), controles segmentados acessíveis (radios/cards selecionáveis) e etapa explícita de **Conferir** antes de salvar.
- **Modais (`<dialog>`):** cabeçalho fixo, corpo com rolagem interna, rodapé fixo com ações e `::backdrop` escuro translúcido (`rgba(15, 21, 19, 0.45)`).
- **Itens futuros:** funcionalidades planejadas mas não implementadas em demandas visuais devem aparecer desativadas com selo **"Em breve"**, sem criar mecanismos novos fora do escopo.
- **Menu lateral:** **não alterar o menu lateral (`base.html`) sem solicitação explícita.**

---

## 3. Responsividade, Usabilidade e Experiência do Usuário (UX)

### 3.1 Mobile-First e Breakpoints
- Desenvolva pensando primeiro no uso em campo pelo celular, evoluindo progressivamente para tablet e desktop:
  - **Celular:** `≤ 639px` (1 coluna, ações empilhadas ou em largura total, rodapé sticky em formulários/wizards, sem colunas secundárias desnecessárias).
  - **Tablet:** `640px` a `1023px` (2 colunas em KPIs e listas simplificadas; resumo lateral deixa de ser sticky).
  - **Desktop:** `≥ 1024px` (layout completo com menu lateral, grids de 3–4 colunas e tabelas completas).
  - **Desktop largo:** `≥ 1400px` (conteúdo centralizado com `max-width: 1320px` em `.pd-page` para manter leitura confortável).
- **Zero rolagem horizontal** na página inteira. Tabelas largas ou listas de chips usam contêiner próprio ou colapsam para layout de cartão no mobile.
- **Alvos de toque:** botões, switches, checkboxes, chips interativos e campos devem ter altura/área de toque **≥ 44px** (`2.75rem`) no mobile.
- **Modais no celular:** painéis e `<dialog>` devem se transformar em **gaveta inferior (bottom sheet)** com `border-radius: 22px 22px 0 0` e altura máxima `92dvh` em `≤ 639px`.

### 3.2 Fluxo Seguro e Clareza Operacional
- **Nada é registrado sem confirmação do usuário:** especialmente em ações do assistente ou formulários críticos, siga o padrão **rascunho → conferir → confirmar**.
- **Transparência de impacto:** sempre que uma operação alterar saldos, estoque, financeiro ou status de lotes, exiba claramente o impacto (ex.: *antes → depois*, saldo restante, total líquido).
- **Ações destrutivas:** exija confirmação clara e contextual (ex.: caixa de confirmação inline com `role="alert"` explicando se a ação pode ou não ser desfeita).
- **Estados vazios úteis (`.sf-empty` / `.pd-empty`):** nunca exiba tabelas em branco sem explicação. Informe o motivo (ex.: nenhum lote no filtro vs. safra sem colheitas) e ofereça o próximo passo com botão/link direto.
- **Erros acionáveis:** mensagens de validação e erro devem usar linguagem simples e orientar exatamente como corrigir.

### 3.3 Acessibilidade
- Todo botão composto apenas por ícone deve ter `aria-label` descritivo; ícones decorativos devem usar `aria-hidden="true"`.
- Utilize semântica HTML correta (`<header>`, `<nav>`, `<main>`, `<section>`, `<article>`, `<fieldset>`, `<legend>`, `<dialog>`) e atributos ARIA (`aria-current`, `aria-pressed`, `aria-expanded`, `aria-checked`, `role="alert"`, `role="switch"`, `.sr-only`).
- Garanta **foco visível** em todos os elementos interativos (`:focus-visible` com outline de alta visibilidade em `#446A36` ou `#9BCB6B`).
- Mantenha contraste adequado entre texto e fundo e respeite `@media (prefers-reduced-motion: reduce)` em animações e transições.

---

## 4. Arquitetura, Tecnologias e Convenções de Código

### 4.1 Stack Tecnológica
- **Backend:** Python 3.12+, **FastAPI** (`0.115.x`), **Uvicorn**, **Jinja2**, **SQLAlchemy 2.0**, **Pydantic Settings**.
- **Banco de dados:** **PostgreSQL** (`psycopg`) em produção; **SQLite** nos testes automatizados.
- **Frontend:** Templates SSR em Jinja2, **Tailwind CSS** (CDN com plugin `forms`), CSS customizado em `app/static/css/styles.css`, **JavaScript vanilla** organizado por página em `app/static/js/` (sem bundler/Node obrigatório).
- **Visualização e Mapas:** **Chart.js** (gráficos), **Leaflet** e **Google Maps Static API** (mapas e perímetros de fazendas/talhões).
- **Documentos e Planilhas:** **ReportLab** (PDFs), **OpenPyXL / xlrd** (Excel), **Pillow** (imagens), **Segno** (QR codes).

### 4.2 Estrutura de Diretórios
```text
app/
  api/             # Endpoints REST versionados (/api/v1)
  core/            # Configurações (config.py), segurança, CSRF, sessão, timezone, permissões
  crud/            # Operações CRUD genéricas/recursos
  data/            # Dados iniciais (ex.: série histórica CEPEA)
  db/              # Sessão SQLAlchemy, Base e init_db.py (criação e sync de schema)
  middleware/      # Permissões de módulo web, manutenção e enforcement de assinaturas
  models/          # Modelos ORM SQLAlchemy do domínio agrícola, financeiro, RBAC e IA
  repositories/    # Camada de acesso a dados (ex.: FarmRepository)
  routers/         # Autenticação, API JSON e webhooks (Asaas, WhatsApp, relatórios compartilhados)
  schemas/         # Schemas Pydantic
  services/        # Regras de negócio, cálculos, integrações externas, IA e rotinas de fundo
  static/          # CSS, JS por página, imagens e SVGs gerados (ex.: Pezinho)
  templates/       # Templates Jinja2 (base.html = layout/sidebar/header; partials/ = macros e fragmentos)
  testing/         # Catálogo dos testes automatizados executáveis pelo painel
  web/             # routes.py — rotas HTML do frontend
scripts/           # Scripts operacionais e de inicialização
tests/             # Suíte de testes automatizados (pytest)
```

### 4.3 Convenções de Código
- **`app/web/routes.py`:** arquivo extenso — **nunca leia o arquivo inteiro**; use buscas pontuais (`grep -n`) e leitura de trechos específicos. Funções auxiliares internas devem usar prefixo `_`.
- **Separação de responsabilidades:** mantenha lógica de negócio, cálculos e integrações em `app/services/`, deixando `routes.py` focado em orquestração HTTP, validação de entrada e renderização de templates.
- **Banco de dados sem migrations:** não existe Alembic. Tabelas novas são criadas via `Base.metadata.create_all` e ajustes incrementais de colunas/índices em bases existentes são tratados de forma idempotente em `app/db/init_db.py`. Sempre registre novos modelos em `app/models/__init__.py`.
- **Templates e Partials:** reutilize macros e componentes compartilhados em `app/templates/partials/` (ex.: `production_nav.html`, `buyer_picker.html`, `attachment_upload_field.html`, `history_pagination.html`). Use os filtros Jinja do projeto (como `decimal_br`).
- **JavaScript vanilla:** encapsule cada arquivo de página em uma IIFE (`(() => { ... })()`), use atributos `data-*` como hooks de DOM (em vez de acoplar a classes CSS de estilo), escape sempre strings dinâmicas inseridas em HTML (`esc()`) e valide sintaxe quando necessário.
- **Permissões e CSRF nas páginas:** todo formulário `POST` web deve incluir `<input type="hidden" name="csrf_token" value="{{ csrf_token }}">` e respeitar a flag `can_write_page` para ocultar/desabilitar ações de escrita em modo somente leitura ou sem permissão.

---

## 5. Preservação de Funcionalidades e Integrações Existentes

Qualquer alteração deve preservar integralmente os mecanismos transversais e integrações ativas do SiSFarm:

- **Autenticação, Segurança e Multi-tenancy:**
  - Sessões com expiração por inatividade (`session_idle_timeout_hours`), login com **2FA por e-mail** (SMTP), navegadores confiáveis (`trusted_browser`), login social **Google OAuth** e proteção **CSRF**.
  - **RBAC e Permissões:** `WebModulePermissionMiddleware` controla acesso por módulo e escrita (`can_write_page`).
  - **Contexto ativo:** dados operacionais respeitam o escopo da **organização**, **fazenda** e **safra ativa** (`active_season`).
  - **Auditoria automática:** listeners SQLAlchemy (`data_change_audit`) e logs HTTP (`AuditLog`) registram alterações críticas sem intervenção manual.
- **Assinaturas e Cobrança (Asaas):**
  - `SubscriptionEnforcementMiddleware` e `farm_plan_guard` aplicam limites de plano (fazendas, área total, usuários ativos) e modo **somente leitura** após o período de carência (`SUBSCRIPTION_GRACE_DAYS`).
  - Webhook `/api/webhooks/asaas`, checkout de cartão, conciliação periódica em background e separação estrita entre `sandbox` e `production`.
- **Assistente Pezinho (IA) e Mascote:**
  - Agente principal em `app/services/stock_assistant.py` (formato `chat.completions`), com provedor configurável por `ASSISTANT_PROVIDER=anthropic|openai` (`app/services/anthropic_adapter.py`).
  - **OpenAI** permanece responsável por transcrição de áudio (`assistant_audio.py`) e recomendações agronômicas de adubação.
  - **Mascote Pezinho:** desenho e animações definidos em `app/services/mascot.py`; widget em `app/templates/partials/pezinho.html` e `app/static/js/pezinho.js`. Se alterar o mascote, **regere os SVGs com `python -m app.services.mascot`** (há teste automatizado validando a sincronia).
- **WhatsApp Business:**
  - Webhook (`whatsapp_webhook.py`), bot conversacional (`whatsapp_bot.py`), rotina de ciclo de vida/encerramento/pesquisa de experiência (`whatsapp_lifecycle.py`), painel de avaliações (`whatsapp_surveys.html`) e acesso temporário a registros (`whatsapp_record_access.py`).
- **Outras Integrações e Rotinas de Fundo (`app/main.py`):**
  - Cotações de café CEPEA (`coffee_quote_sync_service.py` + seed CSV).
  - Previsão do tempo (`weather_forecast.py`) e consulta pública de CAR (`car_public_lookup.py`).
  - Backups automatizados no **Supabase** (`backup_service.py`).
  - Modo manutenção (`MaintenanceMiddleware`), relatórios compartilhados (`assistant_shared_reports.py`) e painel de testes automatizados (`/qualidade/testes-automatizados`).

---

## 6. Fluxo de Trabalho, Testes e Deploy

- **Testes automatizados (obrigatório):** antes de finalizar qualquer mudança de código, execute toda a suíte:
  ```bash
  source .venv/bin/activate && python -m pytest -q
  ```
  Todos os testes devem passar sem regressões. Para alterações relevantes em JavaScript, valide também a sintaxe (ex.: via `jsc`).
- **Mudanças visuais de maior porte:** priorize consistência com as telas de referência do módulo Produção; não altere o escopo funcional quando o pedido for exclusivamente visual.
- **Limpeza antes do commit:** nunca versione arquivos temporários, caches ou pastas locais de prévia (como `.claude/preview/`).
- **Git e Deploy:**
  - O fluxo principal trabalha diretamente na branch `main` (sem PR obrigatório), com mensagens de commit claras em **pt-BR**.
  - A CI (`.github/workflows/ci.yml`) executa `pytest` a cada push na `main`.
  - O deploy em produção ocorre no **Render** conforme configurado em `render.yaml` (Python `3.12.8`, healthcheck em `/health`).
