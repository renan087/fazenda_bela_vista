# SiSFarm — guia para agentes

ERP agrícola para fazendas de café (pt-BR). Usuários: produtores e equipe de campo — linguagem simples, sem jargão técnico.

## Stack e estrutura
- FastAPI + Jinja2 + SQLAlchemy (PostgreSQL; SQLite nos testes). Tabelas novas via `Base.metadata.create_all` (sem migrations).
- Frontend: templates Jinja, Tailwind via CDN + `app/static/css/styles.css`, JS vanilla por página em `app/static/js/`. Leaflet/Google Maps no mapa.
- `app/web/routes.py`: rotas HTML (arquivo grande — use `grep -n`, nunca leia inteiro). Helpers internos com prefixo `_`.
- `app/services/`: regras de negócio. `app/models/`, `app/repositories/`, `app/templates/` (`base.html` = layout, menu e cabeçalho).
- Deploy no Render (`render.yaml`); variáveis de ambiente no painel do Render.

## Fluxo de trabalho
- Commits direto na `main`, sem PR. Ao terminar uma implementação com testes passando: commit (mensagem em pt-BR) + push sem perguntar. Design só no canvas não gera commit.
- Testes: `source .venv/bin/activate && python -m pytest -q` (todos devem passar). JS sem node: `jsc` para checar sintaxe.
- Economizar tokens: ler só trechos (`sed -n`, `grep`), editar com scripts/Edit, evitar reler arquivos.
- Mudanças de visual: primeiro desenho no canvas de design para aprovação (https://claude.ai/artifact/CjGQw3EFiVjGxnwSih7u3a), depois implementar.
- Não alterar o menu lateral sem pedido explícito. Não criar mecanismos novos quando o pedido é só de visual; itens futuros aparecem desativados com selo “Em breve”.
- Prévia local: renderizar o template em `.claude/preview/` (servir com `python3 -m http.server`) e apagar a pasta antes do commit.

## Identidade visual
- Fonte Inter. Cores: tinta `#16201A`, texto secundário `#5B6660`/`#7A857E`, linhas `#E3E8E2`/`#EEF1ED`, fundo da página `#F3F5F2`, superfície `#FFFFFF`, suave `#F6F8F5`.
- Verde da marca `#446A36` (hover `#3A5C2E`, escuro `#294127`, claro `#EEF3EB`, destaque `#9BCB6B`). Menu lateral escuro `#0F1513`.
- Estados: alerta/pendente âmbar `#D18A00` (fundo `#FFF6E5`), erro `#B42318` (fundo `#FDECEA`), sucesso `#1F7A3A` (fundo `#E6F4E2`), informação/chuva azul `#1F5FA8` (fundo `#E8F1FB`).
- Cartões brancos, borda 1px `#E3E8E2`, raio 16–18px, sombra só em painéis flutuantes. Botões 36–44px de altura, raio 10–12px; primário verde, secundário branco com borda.
- Tokens `.sf-page` (`--sf-*`) e classes `sf-btn`, `sf-page-header` em `styles.css`. Cada página nova usa prefixo próprio de classes (ex.: `db-`, `fm-`, `as-`, `hd-`, `pzw-`).
- Ao mudar `styles.css` ou um JS, incrementar o `?v=` correspondente no template.

## UX e responsividade
- Mobile-first: quebras em 639px (celular), 1023px (tablet), 1400px+ (desktop largo). Sem rolagem horizontal; alvos de toque ≥ 44px; painéis viram gaveta inferior no celular.
- Nada é registrado sem confirmação do usuário (rascunho → conferir → confirmar). Mostrar impacto (ex.: estoque antes → depois).
- Estados vazios com mensagem útil; erros em linguagem clara com o próximo passo. Respeitar `prefers-reduced-motion`.
- Acessibilidade: `aria-label` em botões só com ícone, foco visível, contraste adequado.

## Assistente Pezinho (IA)
- Agente em `app/services/stock_assistant.py` (formato chat.completions). Provedor por env: `ASSISTANT_PROVIDER=anthropic|openai`; Anthropic via `app/services/anthropic_adapter.py` (`ANTHROPIC_API_KEY`, `ANTHROPIC_ASSISTANT_MODEL`, padrão `claude-opus-5-5`, `ANTHROPIC_ASSISTANT_EFFORT`). Transcrição de áudio e recomendação de adubação seguem na OpenAI.
- Mascote: desenho e animações em `app/services/mascot.py`; regerar os SVGs com `python -m app.services.mascot` (há teste que compara). Widget do canto: `partials/pezinho.html` + `static/js/pezinho.js`; estados: acenando, pronto, falando, pensando, alerta (bate na tela), comemorando, dormindo, zangado e espiando (escondido).
