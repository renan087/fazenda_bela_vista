"""Conversational consumption assistant and stock confirmation safeguards."""

import json
import logging
import os
import re
import unicodedata
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from app.models.stock_assistant_draft import StockAssistantDraft
from app.core.config import get_settings
from app.services.forms import create_manual_stock_output

_UNITS = {"kg": "kg", "g": "g", "l": "L", "ml": "mL", "un": "un"}
_COUNTS = {"um": 1, "uma": 1, "dois": 2, "duas": 2, "tres": 3, "quatro": 4, "cinco": 5}
_NUMBER = r"[0-9]{1,9}(?:[,.][0-9]{1,2})?"
_START = r"(?P<day>hoje|ontem|\d{2}/\d{2}/\d{4}) (?:usei|consumi) "
_PACKS = rf"(?P<packs>[0-9]{{1,6}}|{'|'.join(_COUNTS)}) (?:sacos?|embalagens?) de "
_AMOUNT = rf"(?P<quantity>{_NUMBER})\s*(?P<unit>kg|g|ml|l|un)"
_END = r" (?:no|na|em) (?P<plot>.+?)[.!]?"
_PATTERN = re.compile(
    rf"{_START}(?:{_PACKS})?{_AMOUNT} de (?P<product>.+?){_END}"
)
_PACK_PRODUCT_FIRST = re.compile(rf"{_START}{_PACKS}(?P<product>.+?) de {_AMOUNT}{_END}")


AGENT_MAX_STEPS = 8
DRAFT_READY_REPLY = "Preparei o lançamento abaixo. Confira os dados e confirme para registrar."
_WEEKDAYS = ("segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado", "domingo")

AGENT_TOOLS = [
    {"type": "function", "function": {
        "name": "buscar_itens",
        "description": "Procura insumos agrícolas e combustíveis com estoque nesta fazenda pelo nome ou parte dele. "
                       "Use sempre antes de preparar um consumo para obter o item_id, a unidade e o saldo.",
        "parameters": {"type": "object", "additionalProperties": False, "required": ["termo"],
                       "properties": {"termo": {"type": "string", "description": "Nome ou parte do nome, ex.: gasolina, ureia."}}},
    }},
    {"type": "function", "function": {
        "name": "listar_setores",
        "description": "Lista os setores (talhões) disponíveis na safra ativa, com a área em hectares.",
        "parameters": {"type": "object", "additionalProperties": False, "properties": {}},
    }},
    {"type": "function", "function": {
        "name": "consultar_saldo",
        "description": "Consulta o saldo em estoque de um item nesta fazenda.",
        "parameters": {"type": "object", "additionalProperties": False, "required": ["item_id"],
                       "properties": {"item_id": {"type": "integer"}}},
    }},
    {"type": "function", "function": {
        "name": "preparar_consumo",
        "description": "Prepara um rascunho de consumo (saída de estoque) para o usuário conferir e confirmar. "
                       "Não registra nada sozinho. Só chame quando item, quantidade total, unidade e data estiverem claros "
                       "(e o setor, se for insumo agrícola).",
        "parameters": {"type": "object", "additionalProperties": False,
                       "required": ["item_id", "quantidade", "unidade", "data", "setor", "observacao"],
                       "properties": {
                           "item_id": {"type": "integer"},
                           "quantidade": {"type": "number", "description": "Quantidade total consumida."},
                           "unidade": {"type": "string", "description": "Unidade do cadastro do item."},
                           "data": {"type": "string", "description": "Data do consumo, AAAA-MM-DD."},
                           "setor": {"type": ["string", "null"], "description": "Nome do setor; null só para combustível de uso geral."},
                           "observacao": {"type": ["string", "null"], "description":
                               "Detalhes úteis que o usuário contou e que não cabem nos outros campos (máquina ou equipamento "
                               "abastecido, operador, motivo, serviço, condição). Frase curta e objetiva, ex.: "
                               "'Abastecimento do tratorito'. null se não houver."},
                       }},
    }},
]


class FarmAssistant:
    """Agente com ferramentas: a IA consulta o SiSFarm e so prepara rascunhos; o servidor valida tudo."""

    def __init__(self, *, farm_name, season, items, balances, plots, today, prepare_draft, extra_tools=()):
        self.farm_name = farm_name
        self.season = season
        self.items = {item.id: item for item in items if item.is_active and item.item_type in STOCK_ITEM_TYPES}
        self.balances = balances
        self.plots = list(plots)
        self.today = today
        self.prepare_draft = prepare_draft
        self.draft_id = None
        self.trace = []  # o que o agente consultou, mostrado ao usuario abaixo da resposta
        self.extra_tools = list(extra_tools)  # [(schema, handler)] de consultas somente leitura
        self.handlers = {"buscar_itens": self.buscar_itens, "listar_setores": self.listar_setores,
                         "consultar_saldo": self.consultar_saldo, "preparar_consumo": self.preparar_consumo}
        for schema, handler in self.extra_tools:
            self.handlers[schema["function"]["name"]] = handler

    @property
    def tools(self):
        return AGENT_TOOLS + [schema for schema, _ in self.extra_tools]

    # ---------------------------------------------------------------- ferramentas
    def _item_view(self, item):
        return {"item_id": item.id, "nome": item.name, "unidade": item.default_unit,
                "tipo": "combustível" if item.item_type == "combustivel" else "insumo agrícola",
                "saldo_na_fazenda": round(float(self.balances.get(item.id, 0)), 2)}

    def buscar_itens(self, termo=""):
        tokens = normalized(str(termo)).split()
        scored = []
        for item in self.items.values():
            name = normalized(item.name)
            hits = sum(1 for token in tokens if token in name)
            if not tokens or hits:
                scored.append((-(hits == len(tokens)), -hits, item.name, item))
        found = [self._item_view(entry[-1]) for entry in sorted(scored)[:8]]
        if not found:
            return {"itens": [], "aviso": "Nenhum item com estoque nesta fazenda corresponde. O usuário precisa lançar a compra antes."}
        return {"itens": found}

    def listar_setores(self):
        return {"setores": [{"nome": plot.name, "area_ha": float(plot.area_hectares) if getattr(plot, "area_hectares", None) is not None else None}
                            for plot in self.plots]}

    def consultar_saldo(self, item_id):
        item = self.items.get(int(item_id))
        return self._item_view(item) if item else {"erro": "Item não encontrado nesta fazenda."}

    def _find_plot(self, name):
        wanted = normalized(str(name))
        exact = [plot for plot in self.plots if normalized(plot.name) == wanted]
        if len(exact) == 1:
            return exact[0]
        partial = [plot for plot in self.plots if wanted and wanted in normalized(plot.name)]
        if len(partial) == 1:
            return partial[0]
        options = ", ".join(plot.name for plot in (partial or self.plots)[:10])
        raise ValueError(f"Setor '{name}' não encontrado ou ambíguo. Opções: {options or 'nenhum setor na safra'}.")

    def preparar_consumo(self, item_id, quantidade, unidade, data, setor=None, observacao=None):
        item = self.items.get(int(item_id))
        if not item:
            raise ValueError("Item não encontrado nesta fazenda. Use buscar_itens.")
        unit = stock_unit(item, unidade)
        quantity = Decimal(str(quantidade))
        if not quantity.is_finite() or not 0 < quantity < Decimal("10000000000"):
            raise ValueError("Quantidade inválida: informe a quantidade total consumida.")
        quantity = quantity.quantize(Decimal("0.01"))
        try:
            movement_date = date.fromisoformat(str(data))
        except ValueError:
            raise ValueError("Data inválida: use AAAA-MM-DD.") from None
        if movement_date > self.today:
            raise ValueError("A data do consumo não pode ser futura.")
        if self.season and not self.season.start_date <= movement_date <= self.season.end_date:
            raise ValueError(f"A data precisa estar dentro da safra ({self.season.start_date:%d/%m/%Y} a {self.season.end_date:%d/%m/%Y}).")
        plot = self._find_plot(setor) if setor else None
        if plot is None and item.item_type != "combustivel":
            raise ValueError("Insumo agrícola precisa de setor. Pergunte ao usuário em qual setor foi usado.")
        balance = Decimal(str(self.balances.get(item.id, 0)))
        if quantity > balance:
            raise ValueError(f"Saldo insuficiente: há {balance} {unit} de {item.name} nesta fazenda.")
        notes = " ".join(str(observacao or "").split())[:300] or None
        self.draft_id = self.prepare_draft(item=item, plot=plot, quantity=quantity, unit=unit, movement_date=movement_date, notes=notes)
        return {"ok": True, "rascunho": self.draft_id}

    def call_tool(self, name, raw_args):
        try:
            args = json.loads(raw_args or "{}")
            if not isinstance(args, dict):
                raise ValueError("Argumentos inválidos.")
            handler = self.handlers.get(name)
            if handler is None:
                raise ValueError(f"Ferramenta desconhecida: {name}")
            result = handler(**args)
        except (ValueError, TypeError, ArithmeticError) as exc:
            result = {"erro": str(exc) or "Dados inválidos."}
        except Exception:  # consulta com falha nao derruba a conversa
            logging.getLogger(__name__).exception("Falha na ferramenta %s do assistente", name)
            result = {"erro": "Não foi possível consultar essa informação agora."}
        label = _trace_label(name, result)
        if label and label not in self.trace:
            self.trace.append(label)
        return result

    # ---------------------------------------------------------------- conversa
    def instructions(self):
        season = self.season
        period = f"{season.start_date:%d/%m/%Y} a {season.end_date:%d/%m/%Y}" if season else "não definida"
        return (
            "Você é o Assistente SiSFarm, o ajudante de campo do produtor rural. Você faz duas coisas: "
            "(1) LANÇA o consumo (saída de estoque) de insumos agrícolas e combustíveis e "
            "(2) RESPONDE perguntas sobre a fazenda usando as ferramentas de consulta disponíveis "
            "(chuva, previsão do tempo, aplicações, colheitas, consumos, estoque, financeiro, cotação do café, safra). "
            "Fale em português do Brasil, de forma natural, breve e cordial, como no WhatsApp.\n"
            f"Hoje é {_WEEKDAYS[self.today.weekday()]}, {self.today:%d/%m/%Y}. Fazenda: {self.farm_name}. "
            f"Safra ativa: {season.name if season else '-'} ({period}).\n"
            "Consultas: use sempre a ferramenta adequada e responda só com os números que ela devolver, citando o período. "
            "Converta datas relativas ('este mês', 'semana passada') para AAAA-MM-DD. Se não houver ferramenta para o assunto, "
            "diga que você ainda não tem acesso a essa informação. Formate valores em reais como R$ 1.234,56.\n"
            "Lançamento de consumo:\n"
            "- Use buscar_itens para achar o item citado (nunca invente item_id). Se houver mais de um parecido, pergunte qual.\n"
            "- Se o item não tiver estoque nesta fazenda, explique e sugira lançar a compra antes.\n"
            "- Insumo agrícola precisa de setor (use listar_setores se precisar). Combustível pode ser de uso geral: "
            "pergunte o setor uma vez e aceite 'geral' ou 'nenhum' (setor null).\n"
            "- Se a data não foi dita, pergunte se foi hoje. 'hoje', 'ontem' e '15/11' são válidas; sem ano, use o ano atual.\n"
            "- Use a unidade do cadastro; converta só o óbvio (litros->L, quilos->kg, 500 g->0,5 kg). Quantidade por área "
            "(kg/ha) não é total: peça o total. Multiplique embalagens só se número e tamanho forem informados.\n"
            "- Pergunte o que faltar, no máximo um ou dois dados por vez. Não invente valores.\n"
            "- Detalhes extras que o usuário contar (máquina/equipamento abastecido, operador, motivo, serviço) vão em "
            "'observacao' do preparar_consumo; não pergunte por eles se o usuário não falou.\n"
            "- Quando tiver tudo, chame preparar_consumo. Se ela devolver erro, explique ao usuário e peça o ajuste.\n"
            "- Nunca diga que registrou: o usuário confirma na tela.\n"
            "- Se o usuário enviar uma foto (nota, etiqueta, bomba de combustível), use os dados legíveis dela e confirme "
            "o que não estiver claro.\n"
            "- Nesta versão você prepara consumo, informa saldo de itens e lista setores. Para compra, despesa, chuva ou outro lançamento, explique com gentileza "
            "que ainda não faz isso, sem transformar em consumo. Não responda assuntos fora do SiSFarm."
        )

    def run(self, history, *, client, model, image=None):
        messages = [{"role": "system", "content": self.instructions()}] + list(history)
        if image and messages[-1]["role"] == "user":
            messages[-1] = {"role": "user", "content": [
                {"type": "text", "text": messages[-1]["content"]},
                {"type": "image_url", "image_url": {"url": image, "detail": "low"}},
            ]}
        for _ in range(AGENT_MAX_STEPS):
            response = client.chat.completions.create(
                model=model, messages=messages, tools=self.tools, tool_choice="auto", parallel_tool_calls=False,
            )
            message = response.choices[0].message
            calls = getattr(message, "tool_calls", None) or []
            if not calls:
                text = (message.content or "").strip()
                return {"resposta": text or "Pode me contar um pouco mais?", "draft_id": self.draft_id}
            messages.append({"role": "assistant", "content": message.content or None, "tool_calls": [
                {"id": call.id, "type": "function", "function": {"name": call.function.name, "arguments": call.function.arguments}}
                for call in calls
            ]})
            for call in calls:
                result = self.call_tool(call.function.name, call.function.arguments)
                messages.append({"role": "tool", "tool_call_id": call.id, "content": json.dumps(result, ensure_ascii=False, default=str)})
            if self.draft_id:
                return {"resposta": DRAFT_READY_REPLY, "draft_id": self.draft_id}
        return {"resposta": "Não consegui concluir. Pode me dizer de novo o item, a quantidade, o setor e a data?", "draft_id": self.draft_id}


def _num_br(value) -> str:
    text = f"{float(value):,.2f}".rstrip("0").rstrip(".")
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def _trace_label(name, result):
    if not isinstance(result, dict):
        return None
    if name == "buscar_itens":
        items = result.get("itens") or []
        return f"Buscou item · {items[0]['nome']}" if items else "Buscou item · nada encontrado"
    if name == "listar_setores":
        return f"Listou setores · {len(result.get('setores') or [])}"
    if name == "consultar_saldo" and "saldo_na_fazenda" in result:
        return f"Conferiu saldo · {_num_br(result['saldo_na_fazenda'])} {result.get('unidade') or ''}".strip()
    if name == "preparar_consumo":
        return "Preparou rascunho" if result.get("ok") else "Validou dados · precisa de ajuste"
    label = _QUERY_LABELS.get(name)
    if label:
        return f"{label} · indisponível" if result.get("erro") else label
    return None


_QUERY_LABELS = {
    "resumo_safra": "Consultou a safra",
    "consultar_chuva": "Consultou a chuva",
    "consultar_previsao_tempo": "Consultou a previsão do tempo",
    "consultar_aplicacoes": "Consultou as aplicações",
    "consultar_colheitas": "Consultou as colheitas",
    "consultar_consumos": "Consultou os consumos",
    "consultar_saldos_estoque": "Consultou o estoque",
    "consultar_financeiro": "Consultou o financeiro",
    "consultar_cotacao_cafe": "Consultou a cotação do café",
}


def run_farm_assistant(history, assistant: "FarmAssistant", image=None):
    """Executa o agente; erros da OpenAI viram ValueError com mensagem clara."""
    api_key = os.getenv("OPENAI_API_KEY") or get_settings().openai_api_key
    if not api_key:
        raise ValueError("O assistente de IA ainda não está configurado. Contate o administrador.")
    from openai import OpenAI

    model = (os.getenv("OPENAI_ASSISTANT_MODEL") or get_settings().openai_assistant_model or "gpt-4o-mini").strip()
    client = OpenAI(api_key=api_key, timeout=get_settings().openai_timeout_seconds, max_retries=1)
    try:
        return assistant.run(history, client=client, model=model, image=image)
    except Exception as exc:
        if assistant.draft_id:  # rascunho ja criado: segue para a conferencia mesmo se algo falhar depois
            return {"resposta": DRAFT_READY_REPLY, "draft_id": assistant.draft_id}
        logging.getLogger(__name__).exception("Falha no agente do assistente (modelo %s)", model)
        raise ValueError(explain_openai_error(exc, model)) from None


def explain_openai_error(exc: Exception, model: str) -> str:
    """Mensagem clara para cada falha comum da OpenAI (o detalhe tecnico vai para o log)."""
    import openai

    admin = " Avise o administrador do SiSFarm."
    body = getattr(exc, "body", None)
    code = (body.get("code") if isinstance(body, dict) else None) or getattr(exc, "code", None) or ""
    if isinstance(exc, openai.AuthenticationError):
        return "A chave da OpenAI configurada no SiSFarm é inválida ou foi revogada." + admin
    if isinstance(exc, openai.RateLimitError):
        if code == "insufficient_quota":
            return "A conta da OpenAI usada pelo SiSFarm está sem créditos." + admin
        return "Muitas mensagens em pouco tempo. Aguarde alguns segundos e tente de novo."
    if isinstance(exc, (openai.NotFoundError, openai.PermissionDeniedError)) or code == "model_not_found":
        return f"A chave da OpenAI não tem acesso ao modelo {model}." + admin
    if isinstance(exc, openai.BadRequestError):
        return "A IA recusou a mensagem. Tente reescrever de forma mais simples."
    if isinstance(exc, (openai.APITimeoutError, openai.APIConnectionError)):
        return "A IA demorou para responder. Tente novamente em instantes."
    return "Não consegui consultar a IA agora. Tente novamente em instantes."


def normalized(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value.casefold())
    return " ".join("".join(c for c in decomposed if not unicodedata.combining(c)).split())


def parse_consumption(message: str, today: date) -> dict:
    clean = normalized(message) if len(message) <= 350 else ""
    if re.match(r"(?:(?:hoje|ontem|\d{2}/\d{2}/\d{4})\s+)?(?:comprei|compra)\b", clean):
        raise ValueError("Esta mensagem descreve uma compra. O assistente ainda registra apenas consumo. Para dar entrada no estoque, use Compras de insumos.")
    match = _PATTERN.fullmatch(clean) or _PACK_PRODUCT_FIRST.fullmatch(clean)
    if not match:
        raise ValueError("Informe uma saída por mensagem, com data, quantidade total, unidade, insumo e setor. Exemplo: Hoje usei 40 kg de Vita Nit S no Setor 3.")
    parts = match.groupdict()
    day = parts["day"]
    try:
        movement_date = today if day == "hoje" else today - timedelta(days=1) if day == "ontem" else datetime.strptime(day, "%d/%m/%Y").date()
    except ValueError:
        raise ValueError("Informe uma data válida.") from None
    quantity = Decimal(parts["quantity"].replace(",", "."))
    packs = parts["packs"]
    if packs:
        quantity *= int(packs) if packs.isdecimal() else _COUNTS[packs]
    if not 0 < quantity < Decimal("10000000000") or movement_date > today:
        raise ValueError("Informe uma quantidade positiva válida e uma data que não seja futura.")
    return dict(movement_date=movement_date, quantity=quantity, unit=_UNITS[parts["unit"]],
                product=parts["product"], plot=parts["plot"])


def match_exact(name, options, label):
    matches = [item for item in options if normalized(item.name) == name]
    if len(matches) != 1:
        raise ValueError(f"{label} não encontrado ou ambíguo. Use o nome exato do cadastro.")
    return matches[0]


_UNIT_ALIASES = {
    "l": "l", "lt": "l", "lts": "l", "litro": "l", "litros": "l",
    "ml": "ml", "mililitro": "ml", "mililitros": "ml",
    "kg": "kg", "quilo": "kg", "quilos": "kg", "kilo": "kg", "kilos": "kg", "quilograma": "kg", "quilogramas": "kg",
    "g": "g", "grama": "g", "gramas": "g",
    "un": "un", "und": "un", "unidade": "un", "unidades": "un",
}
STOCK_ITEM_TYPES = ("insumo_agricola", "combustivel")


def _unit_key(value) -> str:
    key = normalized(str(value or "")).rstrip(".")
    return _UNIT_ALIASES.get(key, key)


def stock_unit(input_item, requested):
    if not input_item or not input_item.is_active or input_item.item_type not in STOCK_ITEM_TYPES:
        raise ValueError("Selecione um insumo agrícola ou combustível ativo.")
    unit = (input_item.default_unit or "").strip()
    if not unit or _unit_key(unit) != _unit_key(requested):
        raise ValueError(f"Informe a quantidade na unidade cadastrada do insumo: {unit or 'não definida'}.")
    return unit


def claim_draft(db, draft, status):
    return db.query(StockAssistantDraft).filter(
        StockAssistantDraft.id == draft.id,
        StockAssistantDraft.status == "pending",
        StockAssistantDraft.expires_at > datetime.now(timezone.utc),
    ).update({"status": status}, synchronize_session=False) == 1


def confirm_consumption(repo, draft):
    if not claim_draft(repo.db, draft, "confirmed"):
        return None
    return create_manual_stock_output(repo, {
        "input_id": draft.input_id, "farm_id": draft.farm_id, "season_id": draft.season_id,
        "plot_id": draft.plot_id, "movement_date": draft.movement_date.isoformat(),
        "quantity": draft.quantity, "unit": draft.unit, "origin": "Assistente SiSFarm",
        "notes": getattr(draft, "notes", None),
    }, commit=False, farm_stock_only=True)
