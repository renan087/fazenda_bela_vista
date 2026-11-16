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


PURCHASE_TOOL = {"type": "function", "function": {
    "name": "preparar_compra",
    "description": "Prepara um rascunho de COMPRA (entrada no estoque) para o usuário conferir e confirmar. Serve para item já "
                   "cadastrado (informe item_id) ou para um item novo (item_id null + nome_novo, tipo e categoria). "
                   "Não registra nada sozinho.",
    "parameters": {"type": "object", "additionalProperties": False,
                   "required": ["item_id", "nome_novo", "tipo", "categoria", "unidade", "quantidade_embalagens",
                                "tamanho_embalagem", "preco_por_embalagem", "valor_total", "data", "fornecedor", "observacao"],
                   "properties": {
                       "item_id": {"type": ["integer", "null"], "description": "Item já cadastrado (de buscar_itens) ou null se for novo."},
                       "nome_novo": {"type": ["string", "null"], "description": "Nome do item novo, como o produtor chama (ex.: 'Ureia 46%'). null se item_id."},
                       "tipo": {"type": ["string", "null"], "enum": ["insumo_agricola", "combustivel", None], "description": "Só para item novo."},
                       "categoria": {"type": ["string", "null"], "description": "Só para item novo; uma das categorias permitidas do tipo."},
                       "unidade": {"type": "string", "description": "Unidade da embalagem (para item cadastrado, a unidade do cadastro)."},
                       "quantidade_embalagens": {"type": "number", "description": "Quantas embalagens/volumes (ex.: 10 sacos -> 10; 200 litros a granel -> 1)."},
                       "tamanho_embalagem": {"type": "number", "description": "Conteúdo de cada embalagem na unidade (ex.: saco de 50 kg -> 50; 200 L a granel -> 200)."},
                       "preco_por_embalagem": {"type": ["number", "null"], "description": "Preço de cada embalagem em R$, se informado."},
                       "valor_total": {"type": ["number", "null"], "description": "Valor total da compra em R$, se informado."},
                       "data": {"type": "string", "description": "Data da compra, AAAA-MM-DD."},
                       "fornecedor": {"type": ["string", "null"], "description": "Loja/fornecedor, se o usuário contou."},
                       "observacao": {"type": ["string", "null"], "description": "Outros detalhes úteis que o usuário contou (nota, forma de pagamento, lote)."},
                   }},
}}


class FarmAssistant:
    """Agente com ferramentas: a IA consulta o SiSFarm e so prepara rascunhos; o servidor valida tudo."""

    def __init__(self, *, farm_name, season, items, balances, plots, today, prepare_draft, extra_tools=(),
                 prepare_purchase=None, purchase_rules=None):
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
        self.prepare_purchase = prepare_purchase
        self.purchase_rules = purchase_rules or {}  # {tipo: {categoria: [unidades]}}
        self.handlers = {"buscar_itens": self.buscar_itens, "listar_setores": self.listar_setores,
                         "consultar_saldo": self.consultar_saldo, "preparar_consumo": self.preparar_consumo}
        if prepare_purchase:
            self.handlers["preparar_compra"] = self.preparar_compra
        for schema, handler in self.extra_tools:
            self.handlers[schema["function"]["name"]] = handler

    @property
    def tools(self):
        return AGENT_TOOLS + ([PURCHASE_TOOL] if self.prepare_purchase else []) + [schema for schema, _ in self.extra_tools]

    # ---------------------------------------------------------------- ferramentas
    def _item_view(self, item):
        return {"item_id": item.id, "nome": item.name, "unidade": item.default_unit,
                "tipo": "combustível" if item.item_type == "combustivel" else "insumo agrícola",
                "categoria": getattr(item, "category", None),
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
            return {"itens": [], "aviso": "Nenhum item cadastrado com esse nome. Para consumo, é preciso lançar a compra antes; "
                                          "se for uma compra, dá para cadastrar como item novo."}
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
        if balance <= 0:
            raise ValueError(f"{item.name} não tem saldo nesta fazenda. Lance a compra antes do consumo.")
        if quantity > balance:
            raise ValueError(f"Saldo insuficiente: há {balance} {unit} de {item.name} nesta fazenda.")
        notes = " ".join(str(observacao or "").split())[:300] or None
        self.draft_id = self.prepare_draft(item=item, plot=plot, quantity=quantity, unit=unit, movement_date=movement_date, notes=notes)
        return {"ok": True, "rascunho": self.draft_id}

    def preparar_compra(self, item_id=None, nome_novo=None, tipo=None, categoria=None, unidade="", quantidade_embalagens=None,
                        tamanho_embalagem=None, preco_por_embalagem=None, valor_total=None, data=None, fornecedor=None,
                        observacao=None):
        def positive(value, label):
            number = Decimal(str(value)) if value is not None else Decimal(0)
            if not number.is_finite() or not 0 < number < Decimal("10000000000"):
                raise ValueError(f"{label} inválido(a).")
            return number

        packs = positive(quantidade_embalagens, "Quantidade de embalagens")
        size = positive(tamanho_embalagem, "Tamanho da embalagem")
        if preco_por_embalagem is not None:
            unit_price = positive(preco_por_embalagem, "Preço por embalagem")
        elif valor_total is not None:
            unit_price = positive(valor_total, "Valor total") / packs
        else:
            raise ValueError("Falta o preço: pergunte o valor total ou o preço de cada embalagem.")
        unit_price = unit_price.quantize(Decimal("0.0001"))
        try:
            purchase_date = date.fromisoformat(str(data))
        except ValueError:
            raise ValueError("Data inválida: use AAAA-MM-DD.") from None
        if purchase_date > self.today:
            raise ValueError("A data da compra não pode ser futura.")

        if item_id:
            item = self.items.get(int(item_id))
            if not item:
                raise ValueError("Item não encontrado. Use buscar_itens ou cadastre como novo.")
            unit = stock_unit(item, unidade)
            name, item_type, category, is_new = item.name, item.item_type, item.category, False
        else:
            name = " ".join(str(nome_novo or "").split())[:160]
            if len(name) < 2:
                raise ValueError("Informe o nome do item novo.")
            existing = [entry for entry in self.items.values() if normalized(entry.name) == normalized(name)]
            if existing:
                raise ValueError(f"{existing[0].name} já está cadastrado (item_id {existing[0].id}); use o item_id.")
            if tipo not in self.purchase_rules:
                raise ValueError("Tipo do item novo: insumo_agricola ou combustivel.")
            categories = self.purchase_rules[tipo]
            category = next((c for c in categories if normalized(c) == normalized(categoria or "")), None)
            if not category:
                raise ValueError(f"Categoria inválida para {tipo}. Opções: {', '.join(categories)}.")
            allowed_units = categories[category]
            unit = next((u for u in allowed_units if _unit_key(u) == _unit_key(unidade)), None)
            if not unit:
                raise ValueError(f"Unidade inválida para {category}. Opções: {', '.join(allowed_units)}.")
            item_type, is_new = tipo, True
        notes = " ".join(str(observacao or "").split())[:300] or None
        supplier = " ".join(str(fornecedor or "").split())[:160] or None
        self.draft_id = self.prepare_purchase(
            item=self.items.get(int(item_id)) if item_id else None, name=name, item_type=item_type, category=category,
            unit=unit, packs=packs, size=size, unit_price=unit_price, purchase_date=purchase_date,
            supplier=supplier, notes=notes, is_new=is_new,
        )
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
        purchase_rules = "; ".join(
            f"{'combustível' if kind == 'combustivel' else 'insumo agrícola'} ({kind}): "
            + ", ".join(f"{category} [{'/'.join(units)}]" for category, units in categories.items())
            for kind, categories in self.purchase_rules.items()
        )
        purchases_on = bool(self.prepare_purchase)
        return (
            "Você é o Assistente SiSFarm, o parceiro de campo do produtor de café. Converse como uma pessoa da roça que "
            "entende de lavoura e de sistema: português do Brasil simples, caloroso e direto, frases curtas, como numa conversa "
            "de WhatsApp. Pode usar expressões do dia a dia ('beleza', 'pode deixar', 'fechou') com moderação; nada de "
            "linguagem robótica, listas longas ou termos técnicos do sistema (não fale em item_id, JSON, ferramenta, rascunho "
            "técnico). Chame o que vai para a tela de 'lançamento para você conferir'. Não use emojis em excesso (no máximo um).\n"
            f"Hoje é {_WEEKDAYS[self.today.weekday()]}, {self.today:%d/%m/%Y}. Fazenda: {self.farm_name}. "
            f"Safra ativa: {season.name if season else '-'} ({period}).\n"
            "O que você faz:\n"
            "1) LANÇA o consumo (saída de estoque) de insumos agrícolas e combustíveis.\n"
            + ("2) LANÇA compras (entrada no estoque), inclusive de itens que ainda não têm cadastro.\n" if purchases_on else "")
            + "3) RESPONDE perguntas sobre a fazenda com as consultas disponíveis (chuva, previsão do tempo, aplicações, "
            "colheitas, consumos, estoque, financeiro, cotação do café, safra).\n"
            "Regras gerais:\n"
            "- Use buscar_itens para achar o item citado; nunca invente item. Se houver mais de um parecido, pergunte qual.\n"
            "- Pergunte só o que falta, um ou dois dados por vez, de forma natural (ex.: 'Foi hoje?' em vez de 'Informe a data'). "
            "Não invente valores. 'hoje', 'ontem' e '15/11' são datas válidas; sem ano, use o ano atual. Data futura não vale.\n"
            "- Use a unidade do cadastro e converta só o óbvio (litros->L, quilos->kg, 500 g->0,5 kg).\n"
            "- Quando tiver tudo, prepare o lançamento e diga, em uma frase, que deixou pronto para conferir. Nunca diga que "
            "registrou: quem confirma é o usuário, no botão. Se a preparação voltar com erro, explique com simplicidade e peça o ajuste.\n"
            "- Consultas: responda só com os números devolvidos, citando o período, valores como R$ 1.234,56. "
            "Converta datas relativas ('este mês', 'semana passada'). Se não houver consulta para o assunto, diga que ainda "
            "não tem acesso a essa informação.\n"
            "- Se o usuário mandar foto (nota fiscal, etiqueta, bomba de combustível), aproveite os dados legíveis e confirme o "
            "que não estiver claro.\n"
            "Consumo:\n"
            "- Insumo agrícola precisa de setor (use listar_setores se precisar). Combustível pode ser de uso geral: pergunte o "
            "setor uma vez e aceite 'geral' ou 'nenhum'.\n"
            "- Quantidade por área (kg/ha) não é total: peça o total. Multiplique embalagens só se número e tamanho forem ditos.\n"
            "- Detalhes extras (máquina abastecida, operador, motivo, serviço) vão na observação; não pergunte se não foi dito.\n"
            + ("Compra:\n"
               "- Primeiro busque o item. Se já existir, use-o (tipo, categoria e unidade vêm do cadastro). Se não existir, "
               "confirme com o usuário que é um item novo e descubra o tipo e a categoria pelo contexto (ex.: diesel -> "
               "combustível/Combustíveis; ureia -> insumo agrícola/Fertilizantes); pergunte só se não der para deduzir.\n"
               f"- Categorias e unidades permitidas para item novo: {purchase_rules}.\n"
               "- Precisa de: quantas embalagens, o conteúdo de cada uma (ex.: 10 sacos de 50 kg; 200 L a granel = 1 x 200 L), "
               "o preço (por embalagem ou o total) e a data. Fornecedor e outros detalhes (nota, pagamento) são opcionais: "
               "registre se o usuário contar, mas não insista.\n"
               "- A compra entra no estoque desta fazenda; não lança no financeiro. Se o usuário perguntar, diga que dá para "
               "vincular a conta depois, na tela de compras.\n" if purchases_on else
               "- Compras ainda não podem ser lançadas por aqui; explique com gentileza.\n")
            + "- Para outros lançamentos (despesa, chuva, aplicação, colheita), explique com gentileza que ainda não faz isso por "
            "aqui, sem transformar em consumo ou compra. Não responda assuntos fora da fazenda e do SiSFarm."
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
    if name in ("preparar_consumo", "preparar_compra"):
        kind = "compra" if name == "preparar_compra" else "consumo"
        return f"Preparou rascunho de {kind}" if result.get("ok") else "Validou dados · precisa de ajuste"
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
