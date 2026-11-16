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


RESPONSE_SCHEMA = {
    "name": "assistente_consumo",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["resposta", "campos"],
        "properties": {
            "resposta": {"type": "string", "description": "Mensagem para o usuário."},
            "campos": {
                "anyOf": [
                    {"type": "null"},
                    {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["product", "plot", "quantity", "unit", "movement_date"],
                        "properties": {
                            "product": {"type": "string", "description": "Nome exato do item no cadastro."},
                            "plot": {"type": ["string", "null"], "description": "Nome exato do setor; null só para combustível de uso geral."},
                            "quantity": {"type": "number"},
                            "unit": {"type": "string"},
                            "movement_date": {"type": "string", "description": "AAAA-MM-DD"},
                        },
                    },
                ]
            },
        },
    },
}


def converse_consumption(history, products, plots, today):
    """Interpret a conversation; the caller must independently validate every field."""
    api_key = os.getenv("OPENAI_API_KEY") or get_settings().openai_api_key
    if not api_key:
        raise ValueError("O assistente de IA ainda não está configurado. Contate o administrador.")
    from openai import OpenAI
    context = {
        "hoje": today.isoformat(),
        "itens_em_estoque": [
            {"nome": p.name, "unidade": p.default_unit, "tipo": "combustível" if getattr(p, "item_type", None) == "combustivel" else "insumo agrícola"}
            for p in products if p.is_active
        ],
        "setores": [p.name for p in plots],
    }
    instructions = (
        "Você é o Assistente SiSFarm, que ajuda o produtor rural a lançar no sistema, por conversa, o CONSUMO "
        "(saída de estoque) de insumos agrícolas e combustíveis. Fale em português do Brasil, de forma natural, breve e cordial, "
        "como num WhatsApp. Nesta versão você só prepara consumo: se o usuário falar de compra, despesa ou outro lançamento, "
        "explique com gentileza que por enquanto registra apenas consumo, sem converter em consumo.\n"
        "Dados necessários: item (de itens_em_estoque), quantidade total, unidade, data e setor. "
        "O setor é obrigatório para insumo agrícola e opcional para combustível (sem setor = uso geral da fazenda; "
        "pergunte o setor do combustível apenas uma vez e aceite 'geral', 'nenhum' ou 'não sei').\n"
        "Se faltar algum dado, pergunte de forma objetiva, um ou dois itens por vez, sem inventar. Se a data não for dita, "
        "pergunte se foi hoje. 'hoje', 'ontem' e datas como 15/11 são válidas (sem ano = ano de hoje). Datas futuras não valem.\n"
        "Associe o que o usuário disser ao cadastro (ex.: 'gasolina' -> item com gasolina no nome; 'litros' -> L; 'quilos' -> kg) "
        "somente quando a correspondência for clara; se houver mais de uma opção, pergunte qual. Se o item não estiver em "
        "itens_em_estoque, diga que ele não tem estoque nesta fazenda e sugira cadastrar a compra antes.\n"
        "A unidade deve ser a do cadastro do item; converta apenas casos óbvios (litros->L, quilos->kg, 500 g->0,5 kg). "
        "Quantidade por área (kg/ha) não é total: peça o total. Multiplique embalagens só quando número e tamanho forem informados.\n"
        "Nunca diga que registrou: quando tiver tudo, preencha 'campos' e diga que preparou o lançamento para o usuário conferir "
        "e confirmar. Enquanto faltar algo, 'campos' deve ser null.\n"
        "Responda sempre no formato JSON pedido. Cadastros desta fazenda e safra: "
        + json.dumps(context, ensure_ascii=False)
    )
    model = (os.getenv("OPENAI_ASSISTANT_MODEL") or get_settings().openai_assistant_model or "gpt-4o-mini").strip()
    try:
        response = OpenAI(api_key=api_key, timeout=get_settings().openai_timeout_seconds, max_retries=1).chat.completions.create(
            model=model,
            response_format={"type": "json_schema", "json_schema": RESPONSE_SCHEMA},
            messages=[{"role": "system", "content": instructions}] + history,
        )
    except Exception as exc:
        logging.getLogger(__name__).exception("Falha ao consultar IA do assistente de estoque (modelo %s)", model)
        raise ValueError(explain_openai_error(exc, model)) from None
    try:
        message = response.choices[0].message
        if getattr(message, "refusal", None):
            return {"resposta": "Não posso ajudar com isso. Posso registrar o consumo de um insumo ou combustível.", "campos": None}
        result = json.loads(message.content or "")
        if not isinstance(result, dict) or not isinstance(result.get("resposta"), str):
            raise ValueError("Resposta inválida")
    except (ValueError, TypeError, IndexError, AttributeError):
        logging.getLogger(__name__).warning("IA do assistente devolveu formato inesperado: %.300r",
                                            getattr(getattr(response.choices[0], "message", None), "content", None) if getattr(response, "choices", None) else None)
        raise ValueError("Não entendi bem. Pode reescrever a mensagem com o insumo, a quantidade, o setor e a data?") from None
    return result


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
    }, commit=False, farm_stock_only=True)
