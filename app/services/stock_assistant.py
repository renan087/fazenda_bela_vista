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


def converse_consumption(history, products, plots, today):
    """Interpret a conversation; the caller must independently validate every field."""
    api_key = os.getenv("OPENAI_API_KEY") or get_settings().openai_api_key
    if not api_key:
        raise ValueError("O assistente de IA ainda não está configurado. Contate o administrador.")
    from openai import OpenAI
    context = {
        "hoje": today.isoformat(),
        "insumos": [{"nome": p.name, "unidade": p.default_unit} for p in products if p.is_active],
        "setores": [p.name for p in plots],
    }
    instructions = (
        "Você é o Assistente SiSFarm. Converse de forma natural, breve e cordial em português brasileiro. "
        "Nesta versão você apenas prepara saída por consumo de insumo agrícola; não registra compras, "
        "despesas ou outros tipos de movimentação. Explique essa limitação se necessário, sem converter compra em consumo. "
        "Use o histórico para identificar consumo, insumo, quantidade total, unidade, setor e data. "
        "Pergunte o que faltar ou estiver ambíguo, sem inventar valores; hoje e ontem são datas explícitas. "
        "Resolva nomes apenas quando houver correspondência inequívoca nos cadastros fornecidos; "
        "use o nome exato do cadastro. Quantidades por área não são quantidade total. "
        "Multiplique embalagens somente quando quantidade e tamanho forem informados. "
        "Não diga que registrou algo: o usuário precisa revisar e confirmar. "
        "Retorne somente JSON com resposta (texto para o usuário) e campos (objeto ou null). "
        "Só preencha campos quando todos os dados estiverem claros e a intenção for consumo: "
        "product, plot, quantity (número total), unit, movement_date (AAAA-MM-DD). "
        "Se faltar ou houver dúvida, campos deve ser null. Contexto cadastrado: "
        + json.dumps(context, ensure_ascii=False)
    )
    try:
        response = OpenAI(api_key=api_key, timeout=25, max_retries=0).chat.completions.create(
            model=os.getenv("OPENAI_ASSISTANT_MODEL", "gpt-4o-mini"),
            response_format={"type": "json_object"},
            messages=[{"role": "system", "content": instructions}] + history,
        )
        result = json.loads(response.choices[0].message.content)
        if not isinstance(result, dict) or not isinstance(result.get("resposta"), str):
            raise ValueError("Resposta inválida")
        return result
    except Exception:
        logging.getLogger(__name__).exception("Falha ao consultar IA do assistente de estoque")
        raise ValueError("Não consegui consultar a IA agora. Tente novamente em instantes.") from None


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


def stock_unit(input_item, requested):
    if not input_item or not input_item.is_active or input_item.item_type != "insumo_agricola":
        raise ValueError("Selecione um insumo agrícola ativo.")
    unit = (input_item.default_unit or "").strip()
    if not unit or normalized(unit) != normalized(requested):
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
