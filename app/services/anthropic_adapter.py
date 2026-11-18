"""Adaptador: o agente do Pezinho fala o formato da OpenAI (chat.completions) e este cliente
traduz para a API de Mensagens da Anthropic (Claude) e de volta, sem mexer no loop do agente."""
from __future__ import annotations

import json
import os
from types import SimpleNamespace

DEFAULT_MODEL = "claude-opus-5-5"


def _data_url(url: str) -> tuple[str, str]:
    head, _, data = url.partition(",")
    return head.removeprefix("data:").split(";")[0], data


def _user_blocks(content):
    if isinstance(content, str):
        return [{"type": "text", "text": content or " "}]
    blocks = []
    for part in content or []:
        kind = part.get("type")
        if kind == "text":
            blocks.append({"type": "text", "text": part.get("text") or " "})
        elif kind == "image_url":
            media, data = _data_url(part["image_url"]["url"])
            blocks.insert(0, {"type": "image", "source": {"type": "base64", "media_type": media, "data": data}})
        elif kind == "file":
            media, data = _data_url(part["file"]["file_data"])
            blocks.insert(0, {"type": "document", "source": {"type": "base64", "media_type": media, "data": data}})
    return blocks


def _tool(spec):
    fn = spec["function"]
    return {"name": fn["name"], "description": fn.get("description", ""),
            "input_schema": fn.get("parameters") or {"type": "object", "properties": {}}}


class _Completions:
    def __init__(self, owner):
        self.owner = owner

    def create(self, *, model, messages, tools=None, parallel_tool_calls=True, **_ignored):
        # temperature/reasoning_effort (da OpenAI) sao ignorados: o Claude usa adaptive thinking + effort.
        system, converted = [], []

        def push(role, blocks):
            if converted and converted[-1]["role"] == role:
                converted[-1]["content"].extend(blocks)
            else:
                converted.append({"role": role, "content": list(blocks)})

        for msg in messages:
            role = msg["role"]
            if role == "system":
                system.append(msg["content"])
            elif role == "user":
                push("user", _user_blocks(msg["content"]))
            elif role == "tool":
                push("user", [{"type": "tool_result", "tool_use_id": msg["tool_call_id"], "content": msg["content"]}])
            elif role == "assistant":
                calls = msg.get("tool_calls") or []
                raw = self.owner.raw_turns.get(calls[0]["id"]) if calls else None
                if raw is not None:  # devolve o turno original (com os blocos de raciocinio) sem alteracoes
                    push("assistant", raw)
                    continue
                blocks = [{"type": "text", "text": msg["content"]}] if msg.get("content") else []
                blocks += [{"type": "tool_use", "id": c["id"], "name": c["function"]["name"],
                            "input": json.loads(c["function"]["arguments"] or "{}")} for c in calls]
                if blocks:
                    push("assistant", blocks)
        params = dict(
            model=model, max_tokens=16000, system="\n\n".join(system), messages=converted,
            output_config={"effort": (os.getenv("ANTHROPIC_ASSISTANT_EFFORT") or "low").strip()},
        )
        if tools:
            params["tools"] = [_tool(t) for t in tools]
            params["tool_choice"] = {"type": "auto", "disable_parallel_tool_use": not parallel_tool_calls}
        if model.startswith(("claude-opus-5", "claude-fable-5", "claude-sonnet-5-5")):
            # Se o filtro de seguranca recusar, a propria API refaz no modelo de reserva.
            params.update(betas=["server-side-fallback-2026-07-01"], fallbacks="default")
            response = self.owner.client.beta.messages.create(**params)
        else:
            response = self.owner.client.messages.create(**params)
        texts = [b.text for b in response.content if b.type == "text"]
        calls = [b for b in response.content if b.type == "tool_use"]
        if response.stop_reason == "refusal" and not calls:
            texts = ["Não posso ajudar com esse pedido. Posso ajudar com lançamentos e consultas da fazenda."]
        tool_calls = [SimpleNamespace(id=b.id, type="function",
                                      function=SimpleNamespace(name=b.name, arguments=json.dumps(b.input, ensure_ascii=False)))
                      for b in calls]
        if calls:
            self.owner.raw_turns[calls[0].id] = [b.model_dump(exclude_none=True) for b in response.content]
        message = SimpleNamespace(content="\n\n".join(t for t in texts if t.strip()) or None, tool_calls=tool_calls or None)
        return SimpleNamespace(choices=[SimpleNamespace(message=message)])


class AnthropicChatClient:
    """Cliente com a interface client.chat.completions.create(...) usada pelo agente."""

    def __init__(self, client):
        self.client = client
        self.raw_turns: dict[str, list] = {}
        self.chat = SimpleNamespace(completions=_Completions(self))


def explain_anthropic_error(exc: Exception, model: str) -> str:
    import anthropic

    admin = " Avise o administrador do SiSFarm."
    if isinstance(exc, anthropic.AuthenticationError):
        return "A chave da Anthropic configurada no SiSFarm é inválida ou foi revogada." + admin
    if isinstance(exc, anthropic.RateLimitError):
        return "Muitas mensagens em pouco tempo. Aguarde alguns segundos e tente de novo."
    if isinstance(exc, (anthropic.NotFoundError, anthropic.PermissionDeniedError)):
        return f"A chave da Anthropic não tem acesso ao modelo {model}." + admin
    if isinstance(exc, anthropic.BadRequestError):
        if "credit" in str(exc).lower():
            return "A conta da Anthropic usada pelo SiSFarm está sem créditos." + admin
        return "A IA recusou a mensagem. Tente reescrever de forma mais simples."
    if isinstance(exc, (anthropic.APITimeoutError, anthropic.APIConnectionError)):
        return "A IA demorou para responder. Tente novamente em instantes."
    return "Não consegui consultar a IA agora. Tente novamente em instantes."
