import json
from types import SimpleNamespace as NS

from app.services.anthropic_adapter import AnthropicChatClient


class _Block(NS):
    def model_dump(self, exclude_none=True):
        return {k: v for k, v in vars(self).items() if v is not None}


class FakeMessages:
    def __init__(self, replies):
        self.replies, self.calls = list(replies), []

    def create(self, **params):
        self.calls.append(params)
        return self.replies.pop(0)


def test_adapter_translates_tools_files_and_keeps_raw_turn():
    first = NS(stop_reason="tool_use", content=[
        _Block(type="thinking", thinking="", signature="sig"),
        _Block(type="tool_use", id="toolu_1", name="buscar_itens", input={"termo": "diesel"})])
    second = NS(stop_reason="end_turn", content=[_Block(type="text", text="Achei o Diesel S10.")])
    fake = FakeMessages([first, second])
    client = AnthropicChatClient(NS(beta=NS(messages=fake), messages=fake))
    tools = [{"type": "function", "function": {"name": "buscar_itens", "description": "Busca",
                                               "parameters": {"type": "object", "properties": {"termo": {"type": "string"}}}}}]
    messages = [{"role": "system", "content": "Você é o Pezinho."},
                {"role": "user", "content": [{"type": "text", "text": "Leia"},
                                             {"type": "file", "file": {"filename": "d.pdf", "file_data": "data:application/pdf;base64,JVBERi0="}}]}]
    reply = client.chat.completions.create(model="claude-opus-5-5", messages=messages, tools=tools, temperature=0.2, parallel_tool_calls=False)
    call = reply.choices[0].message.tool_calls[0]
    assert call.function.name == "buscar_itens" and json.loads(call.function.arguments) == {"termo": "diesel"}
    sent = fake.calls[0]
    assert sent["system"] == "Você é o Pezinho." and "temperature" not in sent
    assert sent["tools"][0]["input_schema"]["properties"]["termo"]["type"] == "string"
    assert sent["tool_choice"] == {"type": "auto", "disable_parallel_tool_use": True}
    assert sent["messages"][0]["content"][0]["type"] == "document" and sent["fallbacks"] == "default"
    messages += [{"role": "assistant", "content": None, "tool_calls": [{"id": call.id, "type": "function", "function": {"name": call.function.name, "arguments": call.function.arguments}}]},
                 {"role": "tool", "tool_call_id": call.id, "content": "{\"itens\": []}"}]
    final = client.chat.completions.create(model="claude-opus-5-5", messages=messages, tools=tools)
    assert final.choices[0].message.content == "Achei o Diesel S10." and final.choices[0].message.tool_calls is None
    turn = fake.calls[1]["messages"]
    assert turn[1]["role"] == "assistant" and turn[1]["content"][0]["type"] == "thinking"
    assert turn[2]["content"][0] == {"type": "tool_result", "tool_use_id": "toolu_1", "content": "{\"itens\": []}"}


def test_provider_switch(monkeypatch):
    from app.services import stock_assistant as sa
    monkeypatch.setenv("ASSISTANT_PROVIDER", "anthropic")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert sa.assistant_available() is False
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    assert sa.assistant_available() is True
