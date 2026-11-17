"""Formatacao leve das respostas do assistente: **negrito** e listas com '- ' ou '1. '.

O texto sempre e escapado antes; so <strong>, <ul>/<ol> e <li> sao gerados aqui.
"""

from __future__ import annotations

import re

from markupsafe import Markup, escape

_BOLD = re.compile(r"\*\*(?=\S)(.+?)(?<=\S)\*\*")
_BULLET = re.compile(r"^\s*[-•*]\s+(.*)$")
_NUMBERED = re.compile(r"^\s*\d{1,2}[.)]\s+(.*)$")
_HEADING = re.compile(r"^\s*#{1,6}\s+(.*)$")


def _inline(text: str) -> str:
    return _BOLD.sub(lambda m: f"<strong>{m.group(1)}</strong>", str(escape(text)))


def to_html(text: str | None) -> Markup:
    """Markdown minimo para HTML seguro (as quebras de linha ficam por conta do CSS pre-wrap)."""
    out, kind, items = [], None, []

    def flush():
        nonlocal kind, items
        if kind:
            out.append(f"<{kind}>" + "".join(f"<li>{_inline(i)}</li>" for i in items) + f"</{kind}>")
        kind, items = None, []

    plain: list[str] = []

    def flush_plain():
        if plain:
            out.append(_inline("\n".join(plain)))
            plain.clear()

    for line in (text or "").strip().splitlines():
        bullet, numbered = _BULLET.match(line), _NUMBERED.match(line)
        match, tag = (bullet, "ul") if bullet else (numbered, "ol") if numbered else (None, None)
        if match:
            flush_plain()
            if kind != tag:
                flush()
                kind = tag
            items.append(match.group(1))
            continue
        flush()
        heading = _HEADING.match(line)
        plain.append(f"**{heading.group(1)}**" if heading else line)
    flush()
    flush_plain()
    return Markup("".join(out))


def to_whatsapp(text: str | None) -> str:
    """WhatsApp usa *negrito* com um asterisco e nao tem titulos; listas viram '• '."""
    lines = []
    for line in (text or "").splitlines():
        heading = _HEADING.match(line)
        if heading:
            line = f"**{heading.group(1)}**"
        bullet = _BULLET.match(line)
        if bullet:
            line = f"• {bullet.group(1)}"
        lines.append(_BOLD.sub(lambda m: f"*{m.group(1)}*", line))
    return "\n".join(lines).strip()
