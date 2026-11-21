"""Fonte única da central de ajuda e das orientações oferecidas ao Pezinho."""
from functools import lru_cache
from pathlib import Path
import re
import unicodedata

from app.core.path_access import required_permission_for_web_path

HELP_PATH = Path(__file__).resolve().parents[2] / 'docs' / 'ajuda.md'
_PATH = re.compile(r'^<!-- path: (/[-a-z0-9/]+) -->$')


@lru_cache(maxsize=1)
def articles():
    category = None
    entries = []
    current = None
    for line in HELP_PATH.read_text(encoding='utf-8').splitlines():
        if line.startswith('## '):
            category = line[3:].strip()
        elif line.startswith('### '):
            current = {'category': category, 'question': line[4:].strip(), 'path': None, 'answer': ''}
            entries.append(current)
        elif current and _PATH.fullmatch(line.strip()):
            current['path'] = _PATH.fullmatch(line.strip()).group(1)
        elif current and line.strip() and not line.startswith('#'):
            current['answer'] += (' ' if current['answer'] else '') + line.strip()
    if any(not all(entry.values()) for entry in entries):
        raise ValueError('Artigo de ajuda incompleto')
    return tuple(entries)


def visible_articles(allowed):
    return [entry for entry in articles() if not required_permission_for_web_path(entry['path'])
            or allowed(required_permission_for_web_path(entry['path']))]


def _words(value):
    value = unicodedata.normalize('NFKD', value.lower())
    stopwords = {'como', 'onde', 'para', 'com', 'uma', 'que', 'qual', 'quero', 'posso', 'faco', 'pelo', 'minha', 'meu', 'encontro', 'quais', 'quando', 'sobre', 'estao'}
    return set(re.findall(r'[a-z0-9]{3,}', ''.join(c for c in value if not unicodedata.combining(c)))) - stopwords


def search_articles(query, allowed):
    terms = _words(str(query or '')[:200])
    if not terms:
        return {'artigos': [], 'orientacao': 'Informe o assunto que deseja encontrar.'}
    matches = []
    for entry in visible_articles(allowed):
        title = _words(entry['question'] + ' ' + entry['category'])
        content = _words(entry['answer'] + ' ' + entry['path'].replace('/', ' '))
        score = 3 * len(terms & title) + len(terms & content)
        if score:
            matches.append((score, entry))
    matches.sort(key=lambda item: -item[0])
    return {'artigos': [entry.copy() for _, entry in matches[:5]],
            'orientacao': 'Se não houver artigo pertinente, diga que não encontrou instruções confirmadas e não invente botões ou caminhos.'}
