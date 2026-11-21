from types import SimpleNamespace
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.db.base import Base
from app.models import User
from app.models.assistant_capability import AssistantCapability
from app.services import dorama_assistant as service


def test_access_is_fixed_to_initial_user_and_not_transferred():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    db = Session(engine)
    owner = User(name='Shara', email='shara.viana85@gmail.com', hashed_password='x', is_active=True)
    other = User(name='Outra', email='other@example.com', hashed_password='x', is_active=True)
    db.add_all([owner, other]); db.commit()
    service.initialize_access(db)
    assert service.allowed(db, owner) and not service.allowed(db, other)
    owner.email = 'changed@example.com'
    other.email = 'shara.viana85@gmail.com'
    db.commit()
    service.initialize_access(db)
    assert service.allowed(db, owner) and not service.allowed(db, other)
    owner.is_active = False; db.commit()
    assert not service.allowed(db, owner)
    db.close()


def test_absent_user_does_not_allow_later_email_claim():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    db = Session(engine)
    service.initialize_access(db)
    user = User(name='Teste', email=service.TARGET_EMAIL, hashed_password='x', is_active=True)
    db.add(user); db.commit()
    service.initialize_access(db)
    assert db.get(AssistantCapability, service.CAPABILITY).user_id is None and not service.allowed(db, user)
    db.close()


def test_search_uses_only_public_query_and_requires_sources(monkeypatch):
    import anthropic
    monkeypatch.setenv('ANTHROPIC_API_KEY', 'test')
    calls = []
    class Messages:
        def create(self, **kwargs):
            calls.append(kwargs)
            citation = SimpleNamespace(url='https://example.com/show')
            return SimpleNamespace(content=[SimpleNamespace(type='text', text='Resumo', citations=[citation])])
    monkeypatch.setattr(anthropic, 'Anthropic', lambda **kwargs: SimpleNamespace(messages=Messages()))
    assert 'erro' in service.search_dorama('contato pessoa@example.com')
    result = service.search_dorama('Dorama estreia 2026')
    assert result['fontes'] == ['https://example.com/show']
    assert calls[0]['messages'] == [{'role': 'user', 'content': 'Pesquise na internet sobre doramas: Dorama estreia 2026'}]
    assert calls[0]['tools'] == [{'type': 'web_search_20250305', 'name': 'web_search', 'max_uses': 2}]


def test_search_without_citations_or_key_fails_closed(monkeypatch):
    import anthropic
    monkeypatch.delenv('ANTHROPIC_API_KEY', raising=False)
    assert 'erro' in service.search_dorama('Dorama estreia')
    monkeypatch.setenv('ANTHROPIC_API_KEY', 'test')
    monkeypatch.setattr(anthropic, 'Anthropic', lambda **kwargs: SimpleNamespace(messages=SimpleNamespace(
        create=lambda **params: SimpleNamespace(content=[SimpleNamespace(type='text', text='Sem fontes', citations=[])]))))
    assert 'erro' in service.search_dorama('Dorama estreia')


def test_only_authorized_assistant_routes_to_claude(monkeypatch):
    from app.services import stock_assistant
    seen = []
    monkeypatch.setattr(stock_assistant, '_run_with_anthropic', lambda *args: seen.append('claude') or {})
    monkeypatch.setattr(stock_assistant, 'assistant_provider', lambda: 'openai')
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    monkeypatch.setattr(stock_assistant, 'get_settings', lambda: SimpleNamespace(openai_api_key=None))
    stock_assistant.run_farm_assistant([], SimpleNamespace(handlers={'pesquisar_doramas': object()}))
    assert seen == ['claude']
    import pytest
    with pytest.raises(ValueError):
        stock_assistant.run_farm_assistant([], SimpleNamespace(handlers={}))
    assert seen == ['claude']


def test_assistant_tools_are_scoped_in_web_and_whatsapp(monkeypatch):
    from tests.test_stock_assistant import _route_env
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    user.email = service.TARGET_EMAIL
    db.commit()
    service.initialize_access(db)
    seen = []
    def run(history, assistant, **kwargs):
        seen.append(('pesquisar_doramas' in assistant.handlers, assistant.instructions()))
        return {'resposta': 'Olá!'}
    monkeypatch.setattr(routes, 'run_farm_assistant', run)
    repo = routes._repository(db)
    routes._assistant_turn(db, user, repo, scope, None, 'gosto de doramas')
    routes._assistant_turn(db, user, repo, scope, None, 'gosto de doramas', title_prefix='WhatsApp · ')
    assert all(flag and 'especialista em doramas' in instructions for flag, instructions in seen)
    grant = db.get(AssistantCapability, service.CAPABILITY)
    grant.user_id = None
    db.commit()
    routes._assistant_turn(db, user, repo, scope, None, 'gosto de doramas')
    assert not seen[-1][0] and 'Não responda assuntos fora da fazenda e do SiSFarm' in seen[-1][1]
    db.close()
