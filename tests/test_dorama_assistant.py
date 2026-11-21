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
    import openai
    monkeypatch.setenv('OPENAI_API_KEY', 'test')
    calls = []
    class Responses:
        def create(self, **kwargs):
            calls.append(kwargs)
            citation = SimpleNamespace(url='https://example.com/show')
            return SimpleNamespace(output=[SimpleNamespace(content=[SimpleNamespace(annotations=[citation])])], output_text='Resumo')
    monkeypatch.setattr(openai, 'OpenAI', lambda **kwargs: SimpleNamespace(responses=Responses()))
    assert 'erro' in service.search_dorama('contato pessoa@example.com')
    result = service.search_dorama('Dorama estreia 2026')
    assert result['fontes'] == ['https://example.com/show']
    assert calls[0]['store'] is False and calls[0]['input'] == 'Pesquisa sobre doramas: Dorama estreia 2026'
    assert calls[0]['tools'] == [{'type': 'web_search_preview'}]


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
