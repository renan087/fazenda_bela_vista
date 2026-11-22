from types import SimpleNamespace

from app.web import routes


def _req(path, voltar=None, session=None, method="GET"):
    return SimpleNamespace(method=method, url=SimpleNamespace(path=path), session=session if session is not None else {},
                           query_params={"voltar": voltar} if voltar else {})


def test_return_stack_guides_sale_to_harvest_to_season_and_back():
    session = {}
    # Nova venda sem lote -> Registrar colheita
    routes._remember_return(_req("/producao/colheitas/nova", "/producao/vendas/nova", session))
    assert routes._active_return(_req("/producao/colheitas/nova", session=session))["label"] == "Nova venda"
    # Colheita sem talhão -> Safras
    routes._remember_return(_req("/safras/3/editar", "/producao/colheitas/nova", session))
    assert routes._active_return(_req("/safras/3/editar", session=session))["label"] == "Registrar colheita"
    # Salvar a safra volta para a colheita; a faixa da venda continua lá
    assert routes._pop_return(_req("/safras/3/editar", session=session, method="POST"), "/safras/") == "/producao/colheitas/nova"
    routes._remember_return(_req("/producao/colheitas/nova", session=session))
    assert routes._active_return(_req("/producao/colheitas/nova", session=session))["path"] == "/producao/vendas/nova"
    # Salvar a colheita volta para a venda
    assert routes._pop_return(_req("/producao", session=session, method="POST"), "/producao/colheitas/nova") == "/producao/vendas/nova"
    assert routes._return_stack(_req("/", session=session)) == []


def test_return_ignores_unknown_paths_and_cleans_on_arrival():
    session = {}
    routes._remember_return(_req("/producao/colheitas/nova", "https://site-externo.com", session))
    routes._remember_return(_req("/producao/colheitas/nova", "/admin", session))
    assert routes._return_stack(_req("/", session=session)) == []
    draft = "/producao/rascunhos-fiscais/" + "a" * 32
    routes._remember_return(_req("/producao/nfe/configuracao", draft, session))
    assert routes._active_return(_req("/producao/nfe/configuracao", session=session))["label"] == "Emissão da nota"
    assert routes._active_return(_req("/notas-fiscais", session=session)) is None  # faixa só na tela de desvio
    routes._remember_return(_req(draft, session=session))  # chegou na nota: sai da pilha
    assert routes._return_stack(_req("/", session=session)) == []
