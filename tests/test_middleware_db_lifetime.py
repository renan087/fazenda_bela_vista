import asyncio
from types import SimpleNamespace

from starlette.responses import Response

from app.middleware import web_module_permission as middleware


def test_permission_connection_is_released_before_downstream_request(monkeypatch):
    state = {"open": False}
    class Session:
        def __enter__(self):
            state["open"] = True
            return self
        def __exit__(self, *args):
            state["open"] = False
        def query(self, *args):
            return self
        def filter(self, *args):
            return self
        def first(self):
            return object()
    monkeypatch.setattr(middleware, "SessionLocal", Session)
    monkeypatch.setattr(middleware, "permission_codes_for_user", lambda db, user: {"read"})
    monkeypatch.setattr(middleware, "required_permissions_for_web_path", lambda *args: {"read"})
    monkeypatch.setattr(middleware, "is_public_web_prefix", lambda path: False)
    request = SimpleNamespace(method="GET", url=SimpleNamespace(path="/dashboard"), query_params={},
                              session={"user_email": "user@example.com"})
    async def next_handler(request):
        assert not state["open"]
        return Response("ok")
    response = asyncio.run(middleware.WebModulePermissionMiddleware(app=None).dispatch(request, next_handler))
    assert response.status_code == 200
