import asyncio
import threading
from types import SimpleNamespace

import httpx
from starlette.datastructures import FormData

from app.main import app
from app.web import routes


def test_health_responds_during_slow_soil_save(monkeypatch):
    started = threading.Event()
    release = threading.Event()
    analysis = SimpleNamespace(
        id=3, farm_id=7, plot=SimpleNamespace(farm_id=7), laboratory_metadata=None, ai_status="generated",
    )
    plot = SimpleNamespace(id=4)
    repository = SimpleNamespace(get_soil_analysis=lambda analysis_id: analysis)

    class Request:
        session = {}
        query_params = {}

        async def form(self):
            return FormData({})

    def slow_save(*args):
        started.set()
        if not release.wait(timeout=5):
            raise RuntimeError("Teste não liberou a chamada simulada")
        return analysis

    monkeypatch.setattr(routes, "validate_csrf", lambda *args: None)
    monkeypatch.setattr(routes, "_repository", lambda db: repository)
    monkeypatch.setattr(routes, "_resolve_farm_plot_in_scope", lambda *args: (plot, {"active_farm_id": 7}, None))
    monkeypatch.setattr(routes, "_soil_payload", lambda **kwargs: {})
    monkeypatch.setattr(routes, "update_soil_analysis", slow_save)
    monkeypatch.setattr(routes, "_apply_soil_ai_recommendation", lambda *args: (_ for _ in ()).throw(
        AssertionError("A edição não deve gerar recomendação"),
    ))

    async def run():
        task = asyncio.create_task(routes.update_soil_analysis_action(
            3, Request(), csrf_token="x", plot_id=4, analysis_date="2026-10-10", laboratory="Lab",
            ph=None, organic_matter=None, phosphorus=None, potassium=None, calcium=None,
            magnesium=None, aluminum=None, h_al=None, ctc=None, base_saturation=None,
            observations=None, analysis_pdf=None, db=object(), user=object(),
        ))
        try:
            for _ in range(100):
                if started.is_set():
                    break
                await asyncio.sleep(0.01)
            assert started.is_set(), "A edição não chegou à gravação"
            transport = httpx.ASGITransport(app=app)
            response = await asyncio.wait_for(
                transport.handle_async_request(httpx.Request("GET", "http://testserver/health")), timeout=1,
            )
            body = await response.aread()
            await response.aclose()
            assert response.status_code == 200 and body == b"ok"
            assert not task.done()
        finally:
            release.set()
            await asyncio.wait_for(task, timeout=2)

    asyncio.run(run())
