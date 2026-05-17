"""Sincronização em background das cotações CEPEA com retentativas limitadas."""

from __future__ import annotations

import asyncio
import logging
import time

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.repositories.farm import FarmRepository
from app.services.coffee_quotes import (
    coffee_quotes_need_background_sync,
    sync_cepea_coffee_quotes_once,
)

logger = logging.getLogger(__name__)


def run_bounded_coffee_quote_sync_burst() -> bool:
    """Tenta buscar e salvar cotações até sucesso ou esgotar limites do burst."""
    settings = get_settings()
    interval = max(30, settings.coffee_quote_sync_retry_interval_seconds)
    max_attempts = max(1, settings.coffee_quote_sync_max_attempts_per_burst)
    deadline = time.monotonic() + max(60, settings.coffee_quote_sync_max_burst_seconds)

    for attempt in range(1, max_attempts + 1):
        if time.monotonic() >= deadline:
            logger.warning(
                "Sincronizacao de cafe interrompida: tempo maximo do burst (%ss) atingido.",
                settings.coffee_quote_sync_max_burst_seconds,
            )
            break
        try:
            if sync_cepea_coffee_quotes_once():
                logger.info("Cotacoes de cafe sincronizadas com sucesso (tentativa %s).", attempt)
                return True
        except Exception:
            logger.exception("Falha na tentativa %s de sincronizar cotacoes de cafe.", attempt)

        if attempt < max_attempts and time.monotonic() + interval < deadline:
            logger.info(
                "Cotacoes de cafe ainda incompletas; nova tentativa em %ss (%s/%s).",
                interval,
                attempt,
                max_attempts,
            )
            time.sleep(interval)

    return False


def coffee_quote_sync_tick() -> bool:
    """Uma verificação: sincroniza em burst se necessário."""
    with SessionLocal() as db:
        if not coffee_quotes_need_background_sync(FarmRepository(db)):
            return True
    return run_bounded_coffee_quote_sync_burst()


async def run_coffee_quote_sync_loop() -> None:
    """Loop único em background: burst com fim definido, depois espera longa."""
    settings = get_settings()
    idle_seconds = max(300, settings.coffee_quote_sync_idle_interval_seconds)
    retry_seconds = max(30, settings.coffee_quote_sync_retry_interval_seconds)

    while True:
        try:
            satisfied = await asyncio.to_thread(coffee_quote_sync_tick)
            sleep_seconds = idle_seconds if satisfied else retry_seconds
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Falha inesperada no loop de sincronizacao de cotacoes de cafe.")
            sleep_seconds = retry_seconds
        await asyncio.sleep(sleep_seconds)
