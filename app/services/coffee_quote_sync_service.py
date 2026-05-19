"""Sincronização em background das cotações CEPEA com retentativas limitadas."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, time as dt_time, timedelta

from app.core.config import get_settings
from app.core.timezone import app_now
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


def _market_poll_start_minutes() -> int:
    settings = get_settings()
    return (
        max(0, min(23, settings.coffee_quote_market_poll_start_hour)) * 60
        + max(0, min(59, settings.coffee_quote_market_poll_start_minute))
    )


def _is_market_polling_window(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    start_minutes = _market_poll_start_minutes()
    current_minutes = now.hour * 60 + now.minute
    return current_minutes >= start_minutes


def _seconds_until_next_market_window(now: datetime) -> int:
    settings = get_settings()
    start_hour = max(0, min(23, settings.coffee_quote_market_poll_start_hour))
    start_minute = max(0, min(59, settings.coffee_quote_market_poll_start_minute))
    for offset in range(0, 8):
        candidate_date = now.date() + timedelta(days=offset)
        if candidate_date.weekday() >= 5:
            continue
        candidate = datetime.combine(candidate_date, dt_time(start_hour, start_minute), tzinfo=now.tzinfo)
        if candidate > now:
            return max(60, int((candidate - now).total_seconds()))
    return 24 * 60 * 60


def coffee_quotes_db_ready() -> bool:
    """Verifica somente o banco; não faz requisição externa."""
    with SessionLocal() as db:
        return not coffee_quotes_need_background_sync(FarmRepository(db))


def coffee_quote_sync_tick(*, single_attempt: bool = False) -> bool:
    """Uma verificação: sincroniza em burst se necessário."""
    if single_attempt:
        return sync_cepea_coffee_quotes_once()
    with SessionLocal() as db:
        if not coffee_quotes_need_background_sync(FarmRepository(db)):
            return True
    return run_bounded_coffee_quote_sync_burst()


async def run_coffee_quote_sync_loop() -> None:
    """Loop único em background com janela pós-pregão para atualização CEPEA."""
    settings = get_settings()
    idle_seconds = max(300, settings.coffee_quote_sync_idle_interval_seconds)
    retry_seconds = max(30, settings.coffee_quote_sync_retry_interval_seconds)
    market_poll_seconds = max(300, settings.coffee_quote_market_poll_interval_seconds)

    while True:
        now = app_now()
        in_market_window = _is_market_polling_window(now)
        try:
            if in_market_window:
                await asyncio.to_thread(coffee_quote_sync_tick, single_attempt=True)
                sleep_seconds = market_poll_seconds
            else:
                await asyncio.to_thread(coffee_quotes_db_ready)
                sleep_seconds = min(idle_seconds, _seconds_until_next_market_window(now))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Falha inesperada no loop de sincronizacao de cotacoes de cafe.")
            sleep_seconds = retry_seconds
        await asyncio.sleep(sleep_seconds)
