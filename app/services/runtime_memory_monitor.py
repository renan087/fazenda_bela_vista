from __future__ import annotations

import asyncio
import logging
import os

# Usa o logger do Uvicorn para garantir saída no painel do Render.
logger = logging.getLogger("uvicorn.error")


def _read_rss_mb() -> float | None:
    """
    Lê memória residente atual (RSS) em MB via /proc/self/status (Linux).
    Retorna None se não conseguir medir.
    """
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as status_file:
            for line in status_file:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        rss_kb = int(parts[1])
                        return rss_kb / 1024.0
    except (OSError, ValueError):
        return None
    return None


async def run_runtime_memory_monitor(interval_seconds: int = 60) -> None:
    """Publica no log o RSS do processo em intervalo fixo."""
    safe_interval = max(10, int(interval_seconds))
    logger.info("MEM_MONITOR started interval_seconds=%s pid=%s", safe_interval, os.getpid())
    while True:
        rss_mb = _read_rss_mb()
        if rss_mb is None:
            logger.info("MEM_MONITOR rss_mb=unknown pid=%s", os.getpid())
        else:
            logger.info("MEM_MONITOR rss_mb=%.2f pid=%s", rss_mb, os.getpid())
        await asyncio.sleep(safe_interval)
