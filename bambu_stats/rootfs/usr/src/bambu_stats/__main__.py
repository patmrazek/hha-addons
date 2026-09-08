"""Vstupní bod add-onu: načte konfiguraci, otevře DB a spustí collector pro každou tiskárnu."""
from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from zoneinfo import ZoneInfo

from . import __version__, config, log
from .collector import Collector
from .db import Database
from .ha_api import HomeAssistant
from .health import HealthServer

LOG = logging.getLogger("main")


def main() -> int:
    settings = config.load()
    log.setup(settings.log_level)
    for p in settings.printers:
        log.REDACT.add(p.access_code)
    log.REDACT.add(settings.supervisor_token, settings.mqtt.password)
    LOG.info("bambu_stats %s startuje, %d tiskáren, data %s", __version__, len(settings.printers), settings.data_dir)
    if not settings.printers:
        LOG.error("Není nakonfigurována žádná tiskárna (options → printers). Čekám, nic nedělám.")
        while True:
            time.sleep(3600)

    tzname = "Europe/Prague"
    cfg = HomeAssistant(settings.supervisor_token).config()
    if cfg and cfg.get("time_zone"):
        tzname = cfg["time_zone"]
    tz = ZoneInfo(tzname)
    LOG.info("časová zóna %s", tzname)

    db = Database(settings.db_path)
    collectors: list[Collector] = []
    for i, p in enumerate(settings.printers):
        prefix = settings.entity_prefix if len(settings.printers) == 1 else f"{settings.entity_prefix}_{p.slug}"
        collectors.append(Collector(settings, p, db, tz, prefix))

    def status():
        parts = [c.health() for c in collectors]
        return {"ok": all(x["ok"] for x in parts) if parts else False, "printers": parts}

    HealthServer(settings.health_port, status).start()
    for c in collectors:
        c.start()

    stop = threading.Event()

    def _sig(*_):
        LOG.info("ukončuji")
        for c in collectors:
            c.stop()
        stop.set()

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)
    while not stop.is_set():
        stop.wait(5)
    db.checkpoint()
    db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
