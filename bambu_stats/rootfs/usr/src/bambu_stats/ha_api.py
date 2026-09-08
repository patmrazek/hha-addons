"""Čtení entit Home Assistantu přes Supervisor proxy (homeassistant_api: true)."""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

LOG = logging.getLogger("ha_api")


class HomeAssistant:
    def __init__(self, token: str | None = None, base: str | None = None):
        self.token = token or os.environ.get("SUPERVISOR_TOKEN", "")
        self.base = base or os.environ.get("HA_API_BASE", "http://supervisor/core/api")

    @property
    def available(self) -> bool:
        return bool(self.token)

    def _get(self, path: str, timeout=10):
        req = urllib.request.Request(f"{self.base}{path}", headers={"Authorization": f"Bearer {self.token}"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())

    def state(self, entity_id: str) -> dict | None:
        if not self.available or not entity_id:
            return None
        try:
            return self._get(f"/states/{entity_id}")
        except urllib.error.HTTPError as e:
            LOG.debug("HA %s → %s", entity_id, e.code)
        except (urllib.error.URLError, OSError, ValueError) as e:
            LOG.debug("HA nedostupný: %s", e)
        return None

    def numeric(self, entity_id: str) -> float | None:
        st = self.state(entity_id)
        if not st:
            return None
        try:
            v = float(st.get("state"))
            return v if v > 0 else None
        except (TypeError, ValueError):
            return None

    def config(self) -> dict | None:
        try:
            return self._get("/config") if self.available else None
        except Exception:
            return None
