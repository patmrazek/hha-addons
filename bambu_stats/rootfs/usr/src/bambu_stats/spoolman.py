"""Odečet spotřeby a cena cívky ze Spoolmanu.

Přiřazení cívky k AMS slotu vede SpoolmanSync (extra.active_tray = '"<printer>_AMS_<ams_sn>_tray_<n>"', n = slot 1–4).
Bambu Stats po uzavření tisku pro každý filament tisku najde cívku podle slotu a zavolá `PUT /spool/{id}/use`
s hmotností ze sliceru. Odečítá jen rozdíl proti už odečtenému (`spool_deducted_g`), takže opakovaný resolve
nic nezdvojí. Cena tisku pak = gramy × cena konkrétní cívky (`spool.price / spool.initial_weight`).
"""
from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.request

LOG = logging.getLogger("spoolman")


class Spoolman:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")
        if self.base and not self.base.endswith("/api/v1"):
            self.base += "/api/v1"

    @property
    def enabled(self) -> bool:
        return bool(self.base)

    def _req(self, path: str, method="GET", payload=None, timeout=10):
        req = urllib.request.Request(f"{self.base}{path}", data=json.dumps(payload).encode() if payload is not None else None,
                                     headers={"Content-Type": "application/json"}, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode() or "null")

    def spools(self) -> list[dict]:
        try:
            return self._req("/spool?allow_archived=false") or []
        except (urllib.error.URLError, OSError, ValueError) as e:
            LOG.debug("Spoolman nedostupný: %s", e)
            return []

    def spool_for_tray(self, tray_global: int | None, tag_uid: str | None = None) -> dict | None:
        """Cívka podle RFID tagu (extra.tag) nebo podle přiřazení slotu (extra.active_tray končí _tray_<n>)."""
        if tray_global is None:
            return None
        slot = tray_global % 4 + 1 if tray_global < 254 else None
        for s in self.spools():
            extra = s.get("extra") or {}
            tag = (extra.get("tag") or "").strip('"').upper()
            if tag_uid and tag and tag == tag_uid.upper() and tag_uid.strip("0"):
                return s
        if slot is None:
            return None
        for s in self.spools():
            at = (s.get("extra") or {}).get("active_tray") or ""
            if re.search(rf'_tray_{slot}"?$', at):
                return s
        return None

    @staticmethod
    def price_per_kg(spool: dict) -> float | None:
        price = spool.get("price") or (spool.get("filament") or {}).get("price")
        weight = spool.get("initial_weight") or (spool.get("filament") or {}).get("weight") or 1000
        return round(float(price) / (float(weight) / 1000), 2) if price else None

    def use(self, spool_id: int, grams: float) -> bool:
        if grams <= 0:
            return True
        try:
            self._req(f"/spool/{spool_id}/use", "PUT", {"use_weight": round(grams, 2)})
            LOG.info("Spoolman: cívka %s −%.1f g", spool_id, grams)
            return True
        except (urllib.error.URLError, OSError, ValueError) as e:
            LOG.warning("Spoolman use selhal (cívka %s): %s", spool_id, e)
            return False
