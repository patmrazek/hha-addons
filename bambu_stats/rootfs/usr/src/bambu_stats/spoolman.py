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
        self.reachable: bool | None = None      # None = ještě se nezkoušelo
        self.last_error: str | None = None
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

    def spools(self, include_archived: bool = False) -> list[dict]:
        try:
            out = self._req(f"/spool?allow_archived={'true' if include_archived else 'false'}") or []
            if self.reachable is False:
                LOG.info("Spoolman je zase dostupný (%s)", self.base)
            self.reachable, self.last_error = True, None
            return out
        except (urllib.error.URLError, OSError, ValueError) as e:
            if self.reachable is not False:
                LOG.warning("Spoolman nedostupný (%s): %s – spotřeba se zapisuje lokálně a doúčtuje se, až bude spojení", self.base, e)
            self.reachable, self.last_error = False, str(e)[:120]
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
        # více cívek může mít stejný slot (staré přiřazení nejde ve Spoolmanu smazat) → vzít tu naposledy použitou
        found = [s for s in self.spools() if re.search(rf'_tray_{slot}"?$', (s.get("extra") or {}).get("active_tray") or "")]
        if not found:
            return None
        found.sort(key=lambda s: (s.get("first_used") or "", s.get("last_used") or "", s.get("registered") or ""), reverse=True)
        return found[0]

    @staticmethod
    def used_g(spool: dict) -> float:
        """Kolik už z cívky ubylo – Spoolman si to vede sám (jinak dopočet z počáteční hmotnosti)."""
        used = spool.get("used_weight")
        if used is None:
            init = spool.get("initial_weight") or (spool.get("filament") or {}).get("weight") or 0
            used = float(init) - float(spool.get("remaining_weight") or 0)
        return max(0.0, float(used))

    @staticmethod
    def price_per_kg(spool: dict) -> float | None:
        price = spool.get("price") or (spool.get("filament") or {}).get("price")
        weight = spool.get("initial_weight") or (spool.get("filament") or {}).get("weight") or 1000
        return round(float(price) / (float(weight) / 1000), 2) if price else None

    def use(self, spool_id: int, grams: float) -> bool:
        """Odečte gramy z cívky; záporná hodnota je vrátí (oprava přeodečtení po revizi plánu)."""
        if abs(grams) < 0.05:
            return True
        try:
            if grams > 0:
                self._req(f"/spool/{spool_id}/use", "PUT", {"use_weight": round(grams, 2)})
            else:   # /use neumí záporné hodnoty → navýšit remaining_weight přímo
                sp = self._req(f"/spool/{spool_id}")
                self._req(f"/spool/{spool_id}", "PATCH", {"remaining_weight": round((sp.get("remaining_weight") or 0) - grams, 2)})
            LOG.info("Spoolman: cívka %s %s%.1f g", spool_id, "−" if grams > 0 else "+", abs(grams))
            return True
        except (urllib.error.URLError, OSError, ValueError) as e:
            LOG.warning("Spoolman use selhal (cívka %s): %s", spool_id, e)
            return False
