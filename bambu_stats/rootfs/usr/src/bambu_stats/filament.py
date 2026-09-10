"""Určení spotřeby filamentu pro session – kombinace zdrojů s prioritou.

1. `3mf`        slice_info.config z 3MF na USB disku tiskárny (přes FTPS) – per filament used_g/used_m
2. `cloud_ha`   entity ha-bambulab (print_weight/print_length) – hodnota z Bambu cloudu
3. `ams_remain` rozdíl `remain` % × hmotnost cívky – jen Bambu RFID cívky (rozlišení 1 % ≈ 10 g)
4. `none`

U nedokončených tisků se plán (1/2) násobí postupem (`mc_percent/100`) a označí jako odhad;
pokud je k dispozici měření `ams_remain` s Δ ≥ 2 %, má přednost (je to skutečná změna).
Ruční hodnota (`manual_override`) se nikdy nepřepisuje.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from . import threemf
from .util import material_group

LOG = logging.getLogger("filament")
EXTERNAL_SLOT_IDS = {254, 255}
LIVE_CAP = 0.9          # během tisku odečítat nejvýš 90 % dosud spotřebovaného odhadu
MIN_STEP_G = 5.0        # menší přírůstky neposílat (šetří zápisy do Spoolmanu)


def _trays(session: dict, key: str) -> list[dict]:
    v = session.get(key)
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            v = []
    return v or []


class FilamentResolver:
    def __init__(self, db, printer, ha, cache_dir: Path, spoolman=None):
        self.db, self.printer, self.ha, self.spoolman = db, printer, ha, spoolman
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._attempts: dict[str, int] = {}

    # --- zdroje ------------------------------------------------------------------
    def _from_3mf(self, session: dict) -> threemf.PlateInfo | None:
        cache = self.cache_dir / f"{session['fingerprint']}.xml"
        if cache.exists():
            plates = threemf.parse_slice_info(cache.read_bytes())
            want = threemf.plate_index_from_gcode(session.get("gcode_file") or "")
            plate = next((p for p in plates if p.index == want), plates[0] if plates else None)
            if plate and session.get("threemf_status") != "ok":
                self.db.update_session(session["id"], threemf_status="ok")
                session["threemf_status"] = "ok"
            return plate
        if session.get("threemf_status") == "ok":
            return None
        if self._attempts.get(session["id"], 0) >= 6 or not session.get("subtask_name"):
            return None
        self._attempts[session["id"]] = self._attempts.get(session["id"], 0) + 1
        status, plate, path = threemf.fetch_plate(self.printer.host, self.printer.access_code,
                                                  session["subtask_name"], session.get("gcode_file") or "")
        self.db.update_session(session["id"], threemf_status=status, threemf_path=path, threemf_fetched_ts=int(time.time()))
        if status == "ok" and plate:
            # uložit surové XML pro pozdější přepočty (USB může být odpojen)
            try:
                ftp = threemf.PrinterFTP(self.printer.host, self.printer.access_code)
                xml = ftp.read_slice_info(path)
                if xml:
                    cache.write_bytes(xml)
            except Exception as e:  # cache není kritická
                LOG.debug("cache 3mf: %s", e)
            LOG.info("3MF nalezen: %s (%d filamentů, %.1f g)", path, len(plate.filaments), plate.weight_g or 0)
        return plate

    def _from_cloud(self, session: dict) -> tuple[float | None, float | None]:
        w = self.ha.numeric(self.printer.ha_weight_entity) if self.printer.ha_weight_entity else None
        l = self.ha.numeric(self.printer.ha_length_entity) if self.printer.ha_length_entity else None
        return w, l

    def _from_remain(self, session: dict) -> list[dict]:
        start = {t["tray_global"]: t for t in _trays(session, "trays_start")}
        rows = []
        for t in _trays(session, "trays_end"):
            s = start.get(t["tray_global"])
            if not s or s.get("remain", -1) < 0 or t.get("remain", -1) < 0 or not s.get("tray_weight"):
                continue
            if s.get("tray_uuid") and t.get("tray_uuid") and s["tray_uuid"] != t["tray_uuid"]:
                continue  # jiná cívka, rozdíl nedává smysl
            delta = s["remain"] - t["remain"]
            if delta < 1:
                continue
            rows.append(self._row(t, used_g=delta / 100 * s["tray_weight"], used_m=None, source="ams_remain",
                                  is_estimate=1, mapping="exact", remain_start=s["remain"], remain_end=t["remain"],
                                  tray_weight=s["tray_weight"]))
        return rows

    # --- mapování 3MF filamentu na slot ----------------------------------------------
    def _map(self, f: threemf.FilamentUse, trays: list[dict], tray_now: int | None, single: bool) -> tuple[dict | None, str]:
        color = (f.color or "").upper()
        for t in trays:
            if f.tray_info_idx and t.get("info_idx") == f.tray_info_idx and (t.get("color") or "").upper() == color:
                return t, "exact"
        if single and tray_now is not None:
            for t in trays:
                if t["tray_global"] == tray_now:
                    return t, "tray_now"
        for t in trays:
            if color and (t.get("color") or "").upper() == color and material_group(t.get("tray_type")) == material_group(f.type):
                return t, "color"
        return None, "unmapped"

    def _row(self, tray: dict | None, used_g, used_m, source, is_estimate, mapping, filament_idx=None,
             material=None, color=None, tray_info_idx=None, remain_start=None, remain_end=None, tray_weight=None) -> dict:
        mat = material or (tray or {}).get("tray_type") or ""
        return {
            "filament_idx": filament_idx,
            "ams_id": tray["ams_id"] if tray else None, "tray_id": tray["tray_id"] if tray else None,
            "tray_global": tray["tray_global"] if tray else None,
            "tray_info_idx": tray_info_idx or (tray or {}).get("info_idx"),
            "material": mat, "material_group": material_group(mat), "brand": (tray or {}).get("sub_brands"),
            "color_hex": color or (tray or {}).get("color"),
            "used_g": round(float(used_g), 2) if used_g is not None else None,
            "used_m": round(float(used_m), 2) if used_m is not None else None,
            "source": source, "is_estimate": int(is_estimate), "mapping_source": mapping,
            "remain_start": remain_start, "remain_end": remain_end, "tray_weight": tray_weight,
        }

    # --- hlavní ------------------------------------------------------------------------
    def fix_start_from_cloud(self, session: dict) -> int | None:
        """Pokud je začátek session jen odhad, zkusí přesný čas z ha-bambulab (`*_start_time`, z Bambu cloudu)."""
        if session.get("start_source") not in ("estimated_pct", "observed") or not session.get("incomplete"):
            return None
        ent = (self.printer.ha_weight_entity or "").replace("_print_weight", "_start_time")
        if not ent or ent == self.printer.ha_weight_entity:
            return None
        st = self.ha.state(ent)
        if not st or st.get("state") in (None, "unknown", "unavailable"):
            return None
        try:
            import datetime as dt
            ts = int(dt.datetime.fromisoformat(st["state"].replace("Z", "+00:00")).timestamp())
        except ValueError:
            return None
        if abs(ts - (session.get("started_ts") or 0)) > 6 * 3600:
            return None  # patří jinému tisku
        self.db.update_session(session["id"], started_ts=ts, print_started_ts=ts, start_source="cloud_ha")
        LOG.info("začátek session %s upřesněn z cloudu: %s", session["id"][-8:], st["state"])
        return ts

    def resolve(self, session: dict, final: bool) -> dict | None:
        """Spočítá a uloží spotřebu. Vrátí shrnutí (g, m, source, is_estimate) nebo None."""
        if session.get("manual_override"):
            return None
        self.fix_start_from_cloud(session)
        trays_start = _trays(session, "trays_start")
        result = session.get("result")
        finished_ok = final and result == "success"
        progress = (session.get("last_percent") or 0) / 100.0
        live_progress = progress
        if not final:
            progress = 1.0  # v přehledu u běžícího tisku hlásíme plán celé úlohy (označený jako odhad)
        elif result != "success" and progress <= 0 and session.get("total_layers"):
            progress = (session.get("last_layer") or 0) / max(session["total_layers"], 1)

        rows: list[dict] = []
        source, is_est = "none", 0
        total_g = total_m = None

        plate = self._from_3mf(session)
        if plate and plate.filaments:
            single = len(plate.filaments) == 1
            for f in plate.filaments:
                tray, mapping = self._map(f, trays_start, session.get("tray_now_start"), single)
                rows.append(self._row(tray, used_g=f.used_g * progress, used_m=f.used_m * progress,
                                      source="3mf" if finished_ok else "estimate", is_estimate=0 if finished_ok else 1,
                                      mapping=mapping, filament_idx=f.idx, material=f.type or (tray or {}).get("tray_type"),
                                      color=f.color, tray_info_idx=f.tray_info_idx))
            self.db.update_session(session["id"], plan_weight_g=plate.weight_g or sum(f.used_g for f in plate.filaments),
                                   plan_length_m=sum(f.used_m for f in plate.filaments), plan_prediction_s=plate.prediction_s,
                                   predicted_s=plate.prediction_s or session.get("predicted_s"),
                                   predicted_source="3mf" if plate.prediction_s else session.get("predicted_source"))
            source, is_est = ("3mf", 0) if finished_ok else ("estimate", 1)
        else:
            # živou cloud hodnotu číst jen u otevřené session – po uzavření už entita patří dalšímu tisku
            w, l = (None, None) if (session.get("ended_ts") and session.get("cloud_weight_g")) else self._from_cloud(session)
            if w:
                self.db.update_session(session["id"], cloud_weight_g=w, cloud_length_m=l, plan_weight_g=w, plan_length_m=l)
            else:
                w, l = session.get("cloud_weight_g"), session.get("cloud_length_m")
            if w:
                tray = next((t for t in trays_start if t["tray_global"] == session.get("tray_now_start")), None)
                rows.append(self._row(tray, used_g=w * progress, used_m=(l or 0) * progress or None,
                                      source="cloud_ha" if finished_ok else "estimate", is_estimate=0 if finished_ok else 1,
                                      mapping="tray_now" if tray else "unmapped"))
                source, is_est = ("cloud_ha", 0) if finished_ok else ("estimate", 1)

        if final:
            remain_rows = self._from_remain(session)
            remain_total = sum(r["used_g"] for r in remain_rows)
            if remain_rows and (not rows or (result != "success" and remain_total >= 20)):
                rows, source, is_est = remain_rows, "ams_remain", 1

        # Spoolman: cívka podle slotu → cena cívky, odečet (jen rozdíl proti už odečtenému, přežije opakovaný resolve)
        previous = {r["tray_global"]: r for r in self.db.filaments(session["id"]) if r.get("tray_global") is not None}
        live = 1.0 if final else max(0.0, min(1.0, live_progress))   # kolik z plánu je reálně protlačeno
        trays_by_global = {t["tray_global"]: t for t in trays_start}
        for r in rows:
            prev = previous.get(r["tray_global"]) or {}
            r["spool_deducted_g"] = prev.get("spool_deducted_g") or 0
            r["spool_id"], r["spool_price_per_kg"] = prev.get("spool_id"), prev.get("spool_price_per_kg")
            if self.spoolman and self.spoolman.enabled and r["tray_global"] is not None:
                tag = (trays_by_global.get(r["tray_global"]) or {}).get("tray_uuid") or ""
                sp = self.spoolman.spool_for_tray(r["tray_global"], tag)
                if sp:
                    r["spool_id"] = sp["id"]
                    r["spool_price_per_kg"] = self.spoolman.price_per_kg(sp)
                    # cíl odečtu: po dokončení celá spotřeba, během tisku max LIVE_CAP plánu (rezerva proti přeodečtení,
                    # když se plán ještě upřesní – např. dorazí 3MF s nižší hodnotou než cloud)
                    target = (r["used_g"] or 0) if final else round((r["used_g"] or 0) * live * LIVE_CAP, 2)
                    delta = target - r["spool_deducted_g"]
                    if final and delta < -0.05:      # plán revidován dolů → vrátit přeodečtené
                        if self.spoolman.use(sp["id"], delta):
                            r["spool_deducted_g"] = target
                    elif delta > MIN_STEP_G or (final and delta > 0.05):
                        if self.spoolman.use(sp["id"], delta):
                            r["spool_deducted_g"] = target
        if rows:
            total_g = round(sum(r["used_g"] or 0 for r in rows), 2)
            ms = [r["used_m"] for r in rows if r["used_m"] is not None]
            total_m = round(sum(ms), 2) if ms else None
        self.db.replace_filaments(session["id"], rows)
        self.db.update_session(session["id"], filament_g=total_g, filament_m=total_m, filament_source=source,
                               filament_is_estimate=is_est)
        return {"g": total_g, "m": total_m, "source": source, "is_estimate": is_est}
