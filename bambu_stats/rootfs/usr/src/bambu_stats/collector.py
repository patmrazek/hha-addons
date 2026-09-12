"""Collector pro jednu tiskárnu: MQTT → Snapshot → StateMachine → SQLite → HA."""
from __future__ import annotations

import datetime as dt
import json
import logging
import threading
import time
from pathlib import Path
from zoneinfo import ZoneInfo

from . import aggregates, hms as hmsmod
from .config import PrinterConfig, Settings
from .db import Database
from .filament import FilamentResolver
from .ha_api import HomeAssistant
from .ha_mqtt import HAPublisher
from .printer_mqtt import PrinterMQTT
from .sampler import Sampler
from .spoolman import Spoolman
from .util import material_group
from .sync import GitSync
from .state_machine import Event, Snapshot, StateMachine

LOG = logging.getLogger("collector")
PROGRESS_WRITE_EVERY_S = 10
STATS_EVERY_S = 300
FILAMENT_RETRY_AT_S = (60, 300, 900)


class Collector:
    def __init__(self, settings: Settings, printer: PrinterConfig, db: Database, tz: ZoneInfo, prefix: str):
        self.settings, self.printer, self.db, self.tz = settings, printer, db, tz
        self.serial = printer.serial
        self.ha = HomeAssistant(settings.supervisor_token)
        self.spoolman = Spoolman(settings.spoolman_url) if settings.spoolman_url else None
        self.filament = FilamentResolver(db, printer, self.ha, settings.data_dir / "3mf_cache", spoolman=self.spoolman)
        self.sampler = Sampler(self.serial)
        open_row = db.open_session_for(self.serial)
        last = db.last_closed_session(self.serial)
        if open_row:
            LOG.info("nalezena otevřená session %s (%s, %s %%) – pokusím se navázat", open_row["id"], open_row.get("subtask_name"), open_row.get("last_percent"))
        self.sm = StateMachine(self.serial, open_row, last["fingerprint"] if last else None)
        pinfo = db.get_printer(self.serial) or {}
        # bez host/access_code (tiskárna je v jiné lokalitě) jede instance jen jako čtečka sdílené historie
        self.live = bool(printer.host and printer.access_code)
        self.mqtt = (PrinterMQTT(printer.host, self.serial, printer.access_code, self.on_state, settings.tls_verify,
                                 pinned_fingerprint=pinfo.get("tls_fingerprint"), on_pin=self._on_pin, on_connection=self._on_conn)
                     if self.live else None)
        self.pub = HAPublisher(settings.mqtt, prefix, self.serial, printer.name, on_command=self.on_command, currency=settings.currency)
        self._lock = threading.Lock()
        self._resolve_lock = threading.Lock()   # resolve smí běžet jen jednou naráz (jinak dvojí odečet ze cívky)
        self._last_progress_write = 0.0
        self._last_stats = 0.0
        self._last_current: tuple | None = None
        self._filament_marks: set[int] = set()
        self._last_snapshot: Snapshot | None = None
        self._stats_dirty = True
        self.db.touch_printer(self.serial, name=printer.name, ip=printer.host)
        self.sync: GitSync | None = None
        if settings.sync_repo and settings.sync_instance:
            self.sync = GitSync(db, settings.sync_repo, settings.sync_token, settings.sync_instance, settings.data_dir)
            LOG.info("sync historie zapnut: %s jako '%s'", settings.sync_repo, settings.sync_instance)
        self._last_sync = 0.0

    # --- start / stop --------------------------------------------------------------
    def start(self):
        self.pub.start()
        if self.mqtt:
            self.mqtt.start()
        else:
            LOG.info("režim jen synchronizace: tiskárna %s se v této lokalitě nesleduje", self.serial)
        threading.Thread(target=self._scheduler, name=f"sched-{self.serial[-4:]}", daemon=True).start()

    def stop(self):
        if self.mqtt:
            self.mqtt.stop()
        self.pub.stop()

    # --- callbacky -----------------------------------------------------------------------
    def _on_pin(self, fp: str):
        self.db.set_tls_fingerprint(self.serial, fp)

    def _on_conn(self, ok: bool):
        self._publish_status()

    def on_command(self, cmd: str):
        LOG.info("příkaz z HA: %s", cmd)
        if cmd == "recompute":
            self._stats_dirty = True
            self.publish_stats(force=True)
            self.publish_slots()
        elif cmd.startswith("assign_slot:"):
            # assign_slot:<konec_id_session>:<tray_global> - rucne doplnit slot u tisku, kde tiskarna slot neposlala
            try:
                _, sid_suffix, tray = cmd.split(":")
                row = next((r for r in self.db.query("SELECT * FROM sessions WHERE id LIKE ?", (f"%{sid_suffix}",))), None)
                if row:
                    self.db.update_session(row["id"], tray_now_start=int(tray))
                    row = self.db.get_session(row["id"])
                    self.filament.resolve(row, final=row.get("ended_ts") is not None)
                    self._stats_dirty = True
                    self.publish_stats(force=True)
                    LOG.info("session %s: slot nastaven na %s a prepocitan", row["id"][-8:], tray)
            except Exception:
                LOG.exception("assign_slot selhal")
        elif cmd.startswith("mark_defect") or cmd.startswith("mark_ok"):
            # mark_defect[:<konec_id>[:<poznámka>]] – tisk doběhl, ale díl je k ničemu (warp, ucpaná tryska…).
            # Filament zůstává spotřebovaný, jen se to nepočítá jako povedený tisk.
            parts = cmd.split(":", 2)
            quality = "defect" if parts[0] == "mark_defect" else "ok"
            sid_suffix = parts[1] if len(parts) > 1 and parts[1] else None
            note = parts[2] if len(parts) > 2 else None
            row = (next(iter(self.db.query("SELECT * FROM sessions WHERE id LIKE ?", (f"%{sid_suffix}",))), None) if sid_suffix
                   else self.db.last_closed_session(self.serial))
            if row:
                self.db.update_session(row["id"], quality=quality, quality_note=note)
                LOG.info("session %s označena jako %s%s", row["id"][-8:], quality, f" ({note})" if note else "")
                self._stats_dirty = True
                self.publish_stats(force=True)
            else:
                LOG.warning("mark_defect: session nenalezena (%s)", sid_suffix)
        elif cmd in ("maintenance_done", "desiccant_changed"):
            now_ts = int(time.time())
            self.db.set_meta(f"{cmd}_ts_{self.serial}", now_ts)
            key = f"{cmd}_history_{self.serial}"          # seznam všech výměn/údržeb kvůli grafu a doložení
            try:
                hist = json.loads(self.db.get_meta(key, "[]") or "[]")
            except ValueError:
                hist = []
            hist = sorted(set(hist + [now_ts]))[-50:]
            self.db.set_meta(key, json.dumps(hist))
            LOG.info("%s zaznamenáno", cmd)
            self._stats_dirty = True
            self.publish_stats(force=True)
        elif cmd.startswith("set_plan:"):
            # set_plan:<konec_id_session>:<gramy>[:<metry>] - rucne opravit planovanou hmotnost (napr. po prepsani cloud hodnotou)
            try:
                parts = cmd.split(":"); sid_suffix, grams = parts[1], float(parts[2]); metres = float(parts[3]) if len(parts) > 3 else None
                row = next((r for r in self.db.query("SELECT * FROM sessions WHERE id LIKE ?", (f"%{sid_suffix}",))), None)
                if row:
                    self.db.update_session(row["id"], cloud_weight_g=grams, cloud_length_m=metres, plan_weight_g=grams, plan_length_m=metres)
                    row = self.db.get_session(row["id"])
                    with self._resolve_lock:
                        self.filament.resolve(row, final=row.get("ended_ts") is not None)
                    self._stats_dirty = True
                    self.publish_stats(force=True)
                    LOG.info("session %s: plan nastaven na %.1f g a prepocitan", row["id"][-8:], grams)
            except Exception:
                LOG.exception("set_plan selhal")
        elif cmd == "refetch_3mf":
            with self._lock:
                sess = self.sm.session
            if sess:
                self.filament._attempts.pop(sess.id, None)
                row = self.db.get_session(sess.id)
                if row:
                    self.db.update_session(sess.id, threemf_status=None)
                    row["threemf_status"] = None
                    with self._resolve_lock:
                        self.filament.resolve(self.db.get_session(sess.id), final=False)
                    self.publish_filament_check(sess.id)
                    self.save_cover(sess.id)
                    self._publish_current_filament(sess)
            else:
                last = self.db.last_closed_session(self.serial)
                if last:
                    self.filament._attempts.pop(last["id"], None)
                    self.db.update_session(last["id"], threemf_status=None)
                    last["threemf_status"] = None
                    with self._resolve_lock:
                        self.filament.resolve(last, final=True)
                    self._stats_dirty = True
                    self.publish_stats(force=True)

    def on_state(self, state: dict, now: float):
        snap = Snapshot.from_print(state, now)
        with self._lock:
            self._last_snapshot = snap
            events = self.sm.feed(snap)
            session = self.sm.session
            for ev in events:
                self._handle(ev)
            if session and (now - self._last_progress_write >= PROGRESS_WRITE_EVERY_S) and not any(e.kind != "updated" for e in events):
                self.db.update_session(session.id, last_percent=session.last_percent, last_layer=session.last_layer,
                                       total_layers=session.total_layers, last_remaining_min=session.last_remaining_min,
                                       last_seen_ts=session.last_seen_ts, predicted_s=session.predicted_s,
                                       predicted_source=session.predicted_source, status=session.status, paused_s=session.paused_s)
                self._last_progress_write = now
            row = self.sampler.maybe_row(snap, session.id if session else None)
            if row:
                self.db.add_sample(row)
            self._track_hms(snap, session.id if session else None)
            self._publish_current(snap, session)

    # --- zpracování událostí -------------------------------------------------------------------
    def _handle(self, ev: Event):
        s = ev.session
        if ev.kind == "updated":
            return
        LOG.info("session %s: %s (%s, %s %%, %s)", s.id[-8:], ev.kind, s.subtask_name or "?", s.last_percent, s.status)
        if ev.kind in ("opened", "resumed", "started"):
            self.db.upsert_session(s.to_row())
            if ev.kind == "opened":
                self._filament_marks = set()
                threading.Thread(target=self._resolve_filament, args=(s.id, False), daemon=True).start()
        elif ev.kind == "paused":
            self.db.upsert_session(s.to_row())
            self.db.open_pause(s.id, ev.snapshot.ts if ev.snapshot else time.time(), reason=ev.note,
                               print_error=ev.snapshot.print_error if ev.snapshot else None,
                               hms_codes=[h["code"] for h in (ev.snapshot.serious_hms if ev.snapshot else [])])
        elif ev.kind == "unpaused":
            self.db.close_pause(s.id, ev.snapshot.ts if ev.snapshot else time.time())
            self.db.upsert_session(s.to_row())
        elif ev.kind in ("closed", "superseded", "lost"):
            self.db.close_pause(s.id, s.ended_ts or time.time())
            self.db.upsert_session(s.to_row())
            threading.Thread(target=self._finalize, args=(s.id,), daemon=True).start()
        self._last_progress_write = time.time()

    def _finalize(self, sid: str):
        row = self.db.get_session(sid)
        if row:
            self.save_cover(sid)
            with self._resolve_lock:
                try:
                    self.filament.resolve(self.db.get_session(sid), final=True)
                except Exception:
                    LOG.exception("resolver filamentu selhal")
        self._stats_dirty = True
        self.publish_stats(force=True)
        self.export_csv()
        self._run_sync()

    def _run_sync(self):
        if not self.sync:
            return
        res = self.sync.run_once()
        self._last_sync = time.time()
        if res.get("imported"):
            self._stats_dirty = True
            self.publish_stats(force=True)

    def export_csv(self):
        """CSV záloha historie do /share/bambu_stats/ (přístupné přes Samba/SSH, součást HA zálohy)."""
        share = Path("/share/bambu_stats")
        if not Path("/share").exists():
            share = self.settings.data_dir / "export"
        try:
            n = self.db.export_csv(share / f"history_{self.printer.slug}.csv", self.serial)
            LOG.debug("CSV export: %d session → %s", n, share)
        except Exception as e:
            LOG.warning("CSV export selhal: %s", e)

    def _resolve_filament(self, sid: str, final: bool):
        if not self._resolve_lock.acquire(blocking=False):
            LOG.debug("resolve filamentu už běží, přeskakuji")
            return
        try:
            self.__resolve_filament(sid, final)
        finally:
            self._resolve_lock.release()

    def __resolve_filament(self, sid: str, final: bool):
        row = self.db.get_session(sid)
        if row and row.get("ended_ts") is None:
            try:
                self.filament.resolve(row, final=final)
                fresh = self.db.get_session(sid) or {}
                with self._lock:
                    sess = self.sm.session
                    if sess and sess.id == sid and fresh.get("started_ts") and fresh.get("start_source") == "cloud_ha":
                        sess.started_ts = fresh["started_ts"]
                        sess.print_started_ts = fresh.get("print_started_ts") or sess.print_started_ts
                        sess.start_source = "cloud_ha"
                        self._last_current = None
                self._publish_current_filament(self.sm.session)
                self.publish_filament_check(sid)
                self.save_cover(sid)
            except Exception:
                LOG.exception("resolver filamentu selhal")

    def _track_hms(self, snap: Snapshot, sid: str | None):
        active = set()
        for h in snap.hms:
            d = hmsmod.decode(h)
            if not d:
                continue
            active.add((d["attr"], d["code_raw"]))
            if self.db.upsert_hms(self.serial, sid, d["code"], d["attr"], d["code_raw"], d["module"], d["severity"], snap.ts):
                LOG.info("HMS %s (závažnost %s)", d["code"], hmsmod.SEVERITY_NAMES.get(d["severity"], d["severity"]))
        self.db.clear_hms_except(self.serial, active, snap.ts)

    # --- publikace -------------------------------------------------------------------------------
    def _publish_current(self, snap: Snapshot, session):
        if session:
            key = (session.status, session.last_percent // 5, session.id)
            attrs = {"session_id": session.id, "name": session.subtask_name, "status": session.status,
                     "started": dt.datetime.fromtimestamp(session.started_ts, self.tz).strftime("%Y-%m-%dT%H:%M"),
                     "start_source": session.start_source, "elapsed_min": round((snap.ts - session.started_ts) / 60),
                     "percent": session.last_percent, "layer": f"{session.last_layer}/{session.total_layers}",
                     "remaining_min": session.last_remaining_min, "pauses": session.pause_count,
                     "paused_min": round(session.paused_s / 60), "predicted_min": round((session.predicted_s or 0) / 60) or None,
                     "gcode_state": snap.gcode_state, "tray_now": snap.tray_now, "print_type": session.print_type}
            state = session.status
        else:
            key = ("idle", snap.gcode_state)
            attrs = {"gcode_state": snap.gcode_state}
            state = "idle"
        if key != self._last_current:
            self._last_current = key
            self.pub.publish_value("current_session", state, attrs)
            self._publish_current_filament(session)
        tray_key = (snap.tray_now, tuple((t.tray_global, t.tray_type, t.color, t.remain) for t in snap.trays))
        if tray_key != getattr(self, "_last_tray_key", None):
            self._last_tray_key = tray_key
            threading.Thread(target=self.publish_slots, daemon=True).start()

    def publish_filament_check(self, sid: str):
        """Plán tisku per slot vs. zbývající gramy na přiřazené cívce ve Spoolmanu → ok / low / unknown."""
        row = self.db.get_session(sid) or {}
        fils = self.db.filaments(sid)
        if not fils or not any(f.get("used_g") for f in fils):
            self.pub.publish_value("filament_check", "unknown", {"session_id": sid, "name": row.get("subtask_name"), "reason": "plán hmotnosti není k dispozici", "slots": []})
            return
        spools = {sp["id"]: sp for sp in (self.spoolman.spools() if self.spoolman else [])}
        slots, state = [], "ok"
        for f in fils:
            sp = spools.get(f.get("spool_id"))
            need = round(f.get("used_g") or 0, 1)
            # co z cívky ubude ještě do konce tisku: plán mínus to, co už z ní tisk odečetl
            # (zbývající hmotnost cívky je průběžným odečtem snížená, porovnávat ji s celým plánem by lhalo)
            left = round(max(0.0, need - (f.get("spool_deducted_g") or 0)), 1)
            if sp is None:
                slots.append({"slot": (f["tray_global"] or 0) % 4 + 1 if f.get("tray_global") is not None else None, "material": f.get("material"),
                              "need_g": need, "need_left_g": left, "remaining_g": None, "status": "unknown"})
                state = "unknown" if state == "ok" else state
                continue
            rem = round(sp.get("remaining_weight") or 0)
            st = "low" if rem < left * 1.05 else "ok"
            if st == "low":
                state = "low"
            slots.append({"slot": f["tray_global"] % 4 + 1, "spool": f"{((sp.get('filament') or {}).get('vendor') or {}).get('name', '')} {(sp.get('filament') or {}).get('name', '')}".strip(),
                          "material": f.get("material"), "need_g": need, "need_left_g": left, "remaining_g": rem,
                          "deficit_g": round(max(0, left - rem)), "status": st})
        self.pub.publish_value("filament_check", state, {"session_id": sid, "name": row.get("subtask_name"), "slots": slots,
                                                         "checked": dt.datetime.now(self.tz).strftime("%Y-%m-%dT%H:%M")})
        if state == "low":
            LOG.warning("kontrola filamentu: %s", [x for x in slots if x["status"] == "low"])

    def save_cover(self, sid: str):
        """Uloží náhled modelu (ha-bambulab image entita, z cloudu/3MF) do /config/www → /local/bambu_stats/covers/<id>.jpg."""
        row = self.db.get_session(sid) or {}
        if row.get("cover") or not self.printer.ha_weight_entity:
            return
        ent = self.printer.ha_weight_entity.replace("sensor.", "image.").replace("_print_weight", "_cover_image")
        data = self.ha.image(ent)
        if not data:
            return
        try:
            d = self.settings.covers_dir
            d.mkdir(parents=True, exist_ok=True)
            (d / f"{sid}.jpg").write_bytes(data)
            url = f"/local/bambu_stats/covers/{sid}.jpg" if str(d).startswith("/homeassistant/www") else None
            self.db.update_session(sid, cover=url)
            LOG.info("náhled modelu uložen: %s (%d kB)", sid[-8:], len(data) // 1024)
        except OSError as e:
            LOG.debug("cover: %s", e)

    def _publish_current_filament(self, session):
        """Odhad zatím spotřebovaného filamentu běžícího tisku = plán (3MF/cloud) × procenta. Vždy odhad."""
        if not session:
            self.pub.publish_value("current_filament_g", 0, {"source": "none", "is_estimate": True, "plan_g": None})
            self.pub.publish_value("current_cost", 0, {"is_estimate": True})
            return
        row = self.db.get_session(session.id) or {}
        plan = row.get("plan_weight_g") or row.get("cloud_weight_g")
        src = "3mf" if row.get("threemf_status") == "ok" else ("cloud_ha" if row.get("cloud_weight_g") else "none")
        fils = self.db.filaments(session.id)
        pct = max(0, min(100, session.last_percent)) / 100
        attrs = {"source": src, "is_estimate": True, "plan_g": round(plan, 1) if plan else None, "percent": session.last_percent,
                 "materials": [{"material": f["material"], "color": f["color_hex"], "slot": f["tray_global"],
                                "g_so_far": round((f["used_g"] or 0) * pct, 1), "g_plan": f["used_g"]} for f in fils]}
        self.pub.publish_value("current_filament_g", round(plan * pct, 1) if plan else None, attrs)
        cost = sum((f["used_g"] or 0) * pct / 1000 * (f.get("spool_price_per_kg") or aggregates.price_of(self.settings.prices, f["material_group"])) for f in fils)
        if not fils and plan:
            cost = plan * pct / 1000 * aggregates.price_of(self.settings.prices, None)
        self.pub.publish_value("current_cost", round(cost, 1) if plan else None,
                               {"is_estimate": True, "plan_cost": round(sum((f["used_g"] or 0) / 1000 * aggregates.price_of(self.settings.prices, f["material_group"]) for f in fils), 1) if fils else None})

    def publish_slots(self):
        """Co je v AMS slotech: cívka ze Spoolmanu (název, barva, zbývá, cena), fallback = údaje z tiskárny."""
        with self._lock:
            snap = self._last_snapshot
        trays = {t.tray_global: t for t in (snap.trays if snap else [])}
        spools = self.spoolman.spools() if self.spoolman else []
        mismatches: list[dict] = []
        import re as _re
        for slot in range(1, 5):
            tg = slot - 1
            tray = trays.get(tg)
            sp = None
            for cand in spools:
                at = (cand.get("extra") or {}).get("active_tray") or ""
                tag = ((cand.get("extra") or {}).get("tag") or "").strip('"').upper()
                if _re.search(rf'_tray_{slot}"?$', at) or (tray and tray.tray_uuid and tray.tray_uuid.strip("0") and tag == tray.tray_uuid.upper()):
                    sp = cand
                    break
            if sp:
                fil = sp.get("filament") or {}
                vendor = (fil.get("vendor") or {}).get("name") or ""
                state = f"{vendor} {fil.get('name') or ''}".strip()[:255]
                # tiskárna hlásí jiný materiál než přiřazená cívka → někdo vyměnil cívku a nepřehodil ji ve SpoolmanSync
                known_type = (tray.tray_type or "").strip() if tray else ""
                mismatch = bool(known_type and known_type not in ("?", "Empty", "Unknown") and fil.get("material")
                                and material_group(known_type) != material_group(fil.get("material")))
                attrs = {"source": "spoolman", "spool_id": sp["id"], "material": fil.get("material"), "color": ("#" + fil["color_hex"]) if fil.get("color_hex") else (tray.color if tray else None),
                         "remaining_g": round(sp.get("remaining_weight") or 0), "used_g": round(sp.get("used_weight") or 0),
                         "price_per_kg": self.spoolman.price_per_kg(sp), "location": sp.get("location"), "comment": sp.get("comment"),
                         "active": bool(snap and snap.tray_now == tg), "printer_type": tray.tray_type if tray else None,
                         "mismatch": mismatch, "printer_color": tray.color if tray else None}
                if mismatch:
                    state = f"⚠ {state}"[:255]
                    LOG.warning("slot %d: tiskárna hlásí %s, ale přiřazená cívka je %s – přehoď ji ve SpoolmanSync",
                                slot, tray.tray_type, fil.get("material"))
            elif tray and tray.tray_type:
                state = f"{tray.sub_brands or ''} {tray.tray_type}".strip()
                attrs = {"source": "printer", "material": tray.tray_type, "color": tray.color, "remaining_g": None if tray.remain < 0 else round(tray.remain / 100 * (tray.tray_weight or 1000)),
                         "active": bool(snap and snap.tray_now == tg), "printer_type": tray.tray_type}
            else:
                state, attrs = "prázdný", {"source": "printer", "active": False}
            self.pub.publish_value(f"slot_{slot}", state, attrs)
            mismatches.append({"slot": slot, "printer": attrs.get("printer_type"), "spool": state.lstrip("⚠ ")}) if attrs.get("mismatch") else None
        self.pub.publish_value("slot_mismatch", "on" if mismatches else "off",
                               {"slots": mismatches, "hint": "Tiskárna hlásí u slotu jiný materiál než cívka přiřazená ve SpoolmanSync – přehoď přiřazení, jinak se spotřeba odečte ze špatné cívky."})

    def pending_spool_sessions(self, days: int = 120) -> list[dict]:
        """Dokončené tisky, které se ještě nestihly odečíst ze cívky (Spoolman byl nedostupný)."""
        since = int(time.time()) - days * 86400
        return self.db.query("""SELECT s.id, s.subtask_name, s.ended_ts, s.filament_g,
                                       SUM(COALESCE(f.used_g, 0) - COALESCE(f.spool_deducted_g, 0)) AS pending_g
                                FROM sessions s JOIN session_filaments f ON f.session_id = s.id
                                WHERE s.printer_serial = ? AND s.ended_ts IS NOT NULL AND s.ended_ts >= ?
                                  AND COALESCE(f.used_g, 0) - COALESCE(f.spool_deducted_g, 0) > 0.5
                                  AND s.manual_override IS NOT 1
                                GROUP BY s.id ORDER BY s.ended_ts""", (self.serial, since))

    def flush_pending_spools(self):
        """Doúčtuje do Spoolmanu spotřebu tisků, které proběhly, když byl nedostupný (jiná lokalita, výpadek VPN)."""
        if not (self.spoolman and self.spoolman.enabled):
            return
        rows = self.pending_spool_sessions()
        self.pub.publish_value("spoolman_pending", len(rows),
                               {"grams": round(sum(r["pending_g"] or 0 for r in rows), 1),
                                "spoolman": self.settings.spoolman_url, "reachable": self.spoolman.reachable,
                                "error": self.spoolman.last_error,
                                "prints": [{"name": (r["subtask_name"] or "?")[:40], "ended": dt.datetime.fromtimestamp(r["ended_ts"], self.tz).strftime("%Y-%m-%dT%H:%M"),
                                            "g": round(r["pending_g"], 1)} for r in rows[-15:]]})
        if not rows or self.spoolman.reachable is False:
            return
        done = 0
        for r in rows:
            row = self.db.get_session(r["id"])
            if not row:
                continue
            with self._resolve_lock:
                try:
                    self.filament.resolve(row, final=True)
                    done += 1
                except Exception:
                    LOG.exception("doúčtování session %s selhalo", r["id"][-8:])
            if self.spoolman.reachable is False:
                break
        if done:
            LOG.info("doúčtováno %d tisků do Spoolmanu", done)
            self._stats_dirty = True
            self.publish_stats(force=True)
            self.flush_pending_spools()

    def spoolman_baseline(self) -> dict:
        """Spotřeba a útrata, kterou add-on nezaznamenal (tisky před jeho zavedením, jiné tiskárny, ruční odvin).

        Bere se ze Spoolmanu: co z cívky ubylo, mínus to, co z ní odečetly naše tiskové session. Počítá se za běhu,
        takže se samo srovná, když se cívka doplní nebo opraví. Cena podle ceny konkrétní cívky.
        """
        if not (self.spoolman and self.spoolman.enabled):
            return {}
        tracked: dict[int, float] = {}
        for r in self.db.query("SELECT spool_id, SUM(spool_deducted_g) g FROM session_filaments WHERE spool_id IS NOT NULL GROUP BY spool_id"):
            tracked[r["spool_id"]] = r["g"] or 0
        out: dict[str, dict] = {}
        spools = self.spoolman.spools(include_archived=True)
        for sp in spools:
            fil = sp.get("filament") or {}
            hist = self.spoolman.used_g(sp) - tracked.get(sp["id"], 0)
            if hist <= 0.5:
                continue
            g = material_group(fil.get("material"))
            per_kg = self.spoolman.price_per_kg(sp) or 0
            b = out.setdefault(g, {"g": 0.0, "cost": 0.0, "spools": []})
            b["g"] += hist
            b["cost"] += hist / 1000 * per_kg
            b["spools"].append({"id": sp["id"], "name": f"{((fil.get('vendor') or {}).get('name') or '')} {fil.get('name') or ''}".strip(),
                                "g": round(hist), "kc": round(hist / 1000 * per_kg)})
        for b in out.values():
            b["g"] = round(b["g"], 1)
            b["cost"] = round(b["cost"], 1)
        return out

    def _publish_status(self):
        st = self.health()
        self.pub.publish_value("collector_status", "ok" if st["ok"] else "degraded", st)

    def publish_stats(self, force=False):
        try:
            with self._lock:
                open_sess = self.sm.session.to_row() if self.sm.session else None
            baseline = self.spoolman_baseline()
            stats = aggregates.compute(self.db, self.serial, time.time(), self.tz, open_sess, self.settings.prices, baseline)
            stats.update(aggregates.maintenance(self.db, self.serial, time.time(), open_sess,
                                                self.settings.maintenance_every_hours, self.settings.desiccant_every_days))
            with self._lock:
                snap = self._last_snapshot
            key_hist = f"desiccant_changed_history_{self.serial}"
            try:
                changes = json.loads(self.db.get_meta(key_hist, "[]") or "[]")
            except ValueError:
                changes = []
            if not changes:   # výměna zaznamenaná před 0.6.0 zná jen poslední čas – doplnit do historie
                last = int(self.db.get_meta(f"desiccant_changed_ts_{self.serial}", 0) or 0)
                if last:
                    changes = [last]
                    self.db.set_meta(key_hist, json.dumps(changes))
            hum = aggregates.humidity_series(self.db, self.serial, time.time(), self.tz, changes)
            stats["ams_humidity_history"] = (snap.ams_humidity if snap and snap.ams_humidity is not None else None)
            stats["ams_humidity_history_attrs"] = hum
            if snap and snap.nozzle_wear is not None:
                stats["nozzle_wear"] = round(snap.nozzle_wear, 1)
                stats["nozzle_wear_attrs"] = {"nozzle_type": snap.nozzle_type, "diameter": snap.nozzle_diameter}
            self.pub.publish_stats(stats, force=force)
            self._last_stats = time.time()
            self._stats_dirty = False
        except Exception:
            LOG.exception("výpočet statistik selhal")

    def health(self) -> dict:
        if not self.mqtt:   # jen sync – zdraví určuje poslední úspěšná synchronizace
            ok = bool(self.sync and self.sync.last_ok and time.time() - self.sync.last_ok < 3 * self.settings.sync_interval_min * 60)
            return {"ok": ok, "version": __import__("bambu_stats").__version__, "printer": self.printer.name, "mode": "sync_only",
                    "printer_mqtt_connected": False, "ha_mqtt_connected": self.pub.connected, "db_size_mb": self.db.size_mb(),
                    "sync": ({"instance": self.sync.instance, "last_ok": int(self.sync.last_ok) if self.sync.last_ok else None,
                              "error": self.sync.last_error, "imported_total": self.sync.imported_total} if self.sync else None)}
        age = (time.monotonic() - self.mqtt.last_msg_ts) if self.mqtt.last_msg_ts else None
        ok = self.mqtt.connected and age is not None and age < 120
        with self._lock:
            sess = self.sm.session
        return {"ok": bool(ok), "version": __import__("bambu_stats").__version__, "printer": self.printer.name,
                "printer_mqtt_connected": self.mqtt.connected, "last_report_age_s": round(age) if age is not None else None,
                "messages": self.mqtt.msg_count, "ha_mqtt_connected": self.pub.connected, "db_size_mb": self.db.size_mb(),
                "open_session": sess.id if sess else None, "open_session_name": sess.subtask_name if sess else None,
                "ha_api": self.ha.available,
                "spoolman": (None if not self.spoolman else {"url": self.settings.spoolman_url, "reachable": self.spoolman.reachable,
                                                            "error": self.spoolman.last_error}),
                "sync": ({"instance": self.sync.instance, "last_ok": int(self.sync.last_ok) if self.sync.last_ok else None,
                          "error": self.sync.last_error, "imported_total": self.sync.imported_total} if self.sync else None)}

    # --- plánovač -------------------------------------------------------------------------------------
    def _scheduler(self):
        last_daily = None
        time.sleep(15)
        self.publish_stats(force=True)
        while True:
            try:
                now = time.time()
                with self._lock:
                    sess = self.sm.session
                if sess:
                    # průběžný odečet ze cívky ve Spoolmanu (jen přírůstky, strop 90 % odhadu)
                    if self.spoolman and now - getattr(self, "_last_live_deduct", 0) >= 300 and sess.last_percent > 0:
                        self._last_live_deduct = now
                        threading.Thread(target=self._resolve_filament, args=(sess.id, False), daemon=True).start()
                    row = self.db.get_session(sess.id) or {}
                    if not row.get("cover") and now - sess.started_ts > 60:
                        self.save_cover(sess.id)
                    elapsed = now - sess.started_ts
                    for mark in FILAMENT_RETRY_AT_S:
                        if elapsed >= mark and mark not in self._filament_marks:
                            self._filament_marks.add(mark)
                            self._resolve_filament(sess.id, False)
                if self.sync and now - self._last_sync >= self.settings.sync_interval_min * 60:
                    self._run_sync()
                if self._stats_dirty or now - self._last_stats >= STATS_EVERY_S:
                    self.publish_stats()
                # přiřazení cívek se mění ve Spoolmanu (mimo tiskárnu) → kontrolovat pravidelně, publikuje se jen změna
                if self.spoolman and now - getattr(self, "_last_flush", 0) >= 300:
                    self._last_flush = now
                    self.flush_pending_spools()
                if self.live and now - getattr(self, "_last_slots", 0) >= 60:
                    self._last_slots = now
                    self.publish_slots()
                self._publish_status()
                today = dt.datetime.fromtimestamp(now, self.tz).date()
                if last_daily != today and dt.datetime.fromtimestamp(now, self.tz).hour >= 3:
                    last_daily = today
                    n = self.db.purge_samples(now - self.settings.samples_retention_days * 86400)
                    self.db.checkpoint()
                    LOG.info("údržba: smazáno %d vzorků, WAL checkpoint, DB %.1f MB", n, self.db.size_mb())
                    self.export_csv()
                    self._stats_dirty = True
                if self.mqtt and self.mqtt.connected:
                    self.db.touch_printer(self.serial, ip=self.printer.host)
            except Exception:
                LOG.exception("chyba plánovače")
            time.sleep(30)
