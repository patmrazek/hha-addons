"""Collector pro jednu tiskárnu: MQTT → Snapshot → StateMachine → SQLite → HA."""
from __future__ import annotations

import datetime as dt
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
        self.mqtt = PrinterMQTT(printer.host, self.serial, printer.access_code, self.on_state, settings.tls_verify,
                                pinned_fingerprint=pinfo.get("tls_fingerprint"), on_pin=self._on_pin, on_connection=self._on_conn)
        self.pub = HAPublisher(settings.mqtt, prefix, self.serial, printer.name, on_command=self.on_command, currency=settings.currency)
        self._lock = threading.Lock()
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
        self.mqtt.start()
        threading.Thread(target=self._scheduler, name=f"sched-{self.serial[-4:]}", daemon=True).start()

    def stop(self):
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
        elif cmd.startswith("set_plan:"):
            # set_plan:<konec_id_session>:<gramy>[:<metry>] - rucne opravit planovanou hmotnost (napr. po prepsani cloud hodnotou)
            try:
                parts = cmd.split(":"); sid_suffix, grams = parts[1], float(parts[2]); metres = float(parts[3]) if len(parts) > 3 else None
                row = next((r for r in self.db.query("SELECT * FROM sessions WHERE id LIKE ?", (f"%{sid_suffix}",))), None)
                if row:
                    self.db.update_session(row["id"], cloud_weight_g=grams, cloud_length_m=metres, plan_weight_g=grams, plan_length_m=metres)
                    row = self.db.get_session(row["id"])
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
                    self.filament.resolve(row, final=False)
            else:
                last = self.db.last_closed_session(self.serial)
                if last:
                    self.filament._attempts.pop(last["id"], None)
                    self.db.update_session(last["id"], threemf_status=None)
                    last["threemf_status"] = None
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
            try:
                self.filament.resolve(row, final=True)
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

    def _publish_status(self):
        st = self.health()
        self.pub.publish_value("collector_status", "ok" if st["ok"] else "degraded", st)

    def publish_stats(self, force=False):
        try:
            with self._lock:
                open_sess = self.sm.session.to_row() if self.sm.session else None
            stats = aggregates.compute(self.db, self.serial, time.time(), self.tz, open_sess, self.settings.prices)
            self.pub.publish_stats(stats, force=force)
            self._last_stats = time.time()
            self._stats_dirty = False
        except Exception:
            LOG.exception("výpočet statistik selhal")

    def health(self) -> dict:
        age = (time.monotonic() - self.mqtt.last_msg_ts) if self.mqtt.last_msg_ts else None
        ok = self.mqtt.connected and age is not None and age < 120
        with self._lock:
            sess = self.sm.session
        return {"ok": bool(ok), "version": __import__("bambu_stats").__version__, "printer": self.printer.name,
                "printer_mqtt_connected": self.mqtt.connected, "last_report_age_s": round(age) if age is not None else None,
                "messages": self.mqtt.msg_count, "ha_mqtt_connected": self.pub.connected, "db_size_mb": self.db.size_mb(),
                "open_session": sess.id if sess else None, "open_session_name": sess.subtask_name if sess else None,
                "ha_api": self.ha.available,
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
                    elapsed = now - sess.started_ts
                    for mark in FILAMENT_RETRY_AT_S:
                        if elapsed >= mark and mark not in self._filament_marks:
                            self._filament_marks.add(mark)
                            self._resolve_filament(sess.id, False)
                if self.sync and now - self._last_sync >= self.settings.sync_interval_min * 60:
                    self._run_sync()
                if self._stats_dirty or now - self._last_stats >= STATS_EVERY_S:
                    self.publish_stats()
                self._publish_status()
                today = dt.datetime.fromtimestamp(now, self.tz).date()
                if last_daily != today and dt.datetime.fromtimestamp(now, self.tz).hour >= 3:
                    last_daily = today
                    n = self.db.purge_samples(now - self.settings.samples_retention_days * 86400)
                    self.db.checkpoint()
                    LOG.info("údržba: smazáno %d vzorků, WAL checkpoint, DB %.1f MB", n, self.db.size_mb())
                    self.export_csv()
                    self._stats_dirty = True
                if self.mqtt.connected:
                    self.db.touch_printer(self.serial, ip=self.printer.host)
            except Exception:
                LOG.exception("chyba plánovače")
            time.sleep(30)
