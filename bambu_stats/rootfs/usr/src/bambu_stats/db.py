"""SQLite úložiště: sessions, filamenty, pauzy, vzorky telemetrie, HMS události.

Jedno připojení, jeden zámek – zapisuje se z více vláken (MQTT callback, plánovač),
ale vždy sériově. WAL + synchronous=NORMAL kvůli šetření flash/SD.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path

LOG = logging.getLogger("db")
SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS printers(
  serial TEXT PRIMARY KEY, name TEXT, model TEXT, ip TEXT,
  first_seen_ts INTEGER, last_seen_ts INTEGER, fw_version TEXT, tls_fingerprint TEXT);
CREATE TABLE IF NOT EXISTS sessions(
  id TEXT PRIMARY KEY, printer_serial TEXT NOT NULL, fingerprint TEXT NOT NULL,
  task_id TEXT, subtask_id TEXT, job_id TEXT, project_id TEXT, profile_id TEXT,
  subtask_name TEXT, gcode_file TEXT, print_type TEXT,
  status TEXT NOT NULL, result TEXT, result_confidence TEXT,
  started_ts INTEGER, start_source TEXT, print_started_ts INTEGER,
  ended_ts INTEGER, end_source TEXT,
  duration_s INTEGER, paused_s INTEGER DEFAULT 0, active_print_s INTEGER, pause_count INTEGER DEFAULT 0,
  total_layers INTEGER, last_layer INTEGER, last_percent INTEGER, last_remaining_min INTEGER,
  predicted_s INTEGER, predicted_source TEXT,
  print_error INTEGER, fail_reason TEXT, hms_serious_count INTEGER DEFAULT 0,
  filament_g REAL, filament_m REAL, filament_source TEXT, filament_is_estimate INTEGER DEFAULT 0,
  cloud_weight_g REAL, cloud_length_m REAL, plan_weight_g REAL, plan_length_m REAL, plan_prediction_s INTEGER,
  nozzle_type TEXT, nozzle_diameter TEXT, spd_lvl INTEGER, tray_now_start INTEGER,
  trays_start TEXT, trays_end TEXT,
  threemf_path TEXT, threemf_status TEXT, threemf_fetched_ts INTEGER,
  incomplete INTEGER DEFAULT 0, manual_override INTEGER DEFAULT 0, notes TEXT,
  created_ts INTEGER, updated_ts INTEGER, last_seen_ts INTEGER);
CREATE UNIQUE INDEX IF NOT EXISTS sessions_open ON sessions(printer_serial, fingerprint) WHERE ended_ts IS NULL;
CREATE INDEX IF NOT EXISTS sessions_ended ON sessions(printer_serial, ended_ts);
CREATE TABLE IF NOT EXISTS session_filaments(
  id INTEGER PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  filament_idx INTEGER, ams_id INTEGER, tray_id INTEGER, tray_global INTEGER,
  tray_info_idx TEXT, material TEXT, material_group TEXT, brand TEXT, color_hex TEXT,
  used_g REAL, used_m REAL, source TEXT, is_estimate INTEGER DEFAULT 0, mapping_source TEXT,
  remain_start INTEGER, remain_end INTEGER, tray_weight INTEGER);
CREATE INDEX IF NOT EXISTS sf_session ON session_filaments(session_id);
CREATE TABLE IF NOT EXISTS session_pauses(
  id INTEGER PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  start_ts INTEGER, end_ts INTEGER, reason TEXT, print_error INTEGER, hms_codes TEXT);
CREATE TABLE IF NOT EXISTS samples(
  ts INTEGER NOT NULL, printer_serial TEXT NOT NULL, session_id TEXT,
  gcode_state TEXT, stg_cur INTEGER, mc_percent INTEGER, layer_num INTEGER, remaining_min INTEGER,
  nozzle_temp REAL, nozzle_target REAL, bed_temp REAL, bed_target REAL, chamber_temp REAL,
  spd_lvl INTEGER, spd_mag INTEGER, fan_part INTEGER, fan_aux INTEGER, fan_chamber INTEGER, fan_heatbreak INTEGER,
  wifi_signal INTEGER, tray_now INTEGER, ams_humidity INTEGER, ams_temp REAL,
  PRIMARY KEY(printer_serial, ts));
CREATE INDEX IF NOT EXISTS samples_session ON samples(session_id, ts);
CREATE TABLE IF NOT EXISTS hms_events(
  id INTEGER PRIMARY KEY, printer_serial TEXT, session_id TEXT,
  code TEXT, attr INTEGER, code_raw INTEGER, module INTEGER, severity INTEGER,
  first_ts INTEGER, last_ts INTEGER, cleared_ts INTEGER, count INTEGER DEFAULT 1,
  UNIQUE(printer_serial, attr, code_raw, first_ts));
"""

SESSION_COLS = None  # doplní se při otevření


class Database:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA busy_timeout=5000")
            self.conn.execute("PRAGMA foreign_keys=ON")
            self.conn.executescript(SCHEMA)
            self._migrate()
        global SESSION_COLS
        SESSION_COLS = [r[1] for r in self.conn.execute("PRAGMA table_info(sessions)")]

    def _migrate(self):
        v = int(self.get_meta("schema_version", "0"))
        if v < SCHEMA_VERSION:
            self.set_meta("schema_version", str(SCHEMA_VERSION))

    # --- meta -----------------------------------------------------------------
    def get_meta(self, key, default=None):
        with self.lock:
            r = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r[0] if r else default

    def set_meta(self, key, value):
        with self.lock:
            self.conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                              (key, str(value)))

    # --- printers ---------------------------------------------------------------
    def touch_printer(self, serial, name=None, ip=None, model=None, fw=None, now=None):
        now = int(now or time.time())
        with self.lock:
            self.conn.execute("""INSERT INTO printers(serial,name,model,ip,first_seen_ts,last_seen_ts,fw_version)
                VALUES(?,?,?,?,?,?,?) ON CONFLICT(serial) DO UPDATE SET
                name=COALESCE(excluded.name,name), model=COALESCE(excluded.model,model), ip=COALESCE(excluded.ip,ip),
                last_seen_ts=excluded.last_seen_ts, fw_version=COALESCE(excluded.fw_version,fw_version)""",
                              (serial, name, model, ip, now, now, fw))

    def get_printer(self, serial):
        with self.lock:
            r = self.conn.execute("SELECT * FROM printers WHERE serial=?", (serial,)).fetchone()
        return dict(r) if r else None

    def set_tls_fingerprint(self, serial, fp):
        with self.lock:
            self.conn.execute("UPDATE printers SET tls_fingerprint=? WHERE serial=?", (fp, serial))

    # --- sessions ---------------------------------------------------------------
    def open_session_for(self, serial) -> dict | None:
        with self.lock:
            r = self.conn.execute("SELECT * FROM sessions WHERE printer_serial=? AND ended_ts IS NULL ORDER BY created_ts DESC LIMIT 1",
                                  (serial,)).fetchone()
        return dict(r) if r else None

    def get_session(self, sid) -> dict | None:
        with self.lock:
            r = self.conn.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone()
        return dict(r) if r else None

    def upsert_session(self, s: dict):
        cols = [c for c in SESSION_COLS if c in s]
        vals = [json.dumps(s[c]) if isinstance(s[c], (dict, list)) else s[c] for c in cols]
        sets = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "id")
        with self.lock:
            self.conn.execute(f"INSERT INTO sessions({','.join(cols)}) VALUES({','.join('?'*len(cols))}) "
                              f"ON CONFLICT(id) DO UPDATE SET {sets}", vals)

    def update_session(self, sid: str, **fields):
        if not fields:
            return
        fields["updated_ts"] = int(time.time())
        cols = [c for c in fields if c in SESSION_COLS]
        vals = [json.dumps(fields[c]) if isinstance(fields[c], (dict, list)) else fields[c] for c in cols]
        with self.lock:
            self.conn.execute(f"UPDATE sessions SET {', '.join(c+'=?' for c in cols)} WHERE id=?", vals + [sid])

    def last_closed_session(self, serial) -> dict | None:
        with self.lock:
            r = self.conn.execute("SELECT * FROM sessions WHERE printer_serial=? AND ended_ts IS NOT NULL ORDER BY ended_ts DESC LIMIT 1",
                                  (serial,)).fetchone()
        return dict(r) if r else None

    def recent_sessions(self, serial, limit=25) -> list[dict]:
        with self.lock:
            rows = self.conn.execute("SELECT * FROM sessions WHERE printer_serial=? AND ended_ts IS NOT NULL ORDER BY ended_ts DESC LIMIT ?",
                                     (serial, limit)).fetchall()
        return [dict(r) for r in rows]

    # --- pauses -------------------------------------------------------------------
    def open_pause(self, sid, start_ts, reason=None, print_error=None, hms_codes=None) -> int:
        with self.lock:
            cur = self.conn.execute("INSERT INTO session_pauses(session_id,start_ts,reason,print_error,hms_codes) VALUES(?,?,?,?,?)",
                                    (sid, int(start_ts), reason, print_error, json.dumps(hms_codes or [])))
            return cur.lastrowid

    def close_pause(self, sid, end_ts):
        with self.lock:
            self.conn.execute("UPDATE session_pauses SET end_ts=? WHERE session_id=? AND end_ts IS NULL", (int(end_ts), sid))

    def pauses(self, sid) -> list[dict]:
        with self.lock:
            return [dict(r) for r in self.conn.execute("SELECT * FROM session_pauses WHERE session_id=? ORDER BY start_ts", (sid,))]

    # --- filaments --------------------------------------------------------------------
    def replace_filaments(self, sid, rows: list[dict]):
        with self.lock:
            self.conn.execute("BEGIN")
            try:
                self.conn.execute("DELETE FROM session_filaments WHERE session_id=?", (sid,))
                for r in rows:
                    r = {**r, "session_id": sid}
                    cols = list(r.keys())
                    self.conn.execute(f"INSERT INTO session_filaments({','.join(cols)}) VALUES({','.join('?'*len(cols))})",
                                      [r[c] for c in cols])
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def filaments(self, sid) -> list[dict]:
        with self.lock:
            return [dict(r) for r in self.conn.execute("SELECT * FROM session_filaments WHERE session_id=?", (sid,))]

    # --- samples ------------------------------------------------------------------------
    def add_sample(self, row: dict):
        cols = list(row.keys())
        with self.lock:
            self.conn.execute(f"INSERT OR REPLACE INTO samples({','.join(cols)}) VALUES({','.join('?'*len(cols))})",
                              [row[c] for c in cols])

    def purge_samples(self, older_than_ts: int) -> int:
        with self.lock:
            cur = self.conn.execute("DELETE FROM samples WHERE ts<?", (int(older_than_ts),))
            return cur.rowcount

    # --- hms -----------------------------------------------------------------------------
    def upsert_hms(self, serial, sid, code, attr, code_raw, module, severity, ts) -> bool:
        """Vrátí True, když jde o novou (nebo znovu aktivní) událost."""
        with self.lock:
            r = self.conn.execute("SELECT id, last_ts FROM hms_events WHERE printer_serial=? AND attr=? AND code_raw=? AND cleared_ts IS NULL",
                                  (serial, attr, code_raw)).fetchone()
            if r:
                self.conn.execute("UPDATE hms_events SET last_ts=?, count=count+1 WHERE id=?", (int(ts), r["id"]))
                return False
            self.conn.execute("""INSERT OR IGNORE INTO hms_events(printer_serial,session_id,code,attr,code_raw,module,severity,first_ts,last_ts)
                                 VALUES(?,?,?,?,?,?,?,?,?)""", (serial, sid, code, attr, code_raw, module, severity, int(ts), int(ts)))
            return True

    def clear_hms_except(self, serial, active_pairs: set[tuple[int, int]], ts):
        with self.lock:
            rows = self.conn.execute("SELECT id, attr, code_raw FROM hms_events WHERE printer_serial=? AND cleared_ts IS NULL", (serial,)).fetchall()
            for r in rows:
                if (r["attr"], r["code_raw"]) not in active_pairs:
                    self.conn.execute("UPDATE hms_events SET cleared_ts=? WHERE id=?", (int(ts), r["id"]))

    def recent_serious_hms(self, serial, since_ts) -> list[dict]:
        with self.lock:
            return [dict(r) for r in self.conn.execute(
                "SELECT * FROM hms_events WHERE printer_serial=? AND severity IN (1,2) AND last_ts>=? ORDER BY last_ts DESC", (serial, int(since_ts)))]

    # --- údržba -------------------------------------------------------------------------------
    def checkpoint(self):
        with self.lock:
            self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def size_mb(self) -> float:
        total = 0
        for suffix in ("", "-wal"):
            p = Path(str(self.path) + suffix)
            if p.exists():
                total += p.stat().st_size
        return round(total / 1_048_576, 2)

    def query(self, sql, params=()):
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def close(self):
        with self.lock:
            self.conn.close()
