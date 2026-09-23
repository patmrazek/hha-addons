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
SCHEMA_VERSION = 7

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
  trays_start TEXT, trays_end TEXT, tray_spans TEXT,
  threemf_path TEXT, threemf_status TEXT, threemf_fetched_ts INTEGER,
  incomplete INTEGER DEFAULT 0, manual_override INTEGER DEFAULT 0, notes TEXT,
  origin TEXT, synced_ts INTEGER, cover TEXT, quality TEXT, quality_note TEXT,
  created_ts INTEGER, updated_ts INTEGER, last_seen_ts INTEGER);
CREATE UNIQUE INDEX IF NOT EXISTS sessions_open ON sessions(printer_serial, fingerprint) WHERE ended_ts IS NULL;
CREATE INDEX IF NOT EXISTS sessions_ended ON sessions(printer_serial, ended_ts);
CREATE TABLE IF NOT EXISTS session_filaments(
  id INTEGER PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  filament_idx INTEGER, ams_id INTEGER, tray_id INTEGER, tray_global INTEGER,
  tray_info_idx TEXT, material TEXT, material_group TEXT, brand TEXT, color_hex TEXT,
  used_g REAL, used_m REAL, source TEXT, is_estimate INTEGER DEFAULT 0, mapping_source TEXT,
  remain_start INTEGER, remain_end INTEGER, tray_weight INTEGER,
  spool_id INTEGER, spool_price_per_kg REAL, spool_deducted_g REAL DEFAULT 0);
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
CREATE TABLE IF NOT EXISTS slot_spools(
  id INTEGER PRIMARY KEY, printer_serial TEXT NOT NULL, tray_global INTEGER NOT NULL,
  spool_id INTEGER, label TEXT, note TEXT,
  from_ts INTEGER NOT NULL, to_ts INTEGER, pushed_ts INTEGER, created_ts INTEGER);
CREATE INDEX IF NOT EXISTS slot_spools_idx ON slot_spools(printer_serial, tray_global, from_ts);
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
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(sessions)")}
        if "origin" not in cols:
            self.conn.execute("ALTER TABLE sessions ADD COLUMN origin TEXT")
        if "synced_ts" not in cols:
            self.conn.execute("ALTER TABLE sessions ADD COLUMN synced_ts INTEGER")
        if "cover" not in cols:
            self.conn.execute("ALTER TABLE sessions ADD COLUMN cover TEXT")
        for col in ("quality", "quality_note"):      # 'ok' | 'defect' – vytištěno, ale zmetek
            if col not in cols:
                self.conn.execute(f"ALTER TABLE sessions ADD COLUMN {col} TEXT")
        if "tray_spans" not in cols:      # úseky tisku po slotech kvůli auto-refillu AMS
            self.conn.execute("ALTER TABLE sessions ADD COLUMN tray_spans TEXT")
        # Deník slotů se nově sdílí mezi lokalitami, takže potřebuje přirozený klíč — jinak by
        # import stejnou výměnu přidával znovu při každé synchronizaci. Starší databáze můžou
        # mít duplicity z doby bez klíče, ty se nejdřív sloučí (ponechá se nejnovější zápis).
        self.conn.execute("""DELETE FROM slot_spools WHERE id NOT IN (
                               SELECT MAX(id) FROM slot_spools GROUP BY printer_serial, tray_global, from_ts)""")
        self.conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS slot_spools_klic
                             ON slot_spools(printer_serial, tray_global, from_ts)""")
        fcols = {r[1] for r in self.conn.execute("PRAGMA table_info(session_filaments)")}
        for col, typ in (("spool_id", "INTEGER"), ("spool_price_per_kg", "REAL"), ("spool_deducted_g", "REAL DEFAULT 0")):
            if col not in fcols:
                self.conn.execute(f"ALTER TABLE session_filaments ADD COLUMN {col} {typ}")
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

    def replace_pauses(self, sid, rows: list[dict]):
        with self.lock:
            self.conn.execute("DELETE FROM session_pauses WHERE session_id=?", (sid,))
            for r in rows:
                r = {**r, "session_id": sid}
                cols = list(r.keys())
                self.conn.execute(f"INSERT INTO session_pauses({','.join(cols)}) VALUES({','.join('?'*len(cols))})", [r[c] for c in cols])

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

    def export_csv(self, path: Path, serial: str | None = None) -> int:
        """Vypíše uzavřené session (s filamenty per materiál) do CSV – čitelná záloha mimo SQLite."""
        import csv
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        where = "WHERE ended_ts IS NOT NULL" + (" AND printer_serial=?" if serial else "")
        rows = self.query(f"SELECT * FROM sessions {where} ORDER BY started_ts", (serial,) if serial else ())
        cols = ["id", "printer_serial", "subtask_name", "result", "result_confidence", "started", "ended", "duration_min",
                "active_min", "paused_min", "pause_count", "layers", "filament_g", "filament_m", "filament_source",
                "filament_is_estimate", "materials", "print_type", "start_source", "end_source", "print_error", "fail_reason"]
        tmp = path.with_suffix(".tmp")
        with tmp.open("w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(cols)
            for r in rows:
                mats = self.query("SELECT material, used_g FROM session_filaments WHERE session_id=?", (r["id"],))
                w.writerow([r["id"], r["printer_serial"], r["subtask_name"], r["result"], r["result_confidence"],
                            time.strftime("%Y-%m-%d %H:%M", time.localtime(r["started_ts"] or 0)),
                            time.strftime("%Y-%m-%d %H:%M", time.localtime(r["ended_ts"] or 0)),
                            round((r["duration_s"] or 0) / 60), round((r["active_print_s"] or 0) / 60), round((r["paused_s"] or 0) / 60),
                            r["pause_count"], f"{r['last_layer']}/{r['total_layers']}", r["filament_g"], r["filament_m"],
                            r["filament_source"], r["filament_is_estimate"],
                            "; ".join(f"{m['material']} {m['used_g']} g" for m in mats), r["print_type"], r["start_source"],
                            r["end_source"], r["print_error"], r["fail_reason"]])
        tmp.replace(path)
        return len(rows)

    # --- osazení slotů (lokální deník, funguje i bez Spoolmanu) ------------------------------
    def set_slot_spool(self, serial, tray_global, spool_id, label=None, note=None, from_ts=None):
        now = int(from_ts or time.time())
        with self.lock:
            self.conn.execute("UPDATE slot_spools SET to_ts=? WHERE printer_serial=? AND tray_global=? AND to_ts IS NULL AND from_ts<=?",
                              (now, serial, tray_global, now))
            self.conn.execute("""INSERT INTO slot_spools(printer_serial,tray_global,spool_id,label,note,from_ts,created_ts)
                                 VALUES(?,?,?,?,?,?,?)""", (serial, tray_global, spool_id, label, note, now, int(time.time())))

    def slot_spool_at(self, serial, tray_global, ts) -> dict | None:
        """Která cívka byla v daném slotu v daný čas (podle lokálního deníku)."""
        with self.lock:
            r = self.conn.execute("""SELECT * FROM slot_spools WHERE printer_serial=? AND tray_global=? AND from_ts<=?
                                     AND (to_ts IS NULL OR to_ts>?) ORDER BY from_ts DESC LIMIT 1""",
                                  (serial, tray_global, int(ts), int(ts))).fetchone()
        return dict(r) if r else None

    def slot_spools(self, serial, only_unpushed=False) -> list[dict]:
        sql = "SELECT * FROM slot_spools WHERE printer_serial=?" + (" AND pushed_ts IS NULL" if only_unpushed else "") + " ORDER BY from_ts"
        return self.query(sql, (serial,))

    def mark_slot_pushed(self, rec_id):
        with self.lock:
            self.conn.execute("UPDATE slot_spools SET pushed_ts=? WHERE id=?", (int(time.time()), rec_id))

    # --- data sdílená mezi lokalitami -------------------------------------------
    # Tiskárna cestuje, takže obě instance mají mít stejnou historii. Kromě tisků sem patří
    # i údržba, silikagel a deník osazení slotů — jinak by po převozu počítadlo údržby začalo
    # od nuly a odečty ze cívek by sahaly na špatnou cívku. Telemetrie (`samples`) se záměrně
    # nesdílí: jsou to desítky MB a mimo svou lokalitu nemají smysl.
    META_SDILENE = ("maintenance_done_ts", "maintenance_done_history",
                    "desiccant_changed_ts", "desiccant_changed_history")

    def meta_pro_tiskarnu(self, serial: str) -> dict:
        """Meta klíče vázané na tiskárnu, ve tvaru {krátký_název: hodnota}."""
        out = {}
        for zaklad in self.META_SDILENE:
            v = self.get_meta(f"{zaklad}_{serial}")
            if v is not None:
                out[zaklad] = v
        return out

    def sluc_meta(self, serial: str, cizi: dict) -> int:
        """Přijme meta z druhé lokality. Časy: vyhrává novější. Historie: sjednotí se.

        U jednoho času (poslední údržba) dává smysl novější zápis — údržba se dělá jednou
        a poslední platí. U historie ne: každá lokalita mohla zaznamenat jinou událost a
        obě se staly, takže se seznamy spojí.
        """
        zmeny = 0
        for zaklad, hodnota in (cizi or {}).items():
            if zaklad not in self.META_SDILENE:
                continue
            klic = f"{zaklad}_{serial}"
            moje = self.get_meta(klic)
            if zaklad.endswith("_history"):
                try:
                    a = json.loads(moje or "[]")
                    b = json.loads(hodnota or "[]")
                except ValueError:
                    continue
                spojeno = sorted({int(x) for x in list(a) + list(b)})[-50:]
                if spojeno != a:
                    self.set_meta(klic, json.dumps(spojeno))
                    zmeny += 1
            else:
                try:
                    if int(hodnota) > int(moje or 0):
                        self.set_meta(klic, int(hodnota))
                        zmeny += 1
                except (TypeError, ValueError):
                    continue
        return zmeny

    def slot_spools_od(self, serial: str, od_ts: int = 0) -> list[dict]:
        return self.query("SELECT * FROM slot_spools WHERE printer_serial=? AND COALESCE(created_ts, from_ts) >= ?"
                          " ORDER BY from_ts", (serial, int(od_ts)))

    def sluc_slot_spools(self, rows: list[dict]) -> int:
        """Přijme cizí záznamy deníku slotů. Klíč je (tiskárna, slot, od kdy) — stejná výměna
        zapsaná na obou místech se tím pádem neztrojí."""
        zmeny = 0
        with self.lock:
            for r in rows or []:
                d = {k: v for k, v in r.items() if k != "id"}
                if not d.get("printer_serial") or d.get("tray_global") is None or not d.get("from_ts"):
                    continue
                cols = list(d)
                try:
                    cur = self.conn.execute(
                        f"INSERT OR IGNORE INTO slot_spools({','.join(cols)}) VALUES({','.join('?' * len(cols))})",
                        [d[c] for c in cols])
                    zmeny += cur.rowcount or 0
                except sqlite3.Error:
                    continue
        return zmeny

    def hms_od(self, serial: str, od_ts: int = 0) -> list[dict]:
        return self.query("SELECT * FROM hms_events WHERE printer_serial=? AND first_ts >= ? ORDER BY first_ts",
                          (serial, int(od_ts)))

    def sluc_hms(self, rows: list[dict]) -> int:
        zmeny = 0
        with self.lock:
            for r in rows or []:
                d = {k: v for k, v in r.items() if k != "id"}
                cols = list(d)
                try:
                    cur = self.conn.execute(
                        f"INSERT OR IGNORE INTO hms_events({','.join(cols)}) VALUES({','.join('?' * len(cols))})",
                        [d[c] for c in cols])
                    zmeny += cur.rowcount or 0
                except sqlite3.Error:
                    continue
        return zmeny

    def query(self, sql, params=()):
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def close(self):
        with self.lock:
            self.conn.close()
