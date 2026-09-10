"""Agregované statistiky ze SQLite → slovník hodnot pro MQTT Discovery entity.

Vše se počítá z uzavřených session (ended_ts NOT NULL). Session s result='unknown'
(ztracené při výpadku) se do success rate nepočítají, ale do času tisku ano.
"""
from __future__ import annotations

import datetime as dt
import statistics
from zoneinfo import ZoneInfo

from .util import MATERIAL_GROUPS

HISTORY_N = 25


def _local(ts: int, tz: ZoneInfo) -> dt.datetime:
    return dt.datetime.fromtimestamp(ts, tz)


def _hours(sec) -> float:
    return round((sec or 0) / 3600, 2)


def price_of(prices: dict | None, material_group: str | None) -> float:
    """Cena za kg pro skupinu materiálu; 'OTHER' nebo 0, když není nastaveno."""
    if not prices:
        return 0.0
    g = (material_group or "other").upper()
    return float(prices.get(g) or prices.get("OTHER") or 0.0)


def maintenance(db, serial: str, now: float, open_session: dict | None, every_hours: int, desiccant_days: int) -> dict:
    """Hodiny/tisky od poslední údržby (meta maintenance_done_ts) a dny od výměny silikagelu (meta desiccant_changed_ts)."""
    now_i = int(now)
    m_ts = int(db.get_meta(f"maintenance_done_ts_{serial}", 0) or 0)
    d_ts = int(db.get_meta(f"desiccant_changed_ts_{serial}", 0) or 0)
    rows = db.query("SELECT duration_s FROM sessions WHERE printer_serial=? AND ended_ts IS NOT NULL AND ended_ts>?", (serial, m_ts))
    hours = sum(r["duration_s"] or 0 for r in rows) / 3600
    if open_session and open_session.get("started_ts"):
        hours += max(0, now_i - max(open_session["started_ts"], m_ts)) / 3600
    days = (now_i - d_ts) / 86400 if d_ts else None
    return {"hours_since_maintenance": round(hours, 1), "prints_since_maintenance": len(rows),
            "hours_since_maintenance_attrs": {"last_maintenance": _iso(m_ts, ZoneInfo("Europe/Prague")) if m_ts else None,
                                              "every_hours": every_hours, "due": hours >= every_hours,
                                              "remaining_hours": round(max(0, every_hours - hours), 1)},
            "days_since_desiccant": round(days, 1) if days is not None else None,
            "days_since_desiccant_attrs": {"last_change": _iso(d_ts, ZoneInfo("Europe/Prague")) if d_ts else None,
                                           "every_days": desiccant_days, "due": (days is not None and days >= desiccant_days)}}


def humidity_series(db, serial: str, now: float, tz, changes: list[int] | None = None) -> dict:
    """Vlhkost v AMS v čase z podvzorkované telemetrie: 48 h po hodinách (průměr) a 90 dní po dnech (min/prům/max).

    Vlhkost se zapisuje do `samples` každých 10 s při tisku / 60 s v klidu, takže trend přežije i restart HA
    (recorder drží jen ~10 dní). `changes` = časy výměn silikagelu, na grafu se zobrazí jako body.
    """
    now_i = int(now)
    h0 = now_i - 48 * 3600
    rows = db.query("SELECT ts, ams_humidity FROM samples WHERE printer_serial=? AND ts>=? AND ams_humidity IS NOT NULL ORDER BY ts",
                    (serial, now_i - 90 * 86400))
    hourly = [[0, 0.0] for _ in range(48)]          # [počet, součet]
    today0 = _local(now_i, tz).replace(hour=0, minute=0, second=0, microsecond=0)
    daily_start = today0 - dt.timedelta(days=89)
    daily = [[0, 0.0, None, None] for _ in range(90)]   # [počet, součet, min, max]
    for r in rows:
        v = r["ams_humidity"]
        if r["ts"] >= h0:
            i = int((r["ts"] - h0) // 3600)
            if 0 <= i < 48:
                hourly[i][0] += 1
                hourly[i][1] += v
        di = (_local(r["ts"], tz).replace(hour=0, minute=0, second=0, microsecond=0) - daily_start).days
        if 0 <= di < 90:
            d = daily[di]
            d[0] += 1
            d[1] += v
            d[2] = v if d[2] is None else min(d[2], v)
            d[3] = v if d[3] is None else max(d[3], v)
    return {
        "hourly": {"start": _local(h0, tz).strftime("%Y-%m-%dT%H:00"),
                   "rows": [round(c[1] / c[0], 1) if c[0] else None for c in hourly]},
        "daily": {"start": daily_start.strftime("%Y-%m-%d"),
                  "rows": [[round(d[1] / d[0], 1), d[2], d[3]] if d[0] else None for d in daily]},
        "changes": [_iso(t, tz) for t in sorted(changes or [])],
        "cols": ["avg", "min", "max"],
    }


def compute(db, serial: str, now: float, tz: ZoneInfo, open_session: dict | None = None, prices: dict | None = None) -> dict:
    now_i = int(now)
    sessions = db.query("SELECT * FROM sessions WHERE printer_serial=? AND ended_ts IS NOT NULL ORDER BY ended_ts", (serial,))
    fil_rows = db.query("""SELECT f.*, s.ended_ts FROM session_filaments f JOIN sessions s ON s.id=f.session_id
                           WHERE s.printer_serial=? AND s.ended_ts IS NOT NULL""", (serial,))
    for r in fil_rows:
        per_kg = r.get("spool_price_per_kg") or price_of(prices, r["material_group"])
        r["cost"] = round((r["used_g"] or 0) / 1000 * per_kg, 2)
    cost_by_session: dict[str, float] = {}
    for r in fil_rows:
        cost_by_session[r["session_id"]] = cost_by_session.get(r["session_id"], 0) + r["cost"]
    known = [s for s in sessions if s["result"] in ("success", "failed", "cancelled")]
    ok = [s for s in known if s["result"] == "success"]
    failed = [s for s in known if s["result"] == "failed"]
    cancelled = [s for s in known if s["result"] == "cancelled"]

    out: dict = {
        "total_prints": len(known),
        "successful_prints": len(ok),
        "failed_prints": len(failed),
        "cancelled_prints": len(cancelled),
        "success_rate": round(100 * len(ok) / len(known), 1) if known else None,
        "total_print_hours": _hours(sum(s["duration_s"] or 0 for s in sessions)),
        "total_active_hours": _hours(sum(s["active_print_s"] or 0 for s in sessions)),
    }

    # filament celkem, podle materiálu, podle slotu
    total_g = sum(r["used_g"] or 0 for r in fil_rows)
    est_g = sum(r["used_g"] or 0 for r in fil_rows if r["is_estimate"])
    out["total_filament_kg"] = round(total_g / 1000, 3)
    out["total_cost"] = round(sum(r["cost"] for r in fil_rows), 0)
    out["prices_set"] = bool(prices)
    cost_by_mat: dict[str, float] = {}
    for r in fil_rows:
        cost_by_mat[r["material_group"] or "other"] = cost_by_mat.get(r["material_group"] or "other", 0) + r["cost"]
    out["total_cost_attrs"] = {"by_material": {k: round(v) for k, v in cost_by_mat.items()},
                               "prices_per_kg": prices or {}, "note": "podle nastavených cen za kg (options add-onu), hmotnost = odhad ze sliceru"}
    out["total_filament_attrs"] = {"estimated_share_pct": round(100 * est_g / total_g, 1) if total_g else 0,
                                   "sources": _count_by(fil_rows, "source")}
    by_mat = {g: 0.0 for g in MATERIAL_GROUPS}
    by_mat["other"] = 0.0
    by_mat_30 = {k: 0.0 for k in by_mat}
    by_slot: dict[str, float] = {}
    slot_attrs: dict[str, dict] = {}
    for r in fil_rows:
        g = r["material_group"] or "other"
        by_mat[g] = by_mat.get(g, 0) + (r["used_g"] or 0)
        if r["ended_ts"] >= now_i - 30 * 86400:
            by_mat_30[g] = by_mat_30.get(g, 0) + (r["used_g"] or 0)
        if r["tray_global"] is not None:
            key = "ext" if r["tray_global"] >= 254 else f"ams{r['tray_global'] // 4 + 1}_slot{r['tray_global'] % 4 + 1}"
            by_slot[key] = by_slot.get(key, 0) + (r["used_g"] or 0)
            slot_attrs[key] = {"last_material": r["material"], "last_color": r["color_hex"]}
        else:
            by_slot["unmapped"] = by_slot.get("unmapped", 0) + (r["used_g"] or 0)
    for g, v in by_mat.items():
        out[f"filament_{g.lower()}_kg"] = round(v / 1000, 3)
    out["filament_by_slot_kg"] = {k: round(v / 1000, 3) for k, v in sorted(by_slot.items())}
    out["filament_by_slot_attrs"] = slot_attrs
    if by_mat and total_g:
        top = max(by_mat.items(), key=lambda kv: kv[1])
        out["most_used_material"] = top[0]
        out["most_used_material_attrs"] = {"grams_all_time": {k: round(v) for k, v in by_mat.items()},
                                           "grams_30d": {k: round(v) for k, v in by_mat_30.items()}}
    else:
        out["most_used_material"] = "—"
        out["most_used_material_attrs"] = {}

    # klouzavá okna
    today0 = _local(now_i, tz).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    for label, since in (("today", today0), ("7d", now_i - 7 * 86400), ("30d", now_i - 30 * 86400)):
        win = [s for s in known if s["ended_ts"] >= since]
        out[f"prints_{label}"] = len(win)
        out[f"hours_{label}"] = _hours(sum(_overlap(s, since, now_i) for s in sessions))
        out[f"filament_{label}_g"] = round(sum(r["used_g"] or 0 for r in fil_rows if r["ended_ts"] >= since), 1)
        out[f"cost_{label}"] = round(sum(r["cost"] for r in fil_rows if r["ended_ts"] >= since), 0)
    for label, days in (("7d", 7), ("30d", 30)):
        span = days * 86400
        busy = sum(_overlap(s, now_i - span, now_i) for s in sessions)
        if open_session and open_session.get("started_ts"):
            busy += max(0, now_i - max(open_session["started_ts"], now_i - span))
        out[f"utilization_{label}"] = round(100 * busy / span, 1)

    durs = [s["duration_s"] for s in ok if s["duration_s"]]
    out["longest_print_h"] = _hours(max(durs)) if durs else 0
    out["average_print_h"] = _hours(statistics.mean(durs)) if durs else 0
    out["median_print_h"] = _hours(statistics.median(durs)) if durs else 0
    gs = [s["filament_g"] for s in ok if s["filament_g"]]
    out["avg_filament_per_print_g"] = round(statistics.mean(gs), 1) if gs else 0
    cs = [cost_by_session.get(s["id"], 0) for s in ok]
    out["avg_cost_per_print"] = round(statistics.mean(cs), 0) if cs else 0

    # poslední tisk + historie
    last = sessions[-1] if sessions else None
    out["last_print"] = (last["result"] or "unknown") if last else "none"
    out["last_print_attrs"] = _session_attrs(db, last, tz) if last else {}
    if last:
        out["last_print_attrs"]["cost"] = round(cost_by_session.get(last["id"], 0), 1)
    hist = []
    for s in reversed(sessions[-HISTORY_N:]):
        hist.append({"id": s["id"][-8:], "n": (s["subtask_name"] or "?")[:40], "s": _iso(s["started_ts"], tz),
                     "e": _iso(s["ended_ts"], tz), "d": round((s["duration_s"] or 0) / 60), "r": s["result"],
                     "g": round(s["filament_g"], 1) if s["filament_g"] is not None else None,
                     "m": _materials(db, s["id"]), "src": s["filament_source"], "est": s["filament_is_estimate"],
                     "c": round(cost_by_session.get(s["id"], 0)), "o": s.get("origin"), "img": s.get("cover")})
    out["print_history"] = len(sessions)
    out["print_history_attrs"] = {"history": hist}

    # statistiky per model (podle názvu úlohy)
    by_model: dict[str, dict] = {}
    for s in known:
        n = (s["subtask_name"] or "?").strip()
        m = by_model.setdefault(n, {"name": n[:40], "n": 0, "ok": 0, "min": 0.0, "g": 0.0, "c": 0.0, "last": 0, "img": None})
        m["n"] += 1; m["ok"] += int(s["result"] == "success"); m["min"] += (s["duration_s"] or 0) / 60
        m["g"] += s["filament_g"] or 0; m["c"] += cost_by_session.get(s["id"], 0); m["last"] = max(m["last"], s["ended_ts"] or 0)
        if s.get("cover"):
            m["img"] = s["cover"]
    models = sorted(by_model.values(), key=lambda m: (-m["n"], -m["last"]))[:15]
    for m in models:
        m["avg_min"] = round(m["min"] / m["n"]); m["avg_g"] = round(m["g"] / m["n"], 1); m["c"] = round(m["c"]); m["g"] = round(m["g"])
        m["last"] = _iso(m["last"], tz); m["ok_pct"] = round(100 * m["ok"] / m["n"]); m.pop("min", None)
    out["models"] = len(by_model)
    out["models_attrs"] = {"models": models}

    # denní / měsíční řady pro grafy
    out["usage_attrs"] = _series(known, fil_rows, now_i, tz, db, serial)
    out["usage"] = out["prints_30d"]
    return out


def _overlap(s: dict, a: int, b: int) -> int:
    start, end = s.get("started_ts") or 0, s.get("ended_ts") or 0
    return max(0, min(end, b) - max(start, a))


def _count_by(rows, key):
    out: dict[str, int] = {}
    for r in rows:
        out[r[key] or "none"] = out.get(r[key] or "none", 0) + 1
    return out


def _iso(ts, tz) -> str | None:
    return _local(ts, tz).strftime("%Y-%m-%dT%H:%M") if ts else None


def _materials(db, sid) -> str:
    rows = db.query("SELECT DISTINCT material FROM session_filaments WHERE session_id=? AND material<>''", (sid,))
    return "+".join(r["material"] for r in rows) if rows else ""


def _session_attrs(db, s: dict, tz) -> dict:
    fils = db.query("SELECT * FROM session_filaments WHERE session_id=?", (s["id"],))
    pauses = db.query("SELECT * FROM session_pauses WHERE session_id=?", (s["id"],))
    return {
        "session_id": s["id"], "name": s["subtask_name"], "result": s["result"], "confidence": s["result_confidence"],
        "started": _iso(s["started_ts"], tz), "ended": _iso(s["ended_ts"], tz), "start_source": s["start_source"],
        "end_source": s["end_source"], "duration_min": round((s["duration_s"] or 0) / 60),
        "active_min": round((s["active_print_s"] or 0) / 60), "paused_min": round((s["paused_s"] or 0) / 60),
        "pauses": len(pauses), "layers": f"{s['last_layer']}/{s['total_layers']}", "percent": s["last_percent"],
        "predicted_min": round((s["predicted_s"] or 0) / 60) if s["predicted_s"] else None,
        "predicted_source": s["predicted_source"],
        "filament_g": s["filament_g"], "filament_m": s["filament_m"], "filament_source": s["filament_source"],
        "filament_is_estimate": bool(s["filament_is_estimate"]),
        "materials": [{"material": f["material"], "color": f["color_hex"], "slot": f["tray_global"], "g": f["used_g"],
                       "m": f["used_m"], "source": f["source"], "estimate": bool(f["is_estimate"])} for f in fils],
        "print_error": s["print_error"], "fail_reason": s["fail_reason"], "hms_serious": s["hms_serious_count"],
        "print_type": s["print_type"], "nozzle": f"{s['nozzle_type']} {s['nozzle_diameter']}".strip(), "speed_level": s["spd_lvl"],
        "origin": s.get("origin"), "cover": s.get("cover"),
    }


def _series(known: list[dict], fil_rows: list[dict], now_i: int, tz, db=None, serial: str | None = None) -> dict:
    """Kompaktní řady pro grafy: hourly (48 h, z telemetrie), daily (365), weekly (104), monthly (vše).

    Formát: {"daily": {"start": "2025-09-10", "rows": [[prints, hours, g, ok, fail], ...]}} – pole místo objektů,
    aby se 365 dní vešlo pod 16 kB limit atributů HA. hours = wall-clock hodiny tisku připsané do dne konce tisku.
    hourly.rows = [[printing_min, idle_min, g_est], ...] za každou hodinu (z 10/60 s vzorků).
    """
    def bucket_rows(n):
        return [[0, 0.0, 0.0, 0, 0, 0.0] for _ in range(n)]

    today = _local(now_i, tz).replace(hour=0, minute=0, second=0, microsecond=0)
    daily_start = today - dt.timedelta(days=364)
    daily = bucket_rows(365)
    week_start = (today - dt.timedelta(days=today.weekday())) - dt.timedelta(weeks=103)
    weekly = bucket_rows(104)
    first_month = None
    if known:
        first_month = _local(min(s["ended_ts"] for s in known), tz).replace(day=1)
    months: list[str] = []
    m = (first_month or today.replace(day=1))
    while m <= today.replace(day=1):
        months.append(m.strftime("%Y-%m"))
        y, mo = m.year, m.month + 1
        if mo > 12:
            y, mo = y + 1, 1
        m = m.replace(year=y, month=mo)
    monthly = {k: [0, 0.0, 0.0, 0, 0, 0.0] for k in months}

    def add(row, prints, hours, g, ok, fail):
        row[0] += prints; row[1] += hours; row[2] += g; row[3] += ok; row[4] += fail

    for s in known:
        end = _local(s["ended_ts"], tz)
        hours = (s["duration_s"] or 0) / 3600
        ok, fail = int(s["result"] == "success"), int(s["result"] in ("failed", "cancelled"))
        di = (end.replace(hour=0, minute=0, second=0, microsecond=0) - daily_start).days
        if 0 <= di < 365:
            add(daily[di], 1, hours, 0, ok, fail)
        wi = (end.replace(hour=0, minute=0, second=0, microsecond=0) - week_start).days // 7
        if 0 <= wi < 104:
            add(weekly[wi], 1, hours, 0, ok, fail)
        mk = end.strftime("%Y-%m")
        if mk in monthly:
            add(monthly[mk], 1, hours, 0, ok, fail)
    for r in fil_rows:
        end = _local(r["ended_ts"], tz)
        g = r["used_g"] or 0
        c = r.get("cost", 0)
        di = (end.replace(hour=0, minute=0, second=0, microsecond=0) - daily_start).days
        if 0 <= di < 365:
            daily[di][2] += g
            daily[di][5] += c
        wi = (end.replace(hour=0, minute=0, second=0, microsecond=0) - week_start).days // 7
        if 0 <= wi < 104:
            weekly[wi][2] += g
            weekly[wi][5] += c
        mk = end.strftime("%Y-%m")
        if mk in monthly:
            monthly[mk][2] += g
            monthly[mk][5] += c

    hourly = []
    if db is not None and serial:
        h0 = now_i - 48 * 3600
        samples = db.query("SELECT ts, gcode_state FROM samples WHERE printer_serial=? AND ts>=? ORDER BY ts", (serial, h0))
        buckets = [[0.0, 0.0] for _ in range(48)]
        prev = None
        for smp in samples:
            if prev is not None:
                span = min(smp["ts"] - prev["ts"], 120)
                idx = int((prev["ts"] - h0) // 3600)
                if 0 <= idx < 48:
                    buckets[idx][0 if prev["gcode_state"] in ("RUNNING", "PAUSE", "PREPARE") else 1] += span / 60
            prev = smp
        hourly = [[round(a, 1), round(b, 1)] for a, b in buckets]

    def rnd(rows):
        return [[r[0], round(r[1], 2), round(r[2], 1), r[3], r[4], round(r[5])] for r in rows]
    return {
        "hourly": {"start": _local(now_i - 48 * 3600, tz).strftime("%Y-%m-%dT%H:00"), "rows": hourly},
        "daily": {"start": daily_start.strftime("%Y-%m-%d"), "rows": rnd(daily)},
        "weekly": {"start": week_start.strftime("%Y-%m-%d"), "rows": rnd(weekly)},
        "monthly": {"keys": months, "rows": rnd(list(monthly.values()))},
        "cols": ["prints", "hours", "g", "ok", "fail", "cost"],
    }
