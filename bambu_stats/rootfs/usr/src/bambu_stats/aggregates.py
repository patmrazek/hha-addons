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


def compute(db, serial: str, now: float, tz: ZoneInfo, open_session: dict | None = None) -> dict:
    now_i = int(now)
    sessions = db.query("SELECT * FROM sessions WHERE printer_serial=? AND ended_ts IS NOT NULL ORDER BY ended_ts", (serial,))
    fil_rows = db.query("""SELECT f.*, s.ended_ts FROM session_filaments f JOIN sessions s ON s.id=f.session_id
                           WHERE s.printer_serial=? AND s.ended_ts IS NOT NULL""", (serial,))
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

    # poslední tisk + historie
    last = sessions[-1] if sessions else None
    out["last_print"] = (last["result"] or "unknown") if last else "none"
    out["last_print_attrs"] = _session_attrs(db, last, tz) if last else {}
    hist = []
    for s in reversed(sessions[-HISTORY_N:]):
        hist.append({"id": s["id"][-8:], "n": (s["subtask_name"] or "?")[:40], "s": _iso(s["started_ts"], tz),
                     "e": _iso(s["ended_ts"], tz), "d": round((s["duration_s"] or 0) / 60), "r": s["result"],
                     "g": round(s["filament_g"], 1) if s["filament_g"] is not None else None,
                     "m": _materials(db, s["id"]), "src": s["filament_source"], "est": s["filament_is_estimate"]})
    out["print_history"] = len(sessions)
    out["print_history_attrs"] = {"history": hist}

    # denní / měsíční řady pro grafy
    out["usage_attrs"] = _series(known, fil_rows, now_i, tz)
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
    }


def _series(known: list[dict], fil_rows: list[dict], now_i: int, tz) -> dict:
    days: dict[str, dict] = {}
    months: dict[str, dict] = {}
    for i in range(30):
        d = (_local(now_i, tz) - dt.timedelta(days=i)).strftime("%Y-%m-%d")
        days[d] = {"d": d, "prints": 0, "hours": 0.0, "g": 0.0}
    for i in range(12):
        m0 = _local(now_i, tz).replace(day=1)
        y, m = m0.year, m0.month - i
        while m <= 0:
            y, m = y - 1, m + 12
        months[f"{y}-{m:02d}"] = {"m": f"{y}-{m:02d}", "prints": 0, "hours": 0.0, "g": 0.0}
    for s in known:
        key = _local(s["ended_ts"], tz).strftime("%Y-%m-%d")
        mk = key[:7]
        if key in days:
            days[key]["prints"] += 1
            days[key]["hours"] += (s["duration_s"] or 0) / 3600
        if mk in months:
            months[mk]["prints"] += 1
            months[mk]["hours"] += (s["duration_s"] or 0) / 3600
    for r in fil_rows:
        key = _local(r["ended_ts"], tz).strftime("%Y-%m-%d")
        if key in days:
            days[key]["g"] += r["used_g"] or 0
        if key[:7] in months:
            months[key[:7]]["g"] += r["used_g"] or 0
    daily = [{**v, "hours": round(v["hours"], 2), "g": round(v["g"], 1)} for v in sorted(days.values(), key=lambda x: x["d"])]
    monthly = [{**v, "hours": round(v["hours"], 1), "g": round(v["g"])} for v in sorted(months.values(), key=lambda x: x["m"])]
    return {"daily": daily, "monthly": monthly}
