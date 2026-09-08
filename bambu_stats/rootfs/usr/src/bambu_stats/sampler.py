"""Podvzorkování telemetrie: 10 s během tisku, 60 s v klidu, plus okamžitě při přechodu stavu."""
from __future__ import annotations

from .state_machine import GS_ACTIVE, Snapshot

PRINT_INTERVAL_S = 10
IDLE_INTERVAL_S = 60


class Sampler:
    def __init__(self, serial: str):
        self.serial = serial
        self._last_ts: float = 0.0
        self._last_gs: str | None = None

    def maybe_row(self, s: Snapshot, session_id: str | None) -> dict | None:
        interval = PRINT_INTERVAL_S if s.gcode_state in GS_ACTIVE else IDLE_INTERVAL_S
        transition = s.gcode_state != self._last_gs
        if not transition and s.ts - self._last_ts < interval:
            return None
        self._last_ts, self._last_gs = s.ts, s.gcode_state
        return {
            "ts": int(s.ts), "printer_serial": self.serial, "session_id": session_id,
            "gcode_state": s.gcode_state, "stg_cur": s.stg_cur, "mc_percent": s.percent, "layer_num": s.layer,
            "remaining_min": s.remaining_min, "nozzle_temp": s.nozzle_temp, "nozzle_target": s.nozzle_target,
            "bed_temp": s.bed_temp, "bed_target": s.bed_target, "chamber_temp": s.chamber_temp,
            "spd_lvl": s.spd_lvl, "spd_mag": s.spd_mag, "fan_part": s.fan_part, "fan_aux": s.fan_aux,
            "fan_chamber": s.fan_chamber, "fan_heatbreak": s.fan_heatbreak, "wifi_signal": s.wifi_signal,
            "tray_now": s.tray_now, "ams_humidity": s.ams_humidity, "ams_temp": s.ams_temp,
        }
