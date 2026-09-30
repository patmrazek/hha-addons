"""Stavový automat tiskových session – čistá logika bez I/O (jen log).

Vstupem je `Snapshot` (normalizovaný výřez sekce `print` z MQTT), výstupem seznam `Event`ů,
které collector persistuje. Automat drží jednu aktivní `Session` a umí se obnovit
z otevřené session v databázi (restart uprostřed tisku).

Fáze:  NONE → PREPARING → RUNNING ⇄ PAUSED → (FINISH | FAILED | IDLE) → NONE
Debounce: přechod se provede až po dvou po sobě jdoucích zprávách se stejným gcode_state
(P2S posílá ~1 zprávu/s, zpoždění je tedy ~2 s).

Systémové úlohy tiskárny (kalibrace z /usr/etc/print/) se neevidují vůbec – nejsou to tisky,
nespotřebují filament a ve statistikách by se počítaly jako úspěšný tisk.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import asdict, dataclass, field

from . import hms as hmsmod
from .util import SYSTEM_GCODE_PREFIX, color_hex, to_float, to_int, ulid

LOG = logging.getLogger("state_machine")

GS_PREPARING = {"PREPARE", "SLICING", "INIT"}
GS_RUNNING = {"RUNNING"}
GS_PAUSED = {"PAUSE"}
GS_ACTIVE = GS_PREPARING | GS_RUNNING | GS_PAUSED
GS_FINISH, GS_FAILED = "FINISH", "FAILED"
GS_TERMINAL = {GS_FINISH, GS_FAILED}

FAILED_GRACE_S = 5.0
SERIOUS_HMS_WINDOW_S = 60.0


# ---------------------------------------------------------------------------
@dataclass
class Tray:
    ams_id: int
    tray_id: int
    tray_type: str = ""
    sub_brands: str = ""
    color: str | None = None
    info_idx: str = ""
    remain: int = -1
    tray_weight: int = 0
    total_len: int = 0
    tray_uuid: str = ""

    @property
    def tray_global(self) -> int:
        return self.ams_id * 4 + self.tray_id if self.ams_id < 128 else self.ams_id

    def as_dict(self) -> dict:
        d = asdict(self)
        d["tray_global"] = self.tray_global
        return d


@dataclass
class Snapshot:
    ts: float
    gcode_state: str = ""
    percent: int = 0
    layer: int = 0
    total_layers: int = 0
    remaining_min: int = 0
    subtask_name: str = ""
    gcode_file: str = ""
    task_id: str = ""
    subtask_id: str = ""
    job_id: str = ""
    project_id: str = ""
    profile_id: str = ""
    print_type: str = ""
    print_error: int = 0
    fail_reason: str = ""
    hms: list[dict] = field(default_factory=list)
    stg_cur: int = 0
    mc_print_stage: int = 0
    nozzle_type: str = ""
    nozzle_diameter: str = ""
    spd_lvl: int = 0
    spd_mag: int = 0
    tray_now: int = 255
    trays: list[Tray] = field(default_factory=list)
    nozzle_temp: float = 0.0
    nozzle_target: float = 0.0
    bed_temp: float = 0.0
    bed_target: float = 0.0
    chamber_temp: float | None = None
    fan_part: int = 0
    fan_aux: int = 0
    fan_chamber: int = 0
    fan_heatbreak: int = 0
    wifi_signal: int = 0
    ams_humidity: int | None = None
    ams_temp: float | None = None
    nozzle_wear: float | None = None

    @classmethod
    def from_print(cls, p: dict, ts: float) -> "Snapshot":
        ams = p.get("ams") or {}
        trays: list[Tray] = []
        for unit in ams.get("ams") or []:
            aid = to_int(unit.get("id"), 0)
            for t in unit.get("tray") or []:
                trays.append(Tray(ams_id=aid, tray_id=to_int(t.get("id"), 0), tray_type=t.get("tray_type") or "",
                                  sub_brands=t.get("tray_sub_brands") or "", color=color_hex(t.get("tray_color") or t.get("cols")),
                                  info_idx=t.get("tray_info_idx") or "", remain=to_int(t.get("remain"), -1),
                                  tray_weight=to_int(t.get("tray_weight"), 0), total_len=to_int(t.get("total_len"), 0),
                                  tray_uuid=t.get("tray_uuid") or ""))
        for t in (p.get("vir_slot") or []) + ([p["vt_tray"]] if isinstance(p.get("vt_tray"), dict) else []):
            if t.get("tray_type"):
                trays.append(Tray(ams_id=to_int(t.get("id"), 255), tray_id=0, tray_type=t.get("tray_type") or "",
                                  sub_brands=t.get("tray_sub_brands") or "", color=color_hex(t.get("tray_color") or t.get("cols")),
                                  info_idx=t.get("tray_info_idx") or "", remain=to_int(t.get("remain"), -1),
                                  tray_weight=to_int(t.get("tray_weight"), 0), total_len=to_int(t.get("total_len"), 0)))
        first_ams = (ams.get("ams") or [{}])[0]
        dev = p.get("device") or {}
        chamber = (((dev.get("ctc") or {}).get("info") or {}).get("temp"))
        if chamber is None:
            chamber = p.get("chamber_temper", (p.get("info") or {}).get("temp"))
        return cls(
            ts=ts, gcode_state=str(p.get("gcode_state") or ""), percent=to_int(p.get("mc_percent")),
            layer=to_int(p.get("layer_num")), total_layers=to_int(p.get("total_layer_num")),
            remaining_min=to_int(p.get("mc_remaining_time")), subtask_name=str(p.get("subtask_name") or ""),
            gcode_file=str(p.get("gcode_file") or ""), task_id=str(p.get("task_id") or ""),
            subtask_id=str(p.get("subtask_id") or ""), job_id=str(p.get("job_id") or ""),
            project_id=str(p.get("project_id") or ""), profile_id=str(p.get("profile_id") or ""),
            print_type=str(p.get("print_type") or ""), print_error=to_int(p.get("print_error")),
            fail_reason=str(p.get("fail_reason") or ""), hms=[h for h in (p.get("hms") or []) if isinstance(h, dict)],
            stg_cur=to_int(p.get("stg_cur")), mc_print_stage=to_int(p.get("mc_print_stage")),
            nozzle_type=str(p.get("nozzle_type") or ""), nozzle_diameter=str(p.get("nozzle_diameter") or ""),
            spd_lvl=to_int(p.get("spd_lvl")), spd_mag=to_int(p.get("spd_mag")),
            tray_now=to_int(ams.get("tray_now"), 255), trays=trays,
            nozzle_temp=to_float(p.get("nozzle_temper")), nozzle_target=to_float(p.get("nozzle_target_temper")),
            bed_temp=to_float(p.get("bed_temper")), bed_target=to_float(p.get("bed_target_temper")),
            chamber_temp=to_float(chamber) if chamber is not None else None,
            fan_part=to_int(p.get("cooling_fan_speed")), fan_aux=to_int(p.get("big_fan1_speed")),
            fan_chamber=to_int(p.get("big_fan2_speed")), fan_heatbreak=to_int(p.get("heatbreak_fan_speed")),
            wifi_signal=to_int(str(p.get("wifi_signal") or "0").replace("dBm", "")),
            ams_humidity=to_int(first_ams.get("humidity_raw"), None) if first_ams else None,
            ams_temp=to_float(first_ams.get("temp"), None) if first_ams.get("temp") is not None else None,
            nozzle_wear=to_float((((dev.get("nozzle") or {}).get("info") or [{}])[0]).get("wear"), None) if dev.get("nozzle") else None)

    @property
    def is_active(self) -> bool:
        return self.gcode_state in GS_ACTIVE

    @property
    def serious_hms(self) -> list[dict]:
        out = []
        for h in self.hms:
            d = hmsmod.decode(h)
            if d and d["severity"] in (1, 2):
                out.append(d)
        return out


def fingerprint(serial: str, s: Snapshot) -> str:
    raw = f"{serial}|{s.task_id}|{s.subtask_id}|{s.job_id}|{s.subtask_name}|{s.gcode_file}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def je_systemova_uloha(s: Snapshot) -> bool:
    """Kalibrace a jiné úlohy firmwaru (29. 9. 2026 auto_cali_for_user_param.gcode, 24 min).

    Musí platit OBĚ pole: vyřazení z evidence je nevratné (nevznikne session, odečet ani historie),
    kdežto omylem evidovanou kalibraci pozdější filtr ve statistikách levně odfiltruje. print_type
    i gcode_file přežívají ve slučovaném stavu MQTT, takže jedno zastaralé pole samo nerozhoduje.
    V datech se obě pole přepínají ve stejné zprávě. Vzor názvu auto_cali* se schválně nepoužívá –
    takhle se klidně může jmenovat i soubor uživatele.
    """
    return s.print_type == "system" and (s.gcode_file or "").startswith(SYSTEM_GCODE_PREFIX)


def _napul_systemova(s: Snapshot) -> bool:
    return s.print_type == "system" or (s.gcode_file or "").startswith(SYSTEM_GCODE_PREFIX)


# ---------------------------------------------------------------------------
@dataclass
class Session:
    id: str
    printer_serial: str
    fingerprint: str
    status: str = "preparing"           # preparing|running|paused|finished|failed|cancelled|unknown
    result: str | None = None            # success|failed|cancelled|unknown
    result_confidence: str | None = None
    task_id: str = ""
    subtask_id: str = ""
    job_id: str = ""
    project_id: str = ""
    profile_id: str = ""
    subtask_name: str = ""
    gcode_file: str = ""
    print_type: str = ""
    started_ts: int = 0
    start_source: str = "observed"
    print_started_ts: int | None = None
    ended_ts: int | None = None
    end_source: str | None = None
    duration_s: int | None = None
    paused_s: int = 0
    active_print_s: int | None = None
    pause_count: int = 0
    total_layers: int = 0
    last_layer: int = 0
    last_percent: int = 0
    last_remaining_min: int = 0
    predicted_s: int | None = None
    predicted_source: str | None = None
    print_error: int = 0
    fail_reason: str = ""
    hms_serious_count: int = 0
    filament_g: float | None = None
    filament_m: float | None = None
    filament_source: str | None = None
    filament_is_estimate: int = 0
    nozzle_type: str = ""
    nozzle_diameter: str = ""
    spd_lvl: int = 0
    tray_now_start: int | None = None
    # Úseky tisku po slotech: [{"tray": 1, "from_pct": 0}, {"tray": 3, "from_pct": 64}, …].
    # AMS umí při došlém filamentu sám přepnout na jinou cívku stejné barvy (auto-refill) a
    # tiskne dál. Bez tohohle záznamu by se celá spotřeba připsala slotu, ve kterém tisk začal,
    # a cívka, ze které se dotisklo, by v evidenci zůstala nedotčená.
    tray_spans: list = field(default_factory=list)
    trays_start: list = field(default_factory=list)
    trays_end: list = field(default_factory=list)
    threemf_status: str | None = None
    incomplete: int = 0
    manual_override: int = 0
    created_ts: int = 0
    updated_ts: int = 0
    last_seen_ts: int = 0
    # běhové (neukládá se)
    _pause_started: float | None = None
    _grace_deadline: float | None = None

    def to_row(self) -> dict:
        return {k: v for k, v in asdict(self).items() if not k.startswith("_")}

    @classmethod
    def from_row(cls, row: dict) -> "Session":
        import json
        allowed = {f for f in cls.__dataclass_fields__ if not f.startswith("_")}
        data = {k: v for k, v in row.items() if k in allowed and v is not None}
        for k in ("trays_start", "trays_end", "tray_spans"):
            if isinstance(data.get(k), str):
                try:
                    data[k] = json.loads(data[k])
                except ValueError:
                    data[k] = []
        return cls(**data)


@dataclass
class Event:
    kind: str          # opened|resumed|started|paused|unpaused|updated|closed|superseded|lost
    session: Session
    snapshot: Snapshot | None = None
    note: str = ""


# ---------------------------------------------------------------------------
class StateMachine:
    def __init__(self, serial: str, open_session: dict | None = None, last_closed_fp: str | None = None,
                 id_factory=ulid):
        self.serial = serial
        self.session: Session | None = Session.from_row(open_session) if open_session else None
        self.last_closed_fp = last_closed_fp
        self._new_id = id_factory
        self._first = True
        self._prev_gs: str | None = None
        self._recent_serious: list[tuple[float, str]] = []
        self._hms_first_seen: dict[str, float] = {}
        self._seeded_hms = False
        self._systemova_hlasena: str | None = None   # otisk systémové úlohy, o které už je řádek v logu

    # --- veřejné -----------------------------------------------------------
    def feed(self, s: Snapshot) -> list[Event]:
        events: list[Event] = []
        self._track_hms(s)
        if not s.is_active:
            self._systemova_hlasena = None           # další kalibrace se zaloguje znovu
        if self._first:
            self._first = False
            self._prev_gs = s.gcode_state
            self._recent_serious.clear()  # HMS aktivní při startu nejsou „nové"
            events += self._recover(s)
            return events
        stable = s.gcode_state == self._prev_gs
        self._prev_gs = s.gcode_state
        if self.session and self.session._grace_deadline is not None:
            events += self._grace(s)
            return events
        if not stable:
            if self.session and s.is_active and not self._systemova_v_priprave(s):
                self._update_progress(s)
            return events
        gs = s.gcode_state
        if self.session is None:
            if s.is_active:
                ev = self._otevri_pokud_neni_systemova(s, start_source="observed")
                if ev:
                    events.append(ev)
            return events
        # aktivní session
        sess = self.session
        if s.is_active and self._superseded(s):
            events.append(self._close(s, result="unknown", confidence="low", end_source="superseded",
                                      ended_ts=sess.last_seen_ts or int(s.ts), kind="superseded"))
            # Po zmeškaném konci tisku může rovnou běžet kalibrace – starou session uzavřít, ale
            # kalibraci neotevírat (jinak by převzala cloud hmotnost předchozího tisku).
            ev = self._otevri_pokud_neni_systemova(s, start_source="observed")
            if ev:
                events.append(ev)
            return events
        if gs in GS_PREPARING:
            self._update_progress(s)
        elif gs in GS_RUNNING:
            if self._systemova_v_priprave(s):
                # Kalibrace během přípravy skutečného tisku: snímek k téhle session nepatří. Kdyby
                # převzal ID a otisk, skutečný tisk by pak session nahradil (superseded se 100 %
                # z kalibrace) a odečetlo by se dvakrát.
                self._hlas_systemovou(s)
                return events
            if sess.status == "preparing":
                sess.status = "running"
                sess.print_started_ts = int(s.ts)
                sess.fingerprint = fingerprint(self.serial, s)
                self._copy_ids(s)
                self._update_progress(s)
                events.append(Event("started", sess, s))
            elif sess.status == "paused":
                if sess.print_started_ts is None:
                    # Pauza ještě v přípravě: tisk teprve teď opravdu začíná – totéž co u 'preparing'.
                    # Bez toho by zůstal bez času startu a otisku a průběh by se nikdy nepřevzal.
                    sess.print_started_ts = int(s.ts)
                    sess.fingerprint = fingerprint(self.serial, s)
                    self._copy_ids(s)
                    # Pauza patří k přípravě, ne k tisku: active_print_s se počítá od print_started_ts,
                    # takže by se v paused_s odečetla podruhé. Řádek pauzy v databázi zůstává.
                    sess._pause_started = None
                    events.append(Event("started", sess, s))
                self._end_pause(s.ts)
                sess.status = "running"
                self._update_progress(s)
                events.append(Event("unpaused", sess, s))
            else:
                self._update_progress(s)
                events.append(Event("updated", sess, s))
        elif gs in GS_PAUSED:
            if sess.status != "paused":
                sess.status = "paused"
                sess._pause_started = s.ts
                sess.pause_count += 1
                self._update_progress(s)
                events.append(Event("paused", sess, s, note=self._pause_reason(s)))
            else:
                self._update_progress(s)
        elif gs in GS_TERMINAL and self._systemova_v_priprave(s):
            # Systémová úloha doběhla (FINISH/FAILED), zatímco otevřená session ještě nezačala tisknout.
            # Tiskárna dvě úlohy naráz nedělá, takže příprava skončila bez tisku (konec zmeškaný) –
            # uzavřít jako zrušení bez převzetí 100 % ani chyb kalibrace. FINISH by jinak z přípravy
            # udělal úspěch a odečetl celý plán; ignorovat ho nejde, session by visela do dalšího tisku
            # a převzala by ho i se starým začátkem.
            LOG.info("systémová úloha %s skončila během přípravy %s – přípravu uzavírám jako zrušenou",
                     s.subtask_name or s.gcode_file, sess.subtask_name or sess.id)
            events.append(self._close(s, result="cancelled", confidence="low", end_source="inferred",
                                      ended_ts=sess.last_seen_ts or int(s.ts)))
        elif gs == GS_FINISH:
            self._update_progress(s)
            events.append(self._close(s, result="success", confidence="high", end_source="observed"))
        elif gs == GS_FAILED:
            self._update_progress(s)
            sess._grace_deadline = s.ts + FAILED_GRACE_S
            self._absorb_errors(s)
        else:  # IDLE / OFFLINE / UNKNOWN bez terminálního stavu
            if sess.status in ("running", "paused", "preparing"):
                res = "cancelled" if sess.last_percent < 100 else "success"
                events.append(self._close(s, result=res, confidence="low", end_source="inferred"))
        return events

    # --- obnova po restartu -------------------------------------------------
    def _recover(self, s: Snapshot) -> list[Event]:
        events: list[Event] = []
        sess = self.session
        fp = fingerprint(self.serial, s)
        if sess:
            if self._pred_startem() and (sess.last_percent or sess.last_layer):
                # Řádek uložený 0.18.2 v přípravě nese procenta a vrstvu MINULÉHO tisku (0.18.2 je
                # v přípravě přebírala). Přenesené dál by průběžný odečet strhl 90 % plánu a zrušení
                # v přípravě by odečetlo celý plán; vadily by i kontrole věrohodnosti níž. Od 0.19.0
                # tu jsou vždy nuly (viz _update_progress), takže se tím mění jen přechodový případ.
                LOG.info("session %s ještě netiskla, ale nese %s %% / vrstvu %s z minulého tisku – nuluji",
                         sess.subtask_name or sess.id, sess.last_percent, sess.last_layer)
                sess.last_percent = 0
                sess.last_layer = 0
            same = (fp == sess.fingerprint) or (s.subtask_name and s.subtask_name == sess.subtask_name
                                                 and s.task_id == sess.task_id)
            plausible = s.percent >= sess.last_percent - 5 or s.layer >= sess.last_layer - 1
            if s.is_active and same and plausible:
                if sess.status == "paused" and s.gcode_state in GS_RUNNING:
                    if sess.print_started_ts is None:
                        # Pauza byla ještě v přípravě: tisk začíná teď a pauza se do doby tisku
                        # nepočítá (active_print_s se odečítá od print_started_ts) – jako ve feed().
                        sess.print_started_ts = int(s.ts)
                    else:
                        sess.paused_s += max(0, int(s.ts) - (sess.last_seen_ts or int(s.ts)))
                    sess.status = "running"
                elif s.gcode_state in GS_PAUSED and sess.status != "paused":
                    sess.status = "paused"
                    sess.pause_count += 1
                    sess._pause_started = s.ts
                elif s.gcode_state in GS_PAUSED:
                    sess._pause_started = sess.last_seen_ts or s.ts
                elif s.gcode_state in GS_RUNNING and sess.status == "preparing":
                    sess.status = "running"
                    sess.print_started_ts = sess.print_started_ts or int(s.ts)
                self._update_progress(s)
                events.append(Event("resumed", sess, s))
                return events
            if s.gcode_state in GS_TERMINAL and same:
                self._update_progress(s)
                if s.gcode_state == GS_FINISH:
                    events.append(self._close(s, "success", "high", "recovered"))
                else:
                    self._absorb_errors(s)
                    res, conf = self._classify_failed(s)
                    events.append(self._close(s, res, conf, "recovered"))
                return events
            events.append(self._close(s, result="unknown", confidence="low", end_source="lost",
                                      ended_ts=sess.last_seen_ts or int(s.ts), kind="lost"))
        if s.is_active:
            # V přípravě tiskárna hlásí procenta minulého tisku (zastaralých 100 %): z nich nejde
            # odhadnout začátek ani poznat, že tisk už běží (incomplete by pustilo fix_start_from_cloud,
            # a to by mohlo převzít začátek předchozího tisku).
            pripravuje = s.gcode_state in GS_PREPARING
            if not pripravuje and s.percent > 0 and s.remaining_min > 0 and s.percent < 100:
                elapsed = s.remaining_min * 60 * s.percent / (100 - s.percent)
                ev = self._otevri_pokud_neni_systemova(s, start_source="estimated_pct", started_ts=int(s.ts - elapsed))
            else:
                ev = self._otevri_pokud_neni_systemova(s, start_source="observed")
            if ev:
                ev.session.incomplete = 1 if s.percent > 0 and not pripravuje else 0
                events.append(ev)
        return events

    # --- pomocné ---------------------------------------------------------------
    def _otevri_pokud_neni_systemova(self, s: Snapshot, start_source: str, started_ts: int | None = None) -> Event | None:
        """Jediné místo, kudy vzniká nová session – systémová úloha (kalibrace) se neotevře nikdy."""
        if je_systemova_uloha(s):
            self._hlas_systemovou(s)
            return None
        if _napul_systemova(s):
            LOG.warning("úloha %s vypadá jako systémová jen napůl (print_type %s, gcode %s) – eviduji ji jako tisk",
                        s.subtask_name or "?", s.print_type or "?", s.gcode_file or "?")
        return self._open(s, start_source=start_source, started_ts=started_ts)

    def _pred_startem(self) -> bool:
        """Otevřená session ještě nezačala tisknout: příprava, nebo pauza v ní."""
        sess = self.session
        return bool(sess and (sess.status == "preparing" or (sess.status == "paused" and sess.print_started_ts is None)))

    def _systemova_v_priprave(self, s: Snapshot) -> bool:
        """Snímek systémové úlohy, zatímco otevřená session ještě nezačala tisknout."""
        return self._pred_startem() and je_systemova_uloha(s)

    def _hlas_systemovou(self, s: Snapshot):
        fp = fingerprint(self.serial, s)
        if self._systemova_hlasena != fp:
            self._systemova_hlasena = fp
            LOG.info("systémová úloha %s se neeviduje", s.subtask_name or s.gcode_file)

    def _open(self, s: Snapshot, start_source: str, started_ts: int | None = None) -> Event:
        now = int(s.ts)
        running = s.gcode_state in GS_RUNNING or s.gcode_state in GS_PAUSED
        sess = Session(id=self._new_id(s.ts), printer_serial=self.serial, fingerprint=fingerprint(self.serial, s),
                       status="running" if s.gcode_state in GS_RUNNING else ("paused" if s.gcode_state in GS_PAUSED else "preparing"),
                       started_ts=started_ts or now, start_source=start_source,
                       print_started_ts=(started_ts or now) if running else None,
                       created_ts=now, updated_ts=now, last_seen_ts=now,
                       tray_now_start=s.tray_now, trays_start=[t.as_dict() for t in s.trays],
                       nozzle_type=s.nozzle_type, nozzle_diameter=s.nozzle_diameter, spd_lvl=s.spd_lvl, print_type=s.print_type)
        if sess.status == "paused":
            sess._pause_started = s.ts
            sess.pause_count = 1
        self.session = sess
        self._copy_ids(s)
        self._update_progress(s)
        return Event("opened", sess, s)

    def _copy_ids(self, s: Snapshot):
        sess = self.session
        sess.task_id, sess.subtask_id, sess.job_id = s.task_id, s.subtask_id, s.job_id
        sess.project_id, sess.profile_id = s.project_id, s.profile_id
        sess.subtask_name, sess.gcode_file, sess.print_type = s.subtask_name, s.gcode_file, s.print_type or sess.print_type

    def _update_progress(self, s: Snapshot):
        sess = self.session
        # Dokud tisk nebyl vidět v RUNNING, hlásí tiskárna procenta a vrstvu MINULÉHO tisku
        # (24. 9. 2026 M39S6SNA: SLICING se 100 % a vrstvou 21/21 z předchozího tisku, pak FAILED).
        # Převzaté by udělaly ze zrušení v přípravě úspěch a zrušený tisk by odečetl celý plán;
        # průběžný odečet by po otevření session rovnou strhl 90 % plánu. Platí pro celou přípravu
        # (PREPARE, SLICING, INIT), pro pauzu v ní i pro FAILED; FINISH se převezme vždy.
        # Přijatá daň: výpadek spojení od přípravy až do FAILED bez zachyceného RUNNING dá 0 g
        # (tisk zrušený v 60 % se neodečte) – opravuje se ručně.
        if not (sess.print_started_ts is None and s.gcode_state not in GS_RUNNING and s.gcode_state != GS_FINISH):
            sess.last_percent = max(s.percent, 0)
            sess.last_layer = s.layer
        sess.total_layers = s.total_layers or sess.total_layers
        sess.last_remaining_min = s.remaining_min
        sess.last_seen_ts = int(s.ts)
        sess.updated_ts = int(s.ts)
        if s.spd_lvl:
            sess.spd_lvl = s.spd_lvl
        if s.gcode_state in GS_RUNNING and s.tray_now != 255:
            if sess.tray_now_start in (None, 255):
                sess.tray_now_start = s.tray_now
                sess.trays_start = [t.as_dict() for t in s.trays] or sess.trays_start
            if not sess.tray_spans:
                sess.tray_spans = [{"tray": s.tray_now, "from_pct": max(s.percent, 0)}]
            elif sess.tray_spans[-1]["tray"] != s.tray_now and sess.status == "running":
                # Nový úsek začíná tam, kde tisk právě je. Procento je jediné měřítko postupu,
                # které tiskárna dává průběžně – čas by u proměnlivé vrstvy klamal víc.
                #
                # Jen za běhu: při pauze na výměnu cívky tiskárna chvíli hlásí cizí slot
                # (22. 9. 2026 takhle přiskočily 4 g šedého PETG k tisku z černého PLA).
                # Ruční výměna filamentu ve stejném slotu se tím pádem nezaznamená vůbec —
                # tu pozná jen obsluha a opraví se ručně; auto-refill, kvůli kterému to
                # celé vzniklo, mění slot za běhu a ten se zachytí správně.
                sess.tray_spans.append({"tray": s.tray_now, "from_pct": max(s.percent, 0)})
        if not sess.subtask_name and s.subtask_name:
            self._copy_ids(s)
        if sess.predicted_s is None and s.remaining_min > 0 and sess.status == "running":
            elapsed = int(s.ts) - (sess.print_started_ts or sess.started_ts)
            sess.predicted_s = s.remaining_min * 60 + max(elapsed, 0)
            sess.predicted_source = "printer"

    def _superseded(self, s: Snapshot) -> bool:
        sess = self.session
        # Session, která ještě nezačala tisknout (příprava i pauza v ní), se nenahrazuje: ID se mezi
        # přípravou a startem mění (24. 9. 2026 M39S6SNA: v přípravě task_id '0') a start ji převezme.
        if self._pred_startem():
            return False
        ids_changed = (s.subtask_name and sess.subtask_name and s.subtask_name != sess.subtask_name) or \
                      (s.task_id and sess.task_id and s.task_id != sess.task_id and s.task_id != "0")
        # Zbývající čas nerozhoduje: skáče při přepnutí rychlosti (silent ho skoro zdvojnásobí) i po
        # kalibracích na začátku tisku (24. 9. 2026 v 8 % z 85 na 156 min) – dělil jeden tisk na dva
        # a odečet pak dával 140 % plánu. Pokles vrstvy se bere až od 5 %: na začátku RUNNING tiskárna
        # ještě pár sekund hlásí vrstvu minulého tisku (56 → 0) a za běhu k poklesu jinak nedochází.
        # Zmeškaný přechod mezi dvěma tisky pozná změna ID (task_id je unikátní i u LAN tisků);
        # zmeškaný přechod pod 5 % bez změny ID se sloučí do jedné session.
        regressed = (s.percent < sess.last_percent - 5) or \
                    (sess.last_percent >= 5 and sess.last_layer > 1 and s.layer < sess.last_layer - 1)
        return bool(ids_changed or (regressed and s.gcode_state in GS_RUNNING))

    def _end_pause(self, ts: float):
        sess = self.session
        if sess._pause_started is not None:
            sess.paused_s += max(0, int(ts - sess._pause_started))
            sess._pause_started = None

    def _pause_reason(self, s: Snapshot) -> str:
        if s.print_error:
            return f"print_error {hmsmod.print_error_hex(s.print_error)}"
        if s.serious_hms:
            return "hms " + ",".join(h["code"] for h in s.serious_hms)
        return "user"

    def _track_hms(self, s: Snapshot):
        """Eviduje jen NOVĚ se objevivší vážné HMS (trvale svítící varování nesmí měnit klasifikaci)."""
        active = {h["code"] for h in s.serious_hms}
        for code in active:
            if code not in self._hms_first_seen:
                self._hms_first_seen[code] = s.ts
                self._recent_serious.append((s.ts, code))
        for code in list(self._hms_first_seen):
            if code not in active:
                del self._hms_first_seen[code]
        self._recent_serious = [(t, c) for t, c in self._recent_serious if s.ts - t <= SERIOUS_HMS_WINDOW_S]

    def _absorb_errors(self, s: Snapshot):
        sess = self.session
        if s.print_error:
            sess.print_error = s.print_error
        if s.fail_reason and s.fail_reason != "0":
            sess.fail_reason = s.fail_reason
        sess.hms_serious_count = max(sess.hms_serious_count, len({c for _, c in self._recent_serious}))

    def _grace(self, s: Snapshot) -> list[Event]:
        sess = self.session
        self._absorb_errors(s)
        if s.ts >= sess._grace_deadline or s.gcode_state not in GS_TERMINAL:
            res, conf = self._classify_failed(s)
            return [self._close(s, res, conf, "observed")]
        return []

    def _classify_failed(self, s: Snapshot) -> tuple[str, str]:
        sess = self.session
        if hmsmod.is_cancel(sess.print_error):
            return "cancelled", "high"
        if sess.print_error or (sess.fail_reason and sess.fail_reason != "0") or self._recent_serious:
            return "failed", "high"
        if sess.last_percent < 100:
            return "cancelled", "low"
        return "failed", "low"

    def _close(self, s: Snapshot, result: str, confidence: str, end_source: str,
               ended_ts: int | None = None, kind: str = "closed") -> Event:
        sess = self.session
        end = ended_ts or int(s.ts)
        if sess._pause_started is not None:
            sess.paused_s += max(0, int(min(end, s.ts) - sess._pause_started))
            sess._pause_started = None
        sess.ended_ts = end
        sess.end_source = end_source
        sess.result = result
        sess.result_confidence = confidence
        sess.status = {"success": "finished", "failed": "failed", "cancelled": "cancelled"}.get(result, "unknown")
        sess.duration_s = max(0, end - sess.started_ts)
        base = sess.print_started_ts or sess.started_ts
        sess.active_print_s = max(0, end - base - sess.paused_s)
        sess.trays_end = [t.as_dict() for t in s.trays]
        sess._grace_deadline = None
        sess.updated_ts = int(s.ts)
        self.last_closed_fp = sess.fingerprint
        self.session = None
        return Event(kind, sess, s)
