"""Collector pro jednu tiskárnu: MQTT → Snapshot → StateMachine → SQLite → HA."""
from __future__ import annotations

import datetime as dt
import json
import logging
import math
import re
import threading
import time
from pathlib import Path
from zoneinfo import ZoneInfo

from . import aggregates, hms as hmsmod, locator
from .config import PrinterConfig, Settings
from .db import Database
from .filament import FilamentResolver
from .ha_api import HomeAssistant
from .ha_mqtt import HAPublisher
from .printer_mqtt import PrinterMQTT
from .sampler import Sampler
from .spoolman import Spoolman
from .util import VOID_SQL, je_vlastni, material_group
from .sync import GitSync
from .state_machine import Event, Snapshot, StateMachine

LOG = logging.getLogger("collector")
PROGRESS_WRITE_EVERY_S = 10
STATS_EVERY_S = 300
LOCATE_EVERY_S = 120        # jak často se ptát, kde tiskárna stojí
SONDA_TIMEOUT_S = 5.0       # TCP pokus sondy umístění (jeden na adresu a kolo)
SONDA_PRESKOCIT_S = 120     # zpráva od tiskárny mladší než tohle = MQTT žije, sonda se přeskočí
SONDA_NEUSPECHU = 3         # živá instance se stáhne až po tolika kolech bez lokální odpovědi (≈ 6 min)
PREVOZ_TICHO_S = 1800       # s otevřenou session jen po takovém tichu MQTT (a 3× jiné adrese přes VPN)
MEZERA_RECOVER_S = 600      # mezera mezi zprávami s otevřenou session → další zpráva jde přes _recover
OZDRAVENI_PO_S = 600        # samoozdravení MQTT: tolik bez zprávy → nový klient, nejvýš jednou za tuto dobu
HEARTBEAT_MAX_S = 900       # /health: plánovač musí proběhnout aspoň jednou za tuto dobu, jinak restart
SYNC_STOJI_S = 3600         # sync_only: collector_status 'degraded', když je poslední úspěšný sync starší
SYNC_GRACE_S = 1800         # …a na první úspěšný sync po startu se čeká tolik
FILAMENT_RETRY_AT_S = (60, 300, 900)
SNIMEK_SLOTU_S = 600        # publish_slots bere údaje tiskárny jen ze snímku mladšího než tohle (M10c)
# filament_check v režimu jen synchronizace bez otevřené session (M10c)
JINDE_DUVOD = "tiskárnu teď sleduje jiná lokalita nebo není v dosahu"
ZIVA_ZPRAVA_S = 300         # živou cloud entitu u otevřené session číst jen se zprávou z tiskárny mladší než tohle
ZIVY_KONEC_S = 900          # …a v _finalize jen do 15 min po konci tisku (pak už entita patří dalšímu)
# stav tisku z cloudu (ha-bambulab), při kterém smí instance bez tiskárny uvolnit cívku ze slotu (M9b)
TISK_NEBEZI = ("idle", "finish", "failed", "offline")
# hash commitu, ze kterého je add-on sestavený – zapisuje ho tools/publish_addon.sh a deploy_addon.sh
# do nasazované kopie, v gitu není (atribut build)
BUILD_SOUBOR = Path(__file__).with_name("BUILD")


def _nacti_build() -> str | None:
    try:
        return BUILD_SOUBOR.read_text().strip() or None
    except OSError:
        return None


class Collector:
    def __init__(self, settings: Settings, printer: PrinterConfig, db: Database, tz: ZoneInfo, prefix: str, *,
                 ha: HomeAssistant | None = None, spoolman: Spoolman | None = None, locate=None,
                 mqtt_factory=None, publisher_factory=None, sync_factory=None):
        # Volitelné závislosti jsou tu kvůli testům (tests/collector_harness.py): podstrčí se
        # falešná tiskárna, HA, Spoolman, sonda umístění a sync. V provozu platí výchozí.
        self.settings, self.printer, self.db, self.tz = settings, printer, db, tz
        self.serial = printer.serial
        self._locate = locate
        self._mqtt_factory = mqtt_factory or PrinterMQTT
        self.ha = ha if ha is not None else HomeAssistant(settings.supervisor_token)
        if spoolman is not None:
            self.spoolman = spoolman
        else:
            self.spoolman = Spoolman(settings.spoolman_url) if settings.spoolman_url else None
        self.filament = FilamentResolver(db, printer, self.ha, settings.data_dir / "3mf_cache", spoolman=self.spoolman,
                                         serial=self.serial, instance=settings.sync_instance)
        self.sampler = Sampler(self.serial)
        open_row = db.open_session_for(self.serial)
        last = db.last_closed_session(self.serial)
        if open_row:
            LOG.info("nalezena otevřená session %s (%s, %s %%) – pokusím se navázat", open_row["id"], open_row.get("subtask_name"), open_row.get("last_percent"))
        self.sm = StateMachine(self.serial, open_row, last["fingerprint"] if last else None)
        self._lock = threading.Lock()
        self._resolve_lock = threading.Lock()   # resolve smí běžet jen jednou naráz (jinak dvojí odečet ze cívky)
        # Heartbeat plánovače pro /health (M7b) – nastavený hned, ne až v prvním kole: plánovač nejdřív
        # 15 s čeká a watchdog Supervisoru si pamatuje i jediné 503. Monotonic, hodiny Pi po NTP skáčou.
        self._hb = self._start_mono = time.monotonic()
        self._neuspechy = 0                     # po sobě jdoucí kola sondy bez lokální odpovědi (probe_fails)
        self._prevoz = 0                        # …z toho po sobě jdoucí s tiskárnou na jiné adrese přes VPN
        self._posledni_zprava: float | None = None   # monotonic poslední přijaté zprávy tiskárny (v živém režimu)
        self._live_od: float | None = None           # monotonic začátku sběru
        self._ozdraveno: float | None = None         # monotonic posledního samoozdravení MQTT
        self._sync_bezi = threading.Lock()      # sync z plánovače běží ve vlastním vlákně, jen jeden naráz
        # Po stop() (SIGTERM) už nesmí vzniknout nový klient tiskárny – kolo plánovače, které právě běží,
        # by jinak přes samoozdravení nebo příjezd založilo spojení, když main() mezitím zavírá DB.
        self._zastaveno = False
        self._build = _nacti_build()
        try:
            self._schema_version = int(db.get_meta("schema_version", "0") or 0)
        except (TypeError, ValueError):
            self._schema_version = None
        # Tiskárna cestuje mezi lokalitami, tak se hledá na všech známých adresách. Sledovat ji
        # smí jen instance, která ji má ve své síti – přes VPN na ni dosáhnou obě a sbíraly by
        # dvakrát. Bez access_code nebo bez jediné adresy jede instance jen jako čtečka historie.
        self._adresa: str | None = None
        self._lokalne: bool | None = False
        if printer.access_code and printer.vsechny_adresy:
            self._adresa, self._lokalne = self._kde_je_tiskarna()
            LOG.info("%s", locator.popis(self._adresa, self._lokalne))
        self.filament.host = self._adresa if self._lokalne else None
        self.live = bool(self._adresa and self._lokalne)
        self.mqtt = self._nove_mqtt(self._adresa) if self.live else None
        if self.live:
            self._live_od = time.monotonic()
        self.pub = (publisher_factory or HAPublisher)(settings.mqtt, prefix, self.serial, printer.name,
                                                      on_command=self.on_command, currency=settings.currency)
        self._last_progress_write = 0.0
        self._last_stats = 0.0
        self._last_current: tuple | None = None
        self._last_tray_key: tuple | None = None
        self._filament_marks: set[int] = set()
        self._last_snapshot: Snapshot | None = None
        self._stats_dirty = True
        self.db.touch_printer(self.serial, name=printer.name, ip=self._adresa or printer.host)
        self.sync: GitSync | None = None
        if settings.sync_repo and settings.sync_instance:
            self.sync = (sync_factory or GitSync)(db, settings.sync_repo, settings.sync_token, settings.sync_instance,
                                                  settings.data_dir)
            LOG.info("sync historie zapnut: %s jako '%s'", settings.sync_repo, settings.sync_instance)
        self._last_sync = 0.0
        self._last_daily = None
        # výsledek posledního příkazu z HA (atribut last_command v collector_status) – bez něj by
        # odmítnutý příkaz na nástěnce vypadal, jako že prošel
        self.last_command: dict | None = None

    def _kde_je_tiskarna(self) -> tuple[str | None, bool | None]:
        """Sonda umístění tiskárny (locator.kde_je_tiskarna, v testech podstrčená): jeden pokus na adresu,
        (adresa, None) = nerozhodnuto (Supervisor nevrátil sítě hostitele)."""
        najdi = self._locate or locator.kde_je_tiskarna
        return najdi(self.printer.vsechny_adresy, timeout=SONDA_TIMEOUT_S)

    def _nove_mqtt(self, adresa: str):
        """Nový klient k tiskárně – jediné místo, kde vzniká (start, příjezd, jiná IP, samoozdravení).

        Otisk certifikátu z DB, tls_verify a callbacky se tak nedají v jedné z cest zapomenout (nové TOFU
        s varováním, collector_status by se po připojení neobnovil). Zprávy jdou do on_state s odkazem
        na klienta: po přepnutí se zprávy klienta, který už není self.mqtt, zahodí."""
        pinfo = self.db.get_printer(self.serial) or {}
        klient = None

        def on_state(state: dict, now: float):
            self.on_state(state, now, klient=klient)

        klient = self._mqtt_factory(adresa, self.serial, self.printer.access_code, on_state, self.settings.tls_verify,
                                    pinned_fingerprint=pinfo.get("tls_fingerprint"), on_pin=self._on_pin,
                                    on_connection=self._on_conn)
        return klient

    def _ticho_s(self) -> float:
        """Jak dlouho živá instance nemá od tiskárny zprávu (od začátku sběru, když ještě žádná nepřišla).
        Nový klient ze samoozdravení ho nenuluje – jinak by ticho nikdy nepřesáhlo 10 min."""
        return time.monotonic() - (self._posledni_zprava or self._live_od or self._start_mono)

    def _prehodnot_umisteni(self):
        """Přijela nebo odjela tiskárna? Instance se podle toho sama zapne nebo stáhne.

        Smysl je plug-and-play: tiskárnu jde zapnout v kterékoliv lokalitě a sběr se rozběhne
        tam, kde stojí, bez sahání do nastavení. Zapnutí je okamžité, stažení opatrné (M7a):

        - Živé MQTT (zpráva < 120 s) je lepší důkaz než sonda – sonda se přeskočí. Jediná neúspěšná
          2s sonda dřív shodila funkční spojení (7 ze 7 přepnutí v logu Haciendy bylo falešných).
        - Sonda běží mimo self._lock – trvá až 2 × 5 s a zprávy tiskárny ani health nesmí čekat.
        - Bez otevřené session se instance stáhne až ve 3. po sobě jdoucím kole bez lokální odpovědi.
        - Otevřená session patří instanci, která ji začala, a přehazování by ji roztrhlo na dvě. Stáhne
          se jen při převozu: MQTT mlčí > 30 min a tiskárna 3 kola po sobě odpovídá na JINÉ adrese přes
          VPN. Vypnutá tiskárna na místě (žádná odpověď) ani vlastní adresa přes VPN (chybně určená
          síť) nestačí. Session se nezavírá – ohlásí ji dohled „instance uvízla“ (open_session v sync_only).
        - Dokud se nepřepne, uložené umístění (_adresa, _lokalne, filament.host) se nemění: host by
          jinak zůstal prázdný i po obnově spojení a 3MF dalších tisků by se nestáhl.
        - Nerozhodnuto (Supervisor neodpověděl) není neúspěch – nic se nemění.
        """
        if self._zastaveno or not (self.printer.access_code and self.printer.vsechny_adresy):
            return
        m = self.mqtt
        if self.live and m is not None and m.connected and m.last_msg_ts \
                and time.monotonic() - m.last_msg_ts < SONDA_PRESKOCIT_S:
            self._neuspechy = self._prevoz = 0
            return
        adresa, lokalne = self._kde_je_tiskarna()
        if lokalne is None:
            LOG.debug("%s – režim beze změny", locator.popis(adresa, lokalne))
            return
        if adresa and lokalne:
            self._neuspechy = self._prevoz = 0
            if not self.live:
                self._zapni_sber(adresa)
            elif m is None or m.host != adresa:
                self._nove_spojeni(m, adresa, "tatáž lokalita, jiná IP (nová rezervace)")
            return
        if not self.live:
            # instance nesbírá – nelokální výsledek jen upřesní popis umístění (atribut umisteni)
            if (adresa, lokalne) != (self._adresa, self._lokalne):
                LOG.info("změna umístění: %s", locator.popis(adresa, lokalne))
                self._adresa, self._lokalne = adresa, lokalne
            return
        self._neuspechy += 1
        # převoz dokládá jen tiskárna na JINÉ adrese přes VPN – žádná odpověď ani vlastní adresa řadu přeruší
        self._prevoz = self._prevoz + 1 if adresa and adresa != self._adresa else 0
        with self._lock:
            sess = self.sm.session
        if sess is not None:
            ticho = self._ticho_s()
            if not (self._prevoz >= SONDA_NEUSPECHU and ticho > PREVOZ_TICHO_S):
                (LOG.info if self._neuspechy == 1 else LOG.debug)(
                    "sonda: %s (%d. kolo, MQTT mlčí %d s) – session %s patří této instanci, sběr nechávám",
                    locator.popis(adresa, lokalne), self._neuspechy, ticho, sess.id[-8:])
                return
        elif self._neuspechy < SONDA_NEUSPECHU:
            LOG.info("sonda: %s (%d/%d) – sběr zatím nechávám", locator.popis(adresa, lokalne),
                     self._neuspechy, SONDA_NEUSPECHU)
            return
        self._vypni_sber(m, adresa, lokalne, prevoz=sess is not None)

    def _zapni_sber(self, adresa: str):
        """Tiskárna dorazila: nový klient a sběr hned (jedno kolo)."""
        novy = self._nove_mqtt(adresa)
        with self._lock:
            if self.live or self._zastaveno:
                return
            self.mqtt, self.live = novy, True
            # První zpráva projde _recover jako po restartu: otevřená session z DB (LOC-1) naváže
            # ('resumed'), nebo se uzavře k poslední zprávě ('lost'), ne k okamžiku návratu.
            self.sm._first = True
            self._last_snapshot = None
            self._last_current = None
            self._last_tray_key = None
            self._live_od, self._posledni_zprava = time.monotonic(), None
        self._adresa, self._lokalne = adresa, True
        self.filament.host = adresa             # před startem – resolve po první zprávě už host potřebuje
        novy.start()
        self.db.touch_printer(self.serial, ip=adresa)
        LOG.info("změna umístění: %s", locator.popis(adresa, True))
        LOG.info("tiskárna dorazila – začínám sbírat")
        self._stats_dirty = True

    def _nove_spojeni(self, stary, adresa: str, duvod: str) -> bool:
        """Vymění klienta tiskárny za nový (jiná IP v téže lokalitě, samoozdravení).

        Nový objekt, ne restart starého: skutečný PrinterMQTT po stop() už znovu nenaběhne. Výměna
        pod zámkem, stop() starého až mimo něj – paho umí zavolat on_disconnect synchronně a ten by
        čekal na zámek, který drží tohle vlákno. Zprávy starého klienta pak on_state zahodí."""
        novy = self._nove_mqtt(adresa)
        with self._lock:
            if self._zastaveno or not self.live or self.mqtt is not stary:
                return False
            self.mqtt = novy
            self.sm._first = True               # mezera ve zprávách: první zpráva přes _recover
        if stary is not None:
            stary.stop()
        self._adresa, self._lokalne = adresa, True
        self.filament.host = adresa
        novy.start()
        self.db.touch_printer(self.serial, ip=adresa)
        LOG.warning("%s – nové spojení s tiskárnou na %s", duvod, adresa)
        return True

    def _vypni_sber(self, stary, adresa: str | None, lokalne: bool, *, prevoz: bool):
        """Tiskárna odjela: přepnout na sdílení historie. Podmínky se pod zámkem ověří znovu – během
        sondy mohl začít tisk nebo dorazit zpráva (LOC-2)."""
        with self._lock:
            sess = self.sm.session
            m = self.mqtt
            cerstva = bool(m is not None and m.last_msg_ts and time.monotonic() - m.last_msg_ts < SONDA_PRESKOCIT_S)
            if not self.live or m is not stary:
                return
            if cerstva or (sess is not None and (not prevoz or self._ticho_s() <= PREVOZ_TICHO_S)):
                LOG.info("během sondy %s – sběr nechávám",
                         f"začal tisk {sess.subtask_name or sess.id[-8:]}" if sess is not None and not prevoz
                         else "se tiskárna ozvala")
                self._neuspechy = self._prevoz = 0
                return
            self.mqtt, self.live = None, False
        if stary is not None:
            stary.stop()
        self._adresa, self._lokalne = adresa, lokalne
        self.filament.host = None
        with self._lock:
            # živé hodnoty z doby sběru v sync_only neplatí a po návratu by je deduplikace nepublikovala (M10c)
            self._last_snapshot = None
            self._last_current = None
            self._last_tray_key = None
        self._live_od = self._posledni_zprava = None
        self._neuspechy = self._prevoz = 0
        LOG.info("změna umístění: %s", locator.popis(adresa, lokalne))
        if sess is not None:
            LOG.warning("tiskárna se přestěhovala uprostřed tisku %s – session %s zůstává otevřená (dohled "
                        "„instance uvízla“), sběr předávám druhé lokalitě", sess.subtask_name or "?", sess.id[-8:])
        else:
            LOG.info("tiskárna odjela – přecházím na sdílení historie")
        self._stats_dirty = True

    def _ozdrav_mqtt(self):
        """Samoozdravení spojení s tiskárnou (M7a bod 7).

        Mrtvé MQTT (připojeno bez jediné zprávy, spadlá smyčka klienta) dřív řešil restart watchdogem
        přes 503. /health teď hlídá jen plánovač, tak se klient vymění tady: po 10 min bez zprávy
        (od vzniku klienta, když žádná nepřišla) nebo když jeho smyčka neběží – nejvýš jednou za 10 min.
        """
        m = self.mqtt
        if self._zastaveno or not (self.live and m is not None):
            return
        ted = time.monotonic()
        ticho = ted - (m.last_msg_ts or m.created_ts)
        if ticho <= OZDRAVENI_PO_S and m.thread_alive:
            return
        if self._ozdraveno is not None and ted - self._ozdraveno < OZDRAVENI_PO_S:
            return
        self._ozdraveno = ted
        duvod = f"tiskárna {int(ticho)} s mlčí" if ticho > OZDRAVENI_PO_S else "smyčka spojení s tiskárnou neběží"
        self._nove_spojeni(m, m.host, f"samoozdravení: {duvod}")

    # --- start / stop --------------------------------------------------------------
    def start(self):
        self.pub.start()
        m = self.mqtt
        if m:
            m.start()
        else:
            LOG.info("režim jen synchronizace: tiskárna %s se v této lokalitě nesleduje", self.serial)
        threading.Thread(target=self._scheduler, name=f"sched-{self.serial[-4:]}", daemon=True).start()

    def stop(self):
        # Příznak a klient pod zámkem: výměna klienta (_nove_spojeni, _zapni_sber) buď proběhne před
        # stop() a zastaví se už nový klient, nebo příznak uvidí a nového klienta nezaregistruje.
        # stop() klienta mimo zámek – paho umí zavolat on_disconnect synchronně (viz _nove_spojeni).
        with self._lock:
            self._zastaveno = True
            m = self.mqtt
        if m:
            m.stop()
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
            self._vysledek(cmd, True)
        elif cmd.startswith("assign_slot:"):
            # assign_slot:<konec_id_session>:<tray_global> - rucne doplnit slot u tisku, kde tiskarna slot neposlala
            try:
                self._assign_slot(cmd)
            except Exception:
                LOG.exception("assign_slot selhal")
                self._vysledek(cmd, False, "chyba – viz log add-onu")
        elif cmd.startswith("set_slot:"):
            # set_slot:<slot 1-4>:<spool_id|-> [:<popis>] – zapíše, co je teď ve slotu. Funguje i bez Spoolmanu:
            # záznam se použije pro doúčtování tisků a po obnovení spojení se přiřazení propíše do Spoolmanu.
            try:
                parts = cmd.split(":", 3)
                slot = int(parts[1]); tray = slot - 1
                spool_id = None if parts[2] in ("-", "", "none") else int(parts[2])
                label, from_ts = (parts[3] if len(parts) > 3 else None), None
                if label and "@" in label:      # popis@2026-09-16T12:38 – kdy se cívka opravdu vyměnila
                    label, when = label.rsplit("@", 1)
                    try:
                        from_ts = int(dt.datetime.fromisoformat(when.strip()).replace(tzinfo=self.tz).timestamp())
                    except ValueError:
                        LOG.warning("set_slot: nečitelný čas %r, beru teď", when)
                rec_id = self.db.set_slot_spool(self.serial, tray, spool_id, label=(label or "").strip() or None,
                                                note="ručně" if spool_id else "vyjmuto", from_ts=from_ts)
                if rec_id is None:
                    # stejný čas jako existující záznam (FIL-10) – původní záznam zůstává, jak byl
                    self._vysledek(cmd, False, f"slot {slot}: záznam s tímto časem už existuje – použij jiný čas")
                    return
                LOG.info("slot %d: zapsána cívka %s%s", slot, spool_id or "žádná", f" ({label})" if label else "")
                self.publish_slots()
                # vlastní záznam do Spoolmanu hned, i na instanci, která tiskárnu nesbírá (M9a)
                self.push_slot_journal(rec_id=rec_id)
                self.flush_pending_spools()
                self._vysledek(cmd, True, f"slot {slot}: cívka {spool_id or 'žádná'}")
            except Exception:
                LOG.exception("set_slot selhal")
                self._vysledek(cmd, False, "chyba – viz log add-onu")
        elif cmd.startswith("mark_defect") or cmd.startswith("mark_ok"):
            # mark_defect[:<konec_id>[:<poznámka>]] – tisk doběhl, ale díl je k ničemu (warp, ucpaná tryska…).
            # Filament zůstává spotřebovaný, jen se to nepočítá jako povedený tisk. Bez ID poslední tisk.
            parts = cmd.split(":", 2)
            quality = "defect" if parts[0] == "mark_defect" else "ok"
            sid_suffix = parts[1] if len(parts) > 1 and parts[1] else None
            note = parts[2] if len(parts) > 2 else None
            row = self._cil_prikazu(cmd, sid_suffix, posledni=True)
            if row:
                self.db.update_session(row["id"], quality=quality, quality_note=note)
                LOG.info("session %s označena jako %s%s", row["id"][-8:], quality, f" ({note})" if note else "")
                self._stats_dirty = True
                self.publish_stats(force=True)
                self._vysledek(cmd, True, f"označeno jako {quality}", row)
        elif cmd.split("@")[0] in ("maintenance_done", "desiccant_changed"):
            # maintenance_done[@<ISO čas>] – čas se hodí, když se na zápis zapomene a doplňuje
            # se zpětně. Bez něj by interval běžel od doplnění, ne od skutečné výměny.
            puvodni, (cmd, _, kdy) = cmd, cmd.partition("@")
            now_ts = int(time.time())
            if kdy.strip():
                try:
                    now_ts = int(dt.datetime.fromisoformat(kdy.strip()).replace(tzinfo=self.tz).timestamp())
                except ValueError:
                    LOG.warning("%s: nečitelný čas %r, beru teď", cmd, kdy)
            self.db.set_meta(f"{cmd}_ts_{self.serial}", now_ts)
            key = f"{cmd}_history_{self.serial}"          # seznam všech výměn/údržeb kvůli grafu a doložení
            try:
                hist = json.loads(self.db.get_meta(key, "[]") or "[]")
            except ValueError:
                hist = []
            hist = sorted(set(hist + [now_ts]))[-50:]
            self.db.set_meta(key, json.dumps(hist))
            kdy_txt = dt.datetime.fromtimestamp(now_ts, self.tz).strftime("%d.%m.%Y %H:%M")
            LOG.info("%s zaznamenáno k %s", cmd, kdy_txt)
            self._stats_dirty = True
            self.publish_stats(force=True)
            self._vysledek(puvodni, True, f"zaznamenáno k {kdy_txt}")
        elif cmd.startswith("resolve:"):
            # resolve:<konec_id_session> – přepočítat spotřebu uzavřeného tisku (např. po opravě výpočtu).
            # Rozdíl proti už odečtenému se ve Spoolmanu dorovná, klidně i vrácením.
            try:
                self._resolve(cmd)
            except Exception:
                LOG.exception("resolve selhal")
                self._vysledek(cmd, False, "chyba – viz log add-onu")
        elif cmd.startswith("set_plan:"):
            # set_plan:<konec_id>:<gramy>[:<metry>][:bez_spoolmanu] – ruční plán tisku (set_plan:<konec_id>:- ho zruší)
            try:
                self._set_plan(cmd)
            except Exception:
                LOG.exception("set_plan selhal")
                self._vysledek(cmd, False, "chyba – viz log add-onu")
        elif cmd.startswith("settled:"):
            # settled:<konec_id> – spotřebu hotového tisku uzavřít ručně, už se nepřepočítá ani nedoúčtuje
            try:
                self._settled(cmd)
            except Exception:
                LOG.exception("settled selhal")
                self._vysledek(cmd, False, "chyba – viz log add-onu")
        elif cmd == "refetch_3mf":
            try:
                self._refetch_3mf(cmd)
            except Exception:
                LOG.exception("refetch_3mf selhal")
                self._vysledek(cmd, False, "chyba – viz log add-onu")
        else:
            # překlep ve skriptu HA apod. – nic se neprovede, ale ať to nevypadá, že příkaz prošel
            LOG.warning("neznámý příkaz z HA: %s – nic se neprovede", cmd)
            self._vysledek(cmd, False, "neznámý příkaz")

    # --- příkazy nad session ----------------------------------------------------------------------
    def _vysledek(self, cmd: str, provedeno: bool, duvod: str | None = None, session: dict | str | None = None):
        """Výsledek příkazu do atributu last_command v collector_status (hned publikovat).

        Volat mimo self._lock – health() si ho bere sám."""
        sid = session.get("id") if isinstance(session, dict) else session
        self.last_command = {"cmd": cmd[:120], "result": "provedeno" if provedeno else "odmítnuto", "reason": duvod,
                             "session": sid, "ts": dt.datetime.fromtimestamp(time.time(), self.tz).strftime("%Y-%m-%dT%H:%M:%S")}
        self._publish_status()

    def _odmitni(self, cmd: str, duvod: str, session: dict | str | None = None):
        LOG.warning("%s: %s – příkaz odmítnut", cmd.split(":")[0], duvod)
        self._vysledek(cmd, False, duvod, session)

    def _cil_prikazu(self, cmd: str, sid_suffix: str | None, *, posledni: bool = False) -> dict | None:
        """Cílová session příkazu z HA – smí to být jen vlastní session (M4a).

        S ID: session, jejíž ID končí na sid_suffix; při víc shodách vyhraje vlastní, víc vlastních
        je nejednoznačné. Bez ID (jen posledni=True: mark_* a refetch_3mf) poslední uzavřená session,
        která je tiskem (ne kalibrace ani prázdná přípravná), bez ohledu na lokalitu – táž, kterou
        ukazuje last_print. Cizí session (přišla gitem z druhé lokality) se nemění: opravuje ji ta
        instance, která ji zapsala, jinak by se změna exportovala z obou míst a do Spoolmanu by
        zapisovaly obě. Vlastní = origin prázdný nebo rovný sync_instance (bez sync_instance jen
        prázdný). Po převozu tiskárny tak starší tisk opraví jen lokalita, kde proběhl.
        Vrátí řádek z DB, nebo None (odmítnutí je v logu i v last_command).
        """
        sid_suffix = (sid_suffix or "").strip()
        if sid_suffix:
            shody = self.db.query("SELECT * FROM sessions WHERE id LIKE ? ORDER BY ended_ts DESC", (f"%{sid_suffix}",))
        elif posledni:
            row = self.db.posledni_tisk(self.serial)
            shody = [row] if row else []
        else:
            self._odmitni(cmd, "chybí ID session")
            return None
        if not shody:
            self._odmitni(cmd, f"session {sid_suffix} nenalezena" if sid_suffix else "žádný uzavřený tisk")
            return None
        vlastni = [r for r in shody if je_vlastni(r, self.settings.sync_instance)]
        if len(vlastni) > 1:
            self._odmitni(cmd, f"konec ID {sid_suffix} má {len(vlastni)} session – zadej delší konec ID")
            return None
        if vlastni:
            return vlastni[0]
        cizi = shody[0]
        self._odmitni(cmd, f"session {cizi['id'][-8:]} patří lokalitě {cizi.get('origin')} – oprav ji tam", cizi)
        return None

    def _resolve(self, cmd: str):
        row = self._cil_prikazu(cmd, cmd.split(":", 1)[1])
        if row is None:
            return
        if not row.get("ended_ts"):
            self._odmitni(cmd, f"session {row['id'][-8:]} ještě běží – přepočítat jde až hotový tisk", row)
            return
        with self._resolve_lock:
            row = self.db.get_session(row["id"]) or row
            out = self.filament.resolve(row, final=True, allow_live=False)
        self._stats_dirty = True
        self.publish_stats(force=True)
        LOG.info("session %s přepočtena: %s", row["id"][-8:], out)
        if out is not None:
            self._vysledek(cmd, True, f"přepočteno: {out['g']} g ({out['source']})", row)
        elif row.get("manual_override"):
            self._vysledek(cmd, True, "spotřeba je uzavřená ručně (settled) – nepřepočítává se", row)
        else:
            self._vysledek(cmd, True, "beze změny – zdroj spotřeby teď není k dispozici", row)

    def _assign_slot(self, cmd: str):
        """assign_slot:<konec_id>:<tray_global> – slot, ze kterého tisk šel (když ho tiskárna neposlala).

        Pod _resolve_lock, ať neběží souběžně s průběžným odečtem (dvojí odečet, FIL-8). U otevřené
        session se slot zapíše i do stavového automatu (pod self._lock, pořadí zámků _resolve_lock →
        _lock jako v __resolve_filament) – jinak by ho průběžný zápis v on_state do 10 s vrátil, a tím
        i odečet na cívku původního slotu. U uzavřené session se slot zapíše až po přepočtu: když pro ni
        teď žádný zdroj spotřeby není (pojistka „beze změny“), zůstane i slot původní.
        """
        try:
            _, sid_suffix, tray_txt = cmd.split(":")
            tray = int(tray_txt)
        except ValueError:
            self._odmitni(cmd, "nečitelný příkaz – formát assign_slot:<konec_id>:<tray_global>")
            return
        row = self._cil_prikazu(cmd, sid_suffix)
        if row is None:
            return
        with self._resolve_lock:
            row = self.db.get_session(row["id"]) or row
            if row.get("ended_ts") is None:
                with self._lock:
                    sess = self.sm.session
                    if sess and sess.id == row["id"]:
                        sess.tray_now_start = tray
                self.db.update_session(row["id"], tray_now_start=tray)
                # příkaz z HA živou entitu nečte (M4b) – přepočítává se z uloženého plánu
                out = self.filament.resolve(self.db.get_session(row["id"]), final=False, allow_live=False)
            elif row.get("manual_override"):
                LOG.warning("assign_slot: session %s má spotřebu uzavřenou ručně (settled) – slot se nemění", row["id"][-8:])
                self._vysledek(cmd, False, "spotřeba je uzavřená ručně (settled) – slot se nemění", row)
                return
            else:
                puvodni = row.get("tray_now_start")
                out = self.filament.resolve({**row, "tray_now_start": tray}, final=True, allow_live=False)
                if out is not None and puvodni != tray:
                    self.db.update_session(row["id"], tray_now_start=tray)
        if out is None and row.get("ended_ts") is not None:
            LOG.warning("assign_slot: session %s – zdroj spotřeby teď není k dispozici, slot i spotřeba beze změny",
                        row["id"][-8:])
            self._vysledek(cmd, True, "beze změny – zdroj spotřeby teď není k dispozici", row)
            return
        self._stats_dirty = True
        self.publish_stats(force=True)
        with self._lock:
            sess = self.sm.session
        if sess and sess.id == row["id"]:
            self._publish_current_filament(sess)
        LOG.info("session %s: slot nastaven na %s a přepočítán: %s", row["id"][-8:], tray, out)
        self._vysledek(cmd, True, f"slot {tray} nastaven a přepočítán", row)

    def _refetch_3mf(self, cmd: str):
        """refetch_3mf – 3MF znovu z USB tiskárny: u běžícího tisku, jinak u posledního tisku (M4c).

        Jen instance, která tiskárnu sbírá (live) – jinde by se stahovalo odnikud a přepočítával cizí
        tisk. U uzavřené session: když se 3MF nenajde, vrátí se původní threemf_status a spotřeba se
        nemění (pojistka „beze změny“ v resolve). S 3MF v cache se nic nestahuje (cache je otisk tisku)
        – špatný 3MF se opravuje přes set_plan. Session se spotřebou uzavřenou ručně (settled) nebo
        s ručním plánem 3MF nepoužije, takže se příkaz odmítne bez stahování. Po převozu tiskárny jde
        poslední tisk staré lokality opravit jen přes set_plan na lokalitě, kde proběhl.
        """
        if not self.live:
            self._odmitni(cmd, "tiskárna tu není (instance jen synchronizuje) – 3MF stáhnout nejde")
            return
        with self._lock:
            sess = self.sm.session
        if sess:
            self.filament.zapomen_3mf(sess.id)
            row = self.db.get_session(sess.id)
            if row:
                self.db.update_session(sess.id, threemf_status=None)
                row["threemf_status"] = None
                with self._resolve_lock:
                    self.filament.resolve(self.db.get_session(sess.id), final=False, allow_live=False)
                self.publish_filament_check(sess.id)
                self.save_cover(sess.id)
                self._publish_current_filament(sess)
            stav = (self.db.get_session(sess.id) or {}).get("threemf_status")
            self._vysledek(cmd, True, f"3MF běžícího tisku: {stav or 'zatím nenačten'}", sess.id)
            return
        last = self._cil_prikazu(cmd, None, posledni=True)
        if last is None:
            return
        v_cache = self.filament.cache_3mf(last).exists()
        with self._resolve_lock:
            row = self.db.get_session(last["id"]) or last
            if row.get("manual_override") or row.get("manual_plan_g") is not None:
                # Ruční spotřeba (settled) i ruční plán mají v resolve přednost před 3MF a FTP se nezkouší.
                # Nic nenulovat ani nevracet – „3MF načten“ nebo „3MF nenalezen“ by hlásilo stažení,
                # které neproběhlo.
                odmitnuti = ("spotřeba je uzavřená ručně (settled) – 3MF se nepoužije" if row.get("manual_override")
                             else f"platí ruční plán {row['manual_plan_g']} g – 3MF se nepoužije (oprav přes set_plan)")
                LOG.warning("refetch_3mf: session %s: %s", row["id"][-8:], odmitnuti)
            elif row.get("threemf_status") == "mismatch" and v_cache:
                # 3MF uzavřeného tisku vyvrátila čerstvá cloud hodnota (křížová kontrola). V cache
                # je právě ten zamítnutý soubor a živá entita už patří dalšímu tisku, takže po
                # vynulování by se vzal bez kontroly a cívce vrátil rozdíl. Opravuje se přes set_plan.
                LOG.warning("refetch_3mf: 3MF session %s nesedí na cloud hmotnost – spotřeba ponechána "
                            "(opravit jde přes set_plan)", row["id"][-8:])
                odmitnuti = "3MF nesedí na cloud hmotnost – spotřeba ponechána, oprav přes set_plan"
            else:
                odmitnuti = None
                puvodni = {k: row.get(k) for k in ("threemf_status", "threemf_path", "threemf_fetched_ts")}
                self.filament.zapomen_3mf(row["id"])
                # threemf_status se nuluje jen v předaném řádku a když stažení neuspěje, vrátí se původní stav:
                # dřív zůstal None, 3MF session bez souboru pak šla přepočítat jako bez zdroje a cívce se
                # vrátil celý odečet (replay Haciendy −2 899,6 g). S refetch=True se neúspěšné stažení do DB
                # vůbec nezapíše – výsledek je jen v předaném řádku, session se nemění ani na okamžik.
                predany = {**row, "threemf_status": None}
                out = self.filament.resolve(predany, final=True, allow_live=False, refetch=True)
                stav = predany.get("threemf_status")
                if stav != "ok":
                    self.filament.vrat_stav_3mf(row["id"], puvodni)
        if odmitnuti:
            self._vysledek(cmd, False, odmitnuti, last)
            return
        self._stats_dirty = True
        self.publish_stats(force=True)
        if stav == "ok":
            LOG.info("refetch_3mf: session %s přepočtena z 3MF: %s", last["id"][-8:], out)
            self._vysledek(cmd, True, ("3MF je v cache – nic se nestahovalo (špatný 3MF oprav přes set_plan)"
                                       if v_cache else f"3MF načten: {out['g'] if out else '?'} g"), last)
        else:
            LOG.warning("refetch_3mf: session %s: 3MF nenalezen – spotřeba ponechána (stažení: %s)", last["id"][-8:], stav)
            self._vysledek(cmd, True, "3MF nenalezen – spotřeba ponechána", last)

    def _set_plan(self, cmd: str):
        """set_plan:<konec_id>:<gramy>[:<metry>][:bez_spoolmanu] – ruční plán celé úlohy (desetinná tečka).

        Uloží se do manual_plan_g/m (ne do cloud_weight_g – s tím pracuje heuristika zastaralé cloud
        hodnoty) a má přednost před 3MF, cloudem i AMS; u nedokončeného tisku se násobí postupem.
        Spoolman se dorovná přes deník odečtů jen o rozdíl, s bez_spoolmanu se opraví jen evidence.
        set_plan:<konec_id>:- ruční plán zruší a spotřeba se spočítá znovu z automatických zdrojů –
        jen když nějaký je (jinak by zůstal zdroj 'manual' bez ručního plánu).
        """
        parts = [x.strip() for x in cmd.split(":")]
        bez_spoolmanu = len(parts) > 3 and parts[-1] == "bez_spoolmanu"
        hodnoty = parts[2:-1] if bez_spoolmanu else parts[2:]
        zrusit = hodnoty == ["-"]
        grams = metres = None
        if not zrusit:
            try:
                grams = float(hodnoty[0])
                metres = float(hodnoty[1]) if len(hodnoty) > 1 and hodnoty[1] else None
            except (IndexError, ValueError):
                grams = None
            # float() vezme i 'inf' a 'nan' – nekonečný plán by šel do Spoolmanu a statistiky by padaly
            if (grams is None or not math.isfinite(grams) or grams < 0 or len(hodnoty) > 2
                    or (metres is not None and (not math.isfinite(metres) or metres < 0))):
                LOG.warning("set_plan: nečitelný příkaz %r – formát set_plan:<id>:<gramy>[:<metry>][:bez_spoolmanu], "
                            "desetinná tečka", cmd)
                self._vysledek(cmd, False, "nečitelný příkaz – formát set_plan:<id>:<gramy>[:<metry>][:bez_spoolmanu], "
                                           "desetinná tečka")
                return
        row = self._cil_prikazu(cmd, parts[1] if len(parts) > 1 else "")
        if row is None:
            return
        with self._resolve_lock:
            row = self.db.get_session(row["id"]) or row
            if row.get("manual_override"):
                LOG.warning("set_plan: session %s má spotřebu uzavřenou ručně (settled) – plán se nemění", row["id"][-8:])
                self._vysledek(cmd, False, "spotřeba je uzavřená ručně (settled) – plán se nemění", row)
                return
            final = row.get("ended_ts") is not None
            if zrusit:
                puvodni = row.get("manual_plan_g")
                if puvodni is None:
                    LOG.info("set_plan: session %s ruční plán nemá", row["id"][-8:])
                    self._vysledek(cmd, False, "session ruční plán nemá", row)
                    return
                # Nejdřív přepočet bez ručního plánu, smazat ho až potom. Uzavřenou session bez jiného zdroje
                # zastaví pojistka „beze změny“ (None) – ruční plán pak musí zůstat, jinak by spotřeba
                # i zdroj 'manual' zůstaly bez plánu, ze kterého vzešly.
                out = self.filament.resolve(row, final=final, allow_live=False, bez_spoolmanu=bez_spoolmanu,
                                            rucni_plan=False)
                if out is None:
                    LOG.warning("set_plan: session %s – ruční plán %.2f g se neruší: jiný zdroj spotřeby (3MF, uložená "
                                "cloud hodnota ani AMS) není, spotřeba zůstává podle ručního plánu", row["id"][-8:], puvodni)
                    self._vysledek(cmd, False, "ruční plán se neruší – jiný zdroj spotřeby není", row)
                    return
                self.db.update_session(row["id"], manual_plan_g=None, manual_plan_m=None)
            else:
                if (row.get("manual_plan_g"), row.get("manual_plan_m")) != (grams, metres):
                    self.db.update_session(row["id"], manual_plan_g=grams, manual_plan_m=metres)
                row = self.db.get_session(row["id"])
                out = self.filament.resolve(row, final=final, allow_live=False, bez_spoolmanu=bez_spoolmanu)
        self._stats_dirty = True
        self.publish_stats(force=True)
        with self._lock:
            sess = self.sm.session
        if sess and sess.id == row["id"]:
            self._publish_current_filament(sess)
        if zrusit:
            LOG.warning("session %s: ruční plán %.2f g zrušen – spotřeba přepočtena z automatických zdrojů%s: %s",
                        row["id"][-8:], puvodni, " (Spoolman beze změny)" if bez_spoolmanu else " a Spoolman dorovnán", out)
            self._vysledek(cmd, True, f"ruční plán zrušen, spotřeba {out['g'] if out else '?'} g", row)
        else:
            LOG.info("session %s: ruční plán %.2f g%s%s nastaven a přepočítán: %s", row["id"][-8:], grams,
                     f" / {metres:.2f} m" if metres is not None else "", " (bez Spoolmanu)" if bez_spoolmanu else "", out)
            self._vysledek(cmd, True, f"ruční plán {grams:g} g nastaven, spotřeba {out['g'] if out else '?'} g", row)

    def _settled(self, cmd: str):
        """settled:<konec_id> – spotřebu hotového tisku uzavřít ručně (manual_override=1): resolve ji už
        nepřepočítá a fronta doúčtování ji vynechá. Jen uzavřená session – u otevřené by příznak při
        pauze nebo konci tisku přepsal upsert ze stavového automatu (Session.to_row)."""
        row = self._cil_prikazu(cmd, cmd.partition(":")[2])
        if row is None:
            return
        with self._resolve_lock:              # ať neběží souběžně s doúčtováním téže session
            row = self.db.get_session(row["id"]) or row
            if row.get("ended_ts") is None:
                LOG.warning("settled: session %s ještě běží – ručně uzavřít jde až hotový tisk", row["id"][-8:])
                self._vysledek(cmd, False, "tisk ještě běží – ručně uzavřít jde až hotový tisk", row)
                return
            if row.get("manual_override"):
                LOG.info("settled: session %s už je uzavřená ručně", row["id"][-8:])
                self._vysledek(cmd, True, "už je uzavřená ručně", row)
                return
            chybi, vratky = 0.0, {}
            for f in self.db.filaments(row["id"]):
                rozdil = (f.get("used_g") or 0) - (f.get("spool_deducted_g") or 0)
                if rozdil > 0 and f.get("tray_global") is not None:
                    chybi += rozdil
                elif rozdil < 0 and f.get("spool_id"):
                    vratky[f["spool_id"]] = vratky.get(f["spool_id"], 0.0) - rozdil
            self.db.update_session(row["id"], manual_override=1)
        vratky = {k: v for k, v in vratky.items() if v > 0.05}
        LOG.warning("session %s uzavřena ručně (settled): spotřeba %s g zůstává, nedoúčtováno %.1f g%s – už se "
                    "nepřepočítá ani nedoúčtuje", row["id"][-8:], row.get("filament_g"), chybi,
                    (", nevrácené vratky " + ", ".join(f"#{k} {v:.1f} g" for k, v in sorted(vratky.items())))
                    if vratky else "")
        self._stats_dirty = True
        self.publish_stats(force=True)
        self._vysledek(cmd, True, f"spotřeba uzavřena ručně, nedoúčtováno {chybi:.1f} g", row)

    def on_state(self, state: dict, now: float, klient=None):
        snap = Snapshot.from_print(state, now)
        with self._lock:
            if klient is not None and klient is not self.mqtt:
                # Zpráva klienta, který už sběr nedělá (vypnutý sběr, nové spojení): paho ji mohlo přijmout
                # těsně před přepnutím. Zpracovaná by otevřela session na instanci, která tiskárnu nesleduje.
                LOG.debug("zpráva od starého spojení s tiskárnou – zahazuji")
                return
            self._posledni_zprava = time.monotonic()
            predchozi = self._last_snapshot
            if (self.sm.session and predchozi is not None and not self.sm._first
                    and snap.ts - predchozi.ts > MEZERA_RECOVER_S):
                # Dlouhá mezera bez restartu (dřív ji „řešil“ restart watchdogem): první zpráva po ní se
                # posoudí jako po restartu – stejný tisk naváže, jinak se session uzavře k poslední zprávě,
                # ne k okamžiku návratu (10 h výpadku by jinak skončilo jako zrušený tisk s 11 h).
                LOG.info("session %s: %d min bez zpráv od tiskárny – navazuji jako po restartu",
                         self.sm.session.id[-8:], (snap.ts - predchozi.ts) // 60)
                self.sm._first = True
            self._last_snapshot = snap
            events = self.sm.feed(snap)
            session = self.sm.session
            for ev in events:
                self._handle(ev)
            if session and (now - self._last_progress_write >= PROGRESS_WRITE_EVERY_S) and not any(e.kind != "updated" for e in events):
                # Slot, ze kterého se tiskne, a úseky po slotech se musí ukládat průběžně taky: průběžný
                # odečet i kontrola filamentu čtou z databáze. Dřív se zapsaly jen při začátku a konci
                # tisku – tisk, který začal před založením filamentu, pak celou dobu „neměl slot"
                # (kontrola hlásila unknown, odečet šel podle barvy ve sliceru na cizí cívku) a po
                # restartu add-onu uprostřed tisku se úseky ztratily.
                self.db.update_session(session.id, last_percent=session.last_percent, last_layer=session.last_layer,
                                       total_layers=session.total_layers, last_remaining_min=session.last_remaining_min,
                                       last_seen_ts=session.last_seen_ts, predicted_s=session.predicted_s,
                                       predicted_source=session.predicted_source, status=session.status, paused_s=session.paused_s,
                                       tray_now_start=session.tray_now_start, trays_start=session.trays_start,
                                       tray_spans=session.tray_spans)
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
            threading.Thread(target=self._finalize, args=(s.id, ev.kind), daemon=True).start()
        self._last_progress_write = time.time()

    def _ziva_otevrena(self) -> bool:
        """Smí průběžný resolve otevřené session číst živou cloud entitu? Jen instance, která tiskárnu
        sbírá a má od ní čerstvou zprávu. Otevřená session po restartu s neúspěšnou sondou (live=False)
        by jinak brala hmotnost tisku, který mezitím běží kdekoli (simulace +118,8 g)."""
        m = self.mqtt
        return bool(self.live and m and m.last_msg_ts and time.monotonic() - m.last_msg_ts < ZIVA_ZPRAVA_S)

    def _finalize(self, sid: str, kind: str = "closed"):
        row = self.db.get_session(sid)
        if row:
            self.save_cover(sid)
            # Živá entita jen hned po řádném konci (i obnoveném po restartu). 'superseded' se vyhlašuje,
            # až když běží další tisk, a 'lost' po restartu třeba i dny po konci – entita pak patří jinému
            # tisku a platí jen cloud hodnota uložená během tisku.
            konec = row.get("ended_ts")
            zive = bool(self.live and kind == "closed" and konec and time.time() - konec < ZIVY_KONEC_S)
            with self._resolve_lock:
                try:
                    self.filament.resolve(self.db.get_session(sid), final=True, allow_live=zive)
                except Exception:
                    LOG.exception("resolver filamentu selhal")
        self._stats_dirty = True
        self.publish_stats(force=True)
        self.export_csv()
        self._run_sync(cekat=True)

    def _spust_sync(self):
        """Sync z plánovače ve vlastním vlákně (M7b/M8d): git umí viset i minuty (pull a push s opakováním)
        a plánovač by mezitím neobnovil heartbeat – /health by pak restartoval add-on klidně uprostřed
        tisku. Když předchozí sync z plánovače ještě běží, kolo se přeskočí."""
        if not self._sync_bezi.acquire(blocking=False):
            LOG.debug("sync ještě běží, přeskakuji")
            return
        try:
            threading.Thread(target=self._sync_ve_vlakne, name=f"sync-{self.serial[-4:]}", daemon=True).start()
        except Exception:
            self._sync_bezi.release()
            raise

    def _sync_ve_vlakne(self):
        try:
            self._run_sync()
        finally:
            self._sync_bezi.release()

    def _run_sync(self, cekat: bool = False):
        """Jedno kolo syncu. Naráz běží jen jedno (GitSync.run_once, M8d): z plánovače se na běžící kolo
        nečeká, _finalize počká – ať dokončený tisk nejde do repa až za další interval. Přeskočené kolo
        nemění _last_sync, takže plánovač to zkusí hned v příštím kole."""
        if not self.sync:
            return
        res = self.sync.run_once(blocking=cekat)
        if res.get("skipped"):
            return
        self._last_sync = time.time()
        if res.get("imported"):
            self._stats_dirty = True
            self.publish_stats(force=True)
            self.export_csv()           # CSV záloha i s tisky z druhé lokality (M8e)

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
                self.filament.resolve(row, final=final, allow_live=self._ziva_otevrena())
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
        if tray_key != self._last_tray_key:
            self._last_tray_key = tray_key
            threading.Thread(target=self.publish_slots, daemon=True).start()

    def _publish_jinde(self):
        """Režim jen synchronizace bez otevřené session (M10c): živé entity přepnout na „jinde“.

        Dřív v nich zůstávaly hodnoty z doby, kdy tu tiskárna stála naposledy (PH 23. 9.), a nástěnka
        během tisku v druhé lokalitě ukazovala cizí kontrolu filamentu a „? g · hmotnost neznámá“.
        current_session zůstává 'idle' (automatizace a zrcadla čtou jeho stav), údaj o umístění je
        v atributech. Publikuje se jen při změně: klíč _last_current se nuluje při každém přechodu
        live ↔ jen synchronizace, takže po návratu sběru se živé hodnoty publikují znovu. S otevřenou
        session se nepublikuje nic – „idle“ by zakrylo uvízlou session, kterou ohlásí dohled podle
        open_session v collector_status. Pod self._lock, ať se nepotká se zapnutím sběru."""
        with self._lock:
            if self.live or self.sm.session is not None:
                return
            umisteni = locator.popis(self._adresa, self._lokalne)
            key = ("jinde", umisteni)
            if key == self._last_current:
                return
            self._last_current = key
            self.pub.publish_value("current_session", "idle", {"mode": "sync_only", "umisteni": umisteni})
            self.pub.publish_value("filament_check", "unknown", {"session_id": None, "name": None, "reason": JINDE_DUVOD,
                                                                 "slots": []})
            # 'None' → v HA unknown; Python None by šel jako prázdný payload a číselný senzor by ho ignoroval
            self.pub.publish_value("current_filament_g", "None", {"source": "jinde", "is_estimate": True, "plan_g": None})
            self.pub.publish_value("current_cost", "None", {"source": "jinde", "is_estimate": True})

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
        if row.get("manual_plan_g") is not None:
            plan = row["manual_plan_g"]         # ruční plán platí i jako 0 g (set_plan:<id>:0)
        else:
            plan = row.get("plan_weight_g") or row.get("cloud_weight_g") or None
        src = ("manual" if row.get("manual_plan_g") is not None else "3mf" if row.get("threemf_status") == "ok"
               else "cloud_ha" if row.get("cloud_weight_g") else "none")
        fils = self.db.filaments(session.id)
        pct = max(0, min(100, session.last_percent)) / 100
        ma_plan = plan is not None
        attrs = {"source": src, "is_estimate": True, "plan_g": round(plan, 1) if ma_plan else None, "percent": session.last_percent,
                 "materials": [{"material": f["material"], "color": f["color_hex"], "slot": f["tray_global"],
                                "g_so_far": round((f["used_g"] or 0) * pct, 1), "g_plan": f["used_g"]} for f in fils]}
        self.pub.publish_value("current_filament_g", round(plan * pct, 1) if ma_plan else None, attrs)
        cost = sum((f["used_g"] or 0) * pct / 1000 * (f.get("spool_price_per_kg") or aggregates.price_of(self.settings.prices, f["material_group"])) for f in fils)
        if not fils and plan:
            cost = plan * pct / 1000 * aggregates.price_of(self.settings.prices, None)
        self.pub.publish_value("current_cost", round(cost, 1) if ma_plan else None,
                               {"is_estimate": True, "plan_cost": round(sum((f["used_g"] or 0) / 1000 * aggregates.price_of(self.settings.prices, f["material_group"]) for f in fils), 1) if fils else None})

    def publish_slots(self):
        """Co je v AMS slotech: cívka ze Spoolmanu (název, barva, zbývá, cena), fallback = údaje z tiskárny.

        Údaje tiskárny (snímek) jen na instanci, která ji sbírá, a jen čerstvé (M10c): zastaralý snímek
        proti přeřazení ve Spoolmanu hlásil falešnou neshodu a push „jiná cívka“. Bez snímku se sloty berou
        ze Spoolmanu (active_tray) a lokálního deníku a neshoda se nehlásí. V režimu jen synchronizace se
        publikuje, jen když Spoolman odpověděl – jinak by všechny sloty ukázaly „prázdný“; zůstanou poslední
        hodnoty."""
        with self._lock:
            snap = self._last_snapshot if self.live else None
        if snap is not None and time.time() - snap.ts >= SNIMEK_SLOTU_S:
            snap = None
        trays = {t.tray_global: t for t in (snap.trays if snap else [])}
        spools = self.spoolman.spools() if self.spoolman else []
        if not self.live and not (self.spoolman and self.spoolman.reachable):
            LOG.debug("sloty: Spoolman neodpověděl – v režimu jen synchronizace nechávám poslední hodnoty")
            return
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
            journal = self.db.slot_spool_at(self.serial, tg, time.time())
            if sp is None and journal and (journal.get("spool_id") or journal.get("label")):
                state = (journal.get("label") or f"cívka #{journal['spool_id']}")[:255]
                if not journal.get("spool_id"):
                    state = f"{state} (mimo Spoolman)"[:255]
                self.pub.publish_value(f"slot_{slot}", state, {"source": "lokální záznam", "spool_id": journal["spool_id"],
                                                               "color": tray.color if tray else None, "material": tray.tray_type if tray else None,
                                                               "active": bool(snap and snap.tray_now == tg), "pending_push": journal.get("pushed_ts") is None})
                continue
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
        # Cizí tisky (přišly gitem z druhé lokality) neodečítáme – do Spoolmanu zapisuje vždy jen ta
        # instance, u které tiskárna stála. Jinak by je odečetly obě a filament by zmizel dvakrát.
        # Nevyřízené = něco zbývá odečíst (a řádek má slot, jinak se to odečíst nikdy nedá),
        # čeká vratka cívce, ze které tisk nakonec nešel, nebo se nepovedla vratka cívce, která
        # v tisku zůstala (odečteno víc, než je spotřeba – plán revidovaný dolů, set_plan, FIL-11).
        # Řádky bez slotu se dřív přepočítávaly každých pět minut navždy a zahlcovaly log hláškou
        # „doúčtováno 5 tisků".
        return self.db.query("""SELECT s.id, s.subtask_name, s.started_ts, s.ended_ts, s.filament_g,
                                       SUM(CASE WHEN COALESCE(f.used_g, 0) > COALESCE(f.spool_deducted_g, 0)
                                                THEN COALESCE(f.used_g, 0) - COALESCE(f.spool_deducted_g, 0) ELSE 0 END) AS pending_g
                                FROM sessions s JOIN session_filaments f ON f.session_id = s.id
                                WHERE s.printer_serial = ? AND s.ended_ts IS NOT NULL AND s.ended_ts >= ?
                                  AND ((COALESCE(f.used_g, 0) - COALESCE(f.spool_deducted_g, 0) > 0.5 AND f.tray_global IS NOT NULL)
                                       OR (f.mapping_source = 'vratka' AND COALESCE(f.spool_deducted_g, 0) > 0.05)
                                       OR (f.spool_id IS NOT NULL
                                           AND COALESCE(f.spool_deducted_g, 0) - COALESCE(f.used_g, 0) > 0.5))
                                  AND s.manual_override IS NOT 1
                                  AND (s.origin IS NULL OR s.origin = ?)
                                GROUP BY s.id ORDER BY s.ended_ts""",
                             (self.serial, since, self.settings.sync_instance or ""))

    def bez_hmotnosti(self, days: int = 120) -> list[dict]:
        """Vlastní dokončené tisky, u kterých není známá hmotnost (zdroj 'none'), a proto se nic neodečetlo (M10d).

        Fronta doúčtování je nezná – není co doúčtovat. Vznikají, když 3MF nesedí nebo chybí a cloud hodnota
        se odmítne jako zastaralá (M2c, M3b), nebo u LAN tisku bez 3MF. Bez tohohle seznamu by to byl tichý
        pododečet. Tisky do 10 min, void a nevlastní se neukazují; zmizí po set_plan nebo settled."""
        since = int(time.time()) - days * 86400
        return self.db.query(f"""SELECT id, subtask_name, ended_ts, duration_s FROM sessions
                                 WHERE printer_serial = ? AND ended_ts IS NOT NULL AND ended_ts >= ?
                                   AND COALESCE(filament_source, 'none') = 'none'
                                   AND result IN ('success', 'failed', 'cancelled') AND COALESCE(duration_s, 0) > 600
                                   AND manual_override IS NOT 1 AND (origin IS NULL OR origin = ?) AND NOT {VOID_SQL}
                                 ORDER BY ended_ts""", (self.serial, since, self.settings.sync_instance or ""))

    def _duvod_fronty(self, r: dict) -> str:
        """Proč tisk čeká ve frontě doúčtování – atribut reason u položky spoolman_pending (M10d).

        Bez důvodu nástěnka ukazovala „Probíhá doúčtování…“ i u tisku, který se sám nedoúčtuje nikdy
        (slot s cívkou, kterou Spoolman nezná)."""
        if self.spoolman.reachable is False:
            return f"Spoolman nedostupný ({self.spoolman.last_error or 'bez spojení'}) – odečte se po obnovení spojení"
        # snímek: do chyby souběžně zapisuje průběžný odečet nebo _finalize (jiné vlákno) a iterace živého
        # slovníku by skončila RuntimeError – kolo plánovače by pak přeskočilo sloty i stav
        smazane = [d for (sid, _), d in list(self.filament.chyby.items()) if sid == r["id"]]
        if smazane:
            return smazane[-1]
        vratka = None
        for f in self.db.filaments(r["id"]):
            tg, zbyva = f.get("tray_global"), (f.get("used_g") or 0) - (f.get("spool_deducted_g") or 0)
            if zbyva > 0.5 and tg is not None:
                slot = f"slot {tg % 4 + 1}" if tg < 254 else "externí cívka"
                zaznam = self.db.slot_spool_at(self.serial, tg, r.get("started_ts") or 0)
                if zaznam and not zaznam.get("spool_id"):
                    return (f"{slot} ({zaznam.get('label') or 'bez popisu'}): cívka bez ID ve Spoolmanu – "
                            f"set_slot nebo settled")
                if not f.get("spool_id"):
                    return f"{slot}: ve Spoolmanu k němu není přiřazená cívka – set_slot nebo settled"
            elif f.get("spool_id") and zbyva < -0.05 and vratka is None:
                vratka = f"čeká vratka {-zbyva:.1f} g cívce #{f['spool_id']}"
        return vratka or "čeká na doúčtování"

    def flush_pending_spools(self, _depth: int = 0):
        """Doúčtuje do Spoolmanu spotřebu tisků, které proběhly, když byl nedostupný (jiná lokalita, výpadek VPN)."""
        if not (self.spoolman and self.spoolman.enabled):
            return
        rows = self.pending_spool_sessions()
        bez = self.bez_hmotnosti()
        cas = lambda ts: dt.datetime.fromtimestamp(ts, self.tz).strftime("%Y-%m-%dT%H:%M")   # noqa: E731
        self.pub.publish_value("spoolman_pending", len(rows),
                               {"grams": round(sum(r["pending_g"] or 0 for r in rows), 1),
                                "spoolman": self.settings.spoolman_url, "reachable": self.spoolman.reachable,
                                "error": self.spoolman.last_error,
                                "prints": [{"name": (r["subtask_name"] or "?")[:40], "ended": cas(r["ended_ts"]),
                                            "g": round(r["pending_g"], 1), "reason": self._duvod_fronty(r)}
                                           for r in rows[-15:]],
                                # zápisy, které nemají kam jít (smazaná cívka) – vzaly se jako vyřízené
                                "chyby": list(self.filament.chyby.values())[-15:],
                                # dokončené tisky bez známé hmotnosti – fronta je nezpracovává, čekají na set_plan
                                "bez_hmotnosti": [{"id": b["id"][-8:], "name": (b["subtask_name"] or "?")[:40],
                                                   "ended": cas(b["ended_ts"]), "min": round((b["duration_s"] or 0) / 60),
                                                   "reason": "bez hmotnosti – set_plan"} for b in bez[-15:]],
                                "bez_hmotnosti_pocet": len(bez)})
        self.push_slot_journal()
        if not rows or self.spoolman.reachable is False:
            return
        done = 0
        for r in rows:
            with self._resolve_lock:
                # Řádek číst až pod zámkem: set_plan nebo settled z HA mohl session mezitím změnit (fronta
                # čeká na zámek třeba za dřívější session) a přepočet ze zastaralého řádku by ruční plán
                # přebil automatickým zdrojem, nebo doúčtoval spotřebu, kterou settled právě zmrazil.
                row = self.db.get_session(r["id"])
                if not row or row.get("manual_override"):
                    continue
                try:
                    self.filament.resolve(row, final=True, allow_live=False)
                    done += 1
                except Exception:
                    LOG.exception("doúčtování session %s selhalo", r["id"][-8:])
            if self.spoolman.reachable is False:
                break
        if done:
            zbyva = len(self.pending_spool_sessions())
            if zbyva < len(rows):     # hlásit jen skutečný posun, ne každé marné kolo
                LOG.info("doúčtováno %d tisků do Spoolmanu (zbývá %d)", len(rows) - zbyva, zbyva)
            self._stats_dirty = True
            self.publish_stats(force=True)
            # Znovu jen když fronta opravdu ubyla. resolve() se u cívky bez ID ve Spoolmanu
            # schválně neodečte, ale projde bez chyby – bez téhle pojistky se flush zacyklil
            # a odečítal tytéž tisky pořád dokola (20. 9. 2026 vyžralo cívky 13 a 17).
            if _depth < 5 and len(self.pending_spool_sessions()) < len(rows):
                self.flush_pending_spools(_depth + 1)

    def _sync_cerstvy(self) -> bool:
        """Poslední úspěšný sync je mladší než 2 intervaly – deník slotů zná i záznamy druhé lokality."""
        s = self.sync
        return bool(s and s.last_ok and time.time() - s.last_ok < 2 * self.settings.sync_interval_min * 60)

    def push_slot_journal(self, rec_id: int | None = None):
        """Přiřazení slotů z lokálního deníku propíše do Spoolmanu (extra.active_tray), jakmile je dostupný.

        Propisuje se jen aktuální záznam slotu – ten, který platí teď (M9a). Starší nepropsané záznamy
        (přišly gitem opožděně, přebil je novější nebo jsou uzavřené) se jen označí: Spoolman by jinak
        ukazoval a SpoolmanSync převzal cívku, která ve slotu už není. Záznam s from_ts v budoucnosti čeká.

        Bez rec_id (fronta doúčtování z plánovače) jen instance, která tiskárnu sbírá, a se zapnutým
        syncem jen s čerstvým syncem. Neživá instance by propisovala zastaralou kopii deníku – závod
        z 27. 9. (export Haciendy předběhl propsání) by spolu s uvolněním držitele trvale přepsal
        správnou cívku. S rec_id (příkaz set_slot) se propisuje jen ten jeden záznam, na kterékoli
        instanci: kdo cívku vyměnil, zapíše ji hned. Když to nevyjde (nebo se uvolnění držitele odložilo),
        propíše ho po syncu živá instance.
        """
        if not (self.spoolman and self.spoolman.enabled) or self.spoolman.reachable is False:
            return
        if rec_id is None and (not self.live or (self.sync is not None and not self._sync_cerstvy())):
            return
        ted = int(time.time())
        pending = self.db.slot_spools(self.serial, only_unpushed=True)
        if rec_id is not None:
            pending = [r for r in pending if r["id"] == rec_id]
        spools: list[dict] | None = None
        for tg in sorted({r["tray_global"] for r in pending}):
            slot = (tg % 4) + 1
            current = self.db.slot_spool_at(self.serial, tg, ted)
            for r in pending:
                if r["tray_global"] != tg or r["from_ts"] > ted or (current and r["id"] == current["id"]):
                    continue
                self.db.mark_slot_pushed(r["id"])
                LOG.info("slot %d: starší záznam deníku (cívka %s od %s) jen označen jako propsaný – platí novější",
                         slot, r.get("spool_id") or "—", dt.datetime.fromtimestamp(r["from_ts"], self.tz).strftime("%d.%m. %H:%M"))
            if not current or current.get("pushed_ts") is not None or not any(r["id"] == current["id"] for r in pending):
                continue
            if spools is None:
                spools = self.spoolman.spools(include_archived=True)
                if self.spoolman.reachable is False:
                    return
            try:
                hotovo = self._propis_slot(current, spools)
            except Exception as e:
                LOG.warning("propsání slotu %d do Spoolmanu selhalo: %s", slot, str(e)[:100])
                return
            # Po zápisu je výpis zastaralý: cívka přesunutá do slotu s nižším číslem by ve výpisu pořád
            # držela klíč svého starého slotu a propsání toho slotu by ji uvolnilo (review P6). Další slot
            # si výpis načte znovu.
            spools = None
            if hotovo:
                self.db.mark_slot_pushed(current["id"])

    def _klic_slotu(self, slot: int, spools: list[dict]) -> str | None:
        """Hodnota extra.active_tray pro slot, jak ji zapisuje SpoolmanSync ('"P2S_<SN>_AMS_<SN AMS>_tray_<n>"').

        Klíč dosavadního držitele slotu (celý, i s SN AMS), jinak složený ze SN AMS jiného slotu téže
        tiskárny. Nikdy 'AMS_*' – takový slot SpoolmanSync nepozná (0.18.2 ho psal, když držitel nebyl)."""
        vzor = re.compile(rf'^"(\w+?)_{re.escape(self.serial)}_AMS_(\w+)_tray_(\d+)"$')
        nalezy = [m for m in (vzor.match((s.get("extra") or {}).get("active_tray") or "") for s in spools) if m]
        for m in nalezy:
            if m.group(3) == str(slot):
                return m.group(0)
        if nalezy:
            m = nalezy[0]
            return f'"{m.group(1)}_{self.serial}_AMS_{m.group(2)}_tray_{slot}"'
        return None

    def _chranene_civky(self, tg: int) -> tuple[set[int], bool]:
        """Cívky, na které uvolnění nesmí sáhnout: cívky z řádků otevřené session a cívka slotu, ze kterého
        otevřená session tiskne. Vrátí (id cívek, tiskne_z_tohoto_slotu) – tiskne-li se právě z tohoto
        slotu, je jeho dosavadní držitel právě tištěná cívka."""
        with self._lock:
            sess = self.sm.session
            snap = self._last_snapshot
        if sess is None:
            return set(), False
        ids = {f["spool_id"] for f in self.db.filaments(sess.id) if f.get("spool_id")}
        tray_now = snap.tray_now if snap is not None and snap.tray_now is not None and 0 <= snap.tray_now < 254 else None
        if tray_now is None:
            tray_now = sess.tray_now_start
        if tray_now is not None:
            zaznam = self.db.slot_spool_at(self.serial, tray_now, sess.started_ts or 0)
            if zaznam and zaznam.get("spool_id"):
                ids.add(zaznam["spool_id"])
        return ids, tray_now == tg

    def _tisk_nebezi(self) -> bool:
        """Na instanci, která tiskárnu nesbírá: podle cloudu (ha-bambulab *_print_status) tiskárna netiskne.
        O otevřené session druhé lokality tahle instance neví, stráž otevřené session tu nic nechrání."""
        ent = (self.printer.ha_weight_entity or "").replace("_print_weight", "_print_status")
        if not ent or ent == self.printer.ha_weight_entity:
            return False
        st = self.ha.state(ent) or {}
        return str(st.get("state") or "").lower() in TISK_NEBEZI

    def _propis_slot(self, rec: dict, spools: list[dict]) -> bool:
        """Aktuální záznam deníku do Spoolmanu (M9b): nová cívka dostane klíč slotu a dosavadní držitel se
        uvolní (active_tray '""'), jinak slot drží dvě cívky a SpoolmanSync, spool_for_tray i publish_slots
        můžou vzít tu starou. Jen v okamžiku propsání nového záznamu – přiřazení se mění i ve SpoolmanSync,
        periodické srovnávání by ho přepisovalo. Klíč se určí PŘED uvolněním (bez držitele by nešel zjistit).
        Uvolňuje se jen přesná shoda celého klíče (SN tiskárny i AMS) a nikdy cívka otevřené session.
        Záznam 'vyjmuto' (bez cívky) jen uvolní držitele.

        Vrátí False, když se uvolnění odložilo (instance bez tiskárny, tiskárna možná tiskne): záznam pak
        zůstane nepropsaný a dokončí ho po importu živá instance se strážemi otevřené session – nová cívka
        už klíč má, takže jen uvolní držitele. Jinak True (záznam je vyřízený, i když se nepropsal)."""
        tg = rec["tray_global"]
        slot = (tg % 4) + 1
        spool_id = rec.get("spool_id")
        klic = self._klic_slotu(slot, spools)
        if spool_id:
            nova = next((x for x in spools if x["id"] == spool_id), None)
            if nova is None:
                LOG.warning("slot %d: cívka #%s ve Spoolmanu není – přiřazení se nepropíše (zkontroluj ID v set_slot)",
                            slot, spool_id)
                return True
            if klic is None:
                LOG.warning("slot %d: klíč slotu pro Spoolman neznám (žádná cívka této tiskárny nemá active_tray) – "
                            "přiřaď cívku #%s ve SpoolmanSync", slot, spool_id)
                return True
            if (nova.get("extra") or {}).get("active_tray") != klic:
                self.spoolman._req(f"/spool/{spool_id}", "PATCH", {"extra": {"active_tray": klic}})
        LOG.info("Spoolman: slot %d = cívka %s (z lokálního deníku)", slot, spool_id or "—")
        if klic is None:
            return True
        # i klíč s hvězdičkou, který pro tenhle slot zapsala 0.18.2, když držitel nebyl
        klice = {klic, f'"P2S_{self.serial}_AMS_*_tray_{slot}"'}
        drzitele = [x for x in spools if x["id"] != spool_id and (x.get("extra") or {}).get("active_tray") in klice]
        if not drzitele:
            return True
        if not self.live and not self._tisk_nebezi():
            LOG.warning("slot %d: uvolnění předchozí cívky %s odloženo – tiskárna možná tiskne na druhé lokalitě, "
                        "dokončí ho po syncu instance, která ji sbírá",
                        slot, ", ".join(f"#{x['id']}" for x in drzitele))
            return False
        chranene, tiskne_ze_slotu = self._chranene_civky(tg)
        for x in drzitele:
            if tiskne_ze_slotu or x["id"] in chranene:
                LOG.info("slot %d: cívka #%s se neuvolní – používá ji běžící tisk", slot, x["id"])
                continue
            self.spoolman._req(f"/spool/{x['id']}", "PATCH", {"extra": {"active_tray": '""'}})
            LOG.info("Spoolman: cívka #%s uvolněna ze slotu %d", x["id"], slot)
        return True

    def spoolman_baseline(self) -> dict:
        """Spotřeba a útrata, kterou add-on nezaznamenal (tisky před jeho zavedením, jiné tiskárny, ruční odvin).

        Bere se ze Spoolmanu po cívkách (M10b): co z cívky ubylo – nejvýš její počáteční hmotnost, přečerpaná
        cívka (used 1279 g z 1000 g) je chyba evidence, ne spotřeba – mínus to, co z ní odečetly UZAVŘENÉ
        tiskové session. Průběžné odečty běžícího tisku zná jen sbírající instance, druhá lokalita tu session
        nemá, a „mimo evidenci“ by se mezi lokalitami během každého tisku rozcházelo – do jeho konce jsou proto
        na obou v „mimo evidenci“. Void session se z ledgeru nevynechávají: co odečetly, ve Spoolmanu chybí.

        Sčítá se podle materiálu CÍVKY (přesun spotřeby mezi cívkami téhož materiálu se vyruší) a odečte se
        spotřeba evidovaných tisků, jejichž řádek cívku nemá (slot bez přiřazení) – ta z cívek ubyla taky,
        jen ji ledger nezná. Jen z uzavřených tisků, které nejsou void: fantomy ve Spoolmanu nikdy nebyly.
        Výsledek po materiálech nejméně 0. Cena = gramy × průměrná cena cívek materiálu vážená jejich kladnými
        rozdíly (cívka bez ceny a materiál bez takové cívky podle cen z options). Atribut spools je jen
        informativní (kladné rozdíly po cívkách). Počítá se za běhu, takže se samo srovná, když se cívka
        doplní nebo opraví.
        """
        if not (self.spoolman and self.spoolman.enabled):
            return {}
        tracked: dict[int, float] = {}
        for r in self.db.query("""SELECT f.spool_id, SUM(f.spool_deducted_g) g FROM session_filaments f
                                  JOIN sessions s ON s.id = f.session_id
                                  WHERE f.spool_id IS NOT NULL AND s.ended_ts IS NOT NULL GROUP BY f.spool_id"""):
            tracked[r["spool_id"]] = r["g"] or 0
        bez_civky: dict[str, float] = {}
        for r in self.db.query(f"""SELECT f.material_group, SUM(f.used_g) g FROM session_filaments f
                                   JOIN sessions s ON s.id = f.session_id
                                   WHERE f.spool_id IS NULL AND s.ended_ts IS NOT NULL AND NOT {VOID_SQL}
                                   GROUP BY f.material_group"""):
            g = material_group(r["material_group"])
            bez_civky[g] = bez_civky.get(g, 0.0) + (r["g"] or 0)
        po_mat: dict[str, dict] = {}
        for sp in self.spoolman.spools(include_archived=True):
            fil = sp.get("filament") or {}
            used = self.spoolman.used_g(sp)
            strop = sp.get("initial_weight") or fil.get("weight") or used
            rozdil = min(used, float(strop)) - tracked.get(sp["id"], 0)
            g = material_group(fil.get("material"))
            b = po_mat.setdefault(g, {"g": 0.0, "vaha": 0.0, "kc": 0.0, "spools": []})
            b["g"] += rozdil
            if rozdil <= 0.5:
                continue
            per_kg = self.spoolman.price_per_kg(sp)
            b["vaha"] += rozdil
            b["kc"] += rozdil * (per_kg or aggregates.price_of(self.settings.prices, g))
            b["spools"].append({"id": sp["id"], "name": f"{((fil.get('vendor') or {}).get('name') or '')} {fil.get('name') or ''}".strip(),
                                "g": round(rozdil), "kc": round(rozdil / 1000 * (per_kg or 0))})
        out: dict[str, dict] = {}
        for g, b in po_mat.items():
            net = max(0.0, b["g"] - bez_civky.get(g, 0.0))
            if net <= 0.5 and not b["spools"]:
                continue
            per_kg = b["kc"] / b["vaha"] if b["vaha"] > 0 else aggregates.price_of(self.settings.prices, g)
            out[g] = {"g": round(net, 1), "cost": round(net / 1000 * per_kg, 1), "spools": b["spools"]}
        return out

    def _publish_status(self):
        self.pub.publish_value("collector_status", self._stav_entity(), self.health())

    def publish_stats(self, force=False):
        try:
            with self._lock:
                open_sess = self.sm.session.to_row() if self.sm.session else None
            baseline = self.spoolman_baseline()
            # náhledy jen ty, které tu opravdu leží (M10d); bez adresáře náhledů se nedá ověřit nic
            covers = self.settings.covers_dir if self.settings.covers_dir.is_dir() else None
            stats = aggregates.compute(self.db, self.serial, time.time(), self.tz, open_sess, self.settings.prices, baseline,
                                       covers_dir=covers)
            if stats.get("last_print_attrs"):
                # Tlačítka Zmetek / V pořádku / Načíst 3MF míří na poslední tisk a cizí tisk příkaz odmítne
                # (M4a). Nástěnka je podle tohohle ukáže jen u tisku této instance (M12d).
                stats["last_print_attrs"]["vlastni"] = je_vlastni(stats["last_print_attrs"], self.settings.sync_instance)
            stats.update(aggregates.maintenance(self.db, self.serial, time.time(), open_sess,
                                                self.settings.maintenance_every_hours, self.settings.desiccant_every_days))
            with self._lock:
                snap = self._last_snapshot
                jinde = not self.live and self.sm.session is None
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
            elif jinde:
                # Tiskárnu sleduje druhá lokalita (M10c): živé hodnoty tady neplatí. Řetězec 'None' HA převede
                # na unknown – prázdný payload (Python None) by číselný senzor ignoroval a zůstala by stará hodnota.
                stats["ams_humidity_history"] = stats["nozzle_wear"] = "None"
            self.pub.publish_stats(stats, force=force)
            self._last_stats = time.time()
            self._stats_dirty = False
        except Exception:
            LOG.exception("výpočet statistik selhal")

    def health(self) -> dict:
        """Zdraví instance: `ok` pro HTTP /health a atributy entity collector_status (M7b, M7c).

        ok (watchdog Supervisoru a Docker HEALTHCHECK) = jen heartbeat plánovače: restart pomůže jen
        zaseknutému procesu. Mrtvé MQTT ani stojící sync restart neopraví – dřív 503 kvůli nim restartovalo
        add-on klidně uprostřed tisku a instance pak uvízla v sync_only. Ty ukazuje stav entity
        (_stav_entity) a MQTT se opravuje samo (_ozdrav_mqtt). Klíče jsou v obou režimech stejné, aby
        dohledy („instance uvízla“) i stráž nasazení viděly otevřenou session i v sync_only.
        Nebere self._lock (volá ho HTTP vlákno i callback paho) a nevyhodí výjimku.
        """
        hb_age = time.monotonic() - self._hb
        ok = hb_age < HEARTBEAT_MAX_S
        try:
            m = self.mqtt                       # snímek – přepnutí ho může mezitím nastavit na None
            sess = self.sm.session
            vek = time.monotonic() - self._posledni_zprava if self.live and self._posledni_zprava else None
            sync = self.sync
            return {"ok": ok, "version": __import__("bambu_stats").__version__, "build": self._build,
                    "schema_version": self._schema_version, "printer": self.printer.name,
                    "mode": "live" if self.live else "sync_only", "umisteni": locator.popis(self._adresa, self._lokalne),
                    "probe_fails": self._neuspechy, "heartbeat_age_s": round(hb_age),
                    "printer_mqtt_connected": bool(m is not None and m.connected),
                    "last_report_age_s": round(vek) if vek is not None else None,
                    "messages": m.msg_count if m is not None else None,
                    "ha_mqtt_connected": self.pub.connected, "ha_api": self.ha.available, "db_size_mb": self.db.size_mb(),
                    "open_session": sess.id if sess else None, "open_session_name": sess.subtask_name if sess else None,
                    "spoolman": (None if not self.spoolman else {"url": self.settings.spoolman_url, "reachable": self.spoolman.reachable,
                                                                "error": self.spoolman.last_error}),
                    "sync": ({"instance": sync.instance, "last_ok": int(sync.last_ok) if sync.last_ok else None,
                              "error": sync.last_error, "imported_total": sync.imported_total,
                              "import_errors": getattr(sync, "import_errors", 0)} if sync else None),
                    "last_command": self.last_command}
        except Exception:
            LOG.exception("zjištění stavu collectoru selhalo")
            return {"ok": ok, "heartbeat_age_s": round(hb_age)}

    def _stav_entity(self) -> str:
        """Stav entity collector_status podle provozu (M7b), ne podle watchdogu: 'degraded', když živá
        instance nemá od tiskárny zprávu > 5 min (ani do 5 min od začátku sběru), když instance jen
        se synchronizací nemá úspěšný sync > 60 min (po startu se na první čeká 30 min), nebo když
        stojí plánovač. Jinak 'ok'. Na 'degraded' čeká dohled „collector tiskárny neběží“."""
        try:
            ted = time.monotonic()
            if ted - self._hb >= HEARTBEAT_MAX_S:
                return "degraded"
            if self.live:
                return "degraded" if self._ticho_s() > ZIVA_ZPRAVA_S else "ok"
            sync = self.sync
            if sync:
                if sync.last_ok:
                    return "degraded" if time.time() - sync.last_ok > SYNC_STOJI_S else "ok"
                return "degraded" if ted - self._start_mono > SYNC_GRACE_S else "ok"
            return "ok"
        except Exception:
            LOG.exception("zjištění stavu collectoru selhalo")
            return "degraded"

    # --- plánovač -------------------------------------------------------------------------------------
    def _scheduler(self):
        time.sleep(15)
        if self._zastaveno:
            return
        self.publish_stats(force=True)
        while not self._zastaveno:              # po stop() už další kolo nezačne
            self._kolo_planovace()
            time.sleep(30)

    def _kolo_planovace(self):
        """Jedno kolo plánovače (běží každých 30 s). Samostatně kvůli testům, které ho volají po krocích.

        Heartbeat (/health) se obnoví na začátku i na konci kola; nic v kole nesmí čekat na síť
        bez časového limitu – sync proto běží ve vlastním vlákně."""
        self._hb = time.monotonic()
        try:
            now = time.time()
            with self._lock:
                sess = self.sm.session
            if sess and self._ziva_otevrena():
                # Průběžný odečet, pokusy o plán a náhled jen na instanci, která tiskárnu sbírá a má od ní
                # čerstvou zprávu (M7a bod 8). Jinak session jen drží: osiřelá session (převoz, mrtvé MQTT)
                # by brala živou cloud entitu a náhled tisku, který mezitím běží kdekoli jinde.
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
            if now - getattr(self, "_last_locate", 0) >= LOCATE_EVERY_S:
                self._last_locate = now
                self._prehodnot_umisteni()
            self._ozdrav_mqtt()
            if self.sync and now - self._last_sync >= self.settings.sync_interval_min * 60:
                self._spust_sync()
            if self._stats_dirty or now - self._last_stats >= STATS_EVERY_S:
                self.publish_stats()
            # přiřazení cívek se mění ve Spoolmanu (mimo tiskárnu) → kontrolovat pravidelně, publikuje se jen změna
            if self.spoolman and now - getattr(self, "_last_flush", 0) >= 300:
                self._last_flush = now
                self.flush_pending_spools()
            # sloty i v režimu jen synchronizace – Spoolman je sdílený a jinak by tam zůstaly cívky z doby,
            # kdy tu tiskárna stála naposledy (M10c)
            if now - getattr(self, "_last_slots", 0) >= 60:
                self._last_slots = now
                self.publish_slots()
            self._publish_jinde()
            self._publish_status()
            today = dt.datetime.fromtimestamp(now, self.tz).date()
            if self._last_daily != today and dt.datetime.fromtimestamp(now, self.tz).hour >= 3:
                self._last_daily = today
                n = self.db.purge_samples(now - self.settings.samples_retention_days * 86400)
                self.db.checkpoint()
                LOG.info("údržba: smazáno %d vzorků, WAL checkpoint, DB %.1f MB", n, self.db.size_mb())
                self.export_csv()
                self._stats_dirty = True
            m = self.mqtt
            if m and m.connected:
                # jen „naposledy viděna“ – adresu zapisuje zapnutí sběru a nové spojení (printer.host bývá prázdný)
                self.db.touch_printer(self.serial)
        except Exception:
            LOG.exception("chyba plánovače")
        finally:
            self._hb = time.monotonic()
