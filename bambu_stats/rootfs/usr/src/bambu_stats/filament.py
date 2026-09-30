"""Určení spotřeby filamentu pro session – kombinace zdrojů s prioritou.

1. `3mf`        slice_info.config z 3MF na USB disku tiskárny (přes FTPS) – per filament used_g/used_m;
                jen přesné jméno a tištěná deska, u cloud tisku ho vyvrátí čerstvá cloud hodnota
2. `cloud_ha`   entity ha-bambulab (print_weight/print_length) – hodnota z Bambu cloudu; živá jen
                čerstvá a nepřevzatá z předchozího tisku, nikdy u kalibrace a tisku přes LAN
3. `ams_remain` rozdíl `remain` % × hmotnost cívky – jen Bambu RFID cívky (rozlišení 1 % ≈ 10 g)
4. `none`

U nedokončených tisků se plán (1/2) násobí postupem (`mc_percent/100`) a označí jako odhad;
pokud je k dispozici měření `ams_remain` s Δ ≥ 2 %, má přednost (je to skutečná změna).
Ruční plán (`manual_plan_g`, příkaz set_plan) má přednost přede všemi zdroji a násobí se postupem
stejně. Ruční hodnota (`manual_override`, příkaz settled) se nikdy nepřepisuje.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import time
from pathlib import Path

from . import threemf
from .spoolman import GONE
from .util import je_systemova, je_vlastni, material_group

LOG = logging.getLogger("filament")
EXTERNAL_SLOT_IDS = {254, 255}
LIVE_CAP = 0.9          # během tisku odečítat nejvýš 90 % dosud spotřebovaného odhadu
MIN_STEP_G = 5.0        # menší přírůstky neposílat (šetří zápisy do Spoolmanu)
MIN_PRINT_FOR_LAYERS_S = 60   # odhad postupu z vrstev až po minutě v RUNNING (dřív je vrstva z minulého tisku)
CLOUD_STARA_S = 120     # živá cloud hmotnost změněná víc než 2 min před začátkem tisku patří dřívějšímu tisku
KONTROLA_3MF_OD_S = 10          # 3MF se s cloudem porovnává, jen když se cloud změnil nejdřív 10 s před začátkem…
KONTROLA_3MF_USAZENA_S = 30     # …a hodnota je stará aspoň 30 s (ha-bambulab ji po startu ještě přepisuje z FTP)
KONTROLA_3MF_ODCHYLKA = (1.0, 0.05)   # 3MF nesedí, když se od cloudu liší o víc než max(1 g; 5 %)
MAX_CHYB_SPOOLMANU = 50         # kolik posledních nevyřiditelných zápisů (smazaná cívka) držet pro spoolman_pending


def _barva(c) -> str:
    """Barvu porovnávat bez `#` a bez průhlednosti: slicer píše `#161616FF`, jindy `#161616`,
    tiskárna `161616FF`. Jinak se vícebarevný tisk nespáruje se sloty a neodečte vůbec."""
    return (c or "").strip().lstrip("#").upper()[:6]


# Výchozí hodnoty sloupců session_filaments pro klíče, které řádek z resolveru nemá (vratka, …).
_VYCHOZI_RADKU = {"spool_deducted_g": 0, "is_estimate": 0}


def _radky_stejne(nove: list[dict], ulozene: list[dict]) -> bool:
    """Jsou nové řádky spotřeby stejné jako uložené? Porovnávají se celé řádky (všechny klíče kromě
    id a session_id) ve stejném pořadí – i samotná změna ceny nebo cívky se musí propsat."""
    if len(nove) != len(ulozene):
        return False
    for n, u in zip(nove, ulozene):
        for k in (set(n) | set(u)) - {"id", "session_id"}:
            if n.get(k, _VYCHOZI_RADKU.get(k)) != u.get(k):
                return False
    return True


def _trays(session: dict, key: str) -> list[dict]:
    v = session.get(key)
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            v = []
    return v or []


class FilamentResolver:
    def __init__(self, db, printer, ha, cache_dir: Path, spoolman=None, serial: str | None = None,
                 instance: str | None = None):
        self.db, self.printer, self.ha, self.spoolman = db, printer, ha, spoolman
        # sync_instance: cizí session (origin jiné lokality) resolve nepřepočítá – viz je_vlastni
        self.instance = instance or ""
        # Adresu, na které tiskárna opravdu je, zjistí collector (locator.py) a nastaví sem.
        # `printer.host` bývá od 0.16 prázdný – tiskárna se hledá podle `printer_hosts`.
        self.host: str | None = None
        self.serial = serial or getattr(printer, "serial", "")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._attempts: dict[str, int] = {}
        # Session, jejíž 3MF k tisku nesedí ('mismatch'). Stav v DB přepisuje upsert ze stavového
        # automatu (Session.to_row), takže si ho resolver pamatuje i sám – jinak by se soubor stahoval
        # a zamítal pořád dokola. Vynuluje ho refetch_3mf (zapomen_3mf).
        self._nesedi: set[str] = set()
        self._cloud_odmitnut: set[str] = set()   # session, u kterých už je odmítnutí cloudu v logu
        self._beze_zmeny: set[str] = set()       # session, u kterých už je „přepočet beze změny“ v logu
        # Zápisy do Spoolmanu, které nemají kam jít (cívka smazaná, 404) a považují se za vyřízené:
        # (session, cívka) → důvod. Collector je ukáže v atributech spoolman_pending ('chyby').
        self.chyby: dict[tuple[str, int], str] = {}

    def _zapis_zmeny(self, sid: str, **pole) -> bool:
        """update_session jen s poli, která se od DB opravdu liší. Každý zápis posune updated_ts
        a session jde znovu do exportu – přepočet beze změny (fronta doúčtování každých 5 min)
        by jinak commitoval do sync repa pořád dokola (M2QWSNQF)."""
        ulozena = self.db.get_session(sid) or {}
        zmeny = {k: v for k, v in pole.items() if ulozena.get(k) != v}
        if zmeny:
            self.db.update_session(sid, **zmeny)
        return bool(zmeny)

    def zapomen_3mf(self, sid: str):
        """Další resolve zkusí 3MF znovu od začátku (tlačítko Načíst 3MF)."""
        self._attempts.pop(sid, None)
        self._nesedi.discard(sid)

    def vrat_stav_3mf(self, sid: str, puvodni: dict):
        """Neúspěšný refetch uzavřené session: vrátit původní stav 3MF (status, cesta, čas stažení)
        v paměti resolveru i v DB – 3MF session bez souboru má zůstat, jak byla. Resolve s refetch=True
        neúspěšné stažení do DB nezapisuje, takže v DB se zapíše jen to, co se opravdu liší (jinak by
        posunutý updated_ts poslal session beze změny znovu do exportu)."""
        self._zapis_zmeny(sid, **puvodni)
        if puvodni.get("threemf_status") == "mismatch":
            self._nesedi.add(sid)
        else:
            self._nesedi.discard(sid)

    def cache_3mf(self, session: dict) -> Path:
        """Uložené slice_info 3MF session (klíčem je otisk) – s ním se 3MF už nestahuje."""
        return self.cache_dir / f"{session['fingerprint']}.xml"

    # --- zdroje ------------------------------------------------------------------
    def _from_3mf(self, session: dict, zapsat_neuspech: bool = True) -> threemf.PlateInfo | None:
        """zapsat_neuspech=False (refetch uzavřené session): výsledek stažení, které nevrátilo 'ok', jde
        jen do předaného slovníku, ne do DB – o návratu původního stavu rozhoduje volající."""
        sid = session["id"]
        if session.get("threemf_status") == "mismatch" or sid in self._nesedi:
            # 3MF k tisku nesedí (chybí v něm tištěná deska, nebo ho vyvrátila čerstvá cloud hodnota).
            # Je to konečný stav jako 'ok': cache ani FTP se už nezkouší.
            self._nesedi.add(sid)
            if session.get("threemf_status") != "mismatch":
                self.db.update_session(sid, threemf_status="mismatch")
                session["threemf_status"] = "mismatch"
            return None
        cache = self.cache_3mf(session)
        if cache.exists():
            plates = threemf.parse_slice_info(cache.read_bytes())
            want = threemf.plate_index_from_gcode(session.get("gcode_file") or "")
            plate = next((p for p in plates if p.index == want), plates[0] if plates else None)
            if plate and session.get("threemf_status") != "ok":
                # jen proti DB: refetch posílá status vynulovaný v předaném řádku a 'ok' v DB už bývá
                self._zapis_zmeny(session["id"], threemf_status="ok")
                session["threemf_status"] = "ok"
            return plate
        if session.get("threemf_status") == "ok":
            return None
        if self._attempts.get(session["id"], 0) >= 6 or not session.get("subtask_name"):
            return None
        self._attempts[session["id"]] = self._attempts.get(session["id"], 0) + 1
        host = self.host or self.printer.host
        if not host:
            return None          # tiskárna teď není v dosahu této instance – 3MF zkusit příště
        status, plate, path = threemf.fetch_plate(host, self.printer.access_code,
                                                  session["subtask_name"], session.get("gcode_file") or "")
        if status == "ok" or zapsat_neuspech:
            self.db.update_session(session["id"], threemf_status=status, threemf_path=path,
                                   threemf_fetched_ts=int(time.time()))
        session.update(threemf_status=status, threemf_path=path)
        if status == "mismatch":
            self._nesedi.add(sid)
        if status == "ok" and plate:
            # uložit surové XML pro pozdější přepočty (USB může být odpojen)
            try:
                ftp = threemf.PrinterFTP(host, self.printer.access_code)
                xml = ftp.read_slice_info(path)
                if xml:
                    cache.write_bytes(xml)
            except Exception as e:  # cache není kritická
                LOG.debug("cache 3mf: %s", e)
            LOG.info("3MF nalezen: %s (%d filamentů, %.1f g)", path, len(plate.filaments), plate.weight_g or 0)
        return plate

    def _zivy_cloud(self) -> tuple[float | None, float | None, int | None]:
        """Živá hmotnost a délka z ha-bambulab a čas poslední změny hmotnosti (last_changed).

        Čte se přes state(), protože k posouzení čerstvosti je potřeba last_changed (numeric ho
        zahodí). Klient bez state() (starší testovací náhrady) dá hodnotu bez času – kontroly
        čerstvosti se pak nepoužijí.
        """
        def precti(ent: str | None) -> tuple[float | None, int | None]:
            if not ent:
                return None, None
            state = getattr(self.ha, "state", None)
            if state is None:
                return self.ha.numeric(ent), None
            st = state(ent) or {}
            try:
                v = float(st.get("state"))
            except (TypeError, ValueError):
                return None, None
            zmena = None
            try:
                zmena = int(dt.datetime.fromisoformat(str(st["last_changed"]).replace("Z", "+00:00")).timestamp())
            except (KeyError, ValueError):
                pass
            return (v if v > 0 else None), zmena

        w, zmena = precti(self.printer.ha_weight_entity)
        l, _ = precti(self.printer.ha_length_entity)
        return w, l, zmena

    def _proc_odmitnout_cloud(self, session: dict, w: float, zmena: int | None) -> str | None:
        """Proč živou cloud hmotnost u session nepřijmout (None = přijmout).

        ha-bambulab drží v print_weight hmotnost posledního CLOUDOVÉHO úkolu: kalibrace ji převzala
        po předchozím tisku (29. 9. 2026 121,96 g), tisk přes LAN taky – a ha-bambulab ji u něj
        navíc zapíše znovu, takže vypadá čerstvě (19. 9. 3Dino Puzzle 27,56 g, 26. 9. Scraper Grip 7,91 g).
        U ostatních se odmítne hodnota, která se změnila dávno před začátkem tisku, nebo se na setiny
        rovná předchozímu tisku – kromě reprintu téže desky, ta má stejnou hmotnost právem.
        """
        if je_systemova(session):
            return "systémová úloha – cloud entita drží hmotnost předchozího tisku"
        if session.get("print_type") == "local":
            return "tisk přes LAN – ha-bambulab u něj drží hmotnost posledního cloudového úkolu"
        prev = self.db.predchozi_session(session)
        # Reprint jen se stejnou deskou: projekty mívají pod jedním jménem víc desek (25. 9. 2026 Cube
        # deska 5 a 7, Cube_B deska 4, 6, 7 a 8) a hmotnost jiné desky by se jinak prošla bez kontroly.
        if prev and prev.get("subtask_name") == session.get("subtask_name") \
                and (prev.get("gcode_file") or "") == (session.get("gcode_file") or ""):
            return None
        start = session.get("started_ts") or 0
        if zmena is not None and zmena < start - CLOUD_STARA_S:
            return f"hodnota se naposledy změnila {(start - zmena) // 60} min před začátkem tisku"
        if prev:
            drivejsi = [prev.get("plan_weight_g"), prev.get("filament_g")]
            if prev.get("filament_source") == "cloud_ha":
                # uložená cloud hodnota 3MF session bývá sama zastaralá – reprint by se pak odmítl neprávem
                drivejsi.append(prev.get("cloud_weight_g"))
            if any(x is not None and round(x, 2) == round(w, 2) for x in drivejsi):
                return f"rovná se předchozímu tisku {prev.get('subtask_name') or '?'} ({prev['id'][-8:]})"
        return None

    def _hlas_odmitnuti(self, session: dict, zprava: str):
        if session["id"] not in self._cloud_odmitnut:
            self._cloud_odmitnut.add(session["id"])
            LOG.warning("session %s: %s", session["id"][-8:], zprava)

    def _civka_chybi(self, session: dict, spool_id: int, grams: float):
        """Cívka ve Spoolmanu není (404): odečet nebo vratka se považuje za vyřízený, jinak by fronta
        doúčtování zkoušela totéž každých 5 min navždy. ERROR jednou za session a cívku."""
        klic = (session["id"], spool_id)
        duvod = (f"cívka #{spool_id} ve Spoolmanu není (404) – {'odečet' if grams > 0 else 'vratka'} {abs(grams):.1f} g "
                 f"u tisku {session.get('subtask_name') or '?'} ({session['id'][-8:]}) se bere jako vyřízená")
        if klic not in self.chyby:
            LOG.error("session %s: %s", session["id"][-8:], duvod)
        self.chyby.pop(klic, None)
        self.chyby[klic] = duvod
        while len(self.chyby) > MAX_CHYB_SPOOLMANU:
            self.chyby.pop(next(iter(self.chyby)))

    def _hlas_beze_zmeny(self, session: dict, zprava: str):
        """Pojistka „beze změny“: WARNING jednou za session, opakování z fronty doúčtování jen DEBUG."""
        if session["id"] in self._beze_zmeny:
            LOG.debug("session %s: %s", session["id"][-8:], zprava)
        else:
            self._beze_zmeny.add(session["id"])
            LOG.warning("session %s: %s", session["id"][-8:], zprava)

    def _hmotnost_z_cloudu(self, session: dict, allow_live: bool, zive=None) -> tuple[float | None, float | None]:
        """Plán z cloudu: živá entita ha-bambulab, když ji jde přijmout, jinak hodnota uložená u session.

        Přijatá živá hodnota se uloží (cloud_weight_g, plan_weight_g), odmítnutá nikam – ani do
        plánu, který ukazuje nástěnka. U otevřené session se přehodnocuje při každém dalším pokusu
        a přijme se, jakmile je čerstvá. `zive` = už přečtená entita (čte se nejvýš jednou za resolve).
        """
        if je_systemova(session):
            # kalibrace nemá plán vůbec – ani uložený (0.18.2 k ní uložil hmotnost předchozího tisku)
            self._hlas_odmitnuti(session, "cloud hmotnost se u systémové úlohy nepoužije")
            return None, None
        ulozena = session.get("cloud_weight_g"), session.get("cloud_length_m")
        # živou cloud hodnotu číst jen u otevřené session – po uzavření už entita patří dalšímu tisku
        if (session.get("ended_ts") and ulozena[0] is not None) or not allow_live:
            return ulozena
        if session.get("print_type") == "local":
            self._hlas_odmitnuti(session, "živá cloud hmotnost se u tisku přes LAN nepoužije "
                                          "(ha-bambulab u něj drží hmotnost posledního cloudového úkolu)")
            return ulozena
        w, l, zmena = zive if zive is not None else self._zivy_cloud()
        if w is None:
            return ulozena
        duvod = self._proc_odmitnout_cloud(session, w, zmena)
        if duvod:
            self._hlas_odmitnuti(session, f"živá cloud hmotnost {w:.2f} g odmítnuta – {duvod}")
            return ulozena
        if session["id"] in self._cloud_odmitnut and session.get("cloud_weight_g") != w:
            LOG.info("session %s: živá cloud hmotnost %.2f g přijata", session["id"][-8:], w)
        self._zapis_zmeny(session["id"], cloud_weight_g=w, cloud_length_m=l, plan_weight_g=w, plan_length_m=l)
        session.update(cloud_weight_g=w, cloud_length_m=l)
        return w, l

    def _3mf_vyvracen_cloudem(self, session: dict, plate: threemf.PlateInfo, zive) -> bool:
        """Křížová kontrola 3MF u cloud tisku: sedí plán z 3MF na čerstvou hmotnost z cloudu?

        Přesné jméno nestačí: pod obecným jménem leží starší soubor jiného projektu (27. 9. 2026
        „(Unsaved)" 36,81 g PLA z předchozího dne, cloud 99,84 g ASA). Správná 3MF sedí na čerstvý
        cloud na ±0,05 g, proto se při odchylce nad max(1 g; 5 %) věří cloudu. Porovnává se jen
        hodnota, která se změnila až s tímto tiskem a pak se ustálila – nikdy uložená cloud_weight_g.
        U uzavřené session jen hodnota změněná do konce tisku (pozdější patří dalšímu tisku).
        """
        w, l, zmena = zive
        plan = plate.weight_g or sum(f.used_g for f in plate.filaments)
        start, konec = session.get("started_ts") or 0, session.get("ended_ts")
        if w is None or zmena is None or not plan or zmena < start - KONTROLA_3MF_OD_S:
            return False
        if time.time() - zmena < KONTROLA_3MF_USAZENA_S or (konec and zmena > konec):
            return False
        if self._proc_odmitnout_cloud(session, w, zmena):
            return False
        min_g, podil = KONTROLA_3MF_ODCHYLKA
        if abs(w - plan) <= max(min_g, podil * w):
            return False
        cesta = f" {session['threemf_path']}" if session.get("threemf_path") else ""
        LOG.warning("session %s: 3MF%s (%.2f g) nesedí na čerstvou cloud hmotnost %.2f g – platí cloud",
                    session["id"][-8:], cesta, plan, w)
        self.db.update_session(session["id"], threemf_status="mismatch", cloud_weight_g=w, cloud_length_m=l,
                               plan_weight_g=w, plan_length_m=l)
        session.update(threemf_status="mismatch", cloud_weight_g=w, cloud_length_m=l)
        self._nesedi.add(session["id"])
        return True

    def _spotreba_z_3mf(self, session: dict, previous: list[dict]) -> bool:
        """Pochází dosavadní spotřeba session z 3MF? Zdroj se čte z DB (předaný slovník bývá upravený)
        a stačí i jediný řádek s filament_idx – u nedokončeného tisku je zdroj 'estimate'."""
        ulozena = self.db.get_session(session["id"]) or session
        return ulozena.get("filament_source") == "3mf" or any(r.get("filament_idx") is not None for r in previous)

    # --- ruční plán (set_plan) ------------------------------------------------------------
    def _deska_z_cache(self, session: dict) -> threemf.PlateInfo | None:
        """Tištěná deska z cache 3MF – bez FTP a bez zápisu stavu. Jen když cache opravdu obsahuje
        desku z gcode_file (Cube: cache měla desku 3 z cizího projektu, tisklo se plate_5) a 3MF
        nebyl zamítnut ('mismatch'). Bez plate_N v názvu platí výběr jako ve fetch_plate."""
        if session.get("threemf_status") == "mismatch" or session["id"] in self._nesedi:
            return None
        cache = self.cache_3mf(session)
        try:
            plates = threemf.parse_slice_info(cache.read_bytes()) if cache.exists() else []
        except (OSError, SyntaxError) as e:     # poškozená cache – ruční plán pak jde na slot tisku
            LOG.debug("cache 3mf %s: %s", cache.name, e)
            return None
        want = threemf.explicitni_deska(session.get("gcode_file") or "")
        if want is None:
            return next((p for p in plates if p.index == 1), plates[0] if plates else None)
        return next((p for p in plates if p.index == want), None)

    def _z_rucniho_planu(self, session: dict, g: float, m: float | None, progress: float, finished_ok: bool,
                         trays_start: list[dict]) -> tuple[list[dict], threemf.PlateInfo | None]:
        """Řádky z ručního plánu: g (a m) je plán celé úlohy, u nedokončeného tisku × postup (jako 3MF).

        Když cache obsahuje tištěnou desku, přeškálují se její řádky poměrem ruční/3MF – rozdělení mezi
        filamenty a sloty zůstane. Jinak jeden řádek na slot, ze kterého tisk začal (materiál ze slotu).
        U jednofilamentového tisku jsou materiál a barva vždy ze slotu: zastaralý 3MF nesl PLA a tisklo
        se ASA (M3HR92MV, M2XV1DZH). Vrátí (řádky, použitá deska nebo None).
        """
        src, est = ("manual", 0) if finished_ok else ("estimate", 1)
        plate = self._deska_z_cache(session)
        soucet_g = sum(f.used_g for f in plate.filaments) if plate else 0.0
        if not plate or soucet_g <= 0:          # i deska se součtem 0 g – dělit by nebylo čím
            tray = next((t for t in trays_start if t["tray_global"] == session.get("tray_now_start")), None)
            return [self._row(tray, used_g=g * progress, used_m=m * progress if m is not None else None,
                              source=src, is_estimate=est, mapping="tray_now" if tray else "unmapped")], None
        soucet_m = sum(f.used_m for f in plate.filaments)
        single = len(plate.filaments) == 1
        rows = []
        for f in plate.filaments:
            tray, mapping = self._map(f, trays_start, session.get("tray_now_start"), single)
            if m is None:
                fm = f.used_m * g / soucet_g            # metry stejným poměrem jako gramy
            elif soucet_m > 0:
                fm = f.used_m * m / soucet_m
            else:
                fm = m * f.used_g / soucet_g
            ze_slotu = bool(single and mapping == "tray_now" and tray)
            rows.append(self._row(tray, used_g=f.used_g * g / soucet_g * progress, used_m=fm * progress,
                                  source=src, is_estimate=est, mapping=mapping, filament_idx=f.idx,
                                  material=None if ze_slotu else (f.type or (tray or {}).get("tray_type")),
                                  color=None if ze_slotu else f.color,
                                  tray_info_idx=None if ze_slotu else f.tray_info_idx))
        return rows, plate

    @staticmethod
    def _dorovnej_na_rucni_plan(rows: list[dict], g: float, m: float | None):
        """Zbytek zaokrouhlení k největšímu řádku, ať součet sedí přesně na ruční plán (× postup).

        Až po dělení podle slotů – to zaokrouhluje každou část zvlášť: 10 g ze tří stejných cívek
        po třetinách dalo 3 × 3,33 = 9,99 g. Metry jen se zadanou délkou (jinak nejsou čím dorovnat)."""
        for pole, cil in (("used_g", g), ("used_m", m)):
            if cil is None or not rows:
                continue
            zbytek = round(round(cil, 2) - sum(r[pole] or 0 for r in rows), 2)
            if zbytek:
                nejvetsi = max(rows, key=lambda r: r["used_g"] or 0)
                nejvetsi[pole] = round((nejvetsi[pole] or 0) + zbytek, 2)

    @staticmethod
    def _plan_z_rucniho(session: dict, g: float, m: float | None, deska) -> dict:
        """Plán session podle ručního plánu. Predikci z 3MF, který k tisku nesedí (nebo není), nenechat:
        u Cube zůstávalo 10 048 s z cizího projektu u čtrnáctiminutového tisku."""
        if m is None and deska:
            m = round(sum(f.used_m for f in deska.filaments) * g / sum(f.used_g for f in deska.filaments), 2)
        plan = {"plan_weight_g": g, "plan_length_m": m}
        if deska:
            plan.update(plan_prediction_s=deska.prediction_s,
                        predicted_s=deska.prediction_s or session.get("predicted_s"),
                        predicted_source="3mf" if deska.prediction_s else session.get("predicted_source"))
        else:
            plan["plan_prediction_s"] = None
            if session.get("predicted_source") == "3mf":
                plan.update(predicted_s=None, predicted_source=None)
        return plan

    def _vratka(self, spool_id: int, deducted: float, previous: list[dict]) -> dict:
        """Nulový řádek, který si pamatuje, co už z cívky odešlo a ještě se nevrátilo."""
        vzor = next((r for r in previous if r.get("spool_id") == spool_id), {}) or {}
        return {"filament_idx": None, "ams_id": vzor.get("ams_id"), "tray_id": vzor.get("tray_id"),
                "tray_global": vzor.get("tray_global"), "tray_info_idx": vzor.get("tray_info_idx"),
                "material": vzor.get("material") or "", "material_group": vzor.get("material_group") or "",
                "brand": vzor.get("brand"), "color_hex": vzor.get("color_hex"), "used_g": 0.0, "used_m": None,
                "source": "refund", "is_estimate": 1, "mapping_source": "vratka",
                "spool_id": spool_id, "spool_price_per_kg": vzor.get("spool_price_per_kg"),
                "spool_deducted_g": round(deducted, 2)}

    def _doparuj_podle_useku(self, rows: list[dict], session: dict, trays_start: list[dict]) -> list[dict]:
        """Filament, který se nespároval podle barvy, přiřadí slotu vylučovací metodou.

        Projekty ze stránek nesou autorovy barvy: 28. 9. 2026 měl projekt zelenou a bílou, Patrik
        zelenou přiřadil černé ve slotu 2. Tiskárna jela do půlky ze slotu 2, pak ze slotu 3 –
        bílá se spárovala se slotem 3 podle barvy, zelená nikam a 21 g černé se neodečetlo vůbec.
        Když zbyde jediný nespárovaný filament a jediný slot, ze kterého tiskárna prokazatelně
        tiskla, stejného materiálu a nezabraný jiným filamentem, patří k sobě.
        """
        nesparovane = [r for r in rows if r.get("tray_global") is None and (r.get("used_g") or 0) > 0]
        spans = session.get("tray_spans")
        if isinstance(spans, str):
            try:
                spans = json.loads(spans)
            except ValueError:
                spans = None
        if len(nesparovane) != 1 or not spans:
            return rows
        konec = (session.get("last_percent") or 100) / 100.0
        trvani: dict[int, float] = {}
        for i, sp in enumerate(spans):
            od = (sp.get("from_pct") or 0) / 100.0
            do = (spans[i + 1].get("from_pct") or 0) / 100.0 if i + 1 < len(spans) else konec
            if do > od:
                trvani[sp["tray"]] = trvani.get(sp["tray"], 0.0) + (do - od)
        by_global = {t["tray_global"]: t for t in trays_start}
        obsazene = {r["tray_global"] for r in rows if r.get("tray_global") is not None}
        r = nesparovane[0]
        kandidati = [t for t, d in trvani.items()
                     if d >= 0.02 and t not in obsazene and t in by_global
                     and material_group(by_global[t].get("tray_type")) == material_group(r.get("material"))]
        if len(kandidati) != 1:
            return rows
        t = by_global[kandidati[0]]
        r.update(tray_global=t["tray_global"], ams_id=t.get("ams_id"), tray_id=t.get("tray_id"),
                 tray_info_idx=t.get("info_idx"), mapping_source="vylouceni")
        LOG.info("session %s: nespárovaný filament %s přiřazen slotu %d vylučovací metodou",
                 session["id"][-8:], r.get("color_hex"), t["tray_global"] + 1)
        return rows

    def _rozdel_podle_slotu(self, rows: list[dict], session: dict, trays_start: list[dict]) -> list[dict]:
        """Spotřebu rozdělí mezi sloty se STEJNÝM filamentem, pokud AMS během tisku přepnul cívku.

        Když ve slotu dojde filament, AMS sáhne po jiné cívce stejné barvy a tiskne dál (auto-refill).
        Slicer o tom neví, takže bez tohohle by celá spotřeba padla na dojetou cívku a ta druhá by
        zůstala v evidenci plná (21. 9. 2026 takhle ušlo 142 g).

        Přepnutí slotu ale není vždycky auto-refill: u vícebarevného tisku je každá výměna barvy taky
        změna slotu. Proto se každý řádek dělí **jen mezi sloty, kde byl na začátku stejný materiál
        a barva** — černá se rozpočítá mezi dvě černé cívky, bílá zůstane na bílé. Verze 0.15 dělila
        všechno mezi všechno a u dvoubarevného tisku by připsala bílé cívce kus černé (24. 9. 2026
        přišlo najevo při tisku černé s bílým popiskem ze zbytku bílé).

        Poměr je podle doby, kterou tisk ve kterém slotu strávil (v procentech postupu). Je to odhad —
        spotřeba na procento není rovnoměrná — takže dělené řádky nesou `is_estimate=1`.
        Když tisk celý běžel z jiného slotu se stejným filamentem, než kam ho přiřadil slicer
        (dvě stejné cívky v AMS), řádek se jen přesune na skutečný slot.
        """
        spans = session.get("tray_spans")
        if isinstance(spans, str):
            try:
                spans = json.loads(spans)
            except ValueError:
                spans = None
        if not spans or not rows:
            return rows

        # kolik postupu strávil tisk ve kterém slotu (součet všech úseků)
        konec = (session.get("last_percent") or 100) / 100.0
        hranice = [(sp["tray"], (sp.get("from_pct") or 0) / 100.0) for sp in spans]
        trvani: dict[int, float] = {}
        for i, (tray, od) in enumerate(hranice):
            do = hranice[i + 1][1] if i + 1 < len(hranice) else konec
            if do > od:
                trvani[tray] = trvani.get(tray, 0.0) + (do - od)

        by_global = {t["tray_global"]: t for t in trays_start}

        def filament(tray: int):
            t = by_global.get(tray)
            if not t or not (t.get("tray_type") or t.get("color")):
                return None          # slot, o kterém nevíme, co v něm bylo – nedělit na něj
            barva = (t.get("color") or "").lstrip("#").upper()[:6]
            return material_group(t.get("tray_type")), barva

        tridy: dict[tuple, list[dict]] = {}
        for r in rows:
            k = filament(r.get("tray_global")) if r.get("tray_global") is not None else None
            if k:
                tridy.setdefault(k, []).append(r)
        obsazene = {r.get("tray_global") for r in rows}

        MIN_PODIL = 0.01          # zákmit na jiném slotu stejného filamentu – ne skutečné přepnutí
        nove: list[dict] = []
        for r in rows:
            T = r.get("tray_global")
            k = filament(T) if T is not None else None
            # Dva řádky se stejným filamentem (dvě stejné barvy v projektu) nejde od sebe oddělit –
            # radši nechat, než je rozpočítat špatně.
            if not k or len(tridy.get(k, [])) != 1:
                nove.append(r)
                continue
            ekv = {s: d for s, d in trvani.items() if filament(s) == k}
            celkem = sum(ekv.values())
            if celkem <= 0:
                nove.append(r)
                continue
            ekv = {s: d for s, d in ekv.items() if d / celkem >= MIN_PODIL}
            celkem = sum(ekv.values())
            cizi_obsazene = (set(ekv) - {T}) & obsazene
            if not ekv or set(ekv) == {T} or cizi_obsazene:
                nove.append(r)
                continue
            for tray, d in sorted(ekv.items()):
                cast = d / celkem
                novy = dict(r)
                novy["tray_global"] = tray
                t = by_global.get(tray)
                if t:
                    novy["ams_id"], novy["tray_id"] = t["ams_id"], t["tray_id"]
                    novy["tray_info_idx"] = t.get("info_idx")
                for pole in ("used_g", "used_m"):
                    if novy.get(pole) is not None:
                        novy[pole] = round(novy[pole] * cast, 2)
                if len(ekv) > 1:
                    novy["is_estimate"] = 1
                    novy["mapping_source"] = "refill_split"
                else:
                    novy["mapping_source"] = "tray_spans"
                novy["spool_deducted_g"] = 0      # řádek je pro tenhle slot nový, cizí odečet nedědí
                nove.append(novy)
            LOG.info("session %s: %s %s rozdělen podle slotů: %s", session["id"][-8:], k[0], k[1] or "?",
                     ", ".join(f"slot {t + 1} {d / celkem:.0%}" for t, d in sorted(ekv.items())))
        return nove

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
        # U tisku z jednoho filamentu rozhoduje slot, ze kterého tiskárna opravdu tiskne – barva
        # ve sliceru je jen popisek projektu. 26. 9. 2026 měl projekt barvu bílou, tisklo se
        # černou ze slotu 4, a párování podle barvy odečítalo z bílé cívky, dokud ji nevyprázdnilo.
        if single and tray_now is not None and tray_now != 255:
            for t in trays:
                if t["tray_global"] == tray_now:
                    return t, "tray_now"
        color = _barva(f.color)
        for t in trays:
            if f.tray_info_idx and t.get("info_idx") == f.tray_info_idx and _barva(t.get("color")) == color:
                return t, "exact"
        for t in trays:
            if color and _barva(t.get("color")) == color and material_group(t.get("tray_type")) == material_group(f.type):
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
        # i do slovníku: čerstvost cloud hmotnosti a křížová kontrola 3MF v tomtéž resolve se měří od
        # začátku tisku. Odhad po restartu vychází pozdě (nezná úvodní fázi s 0 %) a čerstvou hodnotu
        # by první pokus odmítl.
        session.update(started_ts=ts, print_started_ts=ts, start_source="cloud_ha")
        LOG.info("začátek session %s upřesněn z cloudu: %s", session["id"][-8:], st["state"])
        return ts

    def resolve(self, session: dict, final: bool, allow_live: bool = False, bez_spoolmanu: bool = False,
                rucni_plan: bool = True, refetch: bool = False) -> dict | None:
        """Spočítá a uloží spotřebu. Vrátí shrnutí (g, m, source, is_estimate) nebo None.

        Pořadí zdrojů: manual_override (nic se nepřepočítává) → ruční plán (set_plan) → 3MF →
        pojistky „beze změny“ → cloud → AMS remain.

        allow_live: smí se číst živá entita ha-bambulab (cloud hmotnost, křížová kontrola 3MF,
        čas startu)? Entita patří tisku, který právě běží – u kterékoli lokality, protože
        ha-bambulab jede přes cloud na obou HA. Proto ji smí číst jen volající, který ví, že patří
        téhle session: průběžný resolve otevřené session na instanci, která tiskárnu sbírá
        (s čerstvou zprávou MQTT), a _finalize hned po řádném konci tisku. Příkazy z HA, fronta
        doúčtování a refetch pracují jen s tím, co je uložené u session (výchozí False).

        None = beze změny: ruční hodnota (manual_override), cizí session (origin jiné lokality), nebo
        pojistka u uzavřené session, pro kterou teď není žádný zdroj a přepočet by jen smazal dosavadní
        spotřebu (a vrátil ji cívce).

        bez_spoolmanu: opravuje se jen evidence – odečteno se nastaví na cíl a Spoolman se nevolá
        (set_plan …:bez_spoolmanu u archivované nebo ručně srovnané cívky).

        rucni_plan=False: přepočet, jako by ruční plán nebyl – set_plan:<id>:- ho tak zkouší dřív, než
        plán smaže (None = jiný zdroj není a plán musí zůstat). plan_weight_g/m přitom přepíše plánem
        z automatického zdroje (3MF, uložená cloud hodnota, jinak None).

        refetch=True (refetch_3mf uzavřené session): stažení 3MF, které nevrátí 'ok', se do DB nezapíše
        – výsledek je jen v předaném slovníku (threemf_status). Session se tak nezmění ani na chvíli
        (export a publish_stats neberou _resolve_lock) a updated_ts se neposune.
        """
        ulozena = self.db.get_session(session["id"])
        if not je_vlastni(ulozena or session, self.instance):
            # Pojistka za stráží příkazů v collectoru (M4a): cizí session (import z druhé lokality)
            # přepočítává a do Spoolmanu za ni zapisuje jen instance, která ji zapsala – jinak by se
            # odečetla dvakrát a změna by se exportovala z obou míst.
            LOG.warning("session %s patří lokalitě %s – přepočet beze změny", session["id"][-8:],
                        (ulozena or session).get("origin"))
            return None
        if ulozena:
            # Ruční zásahy platí podle DB, ne podle předaného slovníku: volající ho mohl přečíst před
            # _resolve_lock a settled nebo set_plan z HA ho mezitím změnil. Přepočet ze zastaralého
            # řádku by ruční plán přebil automatickým zdrojem, nebo doúčtoval zmrazenou spotřebu.
            session.update({k: ulozena.get(k) for k in ("manual_override", "manual_plan_g", "manual_plan_m")})
        if session.get("manual_override"):
            return None
        uzavrena = session.get("ended_ts") is not None
        if allow_live:
            self.fix_start_from_cloud(session)
        trays_start = _trays(session, "trays_start")
        result = session.get("result")
        finished_ok = final and result == "success"
        progress = (session.get("last_percent") or 0) / 100.0
        live_progress = progress
        if not final:
            progress = 1.0  # v přehledu u běžícího tisku hlásíme plán celé úlohy (označený jako odhad)
        elif finished_ok:
            progress = 1.0  # dokončený tisk spotřeboval celý plán, i kdyby poslední zpráva hlásila 99 %
        elif progress <= 0:
            # Bez procent se postup odhadne z vrstev – ale jen u tisku, který opravdu tiskl, a jen
            # z platného čísla vrstvy. Tiskárna hlásí vrstvu minulého tisku ještě několik sekund
            # po startu RUNNING (27. 9. 2026 vrstva 662 osm sekund): přípravná session nahrazená
            # skutečnou, session „lost" nebo zrušení v první minutě by z ní spočítaly fantomový
            # „postup" (25. 9. 2026 vrstva 935 proti 28 vrstvám kostičky dala 1 269 g, 26. 9. vrstva
            # 3/215 tisku, který ještě nezačal). Proto jen zrušený/selhaný tisk, který běžel
            # v RUNNING aspoň minutu – skutečný tisk drží 0 % i šest minut, než začne tisknout.
            # Bez času startu se počítá, že neběžel vůbec. Vrstva vyšší než počet vrstev tisku je
            # z jiného tisku → tenhle nespotřeboval nic.
            vrstva, vrstev = session.get("last_layer") or 0, session.get("total_layers") or 0
            konec, start = session.get("ended_ts"), session.get("print_started_ts")
            beh = konec - start if konec and start else 0
            if result in ("failed", "cancelled") and beh >= MIN_PRINT_FOR_LAYERS_S and 0 < vrstva <= vrstev:
                progress = vrstva / vrstev
            else:
                progress = 0.0
        progress = max(0.0, min(1.0, progress))   # víc než celý plán se spotřebovat nedá

        rows: list[dict] = []
        source, is_est = "none", 0
        total_g = total_m = None
        previous = self.db.filaments(session["id"])
        # pojistky „beze změny“ platí jen pro uzavřenou session bez živé entity (příkazy, fronta, refetch)
        hlidat = uzavrena and not allow_live

        manual = session.get("manual_plan_g") if rucni_plan else None
        # Zrušení ručního plánu: plan_weight_g/m nese ruční hodnotu a musí se vrátit na automatický zdroj.
        # 3MF plán zapíše sám, uložená cloud hodnota ne – jinak by zůstal ruční plán se zdrojem cloud_ha
        # (nástěnka běžícího tisku by ukazovala ruční plán × postup, _proc_odmitnout_cloud porovnávala
        # další tisk se zastaralým plánem). Bez automatického plánu (jen AMS, nebo nic) → None.
        obnovit_plan = not rucni_plan and session.get("manual_plan_g") is not None
        plan_auto: tuple[float | None, float | None] = (None, None)
        if manual is not None:
            # Ruční plán (set_plan) má přednost před 3MF, cloudem i AMS a nikdy nestahuje 3MF: soubor
            # stažený až teď bývá cizí nebo novější (simulace S11 odečetla ASA cívku, ze které tisk nešel).
            manual_m = session.get("manual_plan_m")
            rows, deska = self._z_rucniho_planu(session, float(manual), manual_m, progress, finished_ok, trays_start)
            source, is_est = "manual", 0 if finished_ok else 1
            self._zapis_zmeny(session["id"], **self._plan_z_rucniho(session, float(manual), manual_m, deska))
        else:
            plate = self._from_3mf(session, zapsat_neuspech=not refetch)
            zive = None
            if plate and plate.filaments and allow_live and session.get("print_type") == "cloud" and not je_systemova(session):
                zive = self._zivy_cloud()
                if self._3mf_vyvracen_cloudem(session, plate, zive):
                    plate = None
            if hlidat and not (plate and plate.filaments) and self._spotreba_z_3mf(session, previous):
                # Pojistka (a): spotřeba pochází z 3MF, který teď není k dispozici (bez cache a stažení
                # neuspělo, nebo 'mismatch'). Přepočet by řádky smazal a cívce vrátil celý odečet, nebo je
                # přepsal uloženou cloud hodnotou – ta u 3MF session bývá zastaralá (replay 113 session
                # Haciendy: refetch bez cache −2 899,6 g vrácených cívkám, s pádem na cloud +752,8 g).
                # Nestojí na threemf_status: refetch ho předem nuluje a upsert ze stavového automatu taky.
                # Až za ručním plánem – set_plan musí jít i u 3MF session bez cache.
                self._hlas_beze_zmeny(session, "3MF teď není k dispozici a spotřeba pochází z 3MF – přepočet "
                                               "beze změny (špatný 3MF se opravuje přes set_plan)")
                return None
            if plate and plate.filaments:
                single = len(plate.filaments) == 1
                for f in plate.filaments:
                    tray, mapping = self._map(f, trays_start, session.get("tray_now_start"), single)
                    rows.append(self._row(tray, used_g=f.used_g * progress, used_m=f.used_m * progress,
                                          source="3mf" if finished_ok else "estimate", is_estimate=0 if finished_ok else 1,
                                          mapping=mapping, filament_idx=f.idx, material=f.type or (tray or {}).get("tray_type"),
                                          color=f.color, tray_info_idx=f.tray_info_idx))
                self._zapis_zmeny(session["id"], plan_weight_g=plate.weight_g or sum(f.used_g for f in plate.filaments),
                                  plan_length_m=sum(f.used_m for f in plate.filaments), plan_prediction_s=plate.prediction_s,
                                  predicted_s=plate.prediction_s or session.get("predicted_s"),
                                  predicted_source="3mf" if plate.prediction_s else session.get("predicted_source"))
                source, is_est = ("3mf", 0) if finished_ok else ("estimate", 1)
                obnovit_plan = False
            else:
                w, l = self._hmotnost_z_cloudu(session, allow_live, zive)
                if w is not None:
                    plan_auto = (w, l)
                    tray = next((t for t in trays_start if t["tray_global"] == session.get("tray_now_start")), None)
                    rows.append(self._row(tray, used_g=w * progress, used_m=(l or 0) * progress or None,
                                          source="cloud_ha" if finished_ok else "estimate", is_estimate=0 if finished_ok else 1,
                                          mapping="tray_now" if tray else "unmapped"))
                    source, is_est = ("cloud_ha", 0) if finished_ok else ("estimate", 1)

        rows = self._doparuj_podle_useku(rows, session, trays_start)
        rows = self._rozdel_podle_slotu(rows, session, trays_start)
        if manual is not None:
            self._dorovnej_na_rucni_plan(rows, float(manual) * progress,
                                         manual_m * progress if manual_m is not None else None)

        if final and manual is None:
            remain_rows = self._from_remain(session)
            remain_total = sum(r["used_g"] for r in remain_rows)
            if remain_rows and (not rows or (result != "success" and remain_total >= 20)):
                rows, source, is_est = remain_rows, "ams_remain", 1

        if hlidat and not rows and any((r.get("used_g") or 0) > 0 or (r.get("spool_deducted_g") or 0) > 0.05
                                       for r in previous):
            # Pojistka (b): 3MF, uložená cloud hodnota ani AMS nedaly nic, ale session spotřebu má.
            # Bez živé entity se nedá poznat, že je opravdu nulová – radši nechat, než všechno vrátit.
            self._hlas_beze_zmeny(session, "žádný zdroj spotřeby (3MF, uložená cloud hodnota ani AMS) – "
                                           "dosavadní spotřeba ponechána")
            return None

        # Spoolman – odečet se účtuje po CÍVKÁCH, ne po slotech.
        # Během tisku se spotřeba průběžně odečítá z cívky, kterou add-on zrovna odhaduje. Když se
        # pak ukáže, že tisk šel z jiné cívky (upřesní se slot, dorazí 3MF, AMS přepne), musí se
        # té první gramy vrátit. Dřív se srovnávalo po slotech: řádek, který se přestěhoval na jiný
        # slot, prostě zmizel i s tím, co už z cívky strhl – 26. 9. 2026 takhle vyprázdnil bílou
        # cívku, ze které se netisklo, a z #21 zmizelo 92 g.
        prev_by_tray = {r["tray_global"]: r for r in previous if r.get("tray_global") is not None}
        prev_by_spool: dict[int, float] = {}
        for r in previous:
            if r.get("spool_id"):
                prev_by_spool[r["spool_id"]] = prev_by_spool.get(r["spool_id"], 0.0) + (r.get("spool_deducted_g") or 0)
        live = 1.0 if final else max(0.0, min(1.0, live_progress))   # kolik z plánu je reálně protlačeno
        trays_by_global = {t["tray_global"]: t for t in trays_start}
        usable = bool(self.spoolman and self.spoolman.enabled and self.spoolman.reachable is not False)

        # 1) ke každému řádku cívku – lokální deník osazení slotů má přednost (ví, co bylo ve slotu
        #    v době tisku, i když se přiřazení ve Spoolmanu mezitím změnilo)
        for r in rows:
            prev = prev_by_tray.get(r["tray_global"]) or {}
            r["spool_deducted_g"] = 0.0
            r["spool_id"], r["spool_price_per_kg"] = prev.get("spool_id"), prev.get("spool_price_per_kg")
            # Výpadek zjištěný už při hledání cívky (spools() nastaví reachable=False): dál se Spoolman
            # nevolá a jde se větví „nedostupný“ – jinak by každý další řádek a pak každé use() čekaly
            # na 10s timeout.
            usable = usable and self.spoolman.reachable is not False
            if not usable or r["tray_global"] is None:
                continue
            tag = (trays_by_global.get(r["tray_global"]) or {}).get("tray_uuid") or ""
            journal = self.db.slot_spool_at(self.serial, r["tray_global"], session.get("started_ts") or 0) if self.serial else None
            sp = None
            if journal and journal.get("spool_id"):
                sp = next((x for x in self.spoolman.spools(include_archived=True) if x["id"] == journal["spool_id"]), None)
            elif journal:
                # ve slotu je cívka, kterou Spoolman ještě nezná – raději neodečítat vůbec, než
                # strhnout spotřebu z předchozí cívky vedené ve Spoolmanu
                LOG.debug("slot %s: cívka bez ID ve Spoolmanu (%s), odečet čeká", r["tray_global"], journal.get("label"))
                r["spool_id"] = None
            elif hlidat and prev.get("spool_id"):
                # Uzavřený tisk, pro jehož dobu deník nic nemá, si nechá cívku, ze které se už odečítal
                # (jen s aktuální cenou). Dnešní přiřazení slotu ve Spoolmanu o minulém tisku nic neví:
                # příkaz nebo fronta by jinak odečet přestěhovaly na cívku, která je ve slotu teď (offline
                # brána, replay Haciendy: resolve:M2E879EX z 13. 9. přesunul 6,04 g z #18 na #23 koupenou
                # 25. 9.). Jinou cívku určí jen deník – set_slot s časem tisku.
                sp = next((x for x in self.spoolman.spools(include_archived=True) if x["id"] == prev["spool_id"]), None)
            else:
                sp = self.spoolman.spool_for_tray(r["tray_global"], tag)
            if sp:
                r["spool_id"] = sp["id"]
                r["spool_price_per_kg"] = self.spoolman.price_per_kg(sp)
        usable = usable and self.spoolman.reachable is not False

        if bez_spoolmanu:
            # Jen evidence: odečteno = cíl, Spoolman se nevolá a vratky se nezakládají. Pro cívku, kterou
            # nejde opravit zápisem (archivovaná #20 u M34WSETP by dostala 15 g na vyprázdněnou cívku).
            ucty: dict[int, float] = {}
            for r in rows:
                if r.get("spool_id"):
                    r["spool_deducted_g"] = (r["used_g"] or 0) if final else round((r["used_g"] or 0) * live * LIVE_CAP, 2)
                    ucty[r["spool_id"]] = ucty.get(r["spool_id"], 0.0) + r["spool_deducted_g"]
            for sid in sorted(set(ucty) | set(prev_by_spool)):
                if abs(ucty.get(sid, 0.0) - prev_by_spool.get(sid, 0.0)) > 0.05:
                    LOG.info("session %s: cívka #%s v evidenci %.2f → %.2f g, Spoolman beze změny (bez_spoolmanu)",
                             session["id"][-8:], sid, prev_by_spool.get(sid, 0.0), ucty.get(sid, 0.0))
        elif not usable:
            # Spoolman teď nejde: nic neodečítat a nic neztratit. Co už z cívky odešlo, se rozepíše na
            # nové řádky téže cívky poměrem used_g (každý nejvýš do výše used_g) a zbytek – u řádků
            # s celkem 0 g všechno – zůstane jako nulový řádek k pozdějšímu vrácení. Součet po cívkách
            # je pak přesně to, co ve Spoolmanu opravdu je. Dřív řádek převzal odečet podle slotu:
            # dva řádky na témž slotu (10 + 100 g, odečteno 110) si ho vzaly oba, deník tvrdil 200 g
            # a po obnovení se cívce vrátilo 90 g navíc (PH M346SC69 má na #10 dva řádky slotu 4).
            po_civkach: dict[int, list[dict]] = {}
            for r in rows:
                if r.get("spool_id"):
                    po_civkach.setdefault(r["spool_id"], []).append(r)
            for sid, done in prev_by_spool.items():
                radky = po_civkach.get(sid, [])
                celkem = sum(max(0.0, r["used_g"] or 0) for r in radky)
                if celkem > 0:
                    rozepsat = min(done, celkem)
                    for r in radky:
                        r["spool_deducted_g"] = round(rozepsat * max(0.0, r["used_g"] or 0) / celkem, 2)
                    zbytek = round(rozepsat - sum(r["spool_deducted_g"] for r in radky), 2)
                    if zbytek:          # zaokrouhlení po řádcích – dorovnat na největším (nad used_g ne)
                        nejvetsi = max(radky, key=lambda r: r["used_g"] or 0)
                        nejvetsi["spool_deducted_g"] = round(min(nejvetsi["used_g"] or 0,
                                                                 nejvetsi["spool_deducted_g"] + zbytek), 2)
                zbyva = done - sum(r["spool_deducted_g"] for r in radky)
                if zbyva > 0.05:
                    rows.append(self._vratka(sid, zbyva, previous))
        else:
            # 2) cíl po cívkách: po dokončení celá spotřeba, během tisku max LIVE_CAP plánu
            cil: dict[int, float] = {}
            for r in rows:
                if r.get("spool_id"):
                    t = (r["used_g"] or 0) if final else round((r["used_g"] or 0) * live * LIVE_CAP, 2)
                    r["_cil"] = t
                    cil[r["spool_id"]] = cil.get(r["spool_id"], 0.0) + t
            provedeno: dict[int, float] = {}
            for sid in set(cil) | set(prev_by_spool):
                target, done = cil.get(sid, 0.0), prev_by_spool.get(sid, 0.0)
                delta = target - done
                odesla = sid not in cil          # tisk z téhle cívky odešel úplně → vrátit hned
                zmena = False
                if delta < -0.05 and (final or odesla):
                    zmena = self.spoolman.use(sid, delta)       # vrácení přeodečteného
                elif delta > MIN_STEP_G or (final and delta > 0.05):
                    zmena = self.spoolman.use(sid, delta)
                if zmena == GONE:           # smazaná cívka: vyřízeno, jen se to ukáže v chybách fronty
                    self._civka_chybi(session, sid, delta)
                provedeno[sid] = target if zmena else done
                if zmena is True and odesla:
                    LOG.info("session %s: cívce #%s vráceno %.1f g (tisk z ní nakonec nešel)",
                             session["id"][-8:], sid, -delta)
            # 3) odečtené rozepsat zpátky na řádky (poměrem k cíli), ať se to příště dá porovnat
            for r in rows:
                sid = r.get("spool_id")
                if sid:
                    celkem = cil.get(sid, 0.0)
                    r["spool_deducted_g"] = round(provedeno.get(sid, 0.0) * (r["_cil"] / celkem), 2) if celkem > 0 else 0.0
                r.pop("_cil", None)
            for sid, done in provedeno.items():
                # Cíl cívky je nula (tisk z ní odešel, nebo její řádky mají celkem 0 g), ale z cívky
                # pořád něco odešlo: vratka se nepovedla, nebo se u běžícího tisku ještě nevrací.
                # Řádky s celkem 0 g by si odečet nepodržely – zrušený tisk bez postupu tak 36 g
                # z deníku ztratil, zatímco ve Spoolmanu zůstaly odečtené (FIL-11).
                if cil.get(sid, 0.0) <= 0 and done > 0.05:
                    rows.append(self._vratka(sid, done, previous))
        if rows:
            total_g = round(sum(r["used_g"] or 0 for r in rows), 2)
            ms = [r["used_m"] for r in rows if r["used_m"] is not None]
            total_m = round(sum(ms), 2) if ms else None
        # Zapisovat jen prokázanou změnu proti čerstvému stavu v DB (M8b): opakovaný přepočet beze změny
        # nesmí posouvat updated_ts. Změněné řádky posunou updated_ts vždy (souhrnem, i když je stejný),
        # jinak by se samotná změna ceny nebo cívky do druhé lokality nedostala.
        souhrn = dict(filament_g=total_g, filament_m=total_m, filament_source=source, filament_is_estimate=is_est)
        if obnovit_plan:
            souhrn.update(plan_weight_g=plan_auto[0], plan_length_m=plan_auto[1])
        if not _radky_stejne(rows, self.db.filaments(session["id"])):
            self.db.replace_filaments(session["id"], rows)
            self.db.update_session(session["id"], **souhrn)
        else:
            self._zapis_zmeny(session["id"], **souhrn)
        return {"g": total_g, "m": total_m, "source": source, "is_estimate": is_est}
