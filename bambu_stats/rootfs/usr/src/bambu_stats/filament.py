"""Určení spotřeby filamentu pro session – kombinace zdrojů s prioritou.

1. `3mf`        slice_info.config z 3MF na USB disku tiskárny (přes FTPS) – per filament used_g/used_m
2. `cloud_ha`   entity ha-bambulab (print_weight/print_length) – hodnota z Bambu cloudu
3. `ams_remain` rozdíl `remain` % × hmotnost cívky – jen Bambu RFID cívky (rozlišení 1 % ≈ 10 g)
4. `none`

U nedokončených tisků se plán (1/2) násobí postupem (`mc_percent/100`) a označí jako odhad;
pokud je k dispozici měření `ams_remain` s Δ ≥ 2 %, má přednost (je to skutečná změna).
Ruční hodnota (`manual_override`) se nikdy nepřepisuje.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from . import threemf
from .util import material_group

LOG = logging.getLogger("filament")
EXTERNAL_SLOT_IDS = {254, 255}
LIVE_CAP = 0.9          # během tisku odečítat nejvýš 90 % dosud spotřebovaného odhadu
MIN_STEP_G = 5.0        # menší přírůstky neposílat (šetří zápisy do Spoolmanu)


def _barva(c) -> str:
    """Barvu porovnávat bez `#` a bez průhlednosti: slicer píše `#161616FF`, jindy `#161616`,
    tiskárna `161616FF`. Jinak se vícebarevný tisk nespáruje se sloty a neodečte vůbec."""
    return (c or "").strip().lstrip("#").upper()[:6]


def _trays(session: dict, key: str) -> list[dict]:
    v = session.get(key)
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            v = []
    return v or []


class FilamentResolver:
    def __init__(self, db, printer, ha, cache_dir: Path, spoolman=None, serial: str | None = None):
        self.db, self.printer, self.ha, self.spoolman = db, printer, ha, spoolman
        # Adresu, na které tiskárna opravdu je, zjistí collector (locator.py) a nastaví sem.
        # `printer.host` bývá od 0.16 prázdný – tiskárna se hledá podle `printer_hosts`.
        self.host: str | None = None
        self.serial = serial or getattr(printer, "serial", "")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._attempts: dict[str, int] = {}

    # --- zdroje ------------------------------------------------------------------
    def _from_3mf(self, session: dict) -> threemf.PlateInfo | None:
        cache = self.cache_dir / f"{session['fingerprint']}.xml"
        if cache.exists():
            plates = threemf.parse_slice_info(cache.read_bytes())
            want = threemf.plate_index_from_gcode(session.get("gcode_file") or "")
            plate = next((p for p in plates if p.index == want), plates[0] if plates else None)
            if plate and session.get("threemf_status") != "ok":
                self.db.update_session(session["id"], threemf_status="ok")
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
        self.db.update_session(session["id"], threemf_status=status, threemf_path=path, threemf_fetched_ts=int(time.time()))
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

    def _from_cloud(self, session: dict) -> tuple[float | None, float | None]:
        w = self.ha.numeric(self.printer.ha_weight_entity) if self.printer.ha_weight_entity else None
        l = self.ha.numeric(self.printer.ha_length_entity) if self.printer.ha_length_entity else None
        return w, l

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
        LOG.info("začátek session %s upřesněn z cloudu: %s", session["id"][-8:], st["state"])
        return ts

    def resolve(self, session: dict, final: bool) -> dict | None:
        """Spočítá a uloží spotřebu. Vrátí shrnutí (g, m, source, is_estimate) nebo None."""
        if session.get("manual_override"):
            return None
        self.fix_start_from_cloud(session)
        trays_start = _trays(session, "trays_start")
        result = session.get("result")
        finished_ok = final and result == "success"
        progress = (session.get("last_percent") or 0) / 100.0
        live_progress = progress
        if not final:
            progress = 1.0  # v přehledu u běžícího tisku hlásíme plán celé úlohy (označený jako odhad)
        elif result != "success" and progress <= 0:
            # Bez procent se postup odhadne z vrstev – ale jen z platného čísla vrstvy. Přípravná
            # session, kterou za pár vteřin nahradí skutečná, zdědí vrstvu z předchozího, už
            # dotištěného tisku: 25. 9. 2026 vrstva 935 z minula proti 28 vrstvám kostičky dala
            # „postup" 3 367 % a fantomových 1 269 g, které srazily plnou cívku na nulu.
            # Vrstva vyšší než počet vrstev tisku je z jiného tisku → tenhle nespotřeboval nic.
            # (Chybějící čas startu to nepozná: fix_start_from_cloud ho doplní i takové session.)
            vrstva, vrstev = session.get("last_layer") or 0, session.get("total_layers") or 0
            progress = vrstva / vrstev if 0 < vrstva <= vrstev else 0.0
        progress = max(0.0, min(1.0, progress))   # víc než celý plán se spotřebovat nedá

        rows: list[dict] = []
        source, is_est = "none", 0
        total_g = total_m = None

        plate = self._from_3mf(session)
        if plate and plate.filaments:
            single = len(plate.filaments) == 1
            for f in plate.filaments:
                tray, mapping = self._map(f, trays_start, session.get("tray_now_start"), single)
                rows.append(self._row(tray, used_g=f.used_g * progress, used_m=f.used_m * progress,
                                      source="3mf" if finished_ok else "estimate", is_estimate=0 if finished_ok else 1,
                                      mapping=mapping, filament_idx=f.idx, material=f.type or (tray or {}).get("tray_type"),
                                      color=f.color, tray_info_idx=f.tray_info_idx))
            self.db.update_session(session["id"], plan_weight_g=plate.weight_g or sum(f.used_g for f in plate.filaments),
                                   plan_length_m=sum(f.used_m for f in plate.filaments), plan_prediction_s=plate.prediction_s,
                                   predicted_s=plate.prediction_s or session.get("predicted_s"),
                                   predicted_source="3mf" if plate.prediction_s else session.get("predicted_source"))
            source, is_est = ("3mf", 0) if finished_ok else ("estimate", 1)
        else:
            # živou cloud hodnotu číst jen u otevřené session – po uzavření už entita patří dalšímu tisku
            w, l = (None, None) if (session.get("ended_ts") and session.get("cloud_weight_g")) else self._from_cloud(session)
            if w:
                self.db.update_session(session["id"], cloud_weight_g=w, cloud_length_m=l, plan_weight_g=w, plan_length_m=l)
            else:
                w, l = session.get("cloud_weight_g"), session.get("cloud_length_m")
            if w:
                tray = next((t for t in trays_start if t["tray_global"] == session.get("tray_now_start")), None)
                rows.append(self._row(tray, used_g=w * progress, used_m=(l or 0) * progress or None,
                                      source="cloud_ha" if finished_ok else "estimate", is_estimate=0 if finished_ok else 1,
                                      mapping="tray_now" if tray else "unmapped"))
                source, is_est = ("cloud_ha", 0) if finished_ok else ("estimate", 1)

        rows = self._rozdel_podle_slotu(rows, session, trays_start)

        if final:
            remain_rows = self._from_remain(session)
            remain_total = sum(r["used_g"] for r in remain_rows)
            if remain_rows and (not rows or (result != "success" and remain_total >= 20)):
                rows, source, is_est = remain_rows, "ams_remain", 1

        # Spoolman – odečet se účtuje po CÍVKÁCH, ne po slotech.
        # Během tisku se spotřeba průběžně odečítá z cívky, kterou add-on zrovna odhaduje. Když se
        # pak ukáže, že tisk šel z jiné cívky (upřesní se slot, dorazí 3MF, AMS přepne), musí se
        # té první gramy vrátit. Dřív se srovnávalo po slotech: řádek, který se přestěhoval na jiný
        # slot, prostě zmizel i s tím, co už z cívky strhl – 26. 9. 2026 takhle vyprázdnil bílou
        # cívku, ze které se netisklo, a z #21 zmizelo 92 g.
        previous = self.db.filaments(session["id"])
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
            else:
                sp = self.spoolman.spool_for_tray(r["tray_global"], tag)
            if sp:
                r["spool_id"] = sp["id"]
                r["spool_price_per_kg"] = self.spoolman.price_per_kg(sp)

        if not usable:
            # Spoolman teď nejde: nic neodečítat a nic neztratit – řádky si ponesou to, co už bylo
            # odečteno, a cívky, ze kterých tisk odešel, zůstanou jako nulové řádky k pozdějšímu vrácení.
            for r in rows:
                prev = prev_by_tray.get(r["tray_global"]) or {}
                if r.get("spool_id") and r["spool_id"] == prev.get("spool_id"):
                    r["spool_deducted_g"] = prev.get("spool_deducted_g") or 0.0
            ucty = {}
            for r in rows:
                if r.get("spool_id"):
                    ucty[r["spool_id"]] = ucty.get(r["spool_id"], 0.0) + r["spool_deducted_g"]
            for sid, done in prev_by_spool.items():
                zbyva = done - ucty.get(sid, 0.0)
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
                provedeno[sid] = target if zmena else done
                if zmena and odesla:
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
                if sid not in cil and done > 0.05:     # vrácení se nepovedlo – nezapomenout na něj
                    rows.append(self._vratka(sid, done, previous))
        if rows:
            total_g = round(sum(r["used_g"] or 0 for r in rows), 2)
            ms = [r["used_m"] for r in rows if r["used_m"] is not None]
            total_m = round(sum(ms), 2) if ms else None
        self.db.replace_filaments(session["id"], rows)
        self.db.update_session(session["id"], filament_g=total_g, filament_m=total_m, filament_source=source,
                               filament_is_estimate=is_est)
        return {"g": total_g, "m": total_m, "source": source, "is_estimate": is_est}
