"""Synchronizace historie tisků mezi lokalitami přes sdílený privátní git repozitář.

Tiskárna se stěhuje (Hacienda ↔ Pod Harfou) a v každé lokalitě běží vlastní instance add-onu s vlastní SQLite.
Aby byly statistiky všude stejné, každá instance:
  1. exportuje své uzavřené session (session + filamenty + pauzy) jako JSON řádky do
     `sessions/<serial>/<instance>.jsonl` – každá instance píše JEN svůj soubor → žádné konflikty,
  2. commitne a pushne,
  3. stáhne (`git pull --rebase`) a naimportuje řádky ostatních instancí (idempotentně podle session.id).
Agregace se pak počítají nad sloučenou DB. Telemetrie (`samples`) se nesynchronizuje – živý stav má smysl jen tam,
kde tiskárna stojí. Token (fine-grained PAT jen na tohle repo) je v options add-onu a nikdy se neloguje.
"""
from __future__ import annotations

import json
import logging
import subprocess
import time
from pathlib import Path

LOG = logging.getLogger("sync")
SESSION_EXPORT_COLS_SKIP = {"created_ts", "updated_ts"}


def _bez_casu(text: str) -> str:
    """Obsah bez řádku s časem exportu — aby se necommitovalo, když se nic věcného nezměnilo."""
    return "\n".join(r for r in (text or "").splitlines() if '"exported_ts"' not in r)


class GitSync:
    def __init__(self, db, repo_url: str, token: str, instance: str, data_dir: Path, branch: str = "main"):
        self.db, self.instance, self.branch = db, instance, branch
        self.dir = Path(data_dir) / "sync"
        self.repo_url = repo_url
        self._auth_url = repo_url.replace("https://", f"https://x-access-token:{token}@") if token else repo_url
        self.last_ok: float | None = None
        self.last_error: str | None = None
        self.imported_total = 0

    # --- git ------------------------------------------------------------------
    def _git(self, *args, check=True, timeout=120) -> subprocess.CompletedProcess:
        r = subprocess.run(["git", *args], cwd=self.dir if self.dir.exists() else None, capture_output=True, text=True, timeout=timeout)
        if check and r.returncode != 0:
            raise RuntimeError((r.stderr or r.stdout).replace(self._auth_url, "<repo>").strip()[:300])
        return r

    def ensure_repo(self):
        if (self.dir / ".git").exists():
            return
        self.dir.parent.mkdir(parents=True, exist_ok=True)
        r = subprocess.run(["git", "clone", "--quiet", "--branch", self.branch, self._auth_url, str(self.dir)], capture_output=True, text=True, timeout=180)
        if r.returncode != 0:
            # prázdné repo bez větve – založit
            subprocess.run(["git", "init", "--quiet", "-b", self.branch, str(self.dir)], check=True, capture_output=True)
            self._git("remote", "add", "origin", self._auth_url)
        self._git("config", "user.email", "bambu-stats@hha.local")
        self._git("config", "user.name", f"bambu_stats {self.instance}")
        self._git("config", "pull.rebase", "true")

    # --- export ---------------------------------------------------------------
    def _export_rows(self) -> int:
        rows = self.db.query("SELECT * FROM sessions WHERE ended_ts IS NOT NULL AND (synced_ts IS NULL OR synced_ts < updated_ts) AND (origin IS NULL OR origin=?)",
                             (self.instance,))
        if not rows:
            return 0
        by_serial: dict[str, list[dict]] = {}
        for s in rows:
            rec = {"v": 1, "instance": self.instance, "exported_ts": int(time.time()),
                   "session": {k: v for k, v in s.items() if k not in SESSION_EXPORT_COLS_SKIP and k not in ("synced_ts", "origin")},
                   "filaments": [{k: v for k, v in f.items() if k not in ("id", "session_id")} for f in self.db.filaments(s["id"])],
                   "pauses": [{k: v for k, v in p.items() if k not in ("id", "session_id")} for p in self.db.pauses(s["id"])]}
            by_serial.setdefault(s["printer_serial"], []).append(rec)
        for serial, recs in by_serial.items():
            path = self.dir / "sessions" / serial / f"{self.instance}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            existing: dict[str, str] = {}
            if path.exists():
                for line in path.read_text().splitlines():
                    if line.strip():
                        try:
                            existing[json.loads(line)["session"]["id"]] = line
                        except (ValueError, KeyError):
                            continue
            for rec in recs:
                existing[rec["session"]["id"]] = json.dumps(rec, ensure_ascii=False, sort_keys=True)
            path.write_text("\n".join(existing[k] for k in sorted(existing)) + "\n")
        now = int(time.time())
        for s in rows:
            self.db.update_session(s["id"], synced_ts=now, origin=self.instance)
        return len(rows)

    # --- import ---------------------------------------------------------------
    def _import_rows(self) -> int:
        n = 0
        for path in sorted((self.dir / "sessions").glob("*/*.jsonl")) if (self.dir / "sessions").exists() else []:
            if path.stem == self.instance:
                continue
            for line in path.read_text().splitlines():
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                    s = rec["session"]
                except (ValueError, KeyError):
                    continue
                cur = self.db.get_session(s["id"])
                if cur and (cur.get("updated_ts") or 0) >= (s.get("updated_ts") or rec.get("exported_ts") or 0) and cur.get("origin") == rec["instance"]:
                    continue
                if cur and cur.get("origin") in (None, self.instance):
                    continue  # naše vlastní session nikdy nepřepisovat cizí kopií
                s = {**s, "origin": rec["instance"], "synced_ts": rec.get("exported_ts"), "updated_ts": s.get("updated_ts") or rec.get("exported_ts"),
                     "created_ts": s.get("created_ts") or s.get("started_ts")}
                self.db.upsert_session(s)
                self.db.replace_filaments(s["id"], rec.get("filaments") or [])
                self.db.replace_pauses(s["id"], rec.get("pauses") or [])
                n += 1
        return n

    # --- provozní stav (údržba, silikagel, deník slotů, chyby) ------------------
    # Tisky nejsou všechno, co má být na obou místech stejné. Bez tohohle by po převozu
    # tiskárny počítadlo údržby začalo od nuly, silikagel by neměl od čeho počítat interval
    # a odečty ze cívek by sahaly na cívku, o které druhá lokalita neví.
    def _export_stav(self) -> int:
        serialy = {r["printer_serial"] for r in self.db.query("SELECT DISTINCT printer_serial FROM sessions")
                   if r.get("printer_serial")}
        serialy |= {r["serial"] for r in self.db.query("SELECT serial FROM printers") if r.get("serial")}
        zapsano = 0
        for serial in sorted(serialy):
            rec = {"v": 1, "instance": self.instance, "exported_ts": int(time.time()),
                   "meta": self.db.meta_pro_tiskarnu(serial),
                   "slot_spools": [{k: v for k, v in r.items() if k != "id"}
                                   for r in self.db.slot_spools_od(serial)],
                   "hms": [{k: v for k, v in r.items() if k != "id"} for r in self.db.hms_od(serial)]}
            if not (rec["meta"] or rec["slot_spools"] or rec["hms"]):
                continue
            path = self.dir / "stav" / serial / f"{self.instance}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            novy = json.dumps(rec, ensure_ascii=False, sort_keys=True, indent=1)
            stary = path.read_text() if path.exists() else ""
            # `exported_ts` se mění pokaždé — porovnává se zbytek, ať se necommituje prázdná změna
            if _bez_casu(stary) != _bez_casu(novy):
                path.write_text(novy + "\n")
                zapsano += 1
        return zapsano

    def _import_stav(self) -> int:
        zmeny = 0
        for path in sorted((self.dir / "stav").glob("*/*.json")) if (self.dir / "stav").exists() else []:
            if path.stem == self.instance:
                continue          # vlastní soubor zpátky nečteme
            try:
                rec = json.loads(path.read_text())
            except ValueError:
                continue
            serial = path.parent.name
            zmeny += self.db.sluc_meta(serial, rec.get("meta") or {})
            zmeny += self.db.sluc_slot_spools(rec.get("slot_spools") or [])
            zmeny += self.db.sluc_hms(rec.get("hms") or [])
        return zmeny

    # --- cyklus -----------------------------------------------------------------
    def run_once(self) -> dict:
        try:
            self.ensure_repo()
            exported = self._export_rows()
            stav = self._export_stav()
            self._git("add", "-A")
            if self._git("status", "--porcelain").stdout.strip():
                popis = f"+{exported} session" + (f", stav {stav}x" if stav else "")
                self._git("commit", "--quiet", "-m", f"{self.instance}: {popis}")
            # pull + push s jedním opakováním
            for attempt in range(2):
                r = self._git("pull", "--quiet", "--rebase", "origin", self.branch, check=False)
                if r.returncode != 0 and "couldn't find remote ref" not in (r.stderr or "").lower():
                    LOG.debug("pull: %s", r.stderr.strip()[:200])
                p = self._git("push", "--quiet", "-u", "origin", self.branch, check=False)
                if p.returncode == 0:
                    break
                if attempt == 1:
                    raise RuntimeError(("push selhal: " + (p.stderr or "")).replace(self._auth_url, "<repo>")[:200])
            imported = self._import_rows()
            stav_in = self._import_stav()
            self.imported_total += imported
            self.last_ok, self.last_error = time.time(), None
            if exported or imported or stav_in:
                LOG.info("sync: export %d, import %d session, převzato %d změn provozního stavu",
                         exported, imported, stav_in)
            return {"ok": True, "exported": exported, "imported": imported, "stav": stav_in}
        except Exception as e:
            self.last_error = str(e)[:200]
            LOG.warning("sync selhal: %s", self.last_error)
            return {"ok": False, "error": self.last_error}
