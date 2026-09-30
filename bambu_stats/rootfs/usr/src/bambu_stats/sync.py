"""Synchronizace historie tisků mezi lokalitami přes sdílený privátní git repozitář.

Tiskárna se stěhuje (Hacienda ↔ Pod Harfou) a v každé lokalitě běží vlastní instance add-onu s vlastní SQLite.
Aby byly statistiky všude stejné, každá instance:
  1. exportuje své uzavřené session (session + filamenty + pauzy) jako JSON řádky do
     `sessions/<serial>/<instance>.jsonl` – každá instance píše JEN svůj soubor → žádné konflikty,
  2. commitne a pushne,
  3. stáhne (`git pull --rebase`) a naimportuje řádky ostatních instancí (idempotentně podle session.id).
Agregace se pak počítají nad sloučenou DB. Telemetrie (`samples`) se nesynchronizuje – živý stav má smysl jen tam,
kde tiskárna stojí. Token (fine-grained PAT jen na tohle repo) je v options add-onu a nikdy se neloguje.

Formát souborů je společný pro všechny verze (0.18.2 i 0.19.0 čtou export té druhé): importér bere jako verzi
záznamu `exported_ts`, neznámé klíče zahodí. Proto se řádek beze věcné změny nepřepisuje (nový `exported_ts`
= nový import na druhé straně) a nové sloupce smí přibýt jen v tabulce sessions.
"""
from __future__ import annotations

import json
import logging
import shutil
import subprocess
import threading
import time
from pathlib import Path

LOG = logging.getLogger("sync")
SESSION_EXPORT_COLS_SKIP = {"created_ts", "updated_ts"}
GIT_TIMEOUT_S = 120             # místní příkazy (add, commit, status, rebase…)
CLONE_TIMEOUT_S = 180
LS_REMOTE_TIMEOUT_S = 60
PULL_PUSH_TIMEOUT_S = 45
# síťové příkazy: zaseknuté spojení (TCP bez dat) git ukončí sám po 30 s, ne až po timeoutu procesu
GIT_SIT = ("-c", "http.lowSpeedLimit=1000", "-c", "http.lowSpeedTime=30")
ZAMEK_CEKANI_S = 300            # _finalize počká na běžící kolo nejvýš tolik (plánovač nečeká vůbec)
CHYBA_IMPORTU_LOG_S = 3600      # chyba importu téže session se loguje nejvýš jednou za hodinu
_V_REPU = object()              # _git: cwd = adresář repa, když existuje


def _bez_null(d: dict | None) -> dict:
    return {k: v for k, v in (d or {}).items() if v is not None}


def _vecny_obsah(rec: dict) -> dict:
    """Záznam exportu session bez času exportu – podle toho se pozná, jestli se session věcně změnila.

    Chybějící klíč a null jsou totéž: řádek zapsaný verzí 0.18.2 nemá manual_plan_g a kdyby se kvůli
    tomu přepsala každá session, druhá strana by ji celou naimportovala znovu (nový exported_ts)."""
    return {**{k: v for k, v in rec.items() if k != "exported_ts"},
            "session": _bez_null(rec.get("session")),
            "filaments": [_bez_null(f) for f in rec.get("filaments") or []],
            "pauses": [_bez_null(p) for p in rec.get("pauses") or []]}


def _stav_vecne(rec: dict) -> dict:
    """Provozní stav bez času exportu a bez čítače a posledního výskytu AKTIVNÍCH HMS: ty se mění pořád
    (tiskárna hlásí aktivní chybu v každé zprávě) a druhá strana je stejně nepřebírá (INSERT OR IGNORE).
    Dřív kvůli nim šel commit každých 10 min (SYNC-1)."""
    return {**{k: v for k, v in rec.items() if k != "exported_ts"},
            "hms": [{k: v for k, v in (h or {}).items()
                     if not ((h or {}).get("cleared_ts") is None and k in ("count", "last_ts"))}
                    for h in rec.get("hms") or []]}


def _nazev_prikazu(args: tuple) -> str:
    """Jméno příkazu gitu pro hlášku bez argv (argv clone/ls-remote nese token)."""
    i = 0
    while i < len(args) and (args[i] == "-c" or str(args[i]).startswith("-")):
        i += 2 if args[i] == "-c" else 1
    if i >= len(args):
        return "git"
    return f"{args[i]} {args[i + 1]}" if args[i] == "remote" and i + 1 < len(args) else str(args[i])


class GitSync:
    def __init__(self, db, repo_url: str, token: str, instance: str, data_dir: Path, branch: str = "main"):
        self.db, self.instance, self.branch = db, instance, branch
        self.dir = Path(data_dir) / "sync"
        self.repo_url = repo_url
        self._token = token or ""
        self._auth_url = repo_url.replace("https://", f"https://x-access-token:{token}@") if token else repo_url
        self.last_ok: float | None = None
        self.last_error: str | None = None
        self.imported_total = 0
        self.import_errors = 0                  # záznamy, které v posledním kole nešly naimportovat
        self._import_chyba: str | None = None   # …a první z chyb (do last_error)
        self._chyby_log: dict[str, float] = {}  # session → kdy se její chyba importu naposledy logovala
        # Naráz jen jedno kolo: plánovač (vlastní vlákno) i _finalize po konci tisku (SYNC-9)
        self._zamek = threading.Lock()
        # Zastaralý zámek indexu po zabitém gitu (restart kontejneru, SIGKILL při timeoutu) by blokoval
        # každé `git add` až do ručního zásahu. Adresář používá jen tenhle proces a git v něm ještě neběží.
        zamek_indexu = self.dir / ".git" / "index.lock"
        try:
            if zamek_indexu.exists():
                zamek_indexu.unlink()
                LOG.warning("sync: smazán zastaralý %s", zamek_indexu)
        except OSError as e:
            LOG.warning("sync: zastaralý %s nejde smazat: %s", zamek_indexu, e)

    # --- git ------------------------------------------------------------------
    def _sanitize(self, text: str | None) -> str:
        """Text chyby bez tokenu: celá adresa s tokenem i token samotný (třeba v adrese jiného tvaru).
        Volá se vždy PŘED ořezem – ořez by jinak mohl nechat kus tokenu."""
        text = (text or "").replace(self._auth_url, "<repo>")
        return text.replace(self._token, "***") if self._token else text

    def _git(self, *args, check=True, timeout=GIT_TIMEOUT_S, sit=False, cwd=_V_REPU) -> subprocess.CompletedProcess:
        """Jediné místo, kudy se volá git. Timeout → RuntimeError('<příkaz>: timeout') bez argv
        (str(TimeoutExpired) obsahuje celý příkaz i s tokenem, SYNC-5), text chyby bez tokenu."""
        if cwd is _V_REPU:
            if not self.dir.exists():
                # bez cwd by git sáhl na repo, ve kterém proces zrovna stojí
                raise RuntimeError(f"{_nazev_prikazu(args)}: adresář {self.dir} neexistuje")
            cwd = self.dir
        argv = ["git", *(GIT_SIT if sit else ()), *args]
        try:
            r = subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"{_nazev_prikazu(args)}: timeout") from None
        if check and r.returncode != 0:
            raise RuntimeError(self._sanitize((r.stderr or r.stdout or "").strip())[:300])
        return r

    def ensure_repo(self):
        """Pracovní kopie repa v /data/sync: clone, nad prázdným repem init.

        Nad existujícím repem vždy `remote set-url` (po rotaci tokenu by git dál bral starý z .git/config)
        a config autora – clone přerušený zabitím procesu ho nemá a commit by trvale padal."""
        if (self.dir / ".git").exists() and self._preruseny_klon():
            LOG.warning("sync: %s nemá jediný commit ani soubor a remote data má (přerušený clone?) – "
                        "klonuji znovu", self.dir)
            shutil.rmtree(self.dir, ignore_errors=True)
        if not (self.dir / ".git").exists():
            self.dir.parent.mkdir(parents=True, exist_ok=True)
            self._klonuj()
        if self._git("remote", "set-url", "origin", self._auth_url, check=False).returncode != 0:
            self._git("remote", "add", "origin", self._auth_url)     # init, po kterém remote add nedoběhl
        self._git("config", "user.email", "bambu-stats@hha.local")
        self._git("config", "user.name", f"bambu_stats {self.instance}")
        self._git("config", "pull.rebase", "true")

    def _preruseny_klon(self) -> bool:
        """.git bez commitu (HEAD neukazuje nikam) a bez souborů v pracovním stromu, zatímco remote větve
        má: clone skončil dřív, než stihl cokoli rozbalit. Čerstvý init nad prázdným repem rozbitý není
        (remote bez větví) a repo se soubory se nemaže nikdy (nevyexportovaná data)."""
        if self._git("rev-parse", "--verify", "-q", "HEAD", check=False).returncode == 0:
            return False
        if any(p.name != ".git" for p in self.dir.iterdir()):
            return False
        try:
            r = self._git("ls-remote", "--heads", self._auth_url, check=False, timeout=LS_REMOTE_TIMEOUT_S,
                          sit=True, cwd=None)
        except RuntimeError:
            return False            # síť nejede – rozhodne se v dalším kole
        return r.returncode == 0 and bool(r.stdout.strip())

    def _klonuj(self):
        """Clone; když selže, `git init` jen nad prokazatelně prázdným repem (ls-remote bez větví).

        Dřív init následoval po JAKÉKOLI chybě clone (i po chvilkovém výpadku sítě po bootu): vznikla
        nesouvisející historie a rebase pak sync trvale rozbil (SYNC-4). Po timeoutu zůstává po gitu
        zabitém SIGKILL rozpracovaný .git – smaže se, další kolo zkusí clone znovu."""
        try:
            r = self._git("clone", "--quiet", "--branch", self.branch, self._auth_url, str(self.dir),
                          check=False, timeout=CLONE_TIMEOUT_S, sit=True, cwd=None)
            if r.returncode == 0:
                return
            chyba = (r.stderr or r.stdout or "").strip()
            heads = self._git("ls-remote", "--heads", self._auth_url, check=False, timeout=LS_REMOTE_TIMEOUT_S,
                              sit=True, cwd=None)
        except RuntimeError:
            shutil.rmtree(self.dir, ignore_errors=True)
            raise
        if heads.returncode != 0 or heads.stdout.strip():
            shutil.rmtree(self.dir, ignore_errors=True)
            raise RuntimeError("clone selhal: " + self._sanitize(chyba))
        LOG.info("sync: repo je prázdné – zakládám větev %s", self.branch)
        self._git("init", "--quiet", "-b", self.branch, str(self.dir), cwd=None)
        self._git("remote", "add", "origin", self._auth_url)

    def _uklid_rebase(self):
        """Visící rebase (pull --rebase skončil konfliktem, git zabitý timeoutem) zruší. HEAD by jinak
        zůstala odpojená, každé další kolo by selhalo a export by přepsal soubory s konfliktními značkami.
        Lokální commity zůstanou – rebase --abort vrátí větev do stavu před pullem."""
        g = self.dir / ".git"
        if not ((g / "rebase-merge").exists() or (g / "rebase-apply").exists()):
            return
        r = self._git("rebase", "--abort", check=False)
        if r.returncode == 0:
            LOG.warning("sync: nedokončený rebase zrušen (rebase --abort)")
            return
        LOG.warning("sync: rebase --abort selhal (%s) – rebase --quit a checkout -f %s",
                    self._sanitize((r.stderr or r.stdout or "").strip())[:200], self.branch)
        self._git("rebase", "--quit", check=False)
        self._git("checkout", "-f", self.branch, check=False)

    # --- export ---------------------------------------------------------------
    def _export_rows(self) -> int:
        """Vyexportuje změněné vlastní session. Vrátí počet session, jejichž řádek se opravdu změnil.

        Řádek beze věcné změny zůstává i se svým exported_ts (M8a): importér obou verzí bere exported_ts
        jako verzi záznamu, takže nový čas by znamenal import na druhé straně a commit každých 10 min
        (SYNC-1, session M2QWSNQF z fronty doúčtování). Soubor se zapisuje, jen když se změnil aspoň
        jeden řádek."""
        rows = self.db.query("SELECT * FROM sessions WHERE ended_ts IS NOT NULL AND (synced_ts IS NULL OR synced_ts < updated_ts) AND (origin IS NULL OR origin=?)",
                             (self.instance,))
        if not rows:
            return 0
        by_serial: dict[str, list[dict]] = {}
        for s in rows:
            by_serial.setdefault(s["printer_serial"], []).append(s)
        zmeneno = 0
        for serial, sessions in by_serial.items():
            path = self.dir / "sessions" / serial / f"{self.instance}.jsonl"
            existing: dict[str, str] = {}
            if path.exists():
                for line in path.read_text().splitlines():
                    if line.strip():
                        try:
                            existing[json.loads(line)["session"]["id"]] = line
                        except (ValueError, KeyError, TypeError):
                            continue
            ted = int(time.time())
            zmena = False
            for s in sessions:
                rec = {"v": 1, "instance": self.instance,
                       "session": {k: v for k, v in s.items() if k not in SESSION_EXPORT_COLS_SKIP and k not in ("synced_ts", "origin")},
                       "filaments": [{k: v for k, v in f.items() if k not in ("id", "session_id")} for f in self.db.filaments(s["id"])],
                       "pauses": [{k: v for k, v in p.items() if k not in ("id", "session_id")} for p in self.db.pauses(s["id"])]}
                stary = None
                if s["id"] in existing:
                    try:
                        stary = json.loads(existing[s["id"]])
                    except ValueError:
                        stary = None
                if isinstance(stary, dict) and _vecny_obsah(stary) == _vecny_obsah(rec):
                    continue
                # exported_ts je verze záznamu: nový musí být vyšší než předchozí, i když jde o tutéž sekundu
                try:
                    predchozi = int((stary or {}).get("exported_ts") or 0)
                except (TypeError, ValueError, AttributeError):
                    predchozi = 0
                rec["exported_ts"] = max(ted, predchozi + 1)
                existing[s["id"]] = json.dumps(rec, ensure_ascii=False, sort_keys=True)
                zmeneno += 1
                zmena = True
            if zmena:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("\n".join(existing[k] for k in sorted(existing)) + "\n")
        for s in rows:
            # bez posunu updated_ts a jen když se session od SELECTu nezměnila – jinak zůstane kandidátem
            if not self.db.oznac_synced(s["id"], s.get("updated_ts"), self.instance):
                LOG.debug("sync: session %s se během exportu změnila – vyexportuje se v příštím kole", s["id"][-8:])
        return zmeneno

    # --- import ---------------------------------------------------------------
    def _import_rows(self) -> int:
        """Naimportuje session ostatních instancí. Každý záznam zvlášť a celý (Database.import_session):
        chyba jednoho záznamu se započítá do import_errors a pokračuje se dalším (M8c)."""
        n = 0
        self.import_errors, self._import_chyba = 0, None
        for path in sorted((self.dir / "sessions").glob("*/*.jsonl")) if (self.dir / "sessions").exists() else []:
            if path.stem == self.instance:
                continue
            try:
                lines = path.read_text().splitlines()
            except OSError as e:
                self._chyba_importu(path.name, e)
                continue
            for line in lines:
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                    s = rec["session"]
                except (ValueError, KeyError, TypeError):
                    continue
                try:
                    if self._importuj(rec, s):
                        n += 1
                except Exception as e:
                    self._chyba_importu(str((s or {}).get("id") if isinstance(s, dict) else "?"), e)
        return n

    def _importuj(self, rec: dict, s: dict) -> bool:
        cur = self.db.get_session(s["id"])
        if cur and (cur.get("updated_ts") or 0) >= (s.get("updated_ts") or rec.get("exported_ts") or 0) and cur.get("origin") == rec["instance"]:
            return False
        if cur and cur.get("origin") in (None, self.instance):
            return False  # naše vlastní session nikdy nepřepisovat cizí kopií
        s = {**s, "origin": rec["instance"], "synced_ts": rec.get("exported_ts"), "updated_ts": s.get("updated_ts") or rec.get("exported_ts"),
             "created_ts": s.get("created_ts") or s.get("started_ts")}
        self.db.import_session(s, rec.get("filaments") or [], rec.get("pauses") or [])
        return True

    def _chyba_importu(self, sid: str, e: Exception):
        self.import_errors += 1
        text = self._sanitize(f"{type(e).__name__}: {e}")
        if self._import_chyba is None:
            self._import_chyba = f"{sid[-8:]}: {text}"
        ted = time.time()
        if ted - self._chyby_log.get(sid, 0) >= CHYBA_IMPORTU_LOG_S:
            self._chyby_log[sid] = ted
            LOG.warning("sync: import session %s selhal (%s) – pokračuji dalším záznamem", sid, text[:200])

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
            stary = None
            if path.exists():
                try:
                    stary = json.loads(path.read_text())
                except (ValueError, OSError):
                    stary = None     # nečitelný (třeba konfliktní značky) → přepsat
            # Porovnává se naparsovaný obsah bez exported_ts a bez čítačů aktivních HMS. Když se liší cokoli
            # jiného, zapíše se soubor celý, i s aktuálními count/last_ts (pole se ze souboru nevypouští).
            if isinstance(stary, dict) and _stav_vecne(stary) == _stav_vecne(rec):
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(rec, ensure_ascii=False, sort_keys=True, indent=1) + "\n")
            zapsano += 1
        return zapsano

    def _import_stav(self) -> int:
        zmeny = 0
        for path in sorted((self.dir / "stav").glob("*/*.json")) if (self.dir / "stav").exists() else []:
            if path.stem == self.instance:
                continue          # vlastní soubor zpátky nečteme
            try:
                rec = json.loads(path.read_text())
            except (ValueError, OSError):
                continue
            if not isinstance(rec, dict):
                continue
            serial = path.parent.name
            zmeny += self.db.sluc_meta(serial, rec.get("meta") or {})
            zmeny += self.db.sluc_slot_spools(rec.get("slot_spools") or [])
            zmeny += self.db.sluc_hms(rec.get("hms") or [])
        return zmeny

    # --- cyklus -----------------------------------------------------------------
    def run_once(self, blocking: bool = False, timeout: float = ZAMEK_CEKANI_S) -> dict:
        """Jedno kolo syncu; naráz běží jen jedno (M8d). Plánovač při běžícím kole nečeká (blocking=False),
        _finalize počká nejvýš timeout. Přeskočené kolo vrátí {'ok': True, 'skipped': True} a nemění
        last_ok ani last_error."""
        ziskano = self._zamek.acquire(timeout=timeout) if blocking else self._zamek.acquire(blocking=False)
        if not ziskano:
            LOG.debug("sync: předchozí kolo ještě běží – přeskakuji")
            return {"ok": True, "skipped": True}
        try:
            return self._kolo()
        finally:
            self._zamek.release()

    def _kolo(self) -> dict:
        try:
            self.ensure_repo()
            self._uklid_rebase()            # před exportem – jinak by přepsal soubory s konfliktními značkami
            exported = self._export_rows()
            stav = self._export_stav()
            self._git("add", "-A")
            if self._git("status", "--porcelain").stdout.strip():
                popis = f"+{exported} session" + (f", stav {stav}x" if stav else "")
                self._git("commit", "--quiet", "-m", f"{self.instance}: {popis}")
            # pull + push s jedním opakováním
            pull_chyba = None
            for attempt in range(2):
                r = self._git("pull", "--quiet", "--rebase", "origin", self.branch, check=False,
                              timeout=PULL_PUSH_TIMEOUT_S, sit=True)
                pull_chyba = None
                if r.returncode != 0 and "couldn't find remote ref" not in (r.stderr or "").lower():
                    pull_chyba = self._sanitize((r.stderr or r.stdout or "").strip()) or f"návratový kód {r.returncode}"
                    LOG.warning("sync: pull selhal: %s", pull_chyba[:200])
                    self._uklid_rebase()
                p = self._git("push", "--quiet", "-u", "origin", self.branch, check=False,
                              timeout=PULL_PUSH_TIMEOUT_S, sit=True)
                if p.returncode == 0:
                    break
                if attempt == 1:
                    raise RuntimeError("push selhal: " + self._sanitize(p.stderr or ""))
            imported = self._import_rows()
            stav_in = self._import_stav()
            self.imported_total += imported
            if exported or imported or stav_in:
                LOG.info("sync: export %d, import %d session, převzato %d změn provozního stavu",
                         exported, imported, stav_in)
            vysledek = {"exported": exported, "imported": imported, "stav": stav_in,
                        "import_errors": self.import_errors}
            if pull_chyba:
                # Nestažené změny druhé lokality = sync neproběhl, i když push nic nepushnul. last_ok se
                # neposouvá, ať to dohled „sync stojí“ vidí (M8d bod 6).
                self.last_error = ("pull selhal: " + pull_chyba)[:200]
                return {"ok": False, "error": self.last_error, **vysledek}
            self.last_ok = time.time()
            # neimportovatelný záznam je vidět v atributech (sync.error, sync.import_errors), sync ale jede dál
            self.last_error = (f"import: {self.import_errors}× chyba – {self._import_chyba}"[:200]
                               if self.import_errors else None)
            return {"ok": True, **vysledek}
        except Exception as e:
            self.last_error = self._sanitize(str(e))[:200]
            LOG.warning("sync selhal: %s", self.last_error)
            return {"ok": False, "error": self.last_error}
