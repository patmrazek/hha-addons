"""Stažení 3MF z tiskárny přes implicitní FTPS (port 990) a přečtení slicer metadat.

Bambu tiskárny vyžadují TLS session reuse na datovém kanálu – standardní ftplib.FTP_TLS
to neumí, proto podtřída (vzor: ha-bambulab pybambu/bambu_client.py). Na P2S je interní
úložiště přes FTP prázdné; soubory jsou vidět jen s zapnutým „Store Sent Files on External
Storage" a připojeným USB diskem.

`Metadata/slice_info.config` (XML):
  <plate>
    <metadata key="index" value="1"/>  <metadata key="prediction" value="5935"/>  <metadata key="weight" value="20.91"/>
    <filament id="1" tray_info_idx="GFA01" type="PLA" color="#000000" used_m="5.45" used_g="17.32"/>
"""
from __future__ import annotations

import ftplib
import io
import logging
import re
import socket
import ssl
import zipfile
from dataclasses import dataclass, field
from xml.etree import ElementTree

LOG = logging.getLogger("threemf")
# '/cache' před '/' jako v ha-bambulab: stejné jméno bývá v obou (25. 9. 2026 „Cube 2 + …" deska 2
# v '/cache', deska 3 ve '/') a novější úlohy leží v '/cache'. Řazení podle času souboru (MDTM/MLSD)
# se přidá až po ověření výpisem FTP na P2S.
SEARCH_DIRS = ("/cache", "/", "/sdcard", "/usb", "/mnt/usb", "/media/usb", "/model", "/data")


class ImplicitFTP_TLS(ftplib.FTP_TLS):
    """FTP_TLS pro implicitní TLS (port 990) se sdílenou TLS session pro datový kanál."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sock = None

    @property
    def sock(self):
        return self._sock

    @sock.setter
    def sock(self, value):
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value

    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(conn, server_hostname=self.host, session=self.sock.session)
        return conn, size


@dataclass
class FilamentUse:
    idx: int
    tray_info_idx: str
    type: str
    color: str | None
    used_m: float
    used_g: float


@dataclass
class PlateInfo:
    index: int
    prediction_s: int | None
    weight_g: float | None
    filaments: list[FilamentUse] = field(default_factory=list)


def parse_slice_info(xml_bytes: bytes) -> list[PlateInfo]:
    root = ElementTree.fromstring(xml_bytes)
    plates = []
    for plate in root.iter("plate"):
        meta = {m.get("key"): m.get("value") for m in plate.findall("metadata")}
        try:
            idx = int(meta.get("index", "1"))
        except ValueError:
            idx = 1
        pred = meta.get("prediction")
        weight = meta.get("weight")
        fils = []
        for f in plate.findall("filament"):
            try:
                fils.append(FilamentUse(idx=int(f.get("id", "0")), tray_info_idx=f.get("tray_info_idx") or "",
                                        type=f.get("type") or "", color=(f.get("color") or None),
                                        used_m=float(f.get("used_m") or 0), used_g=float(f.get("used_g") or 0)))
            except ValueError:
                continue
        plates.append(PlateInfo(index=idx, prediction_s=int(float(pred)) if pred else None,
                                weight_g=float(weight) if weight else None, filaments=fils))
    return plates


def explicitni_deska(gcode_file: str) -> int | None:
    """Číslo desky, jen když ho gcode_file opravdu uvádí (…/plate_N.gcode), jinak None."""
    m = re.search(r"plate_(\d+)\.gcode", gcode_file or "")
    return int(m.group(1)) if m else None


def plate_index_from_gcode(gcode_file: str) -> int:
    deska = explicitni_deska(gcode_file)
    return deska if deska is not None else 1


class PrinterFTP:
    def __init__(self, host: str, access_code: str, timeout: int = 15):
        self.host, self._code, self.timeout = host, access_code, timeout

    def _connect(self) -> ImplicitFTP_TLS:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ftp = ImplicitFTP_TLS(context=ctx, timeout=self.timeout)
        ftp.connect(self.host, 990)
        ftp.login("bblp", self._code)
        ftp.prot_p()
        return ftp

    def list_dirs(self, dirs=SEARCH_DIRS) -> dict[str, list[str]]:
        out = {}
        ftp = self._connect()
        try:
            for d in dirs:
                try:
                    out[d] = ftp.nlst(d)
                except (ftplib.error_perm, ftplib.error_temp, OSError):
                    out[d] = []
        finally:
            ftp.quit()
        return out

    def find_3mf(self, name: str) -> list[str]:
        """Všechny 3MF, které k úloze (subtask_name) patří PŘESNĚ jménem, v pořadí, v jakém je zkoušet.

        Shoda podle začátku jména se nepoužívá: 25. 9. 2026 dostaly tisky „Cube" 3MF projektu
        „Cube 2 + Cube 2 + …" a z cívky odešlo 54,36 g místo 8,81 g. `{name}.gcode.3mf` (vyslicovaná
        úloha) má přednost před `{name}.3mf` (projekt zkopírovaný na USB může nést starý slice_info),
        holé jméno jen tehdy, když samo končí na .3mf. Pak rozhoduje pořadí adresářů (SEARCH_DIRS).
        Vrací se všichni kandidáti – stejné jméno bývá ve více adresářích a desku z nich vybere fetch_plate.
        """
        jmena = [f"{name}.gcode.3mf", f"{name}.3mf"]
        if name.lower().endswith(".3mf"):
            jmena.append(name)
        nalezene: dict[str, tuple[int, int]] = {}
        ftp = self._connect()
        try:
            for poradi, d in enumerate(SEARCH_DIRS):
                try:
                    entries = ftp.nlst(d)
                except (ftplib.error_perm, ftplib.error_temp, OSError):
                    continue
                for e in entries:
                    base = e.rsplit("/", 1)[-1]
                    if base in jmena:
                        cesta = e if e.startswith("/") else f"{d.rstrip('/')}/{base}"
                        nalezene.setdefault(cesta, (jmena.index(base), poradi))
        finally:
            ftp.quit()
        return sorted(nalezene, key=nalezene.__getitem__)

    def read_slice_info(self, path: str) -> bytes | None:
        """Stáhne 3MF (je to ZIP) a vrátí obsah Metadata/slice_info.config."""
        buf = io.BytesIO()
        ftp = self._connect()
        try:
            ftp.retrbinary(f"RETR {path}", buf.write)
        finally:
            ftp.quit()
        buf.seek(0)
        try:
            with zipfile.ZipFile(buf) as z:
                for n in z.namelist():
                    if n.endswith("slice_info.config"):
                        return z.read(n)
        except zipfile.BadZipFile:
            LOG.warning("%s není platný 3MF/ZIP", path)
        return None


def fetch_plate(host: str, access_code: str, subtask_name: str, gcode_file: str) -> tuple[str, PlateInfo | None, str | None]:
    """Vrátí (status, plate, path). status ∈ ok|not_found|mismatch|error.

    Kandidáty z find_3mf zkouší po řadě. Když gcode_file uvádí desku (plate_N), platí první 3MF,
    který tu desku obsahuje – tiskárna umí nechat pod stejným jménem soubor s jinou deskou nebo
    starší verzi projektu. Nemá-li ji žádný, vrátí konečný stav 'mismatch' (cesta prvního kandidáta):
    dřív se vzala první deska souboru a s ní cizí hmotnost i čas (Cube: deska 3 místo 5, 54,36 g).
    Bez plate_N v názvu (kalibrace, tisk ze SD) zůstává dosavadní výběr: deska 1, jinak první deska.
    """
    try:
        ftp = PrinterFTP(host, access_code)
        paths = ftp.find_3mf(subtask_name)
        if not paths:
            return "not_found", None, None
        want = explicitni_deska(gcode_file)
        precteno = False
        for path in paths:
            xml = ftp.read_slice_info(path)
            if not xml:
                continue
            precteno = True
            plates = parse_slice_info(xml)
            if want is None:
                plate = next((p for p in plates if p.index == 1), plates[0] if plates else None)
            else:
                plate = next((p for p in plates if p.index == want), None)
            if plate:
                return "ok", plate, path
        if want is not None and precteno:
            LOG.info("3MF k úloze %s nemá desku %d (%s) – nepoužije se", subtask_name, want, ", ".join(paths))
            return "mismatch", None, paths[0]
        return "error", None, paths[0]
    # ftplib.all_errors je sama n-tice – vnořená do další ji Python 3.12 odmítne až ve chvíli,
    # kdy výjimka opravdu nastane (TypeError místo tichého přeskočení). Proto rozbalit.
    except (OSError, ssl.SSLError, socket.timeout, *ftplib.all_errors) as e:
        LOG.info("FTPS %s: %s", host, e)
        return "error", None, None
