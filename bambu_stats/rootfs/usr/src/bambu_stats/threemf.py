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
SEARCH_DIRS = ("/", "/cache", "/sdcard", "/usb", "/mnt/usb", "/media/usb", "/model", "/data")


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


def plate_index_from_gcode(gcode_file: str) -> int:
    m = re.search(r"plate_(\d+)\.gcode", gcode_file or "")
    return int(m.group(1)) if m else 1


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

    def find_3mf(self, name: str) -> str | None:
        """Najde .3mf soubor pro název úlohy (subtask_name); vrátí cestu nebo None."""
        want = {f"{name}.3mf", f"{name}.gcode.3mf", name}
        ftp = self._connect()
        try:
            for d in SEARCH_DIRS:
                try:
                    entries = ftp.nlst(d)
                except (ftplib.error_perm, ftplib.error_temp, OSError):
                    continue
                for e in entries:
                    base = e.rsplit("/", 1)[-1]
                    if base in want or (base.lower().endswith(".3mf") and base.lower().startswith(name.lower())):
                        return e if e.startswith("/") else f"{d.rstrip('/')}/{base}"
        finally:
            ftp.quit()
        return None

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
    """Vrátí (status, plate, path). status ∈ ok|not_found|error."""
    try:
        ftp = PrinterFTP(host, access_code)
        path = ftp.find_3mf(subtask_name)
        if not path:
            return "not_found", None, None
        xml = ftp.read_slice_info(path)
        if not xml:
            return "error", None, path
        plates = parse_slice_info(xml)
        want = plate_index_from_gcode(gcode_file)
        plate = next((p for p in plates if p.index == want), plates[0] if plates else None)
        return ("ok" if plate else "error"), plate, path
    except (OSError, ssl.SSLError, ftplib.all_errors, socket.timeout) as e:
        LOG.info("FTPS %s: %s", host, e)
        return "error", None, None
