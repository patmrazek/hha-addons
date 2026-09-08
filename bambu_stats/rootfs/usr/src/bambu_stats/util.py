"""Drobné pomocné funkce bez závislostí."""
import os
import time

_ULID_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def ulid(ts: float | None = None) -> str:
    """ULID – časově řaditelný identifikátor (26 znaků), bez externí knihovny."""
    ms = int((ts if ts is not None else time.time()) * 1000)
    out = []
    for _ in range(10):
        out.append(_ULID_ALPHABET[ms & 31])
        ms >>= 5
    rnd = int.from_bytes(os.urandom(10), "big")
    for _ in range(16):
        out.append(_ULID_ALPHABET[rnd & 31])
        rnd >>= 5
    return "".join(reversed(out))


MATERIAL_GROUPS = ("PLA", "PETG", "ABS", "ASA", "TPU")


def material_group(tray_type: str | None) -> str:
    """Zařadí typ filamentu z tiskárny (např. 'PLA-CF', 'PETG HF') do skupiny pro statistiky."""
    t = (tray_type or "").upper().replace(" ", "").replace("-", "").replace("_", "")
    if not t:
        return "other"
    for g in MATERIAL_GROUPS:
        if t.startswith(g):
            return g
    return "other"


def to_int(v, default=0) -> int:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def to_float(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def color_hex(cols) -> str | None:
    """Barva z tray_color/cols ('161616FF' → '#161616')."""
    if isinstance(cols, list):
        cols = cols[0] if cols else None
    if not cols or not isinstance(cols, str) or len(cols) < 6:
        return None
    return "#" + cols[:6].upper()
