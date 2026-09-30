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


# Systémové úlohy firmwaru (kalibrace) spouští tiskárna z tohoto adresáře.
SYSTEM_GCODE_PREFIX = "/usr/etc/print/"


def je_systemova(row: dict) -> bool:
    """Session systémové úlohy (kalibrace) pro výpočty a přehledy: stačí JEDNO z polí.

    Stavový automat chce pro vyřazení z evidence obě pole (state_machine.je_systemova_uloha),
    protože to je nevratné. Tady jde jen o to, čemu u session věřit – a kalibrace s jedním
    zastaralým polem má v cloud entitě pořád hmotnost předchozího tisku.
    """
    return (row.get("print_type") or "") == "system" or (row.get("gcode_file") or "").startswith(SYSTEM_GCODE_PREFIX)


def je_void(row: dict) -> bool:
    """Session, která není tiskem: systémová úloha, nebo přípravná session nahrazená skutečnou
    dřív, než se začalo tisknout (superseded s 0 % do 20 min). Totéž v SQL je VOID_SQL.

    Void se vynechává jen z prezentace (statistiky, historie, last_print, cíl příkazů bez ID) –
    ledger odečtů, fronta doúčtování, sync ani CSV ho neřeší (M10a)."""
    return je_systemova(row) or (row.get("end_source") == "superseded" and (row.get("last_percent") or 0) == 0
                                 and (row.get("duration_s") or 0) <= 1200)


# NULL-safe: každý sloupec přes COALESCE. Holé `end_source = 'superseded'` dá u NULL hodnotu NULL a `NOT (…)`
# pak session vyřadí z obou množin – tisk bez end_source by nebyl ani void, ani tisk. GLOB místo LIKE,
# protože LIKE v SQLite nerozlišuje velikost písmen a Python startswith ano.
VOID_SQL = ("(COALESCE(print_type, '') = 'system' OR COALESCE(gcode_file, '') GLOB '" + SYSTEM_GCODE_PREFIX + "*'"
            " OR (COALESCE(end_source, '') = 'superseded' AND COALESCE(last_percent, 0) = 0"
            " AND COALESCE(duration_s, 0) <= 1200))")


def je_vlastni(row: dict, instance: str | None) -> bool:
    """Session zapsaná touto instancí: bez origin (ještě neexportovaná), nebo origin = sync_instance.

    Cizí session přišla gitem z druhé lokality – opravuje ji a do Spoolmanu za ni zapisuje jen ta.
    Bez sync_instance je vlastní jen session bez origin (stejně jako ve frontě doúčtování).
    """
    origin = row.get("origin")
    return origin is None or (bool(instance) and origin == instance)


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
