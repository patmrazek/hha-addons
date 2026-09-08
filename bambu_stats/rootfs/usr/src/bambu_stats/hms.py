"""Dekódování HMS a print_error kódů Bambu Lab.

HMS položka z MQTT: {"attr": int, "code": int}. Textový kód ve tvaru Bambu wiki:
    f"{attr>>16:04X}_{attr&0xFFFF:04X}_{code>>16:04X}_{code&0xFFFF:04X}"  → např. 0500_0600_0002_0070
Závažnost (dle pybambu): (code >> 16) & 0xFFFF → 1 fatal, 2 serious, 3 common, 4 info.
Modul: (attr >> 24) & 0xFF.

print_error je celé číslo; známé kódy zrušení uživatelem (z tabulky ha-bambulab hms_error_text):
    0x0300400C (50348044) "The task was canceled."
    0x0500400E (83902478) "Printing was cancelled."
"""
from __future__ import annotations

CANCEL_CODES = frozenset({0x0300400C, 0x0500400E})

SEVERITY_NAMES = {1: "fatal", 2: "serious", 3: "common", 4: "info"}


def hms_code(attr: int, code: int) -> str:
    return f"{(attr >> 16) & 0xFFFF:04X}_{attr & 0xFFFF:04X}_{(code >> 16) & 0xFFFF:04X}_{code & 0xFFFF:04X}"


def severity(code: int) -> int:
    return (code >> 16) & 0xFFFF


def module(attr: int) -> int:
    return (attr >> 24) & 0xFF


def decode(item: dict) -> dict | None:
    try:
        attr, code = int(item.get("attr", 0)), int(item.get("code", 0))
    except (TypeError, ValueError):
        return None
    if not attr and not code:
        return None
    return {"attr": attr, "code_raw": code, "code": hms_code(attr, code),
            "severity": severity(code), "module": module(attr)}


def is_cancel(print_error: int | None) -> bool:
    return bool(print_error) and int(print_error) in CANCEL_CODES


def print_error_hex(print_error: int | None) -> str | None:
    return f"{int(print_error):08X}" if print_error else None
