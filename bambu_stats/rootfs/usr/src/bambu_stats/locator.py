"""Najde tiskárnu napříč lokalitami a rozhodne, která instance ji má sledovat.

Tiskárna cestuje mezi lokalitami a add-on běží na obou. Přes VPN na ni ale dosáhnou obě
instance naráz — a kdyby sbíraly obě, vznikly by z jednoho tisku dvě session a filament by
se odečetl dvakrát. Sbírat proto smí jen ta, u které tiskárna fyzicky stojí.

Rozhoduje se podle adresy, kterou si operační systém vybere pro spojení: v jedné podsíti
s tiskárnou jde o přímé spojení v LAN, jinak vede trasa přes VPN. Latence by klamala
(lokální síť pod zátěží umí být pomalejší než tunel) a ruční přepínání v nastavení je přesně
to, čemu se tímhle vyhýbáme — tiskárnu má jít zapnout kdekoliv a ono se to srovná samo.
"""
from __future__ import annotations

import ipaddress
import logging
import socket

LOG = logging.getLogger("locator")

MQTT_PORT = 8883


def _stejna_sit(a: str, b: str, prefix: int = 24) -> bool:
    """Leží obě adresy ve stejné podsíti? (default /24 — běžná domácí síť)"""
    try:
        return ipaddress.ip_network(f"{a}/{prefix}", strict=False) == \
            ipaddress.ip_network(f"{b}/{prefix}", strict=False)
    except ValueError:
        return False


def kde_je_tiskarna(hosts: list[str], port: int = MQTT_PORT, timeout: float = 2.0) -> tuple[str | None, bool]:
    """Vrátí (adresa, je_lokalne) pro první adresu, na které tiskárna odpovídá.

    `je_lokalne=False` znamená, že tiskárna sice odpovídá, ale přes VPN — tedy stojí
    u druhé lokality a sledovat ji má ta druhá instance.
    """
    for host in hosts:
        if not host:
            continue
        try:
            with socket.create_connection((host, port), timeout) as s:
                moje = s.getsockname()[0]
        except OSError:
            continue
        lokalne = _stejna_sit(moje, host)
        LOG.debug("tiskárna na %s odpovídá (moje adresa %s, %s)", host, moje,
                  "lokální síť" if lokalne else "přes VPN")
        return host, lokalne
    return None, False


def popis(host: str | None, lokalne: bool) -> str:
    if not host:
        return "tiskárna neodpovídá na žádné známé adrese"
    return f"tiskárna na {host} " + ("v místní síti – sleduje ji tato instance"
                                     if lokalne else "je vidět jen přes VPN – sleduje ji druhá lokalita")
