"""Najde tiskárnu napříč lokalitami a rozhodne, která instance ji má sledovat.

Tiskárna cestuje mezi lokalitami a add-on běží na obou. Přes VPN na ni ale dosáhnou obě
instance naráz — a kdyby sbíraly obě, vznikly by z jednoho tisku dvě session a filament by
se odečetl dvakrát. Sbírat proto smí jen ta, u které tiskárna fyzicky stojí.

Rozhoduje se podle toho, jestli adresa tiskárny padne do některé podsítě hostitele HA (ty dá
Supervisor). Adresa zvolená pro spojení k tomu nestačí: add-on běží v kontejneru s vlastní
adresou 172.30.x.x, takže by i tiskárna v sousedním pokoji vypadala, že je za VPN — na to se
23. 9. 2026 přišlo až v provozu. Latence by klamala taky (lokální síť pod zátěží umí být
pomalejší než tunel) a ruční přepínání v nastavení je přesně to, čemu se tímhle vyhýbáme.
"""
from __future__ import annotations

import ipaddress
import json
import logging
import os
import socket
import urllib.request

LOG = logging.getLogger("locator")

MQTT_PORT = 8883


def _stejna_sit(a: str, b: str, prefix: int = 24) -> bool:
    """Leží obě adresy ve stejné podsíti? (default /24 — běžná domácí síť)"""
    try:
        return ipaddress.ip_network(f"{a}/{prefix}", strict=False) == \
            ipaddress.ip_network(f"{b}/{prefix}", strict=False)
    except ValueError:
        return False


def moje_site(timeout: float = 5.0) -> list[ipaddress.IPv4Network]:
    """Podsítě, ve kterých stojí sám hostitel HA.

    Add-on běží v kontejneru s vlastní adresou (172.30.x.x), takže adresa zvolená pro spojení
    by vyšla vždy cizí a i tiskárna v sousedním pokoji by vypadala, že je za VPN. Skutečné sítě
    zná Supervisor; bez něj (testy, běh mimo HA) se vrátí prázdný seznam a rozhodne se podle
    adresy spojení.
    """
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        return []
    try:
        req = urllib.request.Request("http://supervisor/network/info",
                                     headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read().decode())
    except Exception as e:  # noqa: BLE001 – bez Supervisoru se prostě rozhodne jinak
        LOG.debug("sítě hostitele se nepodařilo zjistit: %s", e)
        return []
    site = []
    for i in (data.get("data", data).get("interfaces") or []):
        for adr in ((i.get("ipv4") or {}).get("address") or []):
            try:
                site.append(ipaddress.ip_network(adr, strict=False))
            except ValueError:
                continue
    return site


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
        site = moje_site()
        if site:
            try:
                ip = ipaddress.ip_address(host)
                lokalne = any(ip in sit for sit in site)
            except ValueError:
                lokalne = False
        else:
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
