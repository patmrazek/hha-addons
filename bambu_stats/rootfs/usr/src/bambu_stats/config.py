"""Načtení konfigurace add-onu.

Zdroje v tomto pořadí:
1. /data/options.json (cesta v env BAMBU_OPTIONS) – HA add-on options,
2. proměnné prostředí MQTT_* (z run.sh přes bashio::services mqtt),
3. pro vývoj mimo HA: BAMBU_HOST/BAMBU_SERIAL/BAMBU_ACCESS_CODE (např. `set -a; . ~/.hha/bambu-hacienda.env`).
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class PrinterConfig:
    name: str
    host: str
    serial: str
    access_code: str
    ha_weight_entity: str = ""
    ha_length_entity: str = ""

    @property
    def slug(self) -> str:
        return "".join(c for c in self.name.lower().replace(" ", "_") if c.isalnum() or c == "_") or self.serial.lower()


@dataclass
class MqttConfig:
    host: str = "core-mosquitto"
    port: int = 1883
    username: str = ""
    password: str = ""

    @property
    def available(self) -> bool:
        return bool(self.host)


@dataclass
class Settings:
    printers: list[PrinterConfig] = field(default_factory=list)
    entity_prefix: str = "bambu_p2s"
    samples_retention_days: int = 90
    log_level: str = "info"
    tls_verify: str = "tofu"
    data_dir: Path = Path("/data")
    mqtt: MqttConfig = field(default_factory=MqttConfig)
    supervisor_token: str = ""
    health_port: int = 8099
    prices: dict = field(default_factory=dict)   # material_group → cena za kg
    currency: str = "CZK"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "bambu.db"


def load() -> Settings:
    s = Settings()
    s.data_dir = Path(os.environ.get("BAMBU_DATA_DIR", "/data"))
    s.supervisor_token = os.environ.get("SUPERVISOR_TOKEN", "")

    opts_path = Path(os.environ.get("BAMBU_OPTIONS", "/data/options.json"))
    opts = {}
    if opts_path.exists():
        opts = json.loads(opts_path.read_text())
    # jednoduchá varianta (jedna tiskárna, plochá pole – v UI add-onu spolehlivější než seznam)
    if opts.get("printer_host") and opts.get("printer_serial") and opts.get("access_code"):
        s.printers.append(PrinterConfig(
            name=opts.get("printer_name") or "Bambu", host=opts["printer_host"], serial=opts["printer_serial"],
            access_code=opts["access_code"], ha_weight_entity=opts.get("ha_weight_entity") or "",
            ha_length_entity=opts.get("ha_length_entity") or ""))
    for p in opts.get("printers") or []:
        if p.get("host") and p.get("serial") and p.get("access_code"):
            s.printers.append(PrinterConfig(
                name=p.get("name") or "Bambu", host=p["host"], serial=p["serial"], access_code=p["access_code"],
                ha_weight_entity=p.get("ha_weight_entity") or "", ha_length_entity=p.get("ha_length_entity") or ""))
    if not s.printers and os.environ.get("BAMBU_HOST"):
        s.printers.append(PrinterConfig(
            name=os.environ.get("BAMBU_NAME", "Bambu"), host=os.environ["BAMBU_HOST"],
            serial=os.environ["BAMBU_SERIAL"], access_code=os.environ["BAMBU_ACCESS_CODE"],
            ha_weight_entity=os.environ.get("BAMBU_HA_WEIGHT_ENTITY", ""),
            ha_length_entity=os.environ.get("BAMBU_HA_LENGTH_ENTITY", "")))

    s.entity_prefix = opts.get("entity_prefix") or os.environ.get("BAMBU_ENTITY_PREFIX", s.entity_prefix)
    s.samples_retention_days = int(opts.get("samples_retention_days", s.samples_retention_days))
    s.log_level = os.environ.get("LOG_LEVEL") or opts.get("log_level", s.log_level)
    s.tls_verify = opts.get("tls_verify", s.tls_verify)

    s.mqtt = MqttConfig(
        host=os.environ.get("MQTT_HOST", "") or opts.get("mqtt_host", ""),
        port=int(os.environ.get("MQTT_PORT", "0") or opts.get("mqtt_port", 1883) or 1883),
        username=os.environ.get("MQTT_USERNAME", "") or opts.get("mqtt_username", ""),
        password=os.environ.get("MQTT_PASSWORD", "") or opts.get("mqtt_password", ""))
    s.health_port = int(os.environ.get("HEALTH_PORT", s.health_port))
    for p in opts.get("filament_prices") or []:
        try:
            if p.get("material") and float(p.get("price_per_kg", 0)) > 0:
                s.prices[str(p["material"]).upper()] = float(p["price_per_kg"])
        except (TypeError, ValueError):
            continue
    s.currency = (opts.get("currency") or s.currency).upper()
    return s
