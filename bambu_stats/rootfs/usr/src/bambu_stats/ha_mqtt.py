"""Publikace statistik do Home Assistantu přes MQTT Discovery (device-based, retained)."""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from typing import Callable

import paho.mqtt.client as mqtt

from . import __version__

LOG = logging.getLogger("ha_mqtt")

# key → (name, unit, device_class, state_class, icon, kind)   kind: sensor|text|diag|button
COMPONENTS: dict[str, tuple] = {
    "total_prints": ("Celkem tisků", None, None, "total", "mdi:printer-3d", "sensor"),
    "successful_prints": ("Úspěšné tisky", None, None, "total", "mdi:check-circle", "sensor"),
    "failed_prints": ("Neúspěšné tisky", None, None, "total", "mdi:alert-circle", "sensor"),
    "cancelled_prints": ("Zrušené tisky", None, None, "total", "mdi:cancel", "sensor"),
    "success_rate": ("Úspěšnost", "%", None, "measurement", "mdi:percent", "sensor"),
    "total_print_hours": ("Hodiny tisku celkem", "h", "duration", "total", "mdi:clock-outline", "sensor"),
    "total_active_hours": ("Hodiny aktivního tisku", "h", "duration", "total", "mdi:clock-fast", "sensor"),
    "total_filament_kg": ("Filament celkem", "kg", "weight", "total", "mdi:weight-kilogram", "sensor"),
    "filament_pla_kg": ("Filament PLA", "kg", "weight", "total", "mdi:weight-kilogram", "sensor"),
    "filament_petg_kg": ("Filament PETG", "kg", "weight", "total", "mdi:weight-kilogram", "sensor"),
    "filament_abs_kg": ("Filament ABS", "kg", "weight", "total", "mdi:weight-kilogram", "sensor"),
    "filament_asa_kg": ("Filament ASA", "kg", "weight", "total", "mdi:weight-kilogram", "sensor"),
    "filament_tpu_kg": ("Filament TPU", "kg", "weight", "total", "mdi:weight-kilogram", "sensor"),
    "filament_other_kg": ("Filament ostatní", "kg", "weight", "total", "mdi:weight-kilogram", "sensor"),
    "total_cost": ("Náklady na filament celkem", "CUR", "monetary", "total", "mdi:cash-multiple", "sensor"),
    "cost_today": ("Náklady dnes", "CUR", "monetary", None, "mdi:cash", "sensor"),
    "cost_7d": ("Náklady za 7 dní", "CUR", "monetary", None, "mdi:cash", "sensor"),
    "cost_30d": ("Náklady za 30 dní", "CUR", "monetary", None, "mdi:cash", "sensor"),
    "avg_cost_per_print": ("Průměrné náklady na tisk", "CUR", "monetary", None, "mdi:cash", "sensor"),
    "current_cost": ("Náklady běžícího tisku", "CUR", "monetary", None, "mdi:cash-clock", "sensor"),
    "prints_today": ("Tisky dnes", None, None, "measurement", "mdi:calendar-today", "sensor"),
    "prints_7d": ("Tisky za 7 dní", None, None, "measurement", "mdi:calendar-week", "sensor"),
    "prints_30d": ("Tisky za 30 dní", None, None, "measurement", "mdi:calendar-month", "sensor"),
    "hours_today": ("Hodiny tisku dnes", "h", "duration", "measurement", "mdi:clock-outline", "sensor"),
    "hours_7d": ("Hodiny tisku za 7 dní", "h", "duration", "measurement", "mdi:clock-outline", "sensor"),
    "hours_30d": ("Hodiny tisku za 30 dní", "h", "duration", "measurement", "mdi:clock-outline", "sensor"),
    "filament_today_g": ("Filament dnes", "g", "weight", "measurement", "mdi:weight-gram", "sensor"),
    "filament_7d_g": ("Filament za 7 dní", "g", "weight", "measurement", "mdi:weight-gram", "sensor"),
    "filament_30d_g": ("Filament za 30 dní", "g", "weight", "measurement", "mdi:weight-gram", "sensor"),
    "longest_print_h": ("Nejdelší tisk", "h", "duration", None, "mdi:timer-sand", "sensor"),
    "average_print_h": ("Průměrná délka tisku", "h", "duration", None, "mdi:timer-outline", "sensor"),
    "median_print_h": ("Medián délky tisku", "h", "duration", None, "mdi:timer-outline", "sensor"),
    "avg_filament_per_print_g": ("Průměrný filament na tisk", "g", "weight", None, "mdi:weight-gram", "sensor"),
    "utilization_7d": ("Využití za 7 dní", "%", None, "measurement", "mdi:gauge", "sensor"),
    "utilization_30d": ("Využití za 30 dní", "%", None, "measurement", "mdi:gauge", "sensor"),
    "most_used_material": ("Nejpoužívanější materiál", None, None, None, "mdi:star", "text"),
    "last_print": ("Poslední tisk", None, None, None, "mdi:history", "text"),
    "print_history": ("Historie tisků", None, None, None, "mdi:table", "text"),
    "usage": ("Využití v čase", None, None, None, "mdi:chart-bar", "text"),
    "current_session": ("Aktuální session", None, None, None, "mdi:progress-clock", "text"),
    "current_filament_g": ("Filament běžícího tisku", "g", "weight", "measurement", "mdi:weight-gram", "sensor"),
    "collector_status": ("Stav collectoru", None, None, None, "mdi:heart-pulse", "diag"),
    "filament_check": ("Kontrola filamentu před tiskem", None, None, None, "mdi:clipboard-check", "text"),
    "hours_since_maintenance": ("Hodin tisku od údržby", "h", "duration", None, "mdi:wrench-clock", "sensor"),
    "prints_since_maintenance": ("Tisků od údržby", None, None, None, "mdi:wrench", "sensor"),
    "days_since_desiccant": ("Dní od výměny silikagelu", "d", None, None, "mdi:water-percent", "sensor"),
    "nozzle_wear": ("Opotřebení trysky", "%", None, None, "mdi:printer-3d-nozzle-alert", "sensor"),
    "ams_humidity_history": ("Vlhkost AMS v čase", "%", "humidity", "measurement", "mdi:chart-line", "sensor"),
    "models": ("Statistiky modelů", None, None, None, "mdi:cube-outline", "text"),
    "maintenance_done": ("Údržba provedena", None, None, None, "mdi:wrench-check", "button"),
    "desiccant_changed": ("Silikagel vyměněn", None, None, None, "mdi:water-off", "button"),
    "slot_1": ("AMS slot 1", None, None, None, "mdi:circle-slice-8", "text"),
    "slot_2": ("AMS slot 2", None, None, None, "mdi:circle-slice-8", "text"),
    "slot_3": ("AMS slot 3", None, None, None, "mdi:circle-slice-8", "text"),
    "slot_4": ("AMS slot 4", None, None, None, "mdi:circle-slice-8", "text"),
    "recompute": ("Přepočítat statistiky", None, None, None, "mdi:refresh", "button"),
    "refetch_3mf": ("Znovu načíst 3MF", None, None, None, "mdi:file-refresh", "button"),
}
CURRENCY_SYMBOLS = {"CZK": "Kč", "EUR": "€", "USD": "$", "GBP": "£", "PLN": "zł"}
ATTR_KEYS = {"total_filament_kg", "most_used_material", "last_print", "print_history", "usage", "current_session",
             "current_filament_g", "current_cost", "total_cost", "collector_status", "slot_1", "slot_2", "slot_3", "slot_4",
             "filament_check", "hours_since_maintenance", "days_since_desiccant", "models", "ams_humidity_history"}


class HAPublisher:
    def __init__(self, cfg, prefix: str, serial: str, printer_name: str, on_command: Callable[[str], None] | None = None,
                 slot_keys: list[str] | None = None, currency: str = "CZK"):
        self.cfg, self.prefix, self.serial, self.printer_name = cfg, prefix, serial, printer_name
        self.currency = currency
        self.on_command = on_command
        self.base = f"bambu_stats/{serial}"
        self.connected = False
        self._client: mqtt.Client | None = None
        self._hashes: dict[str, str] = {}
        self._last: dict[str, tuple[str, str]] = {}
        self._slot_keys: list[str] = slot_keys or []
        self._lock = threading.Lock()

    # --- životní cyklus -------------------------------------------------------
    def start(self):
        if not self.cfg.available:
            LOG.warning("MQTT k Home Assistantu není nakonfigurováno – statistiky se nepublikují")
            return
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"bambu_stats_pub_{self.serial[-6:]}")
        if self.cfg.username:
            c.username_pw_set(self.cfg.username, self.cfg.password)
        c.will_set(f"{self.base}/availability", "offline", qos=1, retain=True)
        c.on_connect, c.on_disconnect, c.on_message = self._on_connect, self._on_disconnect, self._on_message
        c.reconnect_delay_set(1, 60)
        self._client = c
        threading.Thread(target=self._connect_loop, name="ha-mqtt", daemon=True).start()

    def _connect_loop(self):
        delay = 1
        while True:
            try:
                self._client.connect(self.cfg.host, self.cfg.port, keepalive=60)
                self._client.loop_forever(retry_first_connection=True)
                return
            except (OSError, ValueError) as e:
                LOG.warning("Mosquitto %s:%s nedostupný (%s), zkusím za %d s", self.cfg.host, self.cfg.port, e, delay)
                time.sleep(delay)
                delay = min(delay * 2, 60)

    def stop(self):
        if self._client and self.connected:
            self._client.publish(f"{self.base}/availability", "offline", qos=1, retain=True)
            self._client.disconnect()

    def _on_connect(self, client, userdata, flags, rc, props=None):
        self.connected = True
        LOG.info("připojeno k Mosquittu %s", self.cfg.host)
        client.subscribe("homeassistant/status")
        client.subscribe(f"{self.base}/cmd")
        self._publish_discovery()
        client.publish(f"{self.base}/availability", "online", qos=1, retain=True)
        self._republish_all()

    def _on_disconnect(self, client, userdata, flags, rc, props=None):
        self.connected = False
        LOG.warning("odpojeno od Mosquitta (%s)", rc)

    def _on_message(self, client, userdata, msg):
        payload = msg.payload.decode(errors="replace")
        if msg.topic == "homeassistant/status" and payload == "online":
            LOG.info("Home Assistant nastartoval – znovu publikuji discovery")
            self._publish_discovery()
            client.publish(f"{self.base}/availability", "online", qos=1, retain=True)
            self._republish_all()
        elif msg.topic == f"{self.base}/cmd" and self.on_command:
            self.on_command(payload)

    # --- discovery --------------------------------------------------------------
    def set_slot_keys(self, keys: list[str]):
        new = [k for k in keys if k not in self._slot_keys]
        if new:
            self._slot_keys += new
            if self.connected:
                self._publish_discovery()

    def _publish_discovery(self):
        dev = {"identifiers": [f"bambu_stats_{self.serial}"], "name": f"{self.printer_name} statistiky",
               "manufacturer": "HHA", "model": "bambu_stats add-on", "sw_version": __version__}
        origin = {"name": "bambu_stats", "sw_version": __version__, "support_url": "https://github.com/patmrazek/hha-addons"}
        cmps = {}
        for key, (name, unit, dclass, sclass, icon, kind) in COMPONENTS.items():
            uid = f"bambu_stats_{self.serial}_{key}"
            platform = "button" if kind == "button" else "sensor"
            # HA 2026: entity ID se řídí `default_entity_id` (dřívější `object_id` bylo odstraněno)
            c = {"name": name, "unique_id": uid, "default_entity_id": f"{platform}.{self.prefix}_{key}", "icon": icon,
                 "availability_topic": f"{self.base}/availability"}
            if kind == "button":
                c.update({"p": "button", "command_topic": f"{self.base}/cmd", "payload_press": key})
                if key in ("recompute", "refetch_3mf"):
                    c["entity_category"] = "diagnostic"
            else:
                c.update({"p": "sensor", "state_topic": f"{self.base}/state/{key}"})
                if unit:
                    c["unit_of_measurement"] = CURRENCY_SYMBOLS.get(self.currency, self.currency) if unit == "CUR" else unit
                if dclass and dclass != "monetary":   # monetary vynucuje ISO kód a formát „CZK 7.40" – chceme „7 Kč"
                    c["device_class"] = dclass
                if sclass:
                    c["state_class"] = sclass
                if kind == "diag":
                    c["entity_category"] = "diagnostic"
                if key in ATTR_KEYS:
                    c["json_attributes_topic"] = f"{self.base}/attr/{key}"
            cmps[key] = c
        for sk in self._slot_keys:
            key = f"filament_{sk}_kg"
            label = "externí cívka" if sk == "ext" else ("nezařazeno" if sk == "unmapped" else sk.replace("ams", "AMS ").replace("_slot", " slot "))
            cmps[key] = {"p": "sensor", "name": f"Filament {label}", "unique_id": f"bambu_stats_{self.serial}_{key}",
                         "default_entity_id": f"sensor.{self.prefix}_{key}", "icon": "mdi:weight-kilogram", "unit_of_measurement": "kg",
                         "device_class": "weight", "state_class": "total", "state_topic": f"{self.base}/state/{key}",
                         "json_attributes_topic": f"{self.base}/attr/{key}", "availability_topic": f"{self.base}/availability"}
        payload = {"dev": dev, "o": origin, "cmps": cmps}
        self._client.publish(f"homeassistant/device/bambu_stats_{self.serial}/config", json.dumps(payload, ensure_ascii=False),
                             qos=1, retain=True)

    # --- publikace hodnot ---------------------------------------------------------------
    def _pub(self, topic: str, payload: str, force=False):
        h = hashlib.md5(payload.encode()).hexdigest()
        if not force and self._hashes.get(topic) == h:
            return
        self._hashes[topic] = h
        if self._client and self.connected:
            self._client.publish(topic, payload, qos=1, retain=True)

    def publish_value(self, key: str, value, attrs: dict | None = None, force=False):
        with self._lock:
            state = "" if value is None else (json.dumps(value) if not isinstance(value, str) else value)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                state = str(value)
            state = state[:255]
            self._last[key] = (state, json.dumps(attrs or {}, ensure_ascii=False))
            self._pub(f"{self.base}/state/{key}", state, force)
            if attrs is not None:
                self._pub(f"{self.base}/attr/{key}", json.dumps(attrs, ensure_ascii=False), force)

    def publish_stats(self, stats: dict, force=False):
        slot_keys = list(stats.get("filament_by_slot_kg", {}).keys())
        self.set_slot_keys(slot_keys)
        for key in COMPONENTS:
            if key in stats:
                self.publish_value(key, stats[key], stats.get(f"{key}_attrs"), force)
        for sk, v in stats.get("filament_by_slot_kg", {}).items():
            self.publish_value(f"filament_{sk}_kg", v, stats.get("filament_by_slot_attrs", {}).get(sk, {}), force)

    def _republish_all(self):
        for key, (state, attrs) in list(self._last.items()):
            self._pub(f"{self.base}/state/{key}", state, force=True)
            if attrs and attrs != "{}":
                self._pub(f"{self.base}/attr/{key}", attrs, force=True)
