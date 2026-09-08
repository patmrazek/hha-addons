"""MQTT klient k tiskárně Bambu Lab (lokální rozhraní, TLS 8883, uživatel bblp).

- reconnect s exponenciálním backoffem (1 → 120 s + jitter),
- `pushall` po každém připojení (a jako pojistka každých 5 min),
- deep-merge příchozích zpráv do jednoho stavu (P2S posílá plný stav, P1/X1 delty),
- TOFU připnutí certifikátu tiskárny (cert je self-signed, CN = sériové číslo),
- hlídání ticha: bez zprávy > 60 s → reconnect.

Callback `on_state(state: dict, now: float)` dostává slučovaný obsah sekce `print`.
"""
from __future__ import annotations

import hashlib
import json
import logging
import random
import ssl
import threading
import time
from typing import Callable

import paho.mqtt.client as mqtt

LOG = logging.getLogger("printer")

PUSHALL = {"pushing": {"sequence_id": "0", "command": "pushall"}}
STALE_AFTER_S = 60
PUSHALL_EVERY_S = 300


def deep_merge(dst: dict, src: dict) -> dict:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            deep_merge(dst[k], v)
        else:
            dst[k] = v
    return dst


class PrinterMQTT:
    def __init__(self, host: str, serial: str, access_code: str, on_state: Callable[[dict, float], None],
                 tls_verify: str = "tofu", pinned_fingerprint: str | None = None,
                 on_pin: Callable[[str], None] | None = None, on_connection: Callable[[bool], None] | None = None):
        self.host, self.serial, self._code = host, serial, access_code
        self.on_state, self.on_pin, self.on_connection = on_state, on_pin, on_connection
        self.tls_verify = tls_verify
        self.pinned = pinned_fingerprint
        self.state: dict = {}
        self.connected = False
        self.last_msg_ts: float = 0.0
        self.msg_count = 0
        self._stop = threading.Event()
        self._client: mqtt.Client | None = None
        self._backoff = 1.0
        self._last_pushall = 0.0

    # --- veřejné ---------------------------------------------------------
    def start(self):
        threading.Thread(target=self._run, name=f"printer-{self.serial[-4:]}", daemon=True).start()

    def stop(self):
        self._stop.set()
        if self._client:
            try:
                self._client.disconnect()
            except Exception:
                pass

    def request_pushall(self):
        if self._client and self.connected:
            self._client.publish(f"device/{self.serial}/request", json.dumps(PUSHALL))
            self._last_pushall = time.monotonic()

    @property
    def stale(self) -> bool:
        return self.connected and self.last_msg_ts and (time.monotonic() - self.last_msg_ts) > STALE_AFTER_S

    # --- interní -----------------------------------------------------------
    def _build(self) -> mqtt.Client:
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"bambu_stats_{self.serial[-6:]}",
                        protocol=mqtt.MQTTv311)
        c.username_pw_set("bblp", self._code)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE  # cert je self-signed; ověřujeme otiskem (TOFU) níže
        c.tls_set_context(ctx)
        c.on_connect, c.on_disconnect, c.on_message = self._on_connect, self._on_disconnect, self._on_message
        c.reconnect_delay_set(1, 120)
        return c

    def _check_pin(self, sock) -> bool:
        try:
            der = sock.getpeercert(binary_form=True)
        except Exception:
            return True
        fp = hashlib.sha256(der).hexdigest()
        if self.tls_verify == "none":
            return True
        if not self.pinned:
            self.pinned = fp
            LOG.info("TLS certifikát tiskárny připnut (TOFU): %s…", fp[:16])
            if self.on_pin:
                self.on_pin(fp)
            return True
        if fp != self.pinned:
            LOG.warning("TLS certifikát tiskárny se ZMĚNIL (%s… → %s…). Pokud jsi tiskárnu neměnil, prověř síť. "
                        "Pokračuji, protože Bambu certy nelze ověřit jinak; nový otisk připnu.", self.pinned[:16], fp[:16])
            self.pinned = fp
            if self.on_pin:
                self.on_pin(fp)
        return True

    def _on_connect(self, client, userdata, flags, rc, props=None):
        if rc != 0 and str(rc) != "Success":
            LOG.warning("připojení k tiskárně odmítnuto: %s", rc)
            return
        self.connected = True
        self._backoff = 1.0
        try:
            self._check_pin(client.socket())
        except Exception as e:  # pragma: no cover
            LOG.debug("pin check: %s", e)
        client.subscribe(f"device/{self.serial}/report", qos=0)
        self.request_pushall()
        LOG.info("připojeno k tiskárně %s", self.host)
        if self.on_connection:
            self.on_connection(True)

    def _on_disconnect(self, client, userdata, flags, rc, props=None):
        self.connected = False
        LOG.warning("odpojeno od tiskárny (%s)", rc)
        if self.on_connection:
            self.on_connection(False)

    def _on_message(self, client, userdata, msg):
        self.last_msg_ts = time.monotonic()
        self.msg_count += 1
        try:
            data = json.loads(msg.payload)
        except json.JSONDecodeError:
            LOG.debug("nečitelný payload (%d B)", len(msg.payload))
            return
        p = data.get("print")
        if not isinstance(p, dict):
            return
        deep_merge(self.state, p)
        try:
            self.on_state(self.state, time.time())
        except Exception:
            LOG.exception("chyba při zpracování stavu tiskárny")

    def _run(self):
        while not self._stop.is_set():
            try:
                self._client = self._build()
                self._client.connect(self.host, 8883, keepalive=30)
                self._client.loop_start()
                while not self._stop.is_set():
                    time.sleep(5)
                    if not self.connected:
                        continue
                    if self.stale:
                        LOG.warning("tiskárna %d s mlčí, reconnect", int(time.monotonic() - self.last_msg_ts))
                        break
                    if time.monotonic() - self._last_pushall > PUSHALL_EVERY_S:
                        self.request_pushall()
                self._client.loop_stop()
                try:
                    self._client.disconnect()
                except Exception:
                    pass
            except (OSError, ssl.SSLError, mqtt.WebsocketConnectionError) as e:
                self.connected = False
                LOG.warning("tiskárna %s nedostupná (%s), další pokus za %.0f s", self.host, e, self._backoff)
            except Exception:
                LOG.exception("neočekávaná chyba MQTT smyčky")
            if self._stop.is_set():
                break
            time.sleep(self._backoff + random.uniform(0, 1))
            self._backoff = min(self._backoff * 2, 120)
