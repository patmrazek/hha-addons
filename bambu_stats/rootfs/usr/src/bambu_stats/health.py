"""Mini HTTP endpoint /health pro watchdog add-onu a Docker HEALTHCHECK."""
from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

LOG = logging.getLogger("health")


class HealthServer:
    def __init__(self, port: int, status_fn: Callable[[], dict]):
        self.port, self.status_fn = port, status_fn

    def start(self):
        status_fn = self.status_fn

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                st = status_fn()
                ok = st.get("ok", False)
                body = json.dumps(st).encode()
                self.send_response(200 if ok else 503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):  # ticho
                pass

        srv = ThreadingHTTPServer(("0.0.0.0", self.port), Handler)
        threading.Thread(target=srv.serve_forever, name="health", daemon=True).start()
        LOG.info("health endpoint na :%d/health", self.port)
        return srv
