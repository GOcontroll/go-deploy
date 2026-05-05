#!/usr/bin/env python3
"""GOcontroll MQTT bridge — bidirectioneel.

Richting 1 (Worker → EMQX): HTTP POST /publish van Cloudflare Worker,
  valideert HMAC-SHA256, publiceert via mosquitto_pub naar EMQX.

Richting 2 (EMQX → Worker): paho-mqtt subscriber luistert op
  gocontroll/+/status/update, stuurt berichten door naar Cloudflare Worker
  /api/mqtt-status met HMAC-SHA256 handtekening.
"""

import hashlib
import hmac
import json
import logging
import subprocess
import sys
import threading
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer

import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s mqtt-bridge: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("mqtt-bridge")

# ── Configuratie ────────────────────────────────────────────────────────────
# Zelfde waarde als DOWNLOAD_SECRET in de Cloudflare Worker.
# Haal de waarde op uit het bestaande bridge-script op de VPS.
HMAC_SECRET = b"be041c8641041808e742d4923d2b4dccd0a44fff2e1c614683d5663eda4f70c1"

MQTT_HOST = "localhost"
MQTT_PORT = 28741
MQTT_USER = "bridge"
MQTT_PASS = "GoCtrllBridge2026!"

HTTP_PORT = 9190
WORKER_WEBHOOK = "https://gocontroll-db.rickgijsberts.workers.dev/api/mqtt-status"
# ────────────────────────────────────────────────────────────────────────────


class ReuseServer(HTTPServer):
    allow_reuse_address = True


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass  # journald logs al via stdout

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        sig = self.headers.get("X-Bridge-Sig", "")
        expected = hmac.new(HMAC_SECRET, body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            self.send_response(403)
            self.end_headers()
            return
        try:
            data = json.loads(body)
            topic = data["topic"]
            payload = data["payload"]
            qos = int(data.get("qos", 1))
            subprocess.run(
                [
                    "mosquitto_pub",
                    "-h", MQTT_HOST, "-p", str(MQTT_PORT),
                    "-u", MQTT_USER, "-P", MQTT_PASS,
                    "-t", topic, "-m", payload, "-q", str(qos),
                ],
                capture_output=True,
            )
        except Exception as exc:
            log.error("Publish mislukt: %s", exc)
            self.send_response(500)
            self.end_headers()
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")


def forward_to_worker(topic: str, payload_str: str) -> None:
    try:
        body = json.dumps({"topic": topic, "payload": payload_str}).encode("utf-8")
        sig = hmac.new(HMAC_SECRET, body, hashlib.sha256).hexdigest()
        req = urllib.request.Request(
            WORKER_WEBHOOK,
            data=body,
            headers={
                "Content-Type": "application/json",
                "X-Bridge-Sig": sig,
                "User-Agent": "gocontroll-mqtt-bridge/1.0",
            },
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
        log.info("Status doorgestuurd: %s", topic)
    except Exception as exc:
        log.warning("Doorsturen naar Worker mislukt: %s", exc)


def start_subscriber() -> None:
    def on_connect(client, _ud, _flags, rc):
        if rc == 0:
            client.subscribe("gocontroll/+/status/update", qos=1)
            log.info("Subscriber verbonden — luistert op gocontroll/+/status/update")
        else:
            log.error("Subscriber verbinding mislukt (rc=%d)", rc)

    def on_message(_client, _ud, msg):
        try:
            payload_str = msg.payload.decode("utf-8")
            threading.Thread(
                target=forward_to_worker,
                args=(msg.topic, payload_str),
                daemon=True,
            ).start()
        except Exception as exc:
            log.warning("Verwerking status mislukt: %s", exc)

    client = mqtt.Client(
        client_id="go-bridge-sub",
        protocol=mqtt.MQTTv311,
        clean_session=True,
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.reconnect_delay_set(min_delay=5, max_delay=60)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()


if __name__ == "__main__":
    start_subscriber()
    server = ReuseServer(("127.0.0.1", HTTP_PORT), Handler)
    log.info("HTTP bridge gestart op poort %d", HTTP_PORT)
    server.serve_forever()
