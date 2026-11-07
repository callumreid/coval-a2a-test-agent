"""Temporary, synthetic-only SSE fixture. No credentials, input echo or external calls."""

import hashlib
import hmac
import json
import os
import re
import stat
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MAX_BODY_BYTES = 65536
REPLY_PARTS = ("Hello! This is a synthetic test. ", "Your test request succeeded. ", "Thank you and goodbye.")
OUTBOUND_GATE = "/tmp/outbound-gate.json"
OUTBOUND_RECEIPT = "/tmp/outbound-receipt.json"
OUTBOUND_LOCK = threading.Lock()


class BoundedServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, handler):
        self.slots = threading.BoundedSemaphore(8)
        super().__init__(address, handler)

    def process_request(self, request, address):
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        # Never log input, authorization headers, client addresses or request URLs.
        pass

    def handle(self):
        self.connection.settimeout(10)
        super().handle()

    def empty_response(self, status):
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True

    def do_GET(self):
        self.empty_response(200 if self.path == "/health" else 404)

    def do_POST(self):
        if self.path == "/vapi-events":
            # Explicit sink for the isolated one-call assistant: no parsing,
            # logging, retained body, tools, or external calls.
            self.empty_response(204)
            return
        if self.path == "/outbound-trigger":
            self.outbound_trigger()
            return
        if self.path != "/chat":
            self.empty_response(404)
            return
        if self.headers.get("Transfer-Encoding"):
            self.empty_response(400)
            return
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self.empty_response(411)
            return
        if not 0 <= length <= MAX_BODY_BYTES:
            self.empty_response(413)
            return
        body = self.rfile.read(length)
        if len(body) != length:
            self.empty_response(400)
            return
        try:
            json.loads(body)
        except (ValueError, UnicodeError, RecursionError):
            self.empty_response(400)
            return
        del body
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        try:
            for part in REPLY_PARTS:
                self.emit_event({"type": "text-delta", "delta": part})
                time.sleep(0.05)
            self.emit_event({"type": "finish"})
        except (BrokenPipeError, ConnectionResetError):
            pass

    def emit_event(self, event):
        self.wfile.write(("data: " + json.dumps(event) + "\n\n").encode())
        self.wfile.flush()

    def outbound_trigger(self):
        # A single internal canary tuple, delivered privately through Fly SSH.
        # No provider credential or provider call exists in this server.
        token = self.headers.get("X-Coval-Fixture-Token", "")
        try:
            fd = os.open(OUTBOUND_GATE, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd) as f:
                info = os.fstat(f.fileno())
                if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
                    raise ValueError("gate permissions")
                gate = json.loads(f.read(2048))
            valid = (re.fullmatch(r"[0-9a-f]{64}", token) is not None
                     and gate["expires_at"] > time.time()
                     and hmac.compare_digest(hashlib.sha256(token.encode()).hexdigest(), gate["sha256"]))
        except (OSError, ValueError, KeyError, TypeError):
            valid = False
        if not valid:
            self.empty_response(403)
            return
        if self.headers.get("Transfer-Encoding"):
            self.empty_response(400)
            return
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self.empty_response(411)
            return
        if not 0 < length <= 4096:
            self.empty_response(413)
            return
        try:
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValueError("incomplete request")
            payload = json.loads(raw)
            if (not isinstance(payload, dict) or set(payload) != {"phone_number", "simulation_output_id"}
                    or not isinstance(payload["phone_number"], str)
                    or not re.fullmatch(r"\+[1-9][0-9]{7,14}", payload["phone_number"])
                    or not isinstance(payload["simulation_output_id"], str)
                    or not re.fullmatch(r"[A-Za-z0-9]{22}", payload["simulation_output_id"])):
                raise ValueError("invalid tuple")
        except (ValueError, UnicodeError, RecursionError):
            self.empty_response(400)
            return
        with OUTBOUND_LOCK:
            try:
                fd = os.open(OUTBOUND_RECEIPT, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, "w") as f:
                    json.dump({**payload, "received_at": time.time()}, f)
            except OSError:
                self.empty_response(409)
                return
        self.empty_response(200)


if __name__ == "__main__":
    BoundedServer(("0.0.0.0", int(os.environ.get("PORT", "8000"))), Handler).serve_forever()
