"""Temporary, synthetic-only SSE fixture. No credentials, input echo or external calls."""

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MAX_BODY_BYTES = 65536
REPLY_PARTS = ("Hello! This is a synthetic test. ", "Your test request succeeded. ", "Thank you and goodbye.")


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


if __name__ == "__main__":
    BoundedServer(("0.0.0.0", int(os.environ.get("PORT", "8000"))), Handler).serve_forever()
