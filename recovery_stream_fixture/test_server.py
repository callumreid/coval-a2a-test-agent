"""Wire-level checks for the isolated synthetic streaming fixture."""

import hashlib
import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import server
from server import BoundedServer, Handler, MAX_BODY_BYTES, REPLY_PARTS


class StreamingFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = BoundedServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        status, content_type, data = response.status, response.getheader("Content-Type"), response.read()
        connection.close()
        return status, content_type, data

    def test_complete_synthetic_stream_without_input_echo(self):
        marker = "PRIVATE_INPUT_MUST_NOT_APPEAR"
        status, content_type, data = self.request("POST", "/chat", json.dumps({"text": marker}))
        self.assertEqual(status, 200)
        self.assertTrue(content_type.startswith("text/event-stream"))
        events = [json.loads(line[6:]) for line in data.decode().splitlines() if line.startswith("data: ")]
        self.assertEqual(events[-1], {"type": "finish"})
        self.assertEqual("".join(e["delta"] for e in events[:-1]), "".join(REPLY_PARTS))
        self.assertNotIn(marker.encode(), data)

    def test_health_and_unknown_routes(self):
        self.assertEqual(self.request("GET", "/health")[0], 200)
        self.assertEqual(self.request("POST", "/other", "{}")[0], 404)

    def test_voice_event_sink_has_empty_response(self):
        status, _content_type, body = self.request("POST", "/vapi-events", '{"message":"synthetic"}')
        self.assertEqual((status, body), (204, b""))

    def test_reject_invalid_or_oversized_request(self):
        self.assertEqual(self.request("POST", "/chat", "not-json")[0], 400)
        self.assertEqual(self.request("POST", "/chat", b" " * (MAX_BODY_BYTES + 1))[0], 413)

    def test_reject_transfer_encoding(self):
        self.assertEqual(self.request("POST", "/chat", "{}", {"Transfer-Encoding": "chunked"})[0], 400)

    def test_outbound_private_one_shot_delivery(self):
        # This fixed test-only value is not a deployed gate token.
        token = "a" * 64
        payload = {"phone_number": "+12025550123", "simulation_output_id": "A" * 22}
        with tempfile.TemporaryDirectory() as directory:
            gate = Path(directory) / "gate.json"
            receipt = Path(directory) / "receipt.json"
            gate.write_text(json.dumps({"sha256": hashlib.sha256(token.encode()).hexdigest(), "expires_at": 9999999999}))
            gate.chmod(0o600)
            with mock.patch.object(server, "OUTBOUND_GATE", str(gate)), mock.patch.object(server, "OUTBOUND_RECEIPT", str(receipt)):
                self.assertEqual(self.request("POST", "/outbound-trigger", json.dumps(payload))[0], 403)
                self.assertFalse(receipt.exists())
                self.assertEqual(self.request("POST", "/outbound-trigger", json.dumps(payload), {"X-Coval-Fixture-Token": token})[0], 200)
                self.assertEqual(receipt.stat().st_mode & 0o777, 0o600)
                self.assertEqual(json.loads(receipt.read_text())["simulation_output_id"], payload["simulation_output_id"])
                self.assertEqual(self.request("GET", "/outbound-receipt.json")[0], 404)
                self.assertEqual(self.request("POST", "/outbound-trigger", json.dumps(payload), {"X-Coval-Fixture-Token": token})[0], 409)

    def test_outbound_expiry_and_payload_limits(self):
        token = "b" * 64
        with tempfile.TemporaryDirectory() as directory:
            gate = Path(directory) / "gate.json"
            receipt = Path(directory) / "receipt.json"
            gate.write_text(json.dumps({"sha256": hashlib.sha256(token.encode()).hexdigest(), "expires_at": 1}))
            gate.chmod(0o600)
            with mock.patch.object(server, "OUTBOUND_GATE", str(gate)), mock.patch.object(server, "OUTBOUND_RECEIPT", str(receipt)):
                headers = {"X-Coval-Fixture-Token": token}
                self.assertEqual(self.request("POST", "/outbound-trigger", "{}", headers)[0], 403)
                gate.write_text(json.dumps({"sha256": hashlib.sha256(token.encode()).hexdigest(), "expires_at": 9999999999}))
                self.assertEqual(self.request("POST", "/outbound-trigger", "{}", headers)[0], 400)
                self.assertEqual(self.request("POST", "/outbound-trigger", " " * 4097, headers)[0], 413)
                self.assertFalse(receipt.exists())


if __name__ == "__main__":
    unittest.main()
