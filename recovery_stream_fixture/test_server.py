"""Wire-level checks for the isolated synthetic streaming fixture."""

import http.client
import json
import threading
import unittest

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

    def test_reject_invalid_or_oversized_request(self):
        self.assertEqual(self.request("POST", "/chat", "not-json")[0], 400)
        self.assertEqual(self.request("POST", "/chat", b" " * (MAX_BODY_BYTES + 1))[0], 413)

    def test_reject_transfer_encoding(self):
        self.assertEqual(self.request("POST", "/chat", "{}", {"Transfer-Encoding": "chunked"})[0], 400)


if __name__ == "__main__":
    unittest.main()
