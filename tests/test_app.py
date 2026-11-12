import base64
from dataclasses import asdict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, build_opener, ProxyHandler

from app import Engine, GIB, LOCAL_TZ, Settings, in_window, make_server


PAYLOAD = b"DISCARD_THIS_PAYLOAD_" * 3200


class SourceHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.path == "/error":
            self.send_response(503)
            self.end_headers()
            return
        if self.path == "/bad-chunks":
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            self.wfile.write(b"this-is-not-a-chunk\r\n")
            self.wfile.flush()
            return
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "file:///etc/passwd")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(PAYLOAD) * 1024))
        self.end_headers()
        try:
            for _ in range(1024):
                self.wfile.write(PAYLOAD)
        except (BrokenPipeError, ConnectionResetError):
            pass


def eventually(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.025)
    raise AssertionError("condition did not become true")


class SettingsTests(unittest.TestCase):
    def test_overnight_and_equal_windows(self):
        for hour, expected in [(22, False), (23, True), (0, True), (6, True), (7, False)]:
            now = datetime(2026, 11, 12, hour, tzinfo=LOCAL_TZ)
            self.assertEqual(in_window(now, "23:00", "07:00"), expected)
        self.assertTrue(in_window(datetime(2026, 11, 12, 12), "01:00", "01:00"))
        self.assertFalse(in_window(datetime(2026, 11, 12, 7), "08:00", "20:00"))

    def test_rejects_invalid_settings(self):
        for invalid in [
            {"urls": ["file:///etc/passwd"]}, {"urls": ["https://user:pass@example.com"]},
            {"urls": ["http://example.com:bad"]}, {"urls": ["http://example.com:"]},
            {"urls": ["http://example.com/\nsecret"]}, {"rate_mbps": float("nan")},
            {"daily_limit_gib": -1}, {"schedule_start": "99:00"},
            {"run_seconds": True}, {"pause_seconds": 0}, {"schedule_enabled": "false"},
            {"unknown_field": 1}, {"rate_mbps": 0.01},
        ]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                Settings.parse(invalid)


class IntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = ThreadingHTTPServer(("127.0.0.1", 0), SourceHandler)
        cls.source.daemon_threads = True
        cls.source_thread = threading.Thread(target=cls.source.serve_forever, daemon=True)
        cls.source_thread.start()
        cls.url = f"http://127.0.0.1:{cls.source.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.source.shutdown()
        cls.source.server_close()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.engine = Engine(self.directory.name)
        self.engine.start_worker()

    def tearDown(self):
        self.engine.shutdown()
        self.directory.cleanup()

    def configure(self, **changes):
        settings = asdict(Settings(urls=(self.url + "/stream",), rate_mbps=2))
        settings.update(changes)
        self.engine.configure(settings)

    def test_receive_discard_stop_and_persist(self):
        self.configure()
        self.assertEqual(self.engine.snapshot()["stats"]["total_bytes"], 0)
        self.engine.set_enabled(True)
        eventually(lambda: self.engine.snapshot()["stats"]["total_bytes"] > 100000)
        self.engine.set_enabled(False)
        eventually(lambda: not self.engine.snapshot()["connection_open"])
        stopped_bytes = self.engine.snapshot()["stats"]["total_bytes"]
        time.sleep(0.35)
        self.assertEqual(stopped_bytes, self.engine.snapshot()["stats"]["total_bytes"])
        self.engine.shutdown()
        files = sorted(path.name for path in Path(self.directory.name).iterdir())
        self.assertEqual(files, ["config.json", "stats.json"])
        for path in Path(self.directory.name).iterdir():
            self.assertNotIn(b"DISCARD_THIS_PAYLOAD", path.read_bytes())
        restored = Engine(self.directory.name)
        self.assertEqual(restored.snapshot()["stats"]["total_bytes"], stopped_bytes)
        self.assertFalse(restored.snapshot()["enabled"])

    def test_failed_settings_write_does_not_apply_changes(self):
        self.configure()
        before = self.engine.snapshot()["settings"]
        with patch("app.atomic_json", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.engine.configure({"urls": [self.url + "/error"]})
            self.assertEqual(self.engine.snapshot()["settings"], before)
            with self.assertRaises(OSError):
                self.engine.set_enabled(True)
            self.assertFalse(self.engine.snapshot()["enabled"])

    def test_daily_quota_stops_exact_response_body_count_and_rolls_over(self):
        quota = 64 * 1024
        self.configure(rate_mbps=0, daily_limit_gib=quota / GIB)
        self.engine.set_enabled(True)
        eventually(lambda: self.engine.snapshot()["state"] == "quota")
        self.assertEqual(self.engine.snapshot()["stats"]["daily_bytes"], quota)
        eventually(lambda: not self.engine.snapshot()["connection_open"])
        with self.engine.lock:
            self.engine.stats["day"] = "2000-01-01"
        eventually(lambda: self.engine.snapshot()["stats"]["total_bytes"] >= 2 * quota)
        self.assertEqual(self.engine.snapshot()["stats"]["daily_bytes"], quota)

    def test_intermittent_download_pauses_and_resumes(self):
        self.configure(mode="intermittent", run_seconds=1, pause_seconds=1)
        self.engine.set_enabled(True)
        eventually(lambda: self.engine.snapshot()["stats"]["total_bytes"] > 0)
        eventually(lambda: self.engine.snapshot()["state"] == "paused")
        eventually(lambda: not self.engine.snapshot()["connection_open"])
        paused_bytes = self.engine.snapshot()["stats"]["total_bytes"]
        time.sleep(0.3)
        self.assertEqual(self.engine.snapshot()["stats"]["total_bytes"], paused_bytes)
        eventually(lambda: self.engine.snapshot()["stats"]["total_bytes"] > paused_bytes)

    def test_rate_limit_bounds_payload_growth(self):
        self.configure(rate_mbps=1)
        self.engine.set_enabled(True)
        eventually(lambda: self.engine.snapshot()["stats"]["total_bytes"] >= 31250)
        before = self.engine.snapshot()["stats"]["total_bytes"]
        started = time.monotonic()
        time.sleep(1.2)
        elapsed = time.monotonic() - started
        delta = self.engine.snapshot()["stats"]["total_bytes"] - before
        self.assertGreater(delta, 62500)
        self.assertLessEqual(delta, 125000 * elapsed + 2 * 31250)

    def test_http_failure_falls_back_without_killing_worker(self):
        self.configure(urls=[self.url + "/error", self.url + "/stream"])
        self.engine.set_enabled(True)
        eventually(lambda: self.engine.snapshot()["last_error"] is not None)
        self.assertIn("503", self.engine.snapshot()["last_error"])
        eventually(lambda: self.engine.snapshot()["stats"]["total_bytes"] > 0)
        self.assertTrue(self.engine.worker.is_alive())

    def test_bad_chunked_response_does_not_kill_worker(self):
        self.configure(urls=[self.url + "/bad-chunks", self.url + "/stream"])
        self.engine.set_enabled(True)
        eventually(lambda: self.engine.snapshot()["last_error"] is not None)
        eventually(lambda: self.engine.snapshot()["stats"]["total_bytes"] > 0)
        self.assertTrue(self.engine.worker.is_alive())

    def test_enabled_task_is_restored(self):
        self.configure()
        self.engine.set_enabled(True)
        eventually(lambda: self.engine.snapshot()["stats"]["total_bytes"] > 0)
        self.engine.shutdown()
        restored = Engine(self.directory.name)
        self.assertTrue(restored.snapshot()["enabled"])
        baseline = restored.snapshot()["stats"]["total_bytes"]
        restored.start_worker()
        try:
            eventually(lambda: restored.snapshot()["stats"]["total_bytes"] > baseline)
        finally:
            restored.shutdown()

    def test_api_authentication_origin_guard_and_validation(self):
        server = make_server(self.engine, "long-test-password", ("127.0.0.1", 0))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{server.server_port}"
        auth = "Basic " + base64.b64encode(b"admin:long-test-password").decode()
        client = build_opener(ProxyHandler({}))

        def request(path, data=None, headers=None):
            request_headers = {"Authorization": auth}
            request_headers.update(headers or {})
            if data is not None:
                request_headers["Content-Type"] = "application/json"
            return client.open(Request(base + path, data=json.dumps(data).encode() if data is not None else None, headers=request_headers), timeout=3)

        try:
            with client.open(base + "/healthz") as response:
                self.assertEqual(response.status, 200)
            with self.assertRaises(HTTPError) as failure:
                client.open(base + "/api/status")
            self.assertEqual(failure.exception.code, 401)
            for endpoint in ("/", "/app.js", "/style.css", "/api/status"):
                with request(endpoint) as response:
                    self.assertEqual(response.status, 200)
                    self.assertEqual(response.headers["Cache-Control"], "no-store")
            with self.assertRaises(HTTPError) as failure:
                request("/api/start", {})
            self.assertEqual(failure.exception.code, 400)
            with self.assertRaises(HTTPError) as failure:
                request("/api/settings", {}, {"Origin": "http://evil.invalid"})
            self.assertEqual(failure.exception.code, 403)
            with self.assertRaises(HTTPError) as failure:
                request("/api/settings", {"urls": ["file:///etc/passwd"]})
            self.assertEqual(failure.exception.code, 400)
            with request("/api/settings", asdict(Settings(urls=(self.url + "/stream",)))) as response:
                self.assertTrue(json.load(response)["settings"]["urls"])
            with request("/api/start", {}) as response:
                self.assertTrue(json.load(response)["enabled"])
            with request("/api/stop", {}) as response:
                self.assertFalse(json.load(response)["enabled"])
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
