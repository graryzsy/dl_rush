"""Validate the actual Compose service using a loopback-only generated stream."""

import base64
import json
import math
import os
from pathlib import Path
import secrets
import socket
import subprocess
import time
from urllib.error import URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
SOURCE = '''
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
class Source(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length", str(64 * 1024 * 1024))
        self.end_headers()
        try:
            for _ in range(1024): self.wfile.write(b"x" * 65536)
        except (BrokenPipeError, ConnectionResetError): pass
ThreadingHTTPServer(("127.0.0.1", 9000), Source).serve_forever()
'''


def main():
    environment = os.environ.copy()
    for name in ("DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH"):
        environment.pop(name, None)
    password = secrets.token_urlsafe(24)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    environment.update(ADMIN_PASSWORD=password, WEB_PORT=str(port))
    project = "dl-rush-smoke-" + secrets.token_hex(4)
    docker = ["docker", "--host=unix:///var/run/docker.sock"]
    compose = docker + ["compose", "--project-name", project]

    def run(command):
        return subprocess.check_output(command, cwd=ROOT, env=environment, text=True).strip()

    auth = "Basic " + base64.b64encode(f"admin:{password}".encode()).decode()

    def api(path, body=None):
        headers = {"Authorization": auth}
        if body is not None:
            headers["Content-Type"] = "application/json"
        with urlopen(Request(f"http://127.0.0.1:{port}" + path, headers=headers,
                             data=json.dumps(body).encode() if body is not None else None), timeout=3) as response:
            return json.load(response)

    def wait_for(check, timeout=35):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                value = check()
                if value:
                    return value
            except (URLError, ConnectionError, TimeoutError):
                pass
            time.sleep(0.2)
        raise AssertionError("Docker check timed out")

    try:
        run(compose + ["up", "--detach", "--no-build"])
        container = run(compose + ["ps", "--quiet", "dl-rush"])
        wait_for(lambda: api("/healthz")["ok"])
        status = api("/api/status")
        assert not status["enabled"] and status["stats"]["total_bytes"] == 0
        run(docker + ["exec", "--detach", container, "python", "-u", "-c", SOURCE])
        api("/api/settings", {
            "urls": ["http://127.0.0.1:9000/stream"], "rate_mbps": 20,
            "daily_limit_gib": 0.001,
        })
        api("/api/start", {})
        wait_for(lambda: api("/api/status")["state"] == "quota")
        status = api("/api/status")
        expected = math.ceil(0.001 * 1024**3)
        assert status["stats"]["daily_bytes"] == expected
        wait_for(lambda: run(docker + ["inspect", "--format", "{{.State.Health.Status}}", container]) == "healthy")
        files = run(docker + ["exec", container, "python", "-c", "import os; print(','.join(sorted(os.listdir('/data'))))"])
        assert files == "config.json,stats.json", files
        security = run(docker + ["inspect", "--format", "{{.Config.User}} {{.HostConfig.ReadonlyRootfs}}", container])
        assert security == "10001:10001 true", security
        run(compose + ["restart", "dl-rush"])
        wait_for(lambda: api("/healthz")["ok"])
        status = api("/api/status")
        assert status["enabled"] and status["state"] == "quota"
        assert status["stats"]["total_bytes"] == expected
        api("/api/stop", {})
        platform = run(docker + ["image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", "dl-rush:local"])
        print(f"Docker checks passed ({platform}): healthy, non-root, read-only root, received {expected} bytes,")
        print("quota stopped the task, only config/stats persisted, restart restored statistics and enabled state.")
    finally:
        run(compose + ["down", "--volumes"])


if __name__ == "__main__":
    main()
