"""DL Rush: receive HTTP response bodies and discard them without creating files."""

from __future__ import annotations

import base64
import binascii
from collections import deque
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from http.client import HTTPException
import json
import math
import os
from pathlib import Path
import secrets
import signal
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


LOCAL_TZ = timezone(timedelta(hours=8))
GIB = 1024**3
CHUNK_SIZE = 64 * 1024
WEB_DIR = Path(__file__).parent / "web"


def local_now():
    return datetime.now(LOCAL_TZ)


def validate_url(value):
    if not isinstance(value, str) or len(value) > 2048:
        raise ValueError("下载地址必须是长度不超过 2048 的 HTTP/HTTPS URL")
    try:
        parts = urlsplit(value)
        if parts.scheme not in {"http", "https"} or not parts.hostname or parts.netloc.endswith(":"):
            raise ValueError()
        if parts.username is not None or parts.password is not None or any(c.isspace() or ord(c) < 32 for c in value):
            raise ValueError()
        _ = parts.port
    except ValueError:
        raise ValueError("请输入有效的 HTTP/HTTPS 地址；地址中不能包含用户名、密码或空白字符") from None
    return value


def minute_of_day(value):
    if not isinstance(value, str) or len(value) != 5 or value[2] != ":":
        raise ValueError("时间格式应为 HH:MM")
    try:
        hour, minute = map(int, value.split(":"))
    except ValueError:
        raise ValueError("时间格式应为 HH:MM") from None
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise ValueError("时间范围无效")
    return hour * 60 + minute


def in_window(now, start, end):
    """Equal endpoints mean 24 hours. Overnight windows belong to both dates."""
    current = now.hour * 60 + now.minute
    begin, finish = minute_of_day(start), minute_of_day(end)
    if begin == finish:
        return True
    return begin <= current < finish if begin < finish else current >= begin or current < finish


@dataclass(frozen=True)
class Settings:
    urls: tuple = ()
    mode: str = "continuous"
    run_seconds: int = 60
    pause_seconds: int = 60
    rate_mbps: float = 20.0
    daily_limit_gib: float = 0.0
    schedule_enabled: bool = False
    schedule_start: str = "01:00"
    schedule_end: str = "07:00"

    @classmethod
    def parse(cls, data):
        if not isinstance(data, dict) or set(data) - {f.name for f in fields(cls)}:
            raise ValueError("设置包含未知字段或格式不正确")
        values = asdict(cls())
        values.update(data)
        urls = values["urls"]
        if not isinstance(urls, (list, tuple)) or len(urls) > 20:
            raise ValueError("最多可填写 20 个下载地址")
        values["urls"] = tuple(dict.fromkeys(validate_url(url) for url in urls))
        if not isinstance(values["mode"], str) or values["mode"] not in {"continuous", "intermittent"}:
            raise ValueError("运行模式无效")
        for name in ("run_seconds", "pause_seconds"):
            if type(values[name]) is not int or not 1 <= values[name] <= 86400:
                raise ValueError("运行和间歇时长必须为 1–86400 秒的整数")
        for name, maximum in (("rate_mbps", 10000), ("daily_limit_gib", 10240)):
            value = values[name]
            if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= maximum:
                raise ValueError("速率或流量上限超出允许范围")
            if name == "rate_mbps" and 0 < value < 0.1:
                raise ValueError("限速至少为 0.1 Mbps，或填 0 表示不限速")
        if type(values["schedule_enabled"]) is not bool:
            raise ValueError("定时开关必须为布尔值")
        minute_of_day(values["schedule_start"])
        minute_of_day(values["schedule_end"])
        return cls(**values)


def read_json(path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def atomic_json(path, value):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as target:
        os.chmod(temporary, 0o600)
        json.dump(value, target, ensure_ascii=False)
        target.flush()
        os.fsync(target.fileno())
    temporary.replace(path)


class SafeRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        validate_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class Engine:
    def __init__(self, data_dir):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        saved = read_json(self.data_dir / "config.json", {})
        self.settings = Settings.parse(saved.get("settings", {}))
        self.enabled = saved.get("enabled") is True
        self.stats = read_json(self.data_dir / "stats.json", {
            "total_bytes": 0, "daily_bytes": 0, "day": local_now().date().isoformat(),
        })
        if not isinstance(self.stats, dict) or any(type(self.stats.get(k)) is not int or self.stats[k] < 0 for k in ("total_bytes", "daily_bytes")):
            raise ValueError("stats.json 数据无效，请先备份并检查统计文件")
        self.lock = threading.RLock()
        self.changed = threading.Event()
        self.exiting = threading.Event()
        self.generation = 0
        self.cycle_started = None
        self.connection_open = False
        self.last_error = None
        self.retry_at = 0.0
        self.samples = deque(maxlen=61)
        self.events = deque(maxlen=60)
        self.last_saved = time.monotonic()
        self.worker = threading.Thread(target=self._work, name="download-and-discard", daemon=True)
        self._event("服务已就绪；下载数据只在内存中经过，不保存文件")

    def _event(self, message):
        self.events.appendleft({"time": local_now().strftime("%H:%M:%S"), "message": message})

    def _roll_day_locked(self, now):
        day = now.date().isoformat()
        if self.stats["day"] != day:
            self.stats.update(day=day, daily_bytes=0)

    def _state_locked(self):
        now, monotonic = local_now(), time.monotonic()
        self._roll_day_locked(now)
        if not self.enabled:
            self.cycle_started = None
            return "stopped", None
        if not self.settings.urls:
            return "missing_source", None
        if self.settings.schedule_enabled and not in_window(now, self.settings.schedule_start, self.settings.schedule_end):
            self.cycle_started = None
            start = minute_of_day(self.settings.schedule_start)
            next_start = now.replace(hour=start // 60, minute=start % 60, second=0, microsecond=0)
            if next_start <= now:
                next_start += timedelta(days=1)
            return "waiting", (next_start - now).total_seconds()
        if self.settings.daily_limit_gib and self.stats["daily_bytes"] >= self.settings.daily_limit_gib * GIB:
            self.cycle_started = None
            tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            return "quota", (tomorrow - now).total_seconds()
        if self.cycle_started is None:
            self.cycle_started = monotonic
        if self.settings.mode == "intermittent":
            position = (monotonic - self.cycle_started) % (self.settings.run_seconds + self.settings.pause_seconds)
            if position >= self.settings.run_seconds:
                return "paused", self.settings.run_seconds + self.settings.pause_seconds - position
            return "running", self.settings.run_seconds - position
        return "running", None

    def _persist_locked(self, settings=None, enabled=None):
        atomic_json(self.data_dir / "stats.json", self.stats)
        if settings is not None:
            atomic_json(self.data_dir / "config.json", {"settings": asdict(settings), "enabled": enabled})
        self.last_saved = time.monotonic()

    def start_worker(self):
        self.worker.start()

    def configure(self, data):
        settings = Settings.parse(data)
        with self.lock:
            if self.enabled and not settings.urls:
                raise ValueError("运行时至少需要一个下载地址；请先停止任务")
            self._persist_locked(settings, self.enabled)
            self.settings = settings
            self.generation += 1
            self.cycle_started = None
            self.retry_at = 0
            self.last_error = None
            self._event("设置已保存，新设置立即生效")
            self.changed.set()

    def set_enabled(self, enabled):
        with self.lock:
            if enabled and not self.settings.urls:
                raise ValueError("请先填写并保存下载地址")
            self._persist_locked(self.settings, enabled)
            self.enabled = enabled
            self.generation += 1
            self.cycle_started = None
            self.retry_at = 0
            self.last_error = None
            self._event("任务已启用" if enabled else "任务已停止")
            self.changed.set()

    def snapshot(self):
        with self.lock:
            state, next_change = self._state_locked()
            second = int(time.monotonic())
            buckets = dict(self.samples)
            speed = sum(buckets.get(s, 0) for s in range(second - 2, second + 1)) * 8 / 3 / 1e6
            return {
                "settings": asdict(self.settings), "enabled": self.enabled, "state": state,
                "next_change_seconds": math.ceil(next_change) if next_change is not None else None,
                "connection_open": self.connection_open, "rate_mbps": round(speed, 3),
                "stats": dict(self.stats), "last_error": self.last_error,
                "retry_seconds": max(0, math.ceil(self.retry_at - time.monotonic())),
                "history": [round(buckets.get(s, 0) * 8 / 1e6, 3) for s in range(second - 59, second + 1)],
                "events": list(self.events), "timezone": "UTC+08:00",
            }

    def _allowed(self, generation):
        with self.lock:
            return not self.exiting.is_set() and generation == self.generation and self._state_locked()[0] == "running"

    def _wait(self, seconds, generation):
        deadline = time.monotonic() + seconds
        while self._allowed(generation):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            self.changed.wait(min(remaining, 0.1))
            self.changed.clear()
        return False

    def _record(self, size):
        with self.lock:
            self._roll_day_locked(local_now())
            self.stats["total_bytes"] += size
            self.stats["daily_bytes"] += size
            second = int(time.monotonic())
            if self.samples and self.samples[-1][0] == second:
                self.samples[-1] = (second, self.samples[-1][1] + size)
            else:
                self.samples.append((second, size))
            self.last_error = None
            self.retry_at = 0
            if time.monotonic() - self.last_saved >= 15:
                self._persist_locked()

    def _receive(self, url, settings, generation):
        request = Request(url, headers={
            "User-Agent": "DL-Rush/1.0", "Accept-Encoding": "identity", "Cache-Control": "no-cache",
        })
        # A fixed-size read is the only place the response body enters memory.
        # There is deliberately no file handle for the response body.
        with self.lock:
            self.connection_open = True
        try:
            with build_opener(SafeRedirects()).open(request, timeout=5) as response:
                if response.status != 200:
                    raise ValueError("下载源未返回 HTTP 200")
                rate = settings.rate_mbps * 125000
                read_size = min(CHUNK_SIZE, max(1024, int(rate / 4))) if rate else CHUNK_SIZE
                while self._allowed(generation):
                    size = read_size
                    with self.lock:
                        if settings.daily_limit_gib:
                            remaining = math.ceil(settings.daily_limit_gib * GIB) - self.stats["daily_bytes"]
                            size = min(size, max(0, remaining))
                    if not size:
                        break
                    data = response.read1(size)
                    if not data:
                        break
                    count = len(data)
                    del data
                    self._record(count)
                    if rate and not self._wait(count / rate, generation):
                        break
        finally:
            with self.lock:
                self.connection_open = False

    def _work(self):
        index, failures = 0, 0
        while not self.exiting.is_set():
            with self.lock:
                state, _ = self._state_locked()
                settings, generation = self.settings, self.generation
                if time.monotonic() - self.last_saved >= 15:
                    self._persist_locked()
            if state != "running":
                self.changed.wait(0.2)
                self.changed.clear()
                failures = 0
                continue
            url = settings.urls[index % len(settings.urls)]
            index += 1
            delay = 1
            try:
                self._receive(url, settings, generation)
                failures = 0
            except (HTTPError, URLError, TimeoutError, OSError, ValueError, HTTPException) as error:
                if self._allowed(generation):
                    failures += 1
                    delay = min(60, 2 ** min(failures, 6))
                    if isinstance(error, HTTPError):
                        description = f"下载源返回 HTTP {error.code}"
                        error.close()
                    elif isinstance(error, ValueError):
                        description = "下载源或重定向地址无效"
                    else:
                        description = "下载连接失败或超时，请检查地址、网络和证书"
                    with self.lock:
                        self.last_error = description
                        self.retry_at = time.monotonic() + delay
                        self._event(f"{description}；{delay} 秒后尝试下一个地址")
            self._wait(delay, generation)

    def shutdown(self):
        self.exiting.set()
        self.changed.set()
        if self.worker.is_alive():
            self.worker.join(timeout=6)
        with self.lock:
            self._persist_locked()


def make_server(engine, password, address=("0.0.0.0", 8787)):
    class Handler(BaseHTTPRequestHandler):
        server_version = "DL-Rush/1.0"

        def log_message(self, *args):
            pass  # Do not log URL query strings or credentials.

        def respond(self, status, body, content_type="application/json; charset=utf-8", authenticate=False):
            if not isinstance(body, bytes):
                body = json.dumps(body, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; object-src 'none'; frame-ancestors 'none'; base-uri 'none'")
            if authenticate:
                self.send_header("WWW-Authenticate", 'Basic realm="DL Rush", charset="UTF-8"')
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def authenticated(self):
            try:
                scheme, credentials = self.headers.get("Authorization", "").split(" ", 1)
                user, supplied = base64.b64decode(credentials, validate=True).decode("utf-8").split(":", 1)
                valid = scheme.lower() == "basic" and secrets.compare_digest(user.encode(), b"admin") and secrets.compare_digest(supplied.encode(), password.encode())
            except (ValueError, UnicodeDecodeError, binascii.Error):
                valid = False
            if not valid:
                self.respond(401, {"error": "请使用 admin 和配置的管理员密码登录"}, authenticate=True)
            return valid

        def do_GET(self):
            path = urlsplit(self.path).path
            if path == "/healthz":
                self.respond(200 if engine.worker.is_alive() else 503, {"ok": engine.worker.is_alive()})
                return
            if not self.authenticated():
                return
            if path == "/api/status":
                self.respond(200, engine.snapshot())
            elif path in {"/", "/app.js", "/style.css"}:
                filename, mime = {"/": ("index.html", "text/html"), "/app.js": ("app.js", "text/javascript"), "/style.css": ("style.css", "text/css")}[path]
                self.respond(200, (WEB_DIR / filename).read_bytes(), mime + "; charset=utf-8")
            else:
                self.respond(404, {"error": "未找到该页面"})

        def do_POST(self):
            if not self.authenticated():
                return
            origin = self.headers.get("Origin")
            if self.headers.get("Sec-Fetch-Site") == "cross-site" or (origin and (urlsplit(origin).scheme not in {"http", "https"} or urlsplit(origin).netloc != self.headers.get("Host"))):
                self.respond(403, {"error": "不允许跨站修改设置"})
                return
            if self.headers.get("Content-Type", "").split(";")[0].strip() != "application/json":
                self.respond(415, {"error": "请使用 application/json"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 65536:
                    raise ValueError("请求内容长度应为 1–65536 字节")
                data = json.loads(self.rfile.read(length))
                if self.path == "/api/settings":
                    engine.configure(data)
                elif self.path in {"/api/start", "/api/stop"}:
                    engine.set_enabled(self.path == "/api/start")
                else:
                    self.respond(404, {"error": "接口不存在"})
                    return
                self.respond(200, engine.snapshot())
            except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
                self.respond(400, {"error": "设置无效，请检查下载地址、时间、时长和数值范围"})
            except ValueError as error:
                self.respond(400, {"error": str(error)})
            except OSError:
                self.respond(500, {"error": "设置保存失败，请检查数据卷权限和可用空间"})

    server = ThreadingHTTPServer(address, Handler)
    server.daemon_threads = True
    return server


def main():
    password = os.environ.get("ADMIN_PASSWORD", "")
    if len(password) < 12 or password == "replace-with-your-own-password":
        raise SystemExit("请设置 ADMIN_PASSWORD：至少 12 位，且不能使用示例密码")
    engine = Engine(os.environ.get("DATA_DIR", "/data"))
    server = make_server(engine, password, ("0.0.0.0", int(os.environ.get("PORT", "8787"))))
    engine.start_worker()
    ended = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: ended.set())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"DL Rush 已启动，端口 {server.server_port}；用户名 admin；默认不启动未配置的任务", flush=True)
    try:
        ended.wait()
    finally:
        server.shutdown()
        server.server_close()
        engine.shutdown()


if __name__ == "__main__":
    main()
