"""Exercise the real web UI against a local, generated HTTP data stream."""

from pathlib import Path
import os
import shutil
import sys
import tempfile
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from playwright.sync_api import expect, sync_playwright

from app import Engine, make_server
from test_app import SourceHandler, ThreadingHTTPServer


def main():
    output = Path(__file__).resolve().parents[1] / "test-results"
    output.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory() as directory:
        engine = Engine(directory)
        engine.start_worker()
        source = ThreadingHTTPServer(("127.0.0.1", 0), SourceHandler)
        source.daemon_threads = True
        server = make_server(engine, "browser-test-password", ("127.0.0.1", 0))
        for service in (source, server):
            threading.Thread(target=service.serve_forever, daemon=True).start()
        try:
            with sync_playwright() as playwright:
                executable = os.environ.get("CHROMIUM_EXECUTABLE") or shutil.which("chromium")
                browser = playwright.chromium.launch(
                    executable_path=executable, headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"],
                )
                context = browser.new_context(
                    viewport={"width": 1440, "height": 1100}, device_scale_factor=1,
                    http_credentials={"username": "admin", "password": "browser-test-password"},
                )
                errors = []
                page = context.new_page()
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.on("console", lambda message: errors.append(message.text) if message.type == "error" else None)
                url = f"http://127.0.0.1:{server.server_port}"
                page.goto(url)
                expect(page.locator("#toggle-run")).to_be_enabled()
                expect(page.locator("#state-label")).to_have_text("已停止")
                expect(page.locator("#run-seconds")).to_be_disabled()
                expect(page.locator("#schedule-start")).to_be_disabled()
                page.locator("#urls").fill(f"http://127.0.0.1:{source.server_port}/stream")
                page.locator('input[value="intermittent"]').check()
                expect(page.locator("#run-seconds")).to_be_enabled()
                page.locator("#run-seconds").fill("3")
                page.locator("#pause-seconds").fill("1")
                page.locator("#rate-mbps").fill("2")
                page.locator("#schedule-enabled").check()
                expect(page.locator("#schedule-start")).to_be_enabled()
                page.locator("#schedule-start").fill("00:00")
                page.locator("#schedule-end").fill("00:00")
                page.locator("#save-settings").click()
                expect(page.locator("#dirty-note")).to_have_text("设置已同步")
                page.locator("#toggle-run").click()
                expect(page.locator("#state-label")).to_have_text("运行中")
                page.wait_for_function("() => Number(document.querySelector('#daily').textContent) > 0")
                expect(page.locator("#chart-empty")).to_be_hidden()
                page.screenshot(path=str(output / "desktop.png"), full_page=True)

                # Closing the control page must not stop the container's worker.
                page.close()
                before = engine.snapshot()["stats"]["total_bytes"]
                deadline = time.monotonic() + 5
                while engine.snapshot()["stats"]["total_bytes"] <= before and time.monotonic() < deadline:
                    time.sleep(0.1)
                assert engine.snapshot()["stats"]["total_bytes"] > before

                page = context.new_page()
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.set_viewport_size({"width": 390, "height": 844})
                page.goto(url)
                expect(page.locator("#toggle-run")).to_contain_text("停止任务")
                page.locator("#toggle-run").click()
                expect(page.locator("#state-label")).to_have_text("已停止")
                assert page.evaluate("() => document.documentElement.scrollWidth <= window.innerWidth"), "Mobile layout overflows"
                page.screenshot(path=str(output / "mobile.png"), full_page=True)
                assert not errors, errors
                browser.close()
                print("Browser checks passed: save, start, stop, scheduled mode, background execution, desktop and mobile layouts.")
                print(f"Screenshots: {output}")
        finally:
            for service in (source, server):
                service.shutdown()
                service.server_close()
            engine.shutdown()


if __name__ == "__main__":
    main()
