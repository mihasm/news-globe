#!/usr/bin/env python3
"""
Frontend Debug Script using Playwright (Firefox)

- Navigates ONLY to http://localhost (and other localhost loopback variants).
- Allows all subsequent network requests (normal browser behavior).
- Streams logs immediately to console AND file.
- Captures:
  - console output (text + serialized args when possible)
  - uncaught page errors (pageerror)
  - unhandled errors + unhandled promise rejections (via init script)
  - all requests + responses
  - failed requests

Usage:
  python3 frontend_debug.py
"""

import asyncio
import signal
import time
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

from playwright.async_api import async_playwright, ConsoleMessage, Request, Response, Error

ALLOWED_START_HOSTS = {"localhost", "127.0.0.1", "::1"}
START_URL = "http://localhost/"
AUTO_CLOSE_SECONDS = 15


def _ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _tss() -> str:
    return time.strftime("%H:%M:%S")


def _is_allowed_start_url(url: str) -> bool:
    try:
        u = urlparse(url)
        host = (u.hostname or "").lower()
        return host in ALLOWED_START_HOSTS
    except Exception:
        return False


def _safe_str(x: Any) -> str:
    try:
        return str(x)
    except Exception:
        return "<unprintable>"


def _format_location(msg: ConsoleMessage) -> str:
    try:
        loc = msg.location
        if not loc:
            return "unknown"
        url = loc.get("url") or ""
        line = loc.get("lineNumber")
        col = loc.get("columnNumber")
        if url:
            if line is None:
                return url
            if col is None:
                return f"{url}:{line}"
            return f"{url}:{line}:{col}"
    except Exception:
        pass
    return "unknown"


async def _serialize_console_args(msg: ConsoleMessage, per_arg_timeout_ms: int = 300) -> List[str]:
    out: List[str] = []
    for i, arg in enumerate(msg.args):
        try:
            val = await arg.json_value(timeout=per_arg_timeout_ms)
            out.append(f"arg{i}={val!r}")
        except Exception:
            try:
                preview = await arg.evaluate("a => String(a)")
                out.append(f"arg{i}={preview!r}")
            except Exception:
                out.append(f"arg{i}=<unserializable>")
    return out


class LiveLogger:
    def __init__(self, log_path: Path):
        self.log_path = log_path
        self._fh = open(log_path, "a", encoding="utf-8")
        self._lock = asyncio.Lock()

    async def write(self, line: str) -> None:
        async with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()

    async def close(self) -> None:
        async with self._lock:
            try:
                self._fh.flush()
            finally:
                self._fh.close()


def _request_brief(req: Request) -> str:
    try:
        rt = req.resource_type
    except Exception:
        rt = "unknown"
    return f"{req.method} {req.url} ({rt})"


async def main() -> int:
    if not _is_allowed_start_url(START_URL):
        print(f"Refusing to navigate to non-localhost start url: {START_URL}")
        return 2

    script_dir = Path(__file__).resolve().parent
    log_file = script_dir / "frontend_debug.log"

    with open(log_file, "w", encoding="utf-8") as f:
        f.write("=== Frontend Debug Session Started ===\n")
        f.write(f"Timestamp: {_ts()}\n")
        f.write(f"Start URL: {START_URL}\n")
        f.write(f"Auto-close: {AUTO_CLOSE_SECONDS}s\n")
        f.write("=====================================\n\n")

    logger = LiveLogger(log_file)
    stop_event = asyncio.Event()

    async def _stop(reason: str) -> None:
        if stop_event.is_set():
            return
        line = f"[{_tss()}] [STOP] {reason}"
        print(line)
        await logger.write(line)
        stop_event.set()

    # Ctrl+C / SIGTERM best-effort
    loop = asyncio.get_running_loop()
    for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(_stop(f"signal {s.name}")))
        except NotImplementedError:
            pass

    async def auto_close_task() -> None:
        await asyncio.sleep(AUTO_CLOSE_SECONDS)
        await _stop(f"auto-close after {AUTO_CLOSE_SECONDS}s")

    console_counts: Dict[str, int] = {}

    print("=== Frontend Debug Session Started ===")
    print(f"Timestamp: {_ts()}")
    print(f"Log file: {log_file}")
    print("Launching Firefox...")

    try:
        async with async_playwright() as p:
            browser = await p.firefox.launch(
                headless=False,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            )

            context = await browser.new_context()
            page = await context.new_page()

            # Inject BEFORE any page scripts run: surface unhandled errors/rejections into console.error
            await page.add_init_script(
                """
(() => {
  const safeStringify = (v) => {
    try { return typeof v === 'string' ? v : JSON.stringify(v); }
    catch { try { return String(v); } catch { return '<unprintable>'; } }
  };

  window.addEventListener('error', (e) => {
    // Resource errors sometimes have no stack; still log them
    const msg = e && e.message ? e.message : 'window error event';
    const file = e && e.filename ? e.filename : '';
    const line = e && typeof e.lineno === 'number' ? e.lineno : '';
    const col  = e && typeof e.colno === 'number' ? e.colno : '';
    const err  = e && e.error ? e.error : null;
    const stack = err && err.stack ? err.stack : '';
    console.error('[window.error]', msg, file, line, col, stack);
  }, true);

  window.addEventListener('unhandledrejection', (e) => {
    const r = e ? e.reason : null;
    const reason = r && r.stack ? r.stack : safeStringify(r);
    console.error('[unhandledrejection]', reason);
  });

  // Optional: catch synchronous exceptions not routed elsewhere
  window.onerror = function(message, source, lineno, colno, error) {
    const stack = error && error.stack ? error.stack : '';
    console.error('[window.onerror]', message, source, lineno, colno, stack);
    return false;
  };
})();
"""
            )

            async def on_console(msg: ConsoleMessage) -> None:
                try:
                    level = (msg.type or "log").lower()
                    console_counts[level] = console_counts.get(level, 0) + 1

                    entry_text = msg.text or ""
                    location = _format_location(msg)
                    args: List[str] = []
                    if msg.args:
                        args = await _serialize_console_args(msg)

                    header = f"[{_tss()}] [CONSOLE:{level.upper()}] {entry_text}"
                    print(header)
                    await logger.write(header)

                    if location and location != "unknown":
                        loc_line = f"    Location: {location}"
                        print(loc_line)
                        await logger.write(loc_line)

                    if args:
                        args_line = "    Args: " + " | ".join(args)
                        print(args_line)
                        await logger.write(args_line)

                except Exception as e:
                    line = f"[{_tss()}] [ERROR] console handler: {_safe_str(e)}"
                    print(line)
                    await logger.write(line)

            async def on_page_error(error: Error) -> None:
                line = f"[{_tss()}] [PAGEERROR] {_safe_str(error)}"
                print(line)
                await logger.write(line)

            async def on_request(req: Request) -> None:
                line = f"[{_tss()}] [REQUEST] {_request_brief(req)}"
                print(line)
                await logger.write(line)

            async def on_response(resp: Response) -> None:
                try:
                    req = resp.request
                    line = f"[{_tss()}] [RESPONSE] {resp.status} {req.method} {resp.url}"
                except Exception:
                    line = f"[{_tss()}] [RESPONSE] {_safe_str(resp)}"
                print(line)
                await logger.write(line)

                # Extra: flag HTTP errors immediately (many “console errors” are really 4xx/5xx)
                try:
                    if resp.status >= 400:
                        await logger.write(f"[{_tss()}] [HTTPERROR] {resp.status} {resp.url}")
                except Exception:
                    pass

            async def on_request_failed(req: Request) -> None:
                failure = req.failure
                fail_text = ""
                if failure:
                    try:
                        fail_text = failure.get("errorText") or _safe_str(failure)
                    except Exception:
                        fail_text = _safe_str(failure)
                line = f"[{_tss()}] [REQUESTFAILED] {_request_brief(req)} {fail_text}".rstrip()
                print(line)
                await logger.write(line)

            # Attach listeners BEFORE navigation
            page.on("console", lambda m: asyncio.create_task(on_console(m)))
            page.on("pageerror", lambda e: asyncio.create_task(on_page_error(e)))
            page.on("request", lambda r: asyncio.create_task(on_request(r)))
            page.on("response", lambda r: asyncio.create_task(on_response(r)))
            page.on("requestfailed", lambda r: asyncio.create_task(on_request_failed(r)))

            page.on("close", lambda: asyncio.create_task(_stop("page closed")))
            browser.on("disconnected", lambda: asyncio.create_task(_stop("browser disconnected")))

            nav_line = f"[{_tss()}] Navigating to {START_URL}"
            print(nav_line)
            await logger.write(nav_line)

            resp = await page.goto(START_URL, wait_until="domcontentloaded", timeout=30_000)
            status = resp.status if resp else None
            nav2 = f"[{_tss()}] Navigation status: {status if status is not None else 'No response'}"
            print(nav2)
            await logger.write(nav2)

            try:
                title = await page.title()
            except Exception as e:
                title = f"<unable to read title: {_safe_str(e)}>"
            title_line = f"[{_tss()}] Page title: {title}"
            print(title_line)
            await logger.write(title_line)

            run_line = f"[{_tss()}] Streaming logs for {AUTO_CLOSE_SECONDS}s..."
            print(run_line)
            await logger.write(run_line)

            auto_task = asyncio.create_task(auto_close_task())
            await stop_event.wait()
            auto_task.cancel()

            await logger.write("\n=== SUMMARY ===")
            total = sum(console_counts.values())
            await logger.write(f"Console messages: {total}")
            for lvl, cnt in sorted(console_counts.items(), key=lambda x: x[0]):
                await logger.write(f"{lvl.upper()}: {cnt}")

            await browser.close()

    except Exception as e:
        line = f"[{_ts()}] Fatal error: {_safe_str(e)}"
        print(line)
        await logger.write(line)
        await logger.write("Make sure the frontend is running on http://localhost")
        await logger.close()
        return 1

    await logger.write(f"\n=== Frontend Debug Session Completed ===\nTimestamp: {_ts()}")
    await logger.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))