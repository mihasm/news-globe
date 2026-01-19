#!/usr/bin/env python3
"""
Frontend E2E Test Runner using Playwright (Firefox)

- Discovers and runs all *.py scenarios in tests/scenarios/ (except __init__.py)
- Each scenario runs in a fresh browser context and page
- Logs to timestamped files: tests/logs/<YYYYMMDD_HHMMSS>__<scenario_name>.log
- Only initial navigation restricted to localhost
- Continues on scenario failures

Usage:
  python3 tests/runner.py [--scenario SCENARIO_NAME]

Options:
  --scenario SCENARIO_NAME    Run only the specified scenario (e.g., scenario_1_page_load)
"""

import argparse
import asyncio
import importlib.util
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from playwright.async_api import async_playwright, ConsoleMessage, Page, Request, Error

ALLOWED_START_HOSTS = {"localhost", "127.0.0.1", "::1"}
START_URL = "http://localhost/"


def _ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _tss() -> str:
    return time.strftime("%H:%M:%S")


def _timestamp_filename() -> str:
    return time.strftime("%Y%m%d_%H%M%S")


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


def _request_brief(req: Request) -> str:
    try:
        rt = req.resource_type
    except Exception:
        rt = "unknown"
    return f"{req.method} {req.url} ({rt})"


def write_log_line(log_file: Path, line: str) -> None:
    print(line)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")
        f.flush()


def load_scenarios(scenario_filter: Optional[str] = None) -> List[Path]:
    scenarios_dir = Path(__file__).parent / "scenarios"
    scenarios = []
    for py_file in scenarios_dir.glob("*.py"):
        if py_file.name != "__init__.py":
            if scenario_filter is None or py_file.stem == scenario_filter:
                scenarios.append(py_file)
    return sorted(scenarios)


async def run_scenario(scenario_path: Path, browser) -> int:
    scenario_name = scenario_path.stem
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)

    log_file = logs_dir / f"{scenario_name}.log"

    # Delete old log files for this scenario
    for old_log in logs_dir.glob(f"{scenario_name}.log"):
        old_log.unlink()

    # Write scenario header
    header = f"=== Scenario: {scenario_name} ===\nTimestamp: {_ts()}\nStart URL: {START_URL}\n{'='*50}\n"
    write_log_line(log_file, header)
    print(f"Running scenario: {scenario_name}")

    # Load scenario module
    spec = importlib.util.spec_from_file_location(scenario_name, scenario_path)
    if not spec or not spec.loader:
        error_msg = f"Failed to load scenario: {scenario_name}"
        write_log_line(log_file, f"[{_tss()}] ERROR: {error_msg}")
        print(error_msg)
        return 1

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
        if not hasattr(module, 'run'):
            error_msg = f"Scenario {scenario_name} missing 'run' function"
            write_log_line(log_file, f"[{_tss()}] ERROR: {error_msg}")
            print(error_msg)
            return 1
    except Exception as e:
        error_msg = f"Failed to import scenario {scenario_name}: {_safe_str(e)}"
        write_log_line(log_file, f"[{_tss()}] ERROR: {error_msg}")
        print(error_msg)
        return 1

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

    console_logs: List[Dict[str, Any]] = []

    async def on_console(msg: ConsoleMessage) -> None:
        try:
            entry = {
                "timestamp": _tss(),
                "level": (msg.type or "log").lower(),
                "text": msg.text or "",
                "location": _format_location(msg),
                "args": [],
            }

            if msg.args:
                entry["args"] = await _serialize_console_args(msg)

            console_logs.append(entry)

            header = f"[{entry['timestamp']}] [CONSOLE:{entry['level'].upper()}] {entry['text']}"
            print(header)
            write_log_line(log_file, header)

            if entry["location"] and entry["location"] != "unknown":
                loc_line = f"    Location: {entry['location']}"
                print(loc_line)
                write_log_line(log_file, loc_line)

            if entry["args"]:
                args_line = "    Args: " + " | ".join(entry["args"])
                print(args_line)
                write_log_line(log_file, args_line)

        except Exception as e:
            line = f"[{_tss()}] [ERROR] console handler: {_safe_str(e)}"
            print(line)
            write_log_line(log_file, line)

    async def on_page_error(error: Error) -> None:
        line = f"[{_tss()}] [PAGEERROR] {_safe_str(error)}"
        write_log_line(log_file, line)

    async def on_request(req: Request) -> None:
        line = f"[{_tss()}] [REQUEST] {_request_brief(req)}"
        write_log_line(log_file, line)

    async def on_response(resp) -> None:
        try:
            req = resp.request
            line = f"[{_tss()}] [RESPONSE] {resp.status} {req.method} {resp.url}"
        except Exception:
            line = f"[{_tss()}] [RESPONSE] {_safe_str(resp)}"
        write_log_line(log_file, line)

        # Extra: flag HTTP errors immediately (many "console errors" are really 4xx/5xx)
        try:
            if resp.status >= 400:
                write_log_line(log_file, f"[{_tss()}] [HTTPERROR] {resp.status} {resp.url}")
        except Exception:
            pass

    async def on_request_failed(request: Request) -> None:
        failure = request.failure
        fail_text = ""
        if failure:
            try:
                fail_text = failure.get("errorText") or _safe_str(failure)
            except Exception:
                fail_text = _safe_str(failure)
        line = f"[{_tss()}] [REQUESTFAILED] {_request_brief(request)} {fail_text}".rstrip()
        write_log_line(log_file, line)

    page.on("console", lambda m: asyncio.create_task(on_console(m)))
    page.on("pageerror", lambda e: asyncio.create_task(on_page_error(e)))
    page.on("request", lambda r: asyncio.create_task(on_request(r)))
    page.on("response", lambda r: asyncio.create_task(on_response(r)))
    page.on("requestfailed", lambda r: asyncio.create_task(on_request_failed(r)))

    try:
        start_time = time.time()
        write_log_line(log_file, f"[{_tss()}] STEP start")

        # Initial navigation with localhost validation
        if not _is_allowed_start_url(START_URL):
            raise ValueError(f"Refusing to navigate to non-localhost start url: {START_URL}")

        write_log_line(log_file, f"[{_tss()}] Navigating to {START_URL}")
        resp = await page.goto(START_URL, wait_until="domcontentloaded", timeout=30_000)
        status = resp.status if resp else None
        write_log_line(log_file, f"[{_tss()}] Navigation status: {status if status is not None else 'No response'}")

        # Create a logging function for the scenario
        def scenario_log(message: str) -> None:
            log_line = f"[{_tss()}] SCENARIO: {message}"
            write_log_line(log_file, log_line)

        # Run the scenario and check result
        scenario_result = await module.run(page, scenario_log)

        elapsed_ms = int((time.time() - start_time) * 1000)

        if scenario_result is True:
            write_log_line(log_file, f"[{_tss()}] SCENARIO SUCCESS")
            write_log_line(log_file, f"[{_tss()}] STEP end ({elapsed_ms}ms)")
            print(f"✓ Scenario {scenario_name} PASSED in {elapsed_ms}ms")
            return 0
        else:
            write_log_line(log_file, f"[{_tss()}] SCENARIO FAILED")
            write_log_line(log_file, f"[{_tss()}] STEP end ({elapsed_ms}ms)")
            print(f"✗ Scenario {scenario_name} FAILED in {elapsed_ms}ms")
            return 1

    except Exception as e:
        elapsed_ms = int((time.time() - time.time()) * 1000)  # Rough estimate
        error_msg = f"[{_tss()}] SCENARIO FAILED: {_safe_str(e)}"
        write_log_line(log_file, error_msg)
        write_log_line(log_file, f"[{_tss()}] STEP end ({elapsed_ms}ms)")
        print(f"✗ Scenario {scenario_name} failed: {_safe_str(e)}")
        return 1

    finally:
        await context.close()


async def main() -> int:
    parser = argparse.ArgumentParser(description="Frontend E2E Test Runner")
    parser.add_argument("--scenario", help="Run only the specified scenario (e.g., scenario_1_page_load)")
    args = parser.parse_args()

    print("Starting E2E test runner...")
    if not _is_allowed_start_url(START_URL):
        print(f"Refusing to navigate to non-localhost start url: {START_URL}")
        return 2

    scenarios = load_scenarios(args.scenario)
    if not scenarios:
        if args.scenario:
            print(f"No scenario found matching: {args.scenario}")
        else:
            print("No scenarios found in tests/scenarios/")
        return 1

    print(f"Found {len(scenarios)} scenario(s): {[s.stem for s in scenarios]}")
    print("Launching browser...")

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

        results = []
        scenario_names = []
        for scenario_path in scenarios:
            scenario_names.append(scenario_path.stem)
            result = await run_scenario(scenario_path, browser)
            results.append(result)

        await browser.close()

    # Summary
    print("\n" + "="*50)
    print("TEST RESULTS SUMMARY:")
    print("="*50)

    passed = 0
    failed = 0

    for name, result in zip(scenario_names, results):
        status = "PASSED" if result == 0 else "FAILED"
        marker = "✓" if result == 0 else "✗"
        print(f"{marker} {name}: {status}")
        if result == 0:
            passed += 1
        else:
            failed += 1

    print(f"\nTotal: {passed} passed, {failed} failed")

    if failed > 0:
        print(f"\n❌ {failed}/{len(scenarios)} scenarios failed")
        return 1
    else:
        print(f"\n✅ All {len(scenarios)} scenarios passed")
        return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))