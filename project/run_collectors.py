"""Launch and supervise both collectors, using the project's Python environment."""
from __future__ import annotations

import fcntl
import math
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SERVICES = ("orderbooks", "prices")


def stop_children(children):
    for child in children:
        if child.poll() is None:
            child.send_signal(signal.SIGINT)
    deadline = time.monotonic() + 90
    for child in children:
        try:
            child.wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            child.terminate()
            try:
                child.wait(timeout=3)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


def main():
    interpreter = ROOT / ".venv" / "bin" / "python"
    if interpreter.is_file() and Path(sys.prefix).absolute() != (ROOT / ".venv").absolute():
        os.execv(str(interpreter), [str(interpreter), str(ROOT / "run_collectors.py"), *sys.argv[1:]])

    # Load .env relative to the project, even when invoked from its parent folder.
    import ingest_common as common
    from collection_config import enabled_symbols
    from collection_store import diagnostic_logger

    directory = Path(os.getenv("COLLECTOR_STATE_DIR", str(ROOT / ".collector")))
    directory.mkdir(parents=True, exist_ok=True)
    logger = diagnostic_logger("launcher", directory)
    lock = (directory / "launcher.lock").open("a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        print("Сборщики уже запущены.", flush=True)
        for handler in logger.handlers:
            handler.close()
        return 1
    children, restarted, restart_at = {}, {}, {}
    sleep_guard = None
    stopping = False
    result = 0
    previous_signals = {}

    def request_stop(signum, frame):
        nonlocal stopping
        stopping = True

    def spawn(service, errors):
        return subprocess.Popen(
            [sys.executable, "-u", str(ROOT / "collector_v2.py"), service],
            cwd=ROOT, start_new_session=True, stderr=errors,
        )

    try:
        # A bad flag must fail before either process contacts an external service.
        for service in SERVICES:
            enabled_symbols(service)
        seconds = float(os.getenv("COLLECTOR_RUN_SECONDS", "0"))
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError("COLLECTOR_RUN_SECONDS must be finite and nonnegative")
        deadline = time.monotonic() + seconds if seconds else float("inf")
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous_signals[sig] = signal.signal(sig, request_stop)
        if sys.platform == "darwin" and Path("/usr/bin/caffeinate").exists():
            sleep_guard = subprocess.Popen(["/usr/bin/caffeinate", "-i", "-w", str(os.getpid())],
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with (directory / "process-errors.log").open("ab") as errors:
            for service in SERVICES:
                children[service] = spawn(service, errors)
                restarted[service] = 0
            print("Оба процесса запущены. Для остановки нажмите Ctrl+C.", flush=True)
            while not stopping and children:
                now = time.monotonic()
                if now >= deadline:
                    stopping = True
                    break
                for service, child in list(children.items()):
                    code = child.poll()
                    if code is None:
                        continue
                    if code == 0:
                        del children[service]
                        if not seconds:
                            stopping = True
                        continue
                    if service not in restart_at:
                        logger.error("%s exited with code %s", service, code)
                        if restarted[service] >= 5:
                            result, stopping = 1, True
                            break
                        restart_at[service] = now + min(30, 2 ** restarted[service])
                        print("Есть ошибки. Восстанавливаю процесс.", flush=True)
                    if now >= restart_at[service] and not stopping:
                        children[service] = spawn(service, errors)
                        restarted[service] += 1
                        restart_at.pop(service)
                if not stopping:
                    time.sleep(.2)
    except Exception:
        logger.exception("launcher_failed")
        print("Есть ошибки. Запуск остановлен. Подробности сохранены в журнале.", flush=True)
        result = 1
    finally:
        if children:
            print("Останавливаю процессы...", flush=True)
        stop_children(list(children.values()))
        if any(child.returncode for child in children.values()):
            result = 1
        if sleep_guard is not None:
            sleep_guard.terminate()
            sleep_guard.wait()
        for sig, previous in previous_signals.items():
            signal.signal(sig, previous)
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()
        for handler in logger.handlers:
            handler.close()
    return result


if __name__ == "__main__":
    sys.exit(main())
