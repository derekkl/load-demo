"""Load-demo worker: burns CPU and holds memory as directed by the load console.

Pulls its desired load from the console every few seconds, so pods that are
rescheduled or restarted pick their load back up on their own.
Python stdlib only - runs on registry.access.redhat.com/ubi9/python-311.
"""
import json
import math
import multiprocessing as mp
import os
import signal
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NS = os.environ.get("POD_NAMESPACE", "local")
DEPLOY = os.environ.get("DEPLOY_NAME", "worker")
KEY = f"{NS}/{DEPLOY}"
CONSOLE = os.environ.get("CONSOLE_URL", "http://load-console.load-console.svc:8080")
POD = os.environ.get("POD_NAME", "local")
NODE = os.environ.get("NODE_NAME", "local")
PORT = int(os.environ.get("PORT", "8080"))
SYNC_SECONDS = float(os.environ.get("SYNC_SECONDS", "3"))

CHUNK_MB = 16
CHUNK = CHUNK_MB * 1024 * 1024
GROW_MB_PER_TICK = 64  # 0.5s tick -> ~128 MiB/s ramp, so OOM kills are visible

state = {
    "pod": POD, "node": NODE, "key": KEY,
    "cpu_m": 0, "mem_mb": 0, "alloc_mb": 0, "burners": 0,
    "last_sync": None, "sync_error": None, "started": time.time(),
}
mem_chunks = []
burners = []  # [(Process, shared duty Value)]


def burn(duty):
    """Busy-loop for duty*100ms out of every 100ms (one core max per process)."""
    while True:
        d = duty.value
        if d <= 0:
            time.sleep(0.1)
            continue
        end = time.perf_counter() + 0.1 * d
        while time.perf_counter() < end:
            pass
        if d < 1:
            time.sleep(0.1 * (1 - d))


def set_cpu(millicores):
    m = max(0, int(millicores))
    n = math.ceil(m / 1000) if m > 0 else 0
    while len(burners) < n:
        v = mp.Value("d", 0.0, lock=False)
        p = mp.Process(target=burn, args=(v,), daemon=True)
        p.start()
        burners.append((p, v))
    while len(burners) > n:
        p, _ = burners.pop()
        p.terminate()
        p.join(1)
    for _, v in burners:
        v.value = m / (1000 * n)
    state["cpu_m"] = m
    state["burners"] = n


def mem_loop():
    while True:
        target = state["mem_mb"]
        cur = len(mem_chunks) * CHUNK_MB
        if cur + CHUNK_MB <= target:
            add = min(GROW_MB_PER_TICK, target - cur)
            for _ in range(max(1, add // CHUNK_MB)):
                # multiplying a non-zero byte forces real pages (counts toward RSS)
                mem_chunks.append(bytearray(b"\x01") * CHUNK)
        elif cur > target:
            while mem_chunks and len(mem_chunks) * CHUNK_MB > target:
                mem_chunks.pop()
        state["alloc_mb"] = len(mem_chunks) * CHUNK_MB
        time.sleep(0.5)


def sync_loop():
    url = f"{CONSOLE}/api/desired?key={urllib.parse.quote(KEY)}"
    while True:
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                d = json.load(r)
            set_cpu(d.get("cpu_m", 0))
            state["mem_mb"] = max(0, int(d.get("mem_mb", 0)))
            state["last_sync"] = time.time()
            state["sync_error"] = None
        except Exception as e:  # console unreachable: keep the last load
            state["sync_error"] = str(e)
        time.sleep(SYNC_SECONDS)


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json"):
        data = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/healthz":
            self._send(200, "ok", "text/plain")
        elif path in ("/", "/status"):
            self._send(200, json.dumps(state))
        else:
            self._send(404, "not found", "text/plain")

    def log_message(self, *args):
        pass


def shutdown(signum, frame):
    # PID 1 ignores SIGTERM unless a handler is installed - exit fast and clean
    for p, _ in burners:
        p.terminate()
    os._exit(0)


def main():
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    threading.Thread(target=mem_loop, daemon=True).start()
    threading.Thread(target=sync_loop, daemon=True).start()
    print(f"worker {POD} on {NODE} key={KEY} console={CONSOLE}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
