"""
Gunicorn configuration for the Product Analyzer Scraper on KVM8 (8 vCPU, 32GB RAM).

Worker count is kept LOW (4) because each UvicornWorker boots its own
BrowserPool with BROWSER_POOL_SIZE Patchright contexts, each consuming
~120 MB RSS.  4 workers × 8 contexts = 32 Chromium contexts ≈ 3.8 GB.

The real concurrency comes from asyncio coroutines inside each worker,
not from worker count — this workload is 90 % I/O-bound (waiting on
network for curl_cffi / Patchright responses).

Start command:
    gunicorn main:app -c gunicorn.conf.py
"""

# ─── Binding ─────────────────────────────────────────────────────────────────
bind = "0.0.0.0:8000"

# ─── Workers ─────────────────────────────────────────────────────────────────
workers = 4                        # NOT 2N+1 = 17 — browser RAM is the cap
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 200           # async concurrency per worker

# ─── Worker Recycling ────────────────────────────────────────────────────────
# Chromium contexts leak memory over time (DOM/storage accumulation).
# Recycling workers periodically bounds this. Jitter prevents thundering herd.
max_requests = 2000
max_requests_jitter = 300

# ─── Timeouts ────────────────────────────────────────────────────────────────
# 35s worker timeout: if a worker is stuck for 35s with no heartbeat, Gunicorn
# kills it. Per-request timeouts in the app (asyncio.wait_for) should trigger
# well before this — this is the last-resort safety net.
timeout = 35
graceful_timeout = 25
keepalive = 5

# ─── Critical: Do NOT preload ────────────────────────────────────────────────
# BrowserPool holds async event loops + Patchright browser handles that cannot
# survive a fork(). Each worker must boot its own pool independently.
preload_app = False

# ─── Performance ─────────────────────────────────────────────────────────────
# Use shared memory for temp files — faster than disk I/O
worker_tmp_dir = "/dev/shm"

# ─── Logging ─────────────────────────────────────────────────────────────────
# Reduce I/O in production — app-level logging handles the detail
accesslog = "-"
errorlog = "-"
loglevel = "warning"
