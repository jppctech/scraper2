"""
Proxy Manager — Webshare.io Static Proxy Pool with Round-Robin Rotation & Blacklisting.

Loads proxies from WEB_SHARE_PROXY_LIST env var (ip:port:user:pass format),
provides smart rotation, detects CAPTCHA/blocks, and blacklists failing proxies.

Also hosts the per-domain `DomainBucket` rate-limiter (Phase 3 of the
flash.co anti-block work). It lives here rather than in its own file because
both proxies and rate limits are pre-request gates that the scraper consults
in the same order.
"""

import asyncio
import os
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

# Load .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ─── Constants ───────────────────────────────────────────────────────────────

BLACKLIST_DURATION_SECONDS = 300   # 5 minutes
MAX_FAILURES_BEFORE_BLACKLIST = 5  # Was 2 — too aggressive with few proxies

# CAPTCHA / block detection patterns (case-insensitive)
BLOCK_PATTERNS = [
    r"captcha",
    r"cf-challenge",
    r"challenge-platform",
    r"access[\s_-]*denied",
    r"blocked",
    r"unusual[\s_-]*traffic",
    r"are[\s_-]*you[\s_-]*a[\s_-]*robot",
    r"verify[\s_-]*you[\s_-]*are[\s_-]*human",
    r"just[\s_-]*a[\s_-]*moment",
    r"checking[\s_-]*your[\s_-]*browser",
    r"please[\s_-]*complete[\s_-]*the[\s_-]*security[\s_-]*check",
    r"pardon[\s_-]*our[\s_-]*interruption",
    r"enable[\s_-]*javascript[\s_-]*and[\s_-]*cookies",
]

BLOCK_STATUS_CODES = {403, 429, 503, 407, 401}

_compiled_block_patterns = [re.compile(p, re.IGNORECASE) for p in BLOCK_PATTERNS]


# ─── Data Structures ────────────────────────────────────────────────────────

@dataclass
class ProxyEntry:
    proxy_url: str          # http://user:pass@ip:port
    address: str            # ip:port (for logging)
    fail_count: int = 0
    blacklisted_until: float = 0.0


@dataclass
class ProxyPool:
    proxies: list[ProxyEntry] = field(default_factory=list)
    current_index: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    initialized: bool = False


# ─── Global State ────────────────────────────────────────────────────────────

_pool = ProxyPool()


# ─── CAPTCHA / Block Detection ──────────────────────────────────────────────

def is_blocked(status_code: int, html: str) -> bool:
    """
    Detect if a response indicates a CAPTCHA, block, or rate-limit.
    Returns True if the response is likely blocked.
    """
    # Status code check
    if status_code in BLOCK_STATUS_CODES:
        return True

    if not html:
        return status_code != 200

    # Very short response with non-200 = likely block page
    if len(html) < 500 and status_code != 200:
        return True

    # Pattern matching against known CAPTCHA/block signatures
    html_lower = html[:5000]  # only check first 5KB for performance
    for pattern in _compiled_block_patterns:
        if pattern.search(html_lower):
            return True

    return False


# ─── Proxy Loading ──────────────────────────────────────────────────────────

async def initialize() -> int:
    """
    Load proxies from WEB_SHARE_PROXY_LIST env var.
    Format: ip:port:username:password (comma-separated entries)
    Returns the number of proxies loaded.
    """
    global _pool

    proxy_list_raw = os.getenv("WEB_SHARE_PROXY_LIST", "")
    # Handle multiline .env values — strip all whitespace/newlines
    proxy_list_raw = proxy_list_raw.replace("\r", "").replace("\n", "").strip()
    if not proxy_list_raw:
        print("[Proxy] ⚠️  WEB_SHARE_PROXY_LIST not set — proxy fallback disabled")
        _pool.initialized = True
        return 0

    proxies: list[ProxyEntry] = []

    for entry in proxy_list_raw.split(","):
        entry = entry.strip()
        if not entry:
            continue

        parts = entry.split(":")
        if len(parts) != 4:
            print(f"[Proxy] ⚠️  Skipping malformed entry: {entry[:30]}")
            continue

        ip, port, username, password = [p.strip() for p in parts]
        proxy_url = f"http://{username}:{password}@{ip}:{port}"
        proxies.append(ProxyEntry(
            proxy_url=proxy_url,
            address=f"{ip}:{port}",
        ))

    async with _pool.lock:
        _pool.proxies = proxies
        _pool.current_index = 0
        _pool.initialized = True

    print(f"[Proxy] ✅ Loaded {len(proxies)} proxies")
    for p in proxies[:3]:
        print(f"   → {p.address}")
    if len(proxies) > 3:
        print(f"   → ... and {len(proxies) - 3} more")

    return len(proxies)


# ─── Proxy Selection ────────────────────────────────────────────────────────

def get_next_proxy() -> Optional[str]:
    """
    Get the next available proxy URL using round-robin.
    Skips blacklisted proxies. Returns None if no proxies available.
    """
    if not _pool.proxies:
        return None

    now = time.time()
    total = len(_pool.proxies)

    # Try each proxy once in round-robin order
    for _ in range(total):
        idx = _pool.current_index % total
        _pool.current_index += 1
        proxy = _pool.proxies[idx]

        # Skip blacklisted
        if proxy.blacklisted_until > now:
            continue

        return proxy.proxy_url

    # All blacklisted — un-blacklist the oldest one and use it
    oldest = min(_pool.proxies, key=lambda p: p.blacklisted_until)
    oldest.blacklisted_until = 0
    oldest.fail_count = 0
    print(f"[Proxy] ♻️  Un-blacklisted {oldest.address} (all proxies were down)")
    return oldest.proxy_url


def report_failure(proxy_url: str) -> None:
    """
    Report that a proxy failed. After MAX_FAILURES_BEFORE_BLACKLIST failures,
    temporarily blacklist it.
    """
    for proxy in _pool.proxies:
        if proxy.proxy_url == proxy_url:
            proxy.fail_count += 1
            if proxy.fail_count >= MAX_FAILURES_BEFORE_BLACKLIST:
                proxy.blacklisted_until = time.time() + BLACKLIST_DURATION_SECONDS
                print(f"[Proxy] 🚫 Blacklisted {proxy.address} for {BLACKLIST_DURATION_SECONDS}s (failed {proxy.fail_count}x)")
            break


def report_success(proxy_url: str) -> None:
    """
    Report that a proxy succeeded. Resets fail count.
    """
    for proxy in _pool.proxies:
        if proxy.proxy_url == proxy_url:
            proxy.fail_count = 0
            proxy.blacklisted_until = 0
            break


# ─── Stats ───────────────────────────────────────────────────────────────────

def get_stats() -> dict:
    """Get current proxy pool statistics."""
    now = time.time()
    total = len(_pool.proxies)
    blacklisted = sum(1 for p in _pool.proxies if p.blacklisted_until > now)
    return {
        "total": total,
        "active": total - blacklisted,
        "blacklisted": blacklisted,
        "initialized": _pool.initialized,
    }


def is_available() -> bool:
    """Check if any proxies are loaded and available."""
    return _pool.initialized and len(_pool.proxies) > 0


# ─── Per-domain Rate Limiter ─────────────────────────────────────────────────
# Token-bucket per hostname. Used by the curl_cffi sync path to avoid bursting
# any single store with N parallel scrapes from one VPS IP.
#
# Defaults (DOMAIN_RATE_LIMIT_PER_SEC env): 6 tokens/sec/host with burst 12.
# That maps roughly to "the speed a real user paging through a category"
# — enough to keep Layer 2 fast (15 stores × 1 hit each = no contention)
# while protecting against pathological cases where Searlo returns 8 URLs
# from the same domain.


class DomainBucket:
    """
    Synchronous token-bucket rate limiter keyed by hostname.

    Designed to be called from `scrape_url_sync` (which already runs in a
    thread executor), so we use threading.Lock + time.sleep rather than
    asyncio primitives. A coroutine-friendly wrapper is trivial to add
    later if needed.
    """

    def __init__(self, rate_per_sec: float = 6.0, burst: int = 12) -> None:
        self._rate = float(rate_per_sec)
        self._burst = float(burst)
        self._tokens: dict[str, float] = {}
        self._last_refill: dict[str, float] = {}
        self._lock = threading.Lock()

    def acquire(self, host: str) -> float:
        """
        Block until a token is available for `host`. Returns the number of
        seconds slept (0.0 when the call was non-blocking) so callers can
        log under-the-hood waits when they're surprising.
        """
        if not host:
            return 0.0

        slept = 0.0
        while True:
            with self._lock:
                now = time.time()
                if host not in self._tokens:
                    self._tokens[host] = self._burst
                    self._last_refill[host] = now

                elapsed = now - self._last_refill[host]
                self._tokens[host] = min(
                    self._burst, self._tokens[host] + elapsed * self._rate
                )
                self._last_refill[host] = now

                if self._tokens[host] >= 1.0:
                    self._tokens[host] -= 1.0
                    return slept

                # Need to wait for the next token to drip in.
                deficit = 1.0 - self._tokens[host]
                wait = deficit / self._rate

            time.sleep(wait)
            slept += wait

    def stats(self) -> dict:
        with self._lock:
            return {
                "rate_per_sec": self._rate,
                "burst": self._burst,
                "tracked_hosts": len(self._tokens),
            }


# Module-level singleton. main.py reads DOMAIN_RATE_LIMIT_PER_SEC env at
# import time so production can tune without code changes.
_rate_per_sec = float(os.getenv("DOMAIN_RATE_LIMIT_PER_SEC", "6"))
_burst = int(os.getenv("DOMAIN_RATE_LIMIT_BURST", "12"))
domain_bucket = DomainBucket(rate_per_sec=_rate_per_sec, burst=_burst)
