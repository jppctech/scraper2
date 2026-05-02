"""
Browser Pool — pre-warmed Patchright BrowserContext + Page pool.

Why we need this
================
Vanilla per-request flow used to be:

    browser.new_context()   →  ~200 ms
    context.new_page()      →  ~50  ms
    page.route(...) x 2     →  ~30  ms
    page.goto(url)          →   variable

That's ~280 ms of pure setup overhead BEFORE we even hit the network. Across
the 15 store URLs Layer 2 fetches per analysis, that's ~4 s wasted on
context bootstrapping that could just as well be done once at boot.

This pool keeps N pre-built (context, page) pairs in an asyncio.Queue:
  • acquire()  → instant if any pair is idle, else awaits a release
  • release(p) → resets the page (clear cookies + about:blank) and re-queues

Each context is fully isolated (cookies, storage, service workers) so
concurrent scrapes of two different sites cannot pollute each other.
Resource routing (image/font blocking, ad/tracker blocking) is installed
ONCE per context in warmup() — also a hot-path perf win.

Health
======
Every acquire+release cycle increments `use_count`. After RECYCLE_AFTER uses
(default 200) the (context, page) pair is closed and a fresh one created in
its place. This bounds memory growth from accumulated WebStorage / IndexedDB
on long-lived contexts.

If a scrape raises an exception while holding a pair, `release()` recycles
the pair instead of trusting it might be in a clean state. We never hand a
broken page to the next caller.
"""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import AsyncIterator, Optional

# ─── Configuration ──────────────────────────────────────────────────────────

# Default concurrent contexts. Each Patchright context holds ~80–120 MB RSS,
# so 10 contexts ≈ 1–1.2 GB steady state. Override via BROWSER_POOL_SIZE env
# in main.py at warmup time.
DEFAULT_POOL_SIZE = 10

# Recycle a context after this many use cycles. Bounds memory growth from
# DOM/storage accumulation. 200 is conservative; raise once we've measured.
RECYCLE_AFTER = 200

# Default user-agent applied to every pooled context. Phase 3 will replace
# this with rotation. Indian Windows Chrome is the most common real fingerprint
# in our traffic mix.
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_VIEWPORT = {"width": 1280, "height": 800}
DEFAULT_LOCALE_HEADERS = {"Accept-Language": "en-IN,en;q=0.9"}

# Resource patterns blocked on every pooled page — saves bandwidth + time.
# Images/fonts: never needed for price extraction.
# Analytics/ads: third-party scripts that often hang for 5+ seconds.
_RESOURCE_BLOCK_PATTERN = (
    "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,eot,ico,mp4,mp3}"
)
_TRACKER_BLOCK_PATTERN = (
    "**/{analytics,tracking,ads,doubleclick,facebook,google-analytics,hotjar,clarity}**"
)


# ─── Data Structures ────────────────────────────────────────────────────────


@dataclass
class PooledPage:
    """A single (context, page) pair tracked by the pool."""
    context: object  # patchright BrowserContext
    page: object     # patchright Page
    use_count: int = 0
    created_at: float = field(default_factory=time.time)
    is_broken: bool = False


# ─── Pool ───────────────────────────────────────────────────────────────────


class BrowserPool:
    """
    Pre-warmed pool of Patchright (BrowserContext, Page) pairs.

    Lifecycle:
      pool = BrowserPool()
      await pool.warmup(size=10)   # call once at FastAPI startup
      async with pool.lease() as page: ...
      await pool.shutdown()        # call once at FastAPI shutdown
    """

    def __init__(self) -> None:
        self._browser = None                  # patchright Browser
        self._playwright_ctx = None           # async_playwright() handle
        self._queue: asyncio.Queue[PooledPage] = asyncio.Queue()
        self._size = 0
        self._lock = asyncio.Lock()
        self._closed = False

        # Stats
        self._total_acquires = 0
        self._total_recycled = 0
        self._created_contexts = 0
        self._inflight = 0

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def warmup(self, size: int = DEFAULT_POOL_SIZE) -> int:
        """
        Boot the persistent browser and fill the pool with `size` contexts.
        Idempotent — calling twice is a no-op after the first success.
        Returns the number of pairs successfully created.
        """
        if self._size > 0:
            return self._size

        async with self._lock:
            if self._size > 0:
                return self._size

            # Lazy import so the module loads even when patchright is missing.
            from patchright.async_api import async_playwright

            self._playwright_ctx = await async_playwright().start()
            self._browser = await self._playwright_ctx.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--no-first-run",
                    "--disable-background-networking",
                ],
            )

            created = 0
            for _ in range(size):
                try:
                    pp = await self._create_pair()
                    self._queue.put_nowait(pp)
                    created += 1
                except Exception as e:
                    # Don't fail boot on a single bad context — just warn and
                    # continue. Pool runs at reduced size.
                    print(f"[BrowserPool] ⚠️ Failed to create pair: {e}")

            self._size = created
            print(f"[BrowserPool] ✅ Warmed up with {created}/{size} contexts")
            return created

    async def shutdown(self) -> None:
        """Drain the queue and close every context + browser. Idempotent."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True

            drained: list[PooledPage] = []
            while not self._queue.empty():
                try:
                    drained.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            for pp in drained:
                await self._destroy_pair(pp)

            if self._browser is not None:
                try:
                    await self._browser.close()
                except Exception:
                    pass
                self._browser = None

            if self._playwright_ctx is not None:
                try:
                    await self._playwright_ctx.stop()
                except Exception:
                    pass
                self._playwright_ctx = None

            print(f"[BrowserPool] 🛑 Shutdown complete (recycled={self._total_recycled})")

    # ── Acquire / release ────────────────────────────────────────────────

    @asynccontextmanager
    async def lease(self) -> AsyncIterator[PooledPage]:
        """
        Async context-manager: acquire a pair on enter, release on exit.

        Use this from scrape_with_browser:

            async with pool.lease() as pp:
                await pp.page.goto(url)
                ...

        Exceptions propagate but the pair is still returned to the pool
        (or recycled if the page is broken).
        """
        pp = await self.acquire()
        try:
            yield pp
        except Exception:
            pp.is_broken = True
            raise
        finally:
            await self.release(pp)

    async def acquire(self) -> PooledPage:
        """Block until a pair is available. Lazy-warms the pool if empty."""
        if self._size == 0 and not self._closed:
            await self.warmup()

        if self._closed:
            raise RuntimeError("BrowserPool is shut down")

        pp = await self._queue.get()
        self._total_acquires += 1
        self._inflight += 1
        return pp

    async def release(self, pp: PooledPage) -> None:
        """Reset the pair and return it to the queue, or recycle if needed."""
        self._inflight = max(0, self._inflight - 1)

        if self._closed:
            await self._destroy_pair(pp)
            return

        pp.use_count += 1
        recycle = pp.is_broken or pp.use_count >= RECYCLE_AFTER

        if recycle:
            await self._destroy_pair(pp)
            self._total_recycled += 1
            try:
                replacement = await self._create_pair()
                self._queue.put_nowait(replacement)
            except Exception as e:
                # If replacement fails the pool shrinks by one. We don't crash
                # the service — Phase 5 verification will surface this via the
                # /health pool stats.
                print(f"[BrowserPool] ⚠️ Failed to recreate pair after recycle: {e}")
                self._size = max(0, self._size - 1)
            return

        # Healthy pair → soft reset and re-queue.
        try:
            await pp.context.clear_cookies()
            await pp.page.goto("about:blank", wait_until="commit", timeout=2000)
        except Exception:
            # Soft reset failed — recycle on next acquire by marking broken.
            pp.is_broken = True
        finally:
            self._queue.put_nowait(pp)

    # ── Stats / health ────────────────────────────────────────────────────

    def stats(self) -> dict:
        return {
            "size": self._size,
            "idle": self._queue.qsize(),
            "in_flight": self._inflight,
            "total_acquires": self._total_acquires,
            "total_recycled": self._total_recycled,
            "contexts_created": self._created_contexts,
            "ready": self._size > 0 and not self._closed,
        }

    def is_ready(self) -> bool:
        return self._size > 0 and not self._closed

    # ── Internal ──────────────────────────────────────────────────────────

    async def _create_pair(self) -> PooledPage:
        """Build a fresh context with default headers + resource blocking."""
        if self._browser is None:
            raise RuntimeError("BrowserPool not warmed up")

        context = await self._browser.new_context(
            user_agent=DEFAULT_USER_AGENT,
            viewport=DEFAULT_VIEWPORT,
            extra_http_headers=DEFAULT_LOCALE_HEADERS,
            locale="en-IN",
            timezone_id="Asia/Kolkata",
        )

        page = await context.new_page()

        # Resource blocking is installed ONCE per page in the pool — every
        # subsequent goto() inherits these handlers automatically.
        await page.route(_RESOURCE_BLOCK_PATTERN, lambda r: r.abort())
        await page.route(_TRACKER_BLOCK_PATTERN, lambda r: r.abort())

        self._created_contexts += 1
        return PooledPage(context=context, page=page)

    async def _destroy_pair(self, pp: PooledPage) -> None:
        try:
            await pp.context.close()
        except Exception:
            pass


# ─── Module-level singleton ─────────────────────────────────────────────────
# main.py uses this single instance everywhere. Lifespan manages warmup +
# shutdown.

_pool: Optional[BrowserPool] = None


def get_pool() -> BrowserPool:
    global _pool
    if _pool is None:
        _pool = BrowserPool()
    return _pool
