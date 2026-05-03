"""
curl_cffi Scraper Microservice v5.0
FastAPI service with:
  - /scrape, /scrape/batch — raw HTML scraping (async curl_cffi)
  - /scrape/batch/stream — SSE streaming batch (concurrent static + browser)
  - /search — Google SERP scraping (replaces Jina Search)
  - Webshare proxy fallback on CAPTCHA/block detection
"""

import asyncio
import concurrent.futures
import json as json_mod
import os
import re
from typing import Optional
from urllib.parse import urlparse, urlencode, unquote

from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
from curl_cffi.requests import AsyncSession
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse, StreamingResponse
from contextlib import asynccontextmanager
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

import proxy_manager
import amazon_pa
import browser_pool
import fingerprint_rotator

# ─── App Setup ───────────────────────────────────────────────────────────────

# ─── Module-level shared resources (initialized in lifespan) ─────────────────
_curl_session: Optional[AsyncSession] = None
_thread_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=20, thread_name_prefix="scraper"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _curl_session

    # Startup: Initialize proxy pool from Webshare
    count = await proxy_manager.initialize()
    print(f"[Startup] Proxy pool ready: {count} proxies")

    # Startup: persistent AsyncSession — connection pooling for curl_cffi
    # max_clients=60 allows 60 concurrent curl handles per worker
    _curl_session = AsyncSession(max_clients=60)
    print("[Startup] curl_cffi AsyncSession ready (max_clients=60)")

    # Startup: warm up Patchright browser pool so the FIRST scrape doesn't
    # pay the ~3s Chromium cold-start. Default 8 contexts per worker;
    # tune via the BROWSER_POOL_SIZE env.
    pool_size = int(os.getenv("BROWSER_POOL_SIZE", "8"))
    if pool_size > 0:
        try:
            warmed = await browser_pool.get_pool().warmup(size=pool_size)
            print(f"[Startup] Browser pool ready: {warmed}/{pool_size} contexts")
        except Exception as e:
            # Patchright may not be installed in dev — warn and continue.
            # /scrape/browser will lazy-warm on first request as a fallback.
            print(f"[Startup] ⚠️ Browser pool warmup skipped: {e}")
    else:
        print("[Startup] Browser pool disabled (BROWSER_POOL_SIZE=0)")

    yield

    # Shutdown
    if _curl_session:
        await _curl_session.close()
    try:
        await browser_pool.get_pool().shutdown()
    except Exception as e:
        print(f"[Shutdown] Browser pool shutdown error: {e}")
    _thread_pool.shutdown(wait=False)

app = FastAPI(
    title="Product Analyzer Scraper",
    version="5.0.0",
    lifespan=lifespan,
    default_response_class=ORJSONResponse,
)

app.add_middleware(
    CORSMiddleware,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Config ──────────────────────────────────────────────────────────────────

IMPERSONATE_PROFILE = "chrome"
DEFAULT_TIMEOUT = 12
MAX_BATCH_CONCURRENCY = 12

BROWSER_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

SKIP_DOMAINS = {
    "instagram.com", "youtube.com", "facebook.com",
    "twitter.com", "x.com", "linkedin.com", "reddit.com",
}

# ─── Models ──────────────────────────────────────────────────────────────────


class ScrapeRequest(BaseModel):
    url: str
    timeout: int = DEFAULT_TIMEOUT


class ScrapeResponse(BaseModel):
    url: str
    status_code: int
    html: str
    content_length: int
    success: bool
    error: Optional[str] = None
    extracted_price: Optional[float] = None
    extracted_title: Optional[str] = None
    used_proxy: bool = False


class BatchScrapeRequest(BaseModel):
    urls: list[str]
    timeout: int = DEFAULT_TIMEOUT


class BatchScrapeResponse(BaseModel):
    results: list[ScrapeResponse]
    total: int
    successful: int
    failed: int


class SearchRequest(BaseModel):
    query: str
    num_results: int = 8


class SearchResult(BaseModel):
    url: str
    title: str
    description: str
    price: Optional[float] = None


class SearchResponse(BaseModel):
    query: str
    results: list[SearchResult]
    total: int
    success: bool
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    version: str
    impersonate_profile: str
    proxy_stats: dict
    browser_pool: dict


# ─── Core Scraping ──────────────────────────────────────────────────────────


def should_skip_url(url: str) -> bool:
    try:
        hostname = urlparse(url).hostname or ""
        return any(domain in hostname for domain in SKIP_DOMAINS)
    except Exception:
        return False


def scrape_url_sync(url: str, timeout: int = 5) -> ScrapeResponse:
    if should_skip_url(url):
        return ScrapeResponse(
            url=url, status_code=0, html="", content_length=0,
            success=False, error="Skipped: unsupported domain",
        )

    is_amazon = amazon_pa.is_amazon_url(url)

    # ── Per-domain rate limiting ────────────────────────────────────────
    # Block briefly if we're hammering this host. Slept time is reported
    # at trace level only when >250ms because sub-second waits are noise.
    host = ""
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        pass
    if host:
        slept = proxy_manager.domain_bucket.acquire(host)
        if slept > 0.25:
            print(f"[RateLimit] {host} waited {slept:.2f}s for token")

    # ── Per-request fingerprint rotation ────────────────────────────────
    # Each direct request picks a fresh impersonate profile + Accept-Language
    # so a burst from one VPS IP doesn't cluster on a single fingerprint.
    profile = fingerprint_rotator.pick_profile(default=IMPERSONATE_PROFILE)
    headers = fingerprint_rotator.pick_headers(base=BROWSER_HEADERS)

    # ── Tier 1: Direct scrape (no proxy) ─────────────────────────────────
    try:
        # Give sites enough time to respond — 8s for direct (was 3s, too aggressive)
        direct_timeout = min(timeout, 8) 
        response = curl_requests.get(
            url, headers=headers, impersonate=profile,
            timeout=direct_timeout, allow_redirects=True, max_redirects=5,
        )
        html = response.text
        success = response.status_code == 200 and len(html) > 100

        if success and not proxy_manager.is_blocked(response.status_code, html):
            price, title = extract_price_from_html(html)
            return ScrapeResponse(
                url=url, status_code=response.status_code,
                html=html, content_length=len(html),
                success=success,
                extracted_price=price,
                extracted_title=title,
                used_proxy=False,
            )

        print(f"[Scraper] \U0001f6e1\ufe0f Blocked (HTTP {response.status_code}): {url[:80]}")

    except Exception as e:
        print(f"[Scraper] \u26a0\ufe0f Direct failed: {url[:60]} \u2014 {str(e)[:80]}")

    # ── Tier 2 (Amazon only): PA API fallback ────────────────────────────
    if is_amazon and amazon_pa.is_configured():
        print(f"[Scraper] \U0001f4e6 Trying Amazon PA API for: {url[:60]}")
        product = amazon_pa.lookup_from_url(url)
        if product and product.price:
            return ScrapeResponse(
                url=product.url,
                status_code=200,
                html=f'<title>{product.title}</title>',
                content_length=0,
                success=True,
                extracted_price=product.price,
                extracted_title=product.title,
                used_proxy=False,
            )
        print(f"[Scraper] \u26a0\ufe0f PA API failed, falling through to proxy")

    # ── Tier 3: Proxy scrape ─────────────────────────────────────────────
    if not proxy_manager.is_available():
        return ScrapeResponse(
            url=url, status_code=0, html="", content_length=0,
            success=False, error="Blocked and no proxies available",
        )

    # Only try proxy ONCE to save time and fail-fast to Playwright
    for attempt in range(1):
        proxy_url = proxy_manager.get_next_proxy()
        if not proxy_url:
            break

        try:
            proxy_timeout = min(timeout, 10) # Proxies need more time due to extra hop
            response = curl_requests.get(
                url, headers=headers, impersonate=profile,
                timeout=proxy_timeout, allow_redirects=True, max_redirects=5,
                proxy=proxy_url,
            )
            html = response.text
            success = response.status_code == 200 and len(html) > 100

            if success and not proxy_manager.is_blocked(response.status_code, html):
                proxy_manager.report_success(proxy_url)
                price, title = extract_price_from_html(html)
                print(f"[Scraper] \u2705 Proxy success (attempt {attempt + 1}): {url[:60]}")
                return ScrapeResponse(
                    url=url, status_code=response.status_code,
                    html=html, content_length=len(html),
                    success=success,
                    extracted_price=price,
                    extracted_title=title,
                    used_proxy=True,
                )
            else:
                proxy_manager.report_failure(proxy_url)
                print(f"[Scraper] \U0001f6e1\ufe0f Proxy also blocked (attempt {attempt + 1}): {url[:60]}")

        except Exception as e:
            proxy_manager.report_failure(proxy_url)
            print(f"[Scraper] \u274c Proxy error (attempt {attempt + 1}): {str(e)[:80]}")

    return ScrapeResponse(
        url=url, status_code=0, html="", content_length=0,
        success=False, error="All scrape attempts failed",
        used_proxy=True,
    )


def extract_price_from_html(html: str) -> tuple[Optional[float], Optional[str]]:
    """
    Extract price and title from HTML using multi-layered structured parsing.
    Returns (price, title) or (None, None).
    """
    if not html:
        return None, None

    # Global USD Kill Switch: Pre-check if the page explicitly states USD in meta currency
    currency_usd_pattern = r'<meta[^>]*property=["\'][og|product]+:price:currency["\'][^>]*content=["\']USD["\']'
    if re.search(currency_usd_pattern, html, re.IGNORECASE) or '<meta itemprop="priceCurrency" content="USD"' in html:
        return None, None

    # Strategy 1: JSON-LD structured data
    import json as json_mod

    jsonld_pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
    for match in re.finditer(jsonld_pattern, html, re.DOTALL | re.IGNORECASE):
        try:
            data = json_mod.loads(match.group(1))
            items = data if isinstance(data, list) else [data]

            # Also check @graph
            for item in items:
                if isinstance(item.get("@graph"), list):
                    items.extend(item["@graph"])

            for item in items:
                if item.get("@type") not in ("Product", "IndividualProduct", "ProductModel"):
                    continue
                
                offers = item.get("offers") or item.get("offer", {})
                if isinstance(offers, list):
                    # Take the LOWEST price offer structure that isn't USD mapped
                    valid_offers = [o for o in offers if isinstance(o, dict) 
                                   and o.get("priceCurrency", "INR").upper() != "USD"]
                    if valid_offers:
                        offers = min(valid_offers, 
                                    key=lambda o: float(str(o.get("price", 999999)).replace(",", ""))
                                    if o.get("price") else 999999)
                    else:
                        offers = {}

                # Skip if explicitly USD
                currency = offers.get("priceCurrency", "").upper() if isinstance(offers, dict) else ""
                if currency and currency == "USD":
                    continue

                if isinstance(offers, dict) and offers.get("@type") == "AggregateOffer":
                    price_val = offers.get("lowPrice") or offers.get("price")
                else:
                    price_val = offers.get("price") or offers.get("lowPrice") if isinstance(offers, dict) else None

                if price_val:
                    price = float(str(price_val).replace(",", ""))
                    if 10 < price < 10_000_000:
                        title = item.get("name", "")
                        return price, title
        except Exception:
            continue

    # Strategy 2: Meta tags
    meta_patterns = [
        r'<meta[^>]*property=["\']product:price:amount["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']product:price:amount["\']',
        r'<meta[^>]*property=["\']og:price:amount["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']og:price:amount["\']',
    ]
    for pattern in meta_patterns:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            try:
                price = float(m.group(1).replace(",", ""))
                if 10 < price < 10_000_000:
                    # Try to get title from og:title
                    title_m = re.search(r'<meta[^>]*property=["\']og:title["\'][^>]*content=["\']([^"\']+)["\']', html, re.IGNORECASE)
                    title = title_m.group(1) if title_m else None
                    return price, title
            except (ValueError, IndexError):
                pass

    # Strategy 3: Google Tag Manager (dataLayer) Push Interception
    # Many heavy SPAs (e.g., BigBasket, Tata CLiQ) push state to dataLayer before React starts.
    datalayer_pattern = r'dataLayer\.push\s*\(\s*(\{.*?\})\s*\)'
    for match in re.finditer(datalayer_pattern, html, re.DOTALL):
        try:
            data = json_mod.loads(match.group(1))
            # Drill down into standard GTM ecommerce schema
            products = data.get("ecommerce", {}).get("detail", {}).get("products", [])
            if not products:
                products = data.get("ecommerce", {}).get("items", [])
            if not products:
                products = data.get("ecommerce", {}).get("purchase", {}).get("products", [])

            for product in products:
                if "price" in product:
                    price_val = float(str(product["price"]).replace(",", ""))
                    if 10 < price_val < 10_000_000:
                        title = product.get("name", "")
                        return price_val, title
        except Exception:
            continue

    return None, None


# ─── Async Domain Rate Limiter ──────────────────────────────────────────────

class AsyncDomainBucket:
    """Async token-bucket rate limiter keyed by hostname."""

    def __init__(self, rate_per_sec: float = 6.0, burst: int = 12) -> None:
        self._rate = float(rate_per_sec)
        self._burst = float(burst)
        self._tokens: dict[str, float] = {}
        self._last_refill: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, host: str) -> float:
        if not host:
            return 0.0
        slept = 0.0
        while True:
            async with self._lock:
                now = asyncio.get_event_loop().time()
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
                deficit = 1.0 - self._tokens[host]
                wait = deficit / self._rate
            await asyncio.sleep(wait)
            slept += wait


_async_domain_bucket = AsyncDomainBucket(
    rate_per_sec=float(os.getenv("DOMAIN_RATE_LIMIT_PER_SEC", "6")),
    burst=int(os.getenv("DOMAIN_RATE_LIMIT_BURST", "12")),
)


# ─── Async Core Scraping ───────────────────────────────────────────────────


async def scrape_url_async(url: str, timeout: int = 9) -> ScrapeResponse:
    """
    True async scrape — no thread blocking, no executor overhead.
    Uses curl_cffi AsyncSession for network I/O and run_in_threadpool
    for CPU-bound HTML parsing (BeautifulSoup).

    Tier 1: Direct async request
    Tier 2: Amazon PA API fallback (sync call in threadpool)
    Tier 3: Proxy async request
    """
    global _curl_session

    if should_skip_url(url):
        return ScrapeResponse(
            url=url, status_code=0, html="", content_length=0,
            success=False, error="Skipped: unsupported domain",
        )

    is_amazon = amazon_pa.is_amazon_url(url)

    # Per-domain rate limiting (async version)
    host = ""
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        pass
    if host:
        slept = await _async_domain_bucket.acquire(host)
        if slept > 0.25:
            print(f"[RateLimit] {host} waited {slept:.2f}s for token")

    # Per-request fingerprint rotation
    profile = fingerprint_rotator.pick_profile(default=IMPERSONATE_PROFILE)
    headers = fingerprint_rotator.pick_headers(base=BROWSER_HEADERS)

    # Lazy-init session guard (shouldn't happen if lifespan ran, but safe)
    if _curl_session is None:
        _curl_session = AsyncSession(max_clients=60)

    # ── Tier 1: Direct async scrape ──────────────────────────────────────
    try:
        direct_timeout = min(timeout, 8)
        response = await _curl_session.get(
            url, headers=headers, impersonate=profile,
            timeout=direct_timeout, allow_redirects=True, max_redirects=5,
        )
        html = response.text
        success = response.status_code == 200 and len(html) > 100

        if success and not proxy_manager.is_blocked(response.status_code, html):
            price, title = await run_in_threadpool(extract_price_from_html, html)
            return ScrapeResponse(
                url=url, status_code=response.status_code,
                html=html, content_length=len(html),
                success=success,
                extracted_price=price,
                extracted_title=title,
                used_proxy=False,
            )

        print(f"[Scraper] 🛡️ Blocked (HTTP {response.status_code}): {url[:80]}")

    except Exception as e:
        print(f"[Scraper] ⚠️ Direct failed: {url[:60]} — {str(e)[:80]}")

    # ── Tier 2 (Amazon only): PA API fallback ────────────────────────────
    if is_amazon and amazon_pa.is_configured():
        print(f"[Scraper] 📦 Trying Amazon PA API for: {url[:60]}")
        product = await run_in_threadpool(amazon_pa.lookup_from_url, url)
        if product and product.price:
            return ScrapeResponse(
                url=product.url,
                status_code=200,
                html=f'<title>{product.title}</title>',
                content_length=0,
                success=True,
                extracted_price=product.price,
                extracted_title=product.title,
                used_proxy=False,
            )
        print(f"[Scraper] ⚠️ PA API failed, falling through to proxy")

    # ── Tier 3: Proxy async scrape ───────────────────────────────────────
    if not proxy_manager.is_available():
        return ScrapeResponse(
            url=url, status_code=0, html="", content_length=0,
            success=False, error="Blocked and no proxies available",
        )

    proxy_url = proxy_manager.get_next_proxy()
    if proxy_url:
        try:
            proxy_timeout = min(timeout, 10)
            response = await _curl_session.get(
                url, headers=headers, impersonate=profile,
                timeout=proxy_timeout, allow_redirects=True, max_redirects=5,
                proxy=proxy_url,
            )
            html = response.text
            success = response.status_code == 200 and len(html) > 100

            if success and not proxy_manager.is_blocked(response.status_code, html):
                proxy_manager.report_success(proxy_url)
                price, title = await run_in_threadpool(extract_price_from_html, html)
                print(f"[Scraper] ✅ Proxy success: {url[:60]}")
                return ScrapeResponse(
                    url=url, status_code=response.status_code,
                    html=html, content_length=len(html),
                    success=success,
                    extracted_price=price,
                    extracted_title=title,
                    used_proxy=True,
                )
            else:
                proxy_manager.report_failure(proxy_url)
                print(f"[Scraper] 🛡️ Proxy also blocked: {url[:60]}")

        except Exception as e:
            proxy_manager.report_failure(proxy_url)
            print(f"[Scraper] ❌ Proxy error: {str(e)[:80]}")

    return ScrapeResponse(
        url=url, status_code=0, html="", content_length=0,
        success=False, error="All scrape attempts failed",
        used_proxy=True,
    )


# ─── Google Search Scraper ──────────────────────────────────────────────────


def _google_search_attempt(
    query: str, num_results: int, proxy_url: Optional[str] = None
) -> tuple[Optional["curl_requests.Response"], Optional[str]]:
    """Single Google search attempt. Returns (response, error_msg)."""
    try:
        params = {
            "q": query,
            "num": min(num_results + 2, 15),
            "hl": "en",
            "gl": "in",
        }
        url = f"https://www.google.com/search?{urlencode(params)}"

        # Google fingerprints especially aggressively — rotate per attempt.
        rot_headers = fingerprint_rotator.pick_headers(base=BROWSER_HEADERS)
        rot_profile = fingerprint_rotator.pick_profile(default=IMPERSONATE_PROFILE)
        kwargs = {
            "headers": {**rot_headers, "Referer": "https://www.google.com/"},
            "impersonate": rot_profile,
            "timeout": 10,
            "allow_redirects": True,
        }
        if proxy_url:
            kwargs["proxy"] = proxy_url

        response = curl_requests.get(url, **kwargs)
        return response, None
    except Exception as e:
        return None, str(e)[:200]


def google_search_sync(query: str, num_results: int = 8) -> SearchResponse:
    """
    Scrape Google Search results using curl_cffi + BeautifulSoup.
    Falls back to proxy on CAPTCHA/block detection.
    """
    # ── Attempt 1: Direct ────────────────────────────────────────────────
    response, error = _google_search_attempt(query, num_results)

    if response is not None and proxy_manager.is_blocked(response.status_code, response.text):
        print(f"[Search] 🛡️ Google blocked direct request — trying proxy")
        response = None  # force proxy fallback

    if response is None and proxy_manager.is_available():
        # ── Attempt 2: Proxy ─────────────────────────────────────────────
        proxy_url = proxy_manager.get_next_proxy()
        if proxy_url:
            response, error = _google_search_attempt(query, num_results, proxy_url)
            if response is not None and not proxy_manager.is_blocked(response.status_code, response.text):
                proxy_manager.report_success(proxy_url)
                print(f"[Search] ✅ Proxy search succeeded")
            elif response is not None:
                proxy_manager.report_failure(proxy_url)
                print(f"[Search] 🛡️ Proxy also blocked for Google")
                response = None
            else:
                proxy_manager.report_failure(proxy_url)

    if response is None:
        return SearchResponse(
            query=query, results=[], total=0, success=False,
            error=error or "Blocked after proxy retries",
        )

    if response.status_code != 200:
        return SearchResponse(
            query=query, results=[], total=0, success=False,
            error=f"Google returned HTTP {response.status_code}",
        )

    # ── Parse results ────────────────────────────────────────────────────
    try:
        soup = BeautifulSoup(response.text, "html.parser")
        results: list[SearchResult] = []
        seen_urls: set[str] = set()

        # Strategy 1: Standard search results (div.yuRUbf or div.g)
        for container in soup.select("div.g, div.yuRUbf"):
            link_tag = container.find("a", href=True)
            if not link_tag:
                continue

            href = link_tag.get("href", "")
            if not href.startswith("http"):
                continue

            if "/url?q=" in href:
                href = unquote(href.split("/url?q=")[1].split("&")[0])

            if href in seen_urls:
                continue

            h3 = container.find("h3")
            title = h3.get_text(strip=True) if h3 else ""
            if not title:
                continue

            desc_el = container.find("span", class_="aCOpRe") or \
                       container.find("div", class_="VwiC3b") or \
                       container.find("span", class_="st")
            description = desc_el.get_text(strip=True)[:300] if desc_el else ""

            price = extract_price_from_text(
                container.get_text(" ", strip=True)
            )

            seen_urls.add(href)
            results.append(SearchResult(
                url=href, title=title,
                description=description, price=price,
            ))

            if len(results) >= num_results:
                break

        # Strategy 2: If strategy 1 found few results, try broader selectors
        if len(results) < 3:
            for a_tag in soup.find_all("a", href=True):
                href = a_tag.get("href", "")
                if "/url?q=" in href:
                    href = unquote(href.split("/url?q=")[1].split("&")[0])
                if not href.startswith("http"):
                    continue
                if href in seen_urls:
                    continue
                if "google.com" in href or "googleapis.com" in href:
                    continue

                parent = a_tag.find_parent("div")
                title_el = a_tag.find("h3")
                title = title_el.get_text(strip=True) if title_el else a_tag.get_text(strip=True)[:100]
                if not title or len(title) < 10:
                    continue

                desc = ""
                if parent:
                    desc = parent.get_text(" ", strip=True)[:300]

                price = extract_price_from_text(desc)

                seen_urls.add(href)
                results.append(SearchResult(
                    url=href, title=title,
                    description=desc, price=price,
                ))

                if len(results) >= num_results:
                    break

        return SearchResponse(
            query=query, results=results,
            total=len(results), success=len(results) > 0,
        )

    except Exception as e:
        return SearchResponse(
            query=query, results=[], total=0,
            success=False, error=str(e)[:200],
        )


def extract_price_from_text(text: str) -> Optional[float]:
    """Extract an INR price from text (₹599, Rs. 1,299, etc.)"""
    patterns = [
        r'₹\s*([\d,]+(?:\.\d{1,2})?)',
        r'Rs\.?\s*([\d,]+(?:\.\d{1,2})?)',
        r'INR\s*([\d,]+(?:\.\d{1,2})?)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                price = float(match.group(1).replace(",", ""))
                if 10 < price < 1_000_000:
                    return price
            except ValueError:
                continue
    return None


# ─── API Endpoints ──────────────────────────────────────────────────────────


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    return HealthResponse(
        status="ok", version="5.0.0",
        impersonate_profile=IMPERSONATE_PROFILE,
        proxy_stats=proxy_manager.get_stats(),
        browser_pool=browser_pool.get_pool().stats(),
    )


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape(request: ScrapeRequest) -> ScrapeResponse:
    try:
        return await asyncio.wait_for(
            scrape_url_async(request.url, request.timeout),
            timeout=request.timeout + 3,  # 3s grace over caller's budget
        )
    except asyncio.TimeoutError:
        return ScrapeResponse(
            url=request.url, status_code=0, html="",
            content_length=0, success=False,
            error="Server-side timeout",
        )


@app.post("/scrape/batch", response_model=BatchScrapeResponse)
async def scrape_batch(request: BatchScrapeRequest) -> BatchScrapeResponse:
    semaphore = asyncio.Semaphore(MAX_BATCH_CONCURRENCY)

    async def bounded(url: str) -> ScrapeResponse:
        async with semaphore:
            return await scrape_url_async(url, request.timeout)

    # Per-request safety timeout: total batch budget = per-URL timeout + 5s
    batch_timeout = request.timeout + 5

    async def run_batch():
        tasks = [bounded(url) for url in request.urls]
        return await asyncio.gather(*tasks, return_exceptions=True)

    try:
        results = await asyncio.wait_for(run_batch(), timeout=batch_timeout)
    except asyncio.TimeoutError:
        results = []

    processed: list[ScrapeResponse] = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            processed.append(ScrapeResponse(
                url=request.urls[i], status_code=0, html="",
                content_length=0, success=False, error=str(result)[:200],
            ))
        else:
            processed.append(result)

    # Pad any missing results (from timeout)
    while len(processed) < len(request.urls):
        idx = len(processed)
        processed.append(ScrapeResponse(
            url=request.urls[idx], status_code=0, html="",
            content_length=0, success=False, error="Batch timeout",
        ))

    successful = sum(1 for r in processed if r.success)
    return BatchScrapeResponse(
        results=processed, total=len(processed),
        successful=successful, failed=len(processed) - successful,
    )


# ─── SSE Streaming Batch ────────────────────────────────────────────────────
# Fires static (curl_cffi) and browser (Patchright) scrapes concurrently from
# t=0 and streams each result the moment it finishes. The Next.js caller can
# process results incrementally instead of waiting for the entire batch.

JS_HEAVY_DOMAINS = {"myntra", "flipkart", "meesho", "nykaa", "ajio", "bigbasket", "jiomart"}


def _is_js_heavy(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return any(d in host for d in JS_HEAVY_DOMAINS)


@app.post("/scrape/batch/stream")
async def batch_stream(request: BatchScrapeRequest, http_request: Request):
    """
    Stream scrape results as SSE — client receives each URL result the moment
    it's done. Routes each URL to either async curl_cffi or Patchright browser
    based on domain, fires ALL concurrently from t=0.
    """
    from browser_scraper import scrape_with_browser

    async def generate():
        tasks: dict[asyncio.Task, str] = {}
        for url in request.urls:
            if _is_js_heavy(url):
                task = asyncio.create_task(
                    _safe_browser_scrape(url, request.timeout)
                )
            else:
                task = asyncio.create_task(
                    scrape_url_async(url, request.timeout)
                )
            tasks[task] = url

        completed = 0
        total = len(tasks)

        for coro in asyncio.as_completed(tasks.keys()):
            # Stop wasting resources if client disconnected
            if await http_request.is_disconnected():
                for t in tasks:
                    t.cancel()
                break
            try:
                result = await coro
                completed += 1
                # Normalize result shape (ScrapeResponse or browser dict)
                if isinstance(result, ScrapeResponse):
                    payload = {
                        "url": result.url, "success": result.success,
                        "html": result.html, "content_length": result.content_length,
                        "extracted_price": result.extracted_price,
                        "extracted_title": result.extracted_title,
                        "status_code": result.status_code,
                        "used_proxy": result.used_proxy,
                        "completed": completed, "total": total,
                    }
                else:
                    # Browser scrape returns a dict
                    payload = {
                        "url": result.get("url", ""),
                        "success": result.get("success", False),
                        "html": result.get("html", ""),
                        "content_length": len(result.get("html", "")),
                        "extracted_price": result.get("price"),
                        "extracted_title": result.get("title"),
                        "status_code": 200 if result.get("success") else 0,
                        "used_proxy": result.get("used_proxy", False),
                        "completed": completed, "total": total,
                    }
                yield f"data: {json_mod.dumps(payload)}\n\n"
            except Exception as e:
                completed += 1
                yield f"data: {json_mod.dumps({'error': str(e)[:200], 'completed': completed, 'total': total})}\n\n"

        yield f"data: {json_mod.dumps({'event': 'end', 'completed': completed, 'total': total, 'done': True})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",    # CRITICAL: disables Nginx buffering
            "Connection": "keep-alive",
        },
    )


async def _safe_browser_scrape(url: str, timeout: int = 10) -> dict:
    """Browser scrape with safety timeout wrapper."""
    from browser_scraper import scrape_with_browser
    try:
        return await asyncio.wait_for(
            scrape_with_browser(url, timeout),
            timeout=timeout + 3,
        )
    except asyncio.TimeoutError:
        return {"url": url, "success": False, "error": "Browser timeout", "html": "",
                "price": None, "mrp": None, "title": None}
    except Exception as e:
        return {"url": url, "success": False, "error": str(e)[:200], "html": "",
                "price": None, "mrp": None, "title": None}


@app.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest) -> SearchResponse:
    """Search Google and return structured results with URLs, titles, prices."""
    try:
        return await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(
                _thread_pool, google_search_sync, request.query, request.num_results,
            ),
            timeout=15,  # hard cap for search
        )
    except asyncio.TimeoutError:
        return SearchResponse(
            query=request.query, results=[], total=0,
            success=False, error="Search timeout",
        )


class BrowserScrapeResponse(BaseModel):
    url: str
    price: Optional[float] = None
    mrp: Optional[float] = None
    title: Optional[str] = None
    html: str = ""
    success: bool = False
    error: Optional[str] = None


@app.post("/scrape/browser", response_model=BrowserScrapeResponse)
async def scrape_browser_endpoint(request: ScrapeRequest) -> BrowserScrapeResponse:
    """Scrape a JS-heavy site using Patchright and return rendered HTML and extracted prices."""
    result = await _safe_browser_scrape(request.url, request.timeout)
    return BrowserScrapeResponse(**result)


# ─── Entry Point ─────────────────────────────────────────────────────────────

@app.post("/proxy/refresh")
async def refresh_proxies():
    """Manually trigger a Webshare proxy list refresh."""
    count = await proxy_manager.refresh_proxies()
    return {"message": f"Refreshed proxy pool", "proxies_loaded": count}


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("SCRAPER_PORT", "8765"))
    pa_status = "✅ configured" if amazon_pa.is_configured() else "❌ not configured (set AMAZON_PA_ACCESS_KEY, AMAZON_PA_SECRET_KEY, AMAZON_PA_PARTNER_TAG)"
    print(f"🚀 curl_cffi scraper v5.0 on port {port}")
    print(f"   Profile: {IMPERSONATE_PROFILE}")
    print(f"   Amazon PA API: {pa_status}")
    print(f"   Fallback: Direct → PA API (Amazon) → Proxy")
    print(f"   Endpoints: /scrape, /scrape/batch, /scrape/batch/stream, /search, /scrape/browser")

    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

