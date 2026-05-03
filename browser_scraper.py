"""
Browser Scraper — Patchright (undetected Playwright) for JS-heavy sites.
Persistent browser — launched once on startup, context-per-request.

Phase 1 of the flash.co anti-block work: the import was swapped from
`playwright.async_api` → `patchright.async_api`. Everything else in this
file is identical — the API surface is a true drop-in replacement.

What Patchright gives us "for free":
  • `--disable-blink-features=AutomationControlled` is auto-added → no more
    `navigator.webdriver=true` leak.
  • `--enable-automation` is auto-removed.
  • Runtime.enable / Console.enable CDP leaks are patched — CreepJS headless
    score drops from ~100 % to ~67 %.
  • navigator.plugins, navigator.languages, WebGL, screen.*, etc. return
    values consistent with a real Chrome install.

Important — we deliberately keep our own launch args here rather than moving
to `launch_persistent_context`:
  • `--no-sandbox` / `--disable-setuid-sandbox` are required on a root-less
    Docker / VPS container; Patchright does NOT rewrite these.
  • `headless=True` is kept because the target VPS is headless; `--headless`
    is not in Patchright's detection surface after the blink-features patch.
  • We still create a FRESH context per request so parallel scrapes can't
    pollute each other's cookies — Phase 2 will replace this with a proper
    pre-warmed context pool.
"""

import asyncio
from typing import Optional
import re
from urllib.parse import urlparse

import proxy_manager
import browser_pool

# Module-level cache for the legacy is-available probe. The real
# state-of-truth lives in the BrowserPool; this only avoids paying the
# warmup cost just to answer "is Patchright installed?".
_playwright_available: Optional[bool] = None


# ─── Performance helpers ────────────────────────────────────────────────────
#
# These three helpers implement the high-ROI scraper speedups from the
# perplexity normal-research breakdown:
#
#   smart_wait_for_price     — Tier 1, replaces blind page.wait_for_timeout
#                              fallbacks (saves 1.5–2.5 s per URL).
#   try_api_intercept        — Tier 4, intercepts internal price-API JSON
#                              responses for SPA stores so we don't have to
#                              wait for full DOM render (–800 ms to –2 s).
#   get_site_timeout_ms      — Per-domain page.goto timeout cap so genuinely
#                              hung pages fail fast instead of burning the
#                              full 15 s budget.

# Universal CSS selector that matches the price node on virtually every
# Indian e-commerce site. Used as the race target for smart_wait_for_price.
# Order is irrelevant — we only need ONE of these to attach.
_UNIVERSAL_PRICE_SELECTORS = ", ".join([
    "[itemprop='price']",
    "script[type='application/ld+json']",
    "[class*='price']:not([class*='label']):not([class*='range'])",
    "[class*='Price']:not([class*='label']):not([class*='range'])",
    "[data-testid*='price']",
    "meta[property*='price:amount']",
])


async def smart_wait_for_price(page, hard_timeout_ms: int = 2500) -> None:
    """
    Race a universal price-selector match vs a hard timeout.

    Replaces the previous `page.wait_for_timeout(N)` fallbacks which slept
    the FULL N ms even when the price was already in the DOM. With the
    race-based approach the wait returns the moment the selector attaches
    (typically <500 ms on a warm pool) and only falls back to the full
    `hard_timeout_ms` when the page genuinely hasn't rendered yet.

    Median per-URL savings: 1.5–2.5 s.
    """
    try:
        await page.wait_for_selector(
            _UNIVERSAL_PRICE_SELECTORS,
            state="attached",
            timeout=hard_timeout_ms,
        )
    except Exception:
        # Price wasn't found in the DOM within the budget — give up fast,
        # don't sleep. The downstream JS extractor will still try its full
        # selector cascade on whatever HTML is there.
        pass


# Internal-API URL fragments used by SPA stores to deliver price data.
# These are heuristics built from observed traffic — keep the patterns
# loose-but-distinctive (e.g. "/api/product/" not just "/api/").
_PRICE_API_PATTERNS = {
    "myntra":   ["/gateway/v2/product/", "/api/product/"],
    "meesho":   ["/api/products/", "/product/detail/"],
    "nykaa":    ["/api/catalog/", "/product-detail/"],
    "flipkart": ["/api/", "/_next/data/"],
}

# Regex pulled out of the response JSON body — covers the common price
# field names used across these SPAs without needing a per-site schema.
_PRICE_REGEX = re.compile(
    r'"(?:price|sellingPrice|sp|offerPrice|mrp|discountedPrice)"\s*:\s*"?(\d+(?:\.\d+)?)"?',
    re.I,
)


async def try_api_intercept(page, hostname: str, max_wait_ms: int = 4000) -> Optional[dict]:
    """
    Attempt to capture price data from the FIRST internal API response
    that matches the per-host pattern set, before the SPA finishes
    hydrating its DOM.

    Returns `{ "price": float, "mrp": None, "title": <doc.title> }` on
    success, or `None` if no matching response arrived in `max_wait_ms`.

    Implementation notes:
      - The listener is installed BEFORE goto() so we don't miss the
        first burst of XHRs that fires during the SPA's initial mount.
      - We resolve the future on the first match — duplicates are
        ignored. Most sites fire 5–20 internal calls; we want the
        cheapest one.
      - Price is always pulled by regex over the raw JSON string. Even
        if the schema is nested, the field name + value pattern is
        consistent. This is faster + more resilient than a full schema
        walk per site.
    """
    patterns = next(
        (v for k, v in _PRICE_API_PATTERNS.items() if k in hostname), []
    )
    if not patterns:
        return None

    loop = asyncio.get_event_loop()
    price_future: asyncio.Future = loop.create_future()

    async def handle_response(response):
        if price_future.done():
            return
        url = response.url
        if not any(p in url for p in patterns):
            return
        # Short-circuit obvious non-JSON (image / static asset hits).
        ctype = response.headers.get("content-type", "")
        if "json" not in ctype and "javascript" not in ctype:
            return
        try:
            body_text = await response.text()
        except Exception:
            return
        match = _PRICE_REGEX.search(body_text)
        if not match:
            return
        try:
            value = float(match.group(1))
        except (TypeError, ValueError):
            return
        # Sanity-check the magnitude — JSON sometimes has bogus 0/1 values
        # for non-price fields that happen to share the key name.
        if not (10 < value < 10_000_000):
            return
        if not price_future.done():
            price_future.set_result(value)

    page.on("response", handle_response)
    try:
        price = await asyncio.wait_for(price_future, timeout=max_wait_ms / 1000)
    except (asyncio.TimeoutError, Exception):
        return None
    finally:
        # Always remove the listener — leaking it would keep firing on
        # subsequent goto()s when the page is reused via the pool.
        try:
            page.remove_listener("response", handle_response)
        except Exception:
            pass

    title = ""
    try:
        title = await page.title()
    except Exception:
        pass
    return {"price": price, "mrp": None, "title": title}


# Per-domain `page.goto` budget in milliseconds. Sites with consistently
# fast TTFB (Amazon, Flipkart) get tighter budgets so a genuinely stuck
# page fails fast instead of burning the full 15 s default. Sites known
# to be slow (JioMart) get a more generous cap. Anything not listed here
# falls back to `_DEFAULT_GOTO_TIMEOUT_MS`.
_SITE_GOTO_TIMEOUTS_MS: dict[str, int] = {
    "amazon":          8_000,
    "flipkart":       12_000,   # "commit" is fast; the budget covers selector wait
    "myntra":         10_000,
    "nykaa":           9_000,
    "meesho":         10_000,
    "jiomart":        12_000,
    "bigbasket":      10_000,
    "ajio":            9_000,
    "tatacliq":       10_000,
    "croma":           9_000,
    "tirabeauty":     15_000,   # networkidle needs more time
    "apollopharmacy": 12_000,   # Angular hydration is slow
}
_DEFAULT_GOTO_TIMEOUT_MS = 20_000


def get_site_timeout_ms(hostname: str, base_timeout: int) -> int:
    """
    Resolve the effective `page.goto` timeout for a given hostname.

    Returns the LARGER of the per-site cap and the caller's `base_timeout`
    (in seconds, converted to ms). Site-specific timeouts are tuned for
    each domain's behaviour — they should not be shortened by the caller.
    """
    matched = next(
        (v for k, v in _SITE_GOTO_TIMEOUTS_MS.items() if k in hostname),
        _DEFAULT_GOTO_TIMEOUT_MS,
    )
    return max(matched, base_timeout * 1000)


async def init_browser():
    """
    Back-compat shim: ensure the BrowserPool is warm and return its
    underlying Patchright Browser handle. Returns None if Patchright is
    not installed / fails to launch (legacy callers expected None on
    failure rather than an exception).
    """
    global _playwright_available
    pool = browser_pool.get_pool()
    if pool.is_ready():
        _playwright_available = True
        return pool._browser  # type: ignore[attr-defined]
    try:
        await pool.warmup()
        _playwright_available = pool.is_ready()
        return pool._browser if pool.is_ready() else None  # type: ignore[attr-defined]
    except Exception as e:
        _playwright_available = False
        print(f"[Browser] Patchright not available: {e}")
        return None


async def check_playwright_available() -> bool:
    global _playwright_available
    if _playwright_available is not None:
        return _playwright_available
    b = await init_browser()
    return b is not None


async def scrape_with_browser(url: str, timeout: int = 15) -> dict:
    """
    Scrape a URL using persistent headless Chromium pool.
    Returns { price, mrp, title, html, success, error }
    """
    result = {
        "url": url,
        "price": None,
        "mrp": None,
        "title": None,
        "html": "",
        "success": False,
        "error": None,
    }

    pool = browser_pool.get_pool()
    if not pool.is_ready():
        # Lazy warmup so calls before FastAPI lifespan finishes don't crash.
        # Production path is always already-warm via the lifespan startup hook.
        try:
            await pool.warmup()
        except Exception as e:
            return {**result, "error": f"Browser pool not available: {str(e)[:120]}"}
        if not pool.is_ready():
            return {**result, "error": "Patchright not installed or failed to boot"}

    blocked_after_main = False
    hostname = urlparse(url).netloc

    # ── Tier 1: pool lease ───────────────────────────────────────────────
    try:
        async with pool.lease() as pp:
            page = pp.page

            # ── Phase 2.4: per-domain goto timeout cap ─────────────────
            # Fast-fail genuinely stuck pages instead of burning the full
            # caller-provided 15 s budget. `get_site_timeout_ms` returns
            # the smaller of the site-specific cap and the caller's hint.
            goto_timeout_ms = get_site_timeout_ms(hostname, timeout)

            # ── Phase 2.2: try the XHR API intercept first for SPA stores ──
            # For Myntra / Flipkart / Nykaa / Meesho the price ships in an
            # internal API call that fires DURING navigation — we can grab
            # it the moment the JSON body arrives instead of waiting for
            # React/Vue to render the DOM. The listener has to be set up
            # BEFORE goto(), so we kick off both concurrently.
            api_task = None
            if any(h in hostname for h in ("myntra", "flipkart", "nykaa", "meesho")):
                api_task = asyncio.create_task(
                    try_api_intercept(page, hostname, max_wait_ms=4000)
                )

            # Navigate. Per-domain wait_until strategy:
            #   - "commit": for sites where price is in initial HTML (JSON-LD)
            #   - "networkidle": for Next.js sites that fire a product API call
            #   - "domcontentloaded": default, good for most sites
            if "flipkart" in hostname:
                # Flipkart needs CSS for IntersectionObserver — unblock it.
                # CSS is blocked by default in the pool to speed up other sites.
                if pp.cdp_session:
                    try:
                        await pp.cdp_session.send("Network.setBlockedURLs", {
                            "urls": [
                                "*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg",
                                "*.woff", "*.woff2", "*.ttf", "*.eot", "*.ico",
                                "*.mp4", "*.mp3",
                                "*google-analytics*", "*googletagmanager*",
                                "*facebook.net*", "*hotjar*", "*clarity*",
                                "*doubleclick*", "*adsystem*",
                            ]
                        })
                    except Exception:
                        pass
                await page.goto(url, wait_until="commit", timeout=goto_timeout_ms)
                # Mark context for recycling — CSS was unblocked for this request
                # and needs to be restored for the next non-Flipkart request.
                pp.is_broken = True
            elif "tirabeauty" in hostname:
                # TiraBeauty (Next.js): price API fires after DOM ready.
                # networkidle waits for all network requests to settle.
                await page.goto(url, wait_until="networkidle", timeout=goto_timeout_ms)
            else:
                await page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout_ms)

            # If the API intercept already won the race, we can short-circuit
            # the entire DOM-wait + extraction phase.
            if api_task is not None and api_task.done():
                api_data = api_task.result()
                if api_data and api_data.get("price"):
                    result["price"] = api_data["price"]
                    result["mrp"] = api_data.get("mrp")
                    result["title"] = api_data.get("title") or ""
                    try:
                        result["html"] = await page.content()
                    except Exception:
                        result["html"] = ""
                    result["success"] = True
                    print(f"[Browser] ✅ {url[:65]} → ₹{result['price']} (api-intercept)")
                    return result

            # Site-specific waits before price extraction. The
            # `wait_for_timeout(N)` fallbacks have been replaced with
            # `smart_wait_for_price` (Phase 2.1) which races the universal
            # price selector against a hard timeout — typically returning
            # in <500 ms instead of sleeping the full N ms.
            if "myntra" in hostname:
                try:
                    await page.wait_for_function(
                        "() => window.__myx !== undefined || document.querySelector('.pdp-price, .pdp-discount-container') !== null",
                        timeout=7000,
                    )
                except Exception:
                    await smart_wait_for_price(page, hard_timeout_ms=3000)

            elif "bigbasket" in hostname:
                try:
                    await page.wait_for_function(
                        "() => document.querySelector('[qa=\"product-price\"]') !== null || document.querySelector('[class*=\"PriceWidget\"]') !== null",
                        timeout=7000,
                    )
                except Exception:
                    await smart_wait_for_price(page, hard_timeout_ms=3000)

            elif "jiomart" in hostname:
                try:
                    await page.wait_for_function(
                        "() => (window.__NEXT_DATA__ && window.__NEXT_DATA__.props) || document.querySelector('[class*=\"Price_sp\"]') !== null",
                        timeout=7000,
                    )
                except Exception:
                    await smart_wait_for_price(page, hard_timeout_ms=3000)

            elif "meesho" in hostname:
                try:
                    await page.wait_for_selector("[class*='ProductCard'], [class*='product-card']", timeout=7000)
                except Exception:
                    await smart_wait_for_price(page, hard_timeout_ms=3000)

            elif "flipkart" in hostname:
                # After "commit", wait for the price selector to appear
                try:
                    await page.wait_for_selector(".Nx9bqj, ._30jeq3, [class*='pdp-price'], script[type='application/ld+json']", timeout=7000)
                except Exception:
                    await smart_wait_for_price(page, hard_timeout_ms=2000)

            elif "nykaa" in hostname:
                try:
                    await page.wait_for_selector("[class*='offer-price'], [class*='selling-price']", timeout=7000)
                except Exception:
                    await smart_wait_for_price(page, hard_timeout_ms=2500)

            elif "tirabeauty" in hostname:
                # networkidle already waited; now check for price selector
                try:
                    await page.wait_for_selector("[class*='sellingPrice'], [class*='selling-price'], [data-testid='selling-price']", timeout=5000)
                except Exception:
                    await smart_wait_for_price(page, hard_timeout_ms=3000)

            elif "apollopharmacy" in hostname:
                # Angular: slower hydration than React. Don't use networkidle
                # (analytics keep network busy 10+ seconds).
                try:
                    await page.wait_for_selector("[class*='price'], [class*='Price'], [class*='discountPrice']", timeout=6000)
                except Exception:
                    await smart_wait_for_price(page, hard_timeout_ms=3000)

            else:
                # Generic: wait for any price indicator
                try:
                    await page.wait_for_selector(
                        _UNIVERSAL_PRICE_SELECTORS,
                        timeout=6000,
                        state="attached",
                    )
                except Exception:
                    await smart_wait_for_price(page, hard_timeout_ms=2500)

            # If the API intercept settled DURING the site-specific wait
            # (e.g. Flipkart's _next/data fired ~3 s in), prefer it over
            # DOM extraction — it's the source of truth and saves a
            # page.evaluate round-trip.
            if api_task is not None and api_task.done():
                api_data = api_task.result()
                if api_data and api_data.get("price"):
                    result["price"] = api_data["price"]
                    result["mrp"] = api_data.get("mrp")
                    try:
                        result["html"] = await page.content()
                        result["title"] = api_data.get("title") or await page.title()
                    except Exception:
                        result["html"] = ""
                        result["title"] = api_data.get("title") or ""
                    result["success"] = True
                    print(f"[Browser] ✅ {url[:65]} → ₹{result['price']} (api-intercept)")
                    return result

            # If the api_task is still pending at this point, cancel it —
            # we don't want a dangling listener firing after lease release.
            if api_task is not None and not api_task.done():
                api_task.cancel()
                try:
                    await api_task
                except (asyncio.CancelledError, Exception):
                    pass

            # Price extraction via JS evaluate
            price_data = await page.evaluate("""() => {
                function parseINR(text) {
                    if (!text) return null;
                    if (text.includes('$') || text.toUpperCase().includes('USD')) return null;
                    const m = text.replace(/,/g, '').match(/[₹Rs.]*\\s*(\\d+(?:\\.\\d+)?)/);
                    if (!m) return null;
                    const v = parseFloat(m[1]);
                    return (v > 10 && v < 10000000) ? v : null;
                }

                // ── Strategy 1: JSON-LD ──
                for (const s of document.querySelectorAll('script[type="application/ld+json"]')) {
                    try {
                        let d = JSON.parse(s.textContent);
                        if (Array.isArray(d)) d = d[0];
                        if (d && d['@graph']) {
                            const prod = d['@graph'].find(x => x['@type'] === 'Product');
                            if (prod) d = prod;
                        }
                        if (d && (d['@type'] === 'Product' || d['@type'] === 'IndividualProduct')) {
                            let offers = d.offers || d.offer;
                            if (Array.isArray(offers)) offers = offers[0];
                            if (offers) {
                                const currency = (offers.priceCurrency || '').toUpperCase();
                                if (currency === 'USD') continue;
                                const price = parseFloat(String(offers.price || offers.lowPrice || '0').replace(/,/g, ''));
                                const mrp = parseFloat(String(offers.highPrice || '0').replace(/,/g, ''));
                                if (price > 10) return { price, mrp: mrp > price ? mrp : null, title: d.name || '' };
                            }
                        }
                    } catch {}
                }

                // ── Strategy 2: Meta tags ──
                const metaCurrency = (document.querySelector('meta[property*="price:currency"]')?.content || '').toUpperCase();
                if (metaCurrency !== 'USD') {
                    const metaPrice = document.querySelector('meta[property*="price:amount"]')?.content;
                    if (metaPrice) {
                        const price = parseINR(metaPrice);
                        if (price) return { price, mrp: null, title: document.querySelector('meta[property="og:title"]')?.content || document.title };
                    }
                }

                // ── Strategy 3: Site-specific JS state ──
                const hostname = window.location.hostname;

                // Myntra
                if (hostname.includes('myntra') && window.__myx) {
                    try {
                        const s = JSON.stringify(window.__myx);
                        const m = s.match(/"(?:discounted|selling|mrp|offer)Price"\\s*:\\s*(\\d+)/i);
                        if (m) return { price: parseInt(m[1]), mrp: null, title: document.title };
                    } catch {}
                }

                // BigBasket
                const bbEl = document.querySelector('[qa="product-price"], [class*="PriceWidget__sp"]');
                if (bbEl) {
                    const p = parseINR(bbEl.textContent);
                    if (p) return { price: p, mrp: null, title: document.title };
                }

                // JioMart — __NEXT_DATA__
                if (hostname.includes('jiomart')) {
                    try {
                        const nd = JSON.parse(document.getElementById('__NEXT_DATA__')?.textContent || '{}');
                        const sp = nd?.props?.pageProps?.productDetails?.product?.price?.sp
                                || nd?.props?.pageProps?.product?.sp
                                || nd?.props?.pageProps?.pdpData?.product?.sp
                                || nd?.props?.initialProps?.product?.price;
                        if (sp) {
                            const p = parseFloat(String(sp).replace(/,/g, ''));
                            if (p > 10) return { price: p, mrp: null, title: document.title };
                        }
                    } catch {}
                    const jEl = document.querySelector('[class*="Price_sp"], .sp-price');
                    if (jEl) {
                        const p = parseINR(jEl.textContent);
                        if (p) return { price: p, mrp: null, title: document.title };
                    }
                }

                // TiraBeauty — Next.js with deep __NEXT_DATA__ nesting
                if (hostname.includes('tirabeauty')) {
                    try {
                        const nd = JSON.parse(document.getElementById('__NEXT_DATA__')?.textContent || '{}');
                        const pdp = nd?.props?.pageProps?.productDetails
                                 || nd?.props?.pageProps?.product
                                 || nd?.props?.pageProps?.pdpData;
                        const sp = pdp?.price?.sp || pdp?.sellingPrice || pdp?.discountedPrice
                                || pdp?.priceDetails?.sp;
                        if (sp) {
                            const p = parseFloat(String(sp).replace(/,/g, ''));
                            if (p > 10) return { price: p, mrp: pdp?.price?.mrp || pdp?.mrp || null, title: pdp?.name || document.title };
                        }
                    } catch {}
                    // Fallback: CSS selector (TiraBeauty stable classes)
                    const tEl = document.querySelector('[class*="selling-price"], [class*="sellingPrice"], [data-testid="selling-price"]');
                    if (tEl) {
                        const p = parseINR(tEl.textContent);
                        if (p) return { price: p, mrp: null, title: document.title };
                    }
                }

                // ApolloPharmacy — Angular + Shadow DOM
                if (hostname.includes('apollopharmacy')) {
                    // Shadow DOM piercing helper
                    function deepQuery(root, selector) {
                        let el = root.querySelector(selector);
                        if (el) return el;
                        const hosts = root.querySelectorAll('*');
                        for (const host of hosts) {
                            if (host.shadowRoot) {
                                el = deepQuery(host.shadowRoot, selector);
                                if (el) return el;
                            }
                        }
                        return null;
                    }
                    const aEl = deepQuery(document, '[class*="discounted-price"], [class*="DiscountPrice"], [class*="sp-price"], .product-price, [class*="finalPrice"]');
                    if (aEl) {
                        const p = parseINR(aEl.textContent);
                        if (p) return { price: p, mrp: null, title: document.title };
                    }
                    // Apollo also embeds price in window state
                    try {
                        const str = JSON.stringify(window.__APP_STATE__ || window.__REDUX_STATE__ || {});
                        const m = str.match(/"(?:sp|sellingPrice|discountedPrice)"\\s*:\\s*(\\d+\\.?\\d*)/i);
                        if (m) return { price: parseFloat(m[1]), mrp: null, title: document.title };
                    } catch {}
                }

                // Generic window state keys
                for (const key of ['__PRELOADED_STATE__', '__NEXT_DATA__', '__NUXT__', '__nuxt__']) {
                    try {
                        const state = window[key];
                        if (!state) continue;
                        const str = JSON.stringify(state);
                        const m = str.match(/"(?:price|sellingPrice|offerPrice|salePrice|sp)"\\s*:\\s*"?(\\d+(?:\\.\\d+)?)"?/i);
                        if (m) {
                            const p = parseFloat(m[1]);
                            if (p > 10 && p < 10000000) return { price: p, mrp: null, title: document.title };
                        }
                    } catch {}
                }

                // ── Strategy 4: CSS selectors ──
                const selectors = [
                    '._30jeq3._16Jk6d', '.Nx9bqj.CxhGGd', '._30jeq3',      // Flipkart
                    '.a-price .a-offscreen', '.priceToPay .a-offscreen',       // Amazon
                    '.pdp__offerPrice', '[data-testid="selling-price"]',        // Croma
                    '[class*="sellingPrice"]', '[class*="offer-price"]',
                    '[class*="sale-price"]', '[class*="currentPrice"]',
                    '[class*="pdp-price"]', '[class*="PriceText"]',
                    '[class*="discounted_price"]', '[class*="DiscountedPrice"]',
                    '[class*="ProductPrice__offerPrice"]',
                    '.price .money', '.product-price .money',
                    '[class*="priceInfo__packPrice"]', '.discountPrice',
                    '[class*="PriceBox__offer-price"]',
                    '[itemprop="price"]',
                    '.final-price', '.pa-price span', '.selling-price',
                    '.woocommerce-Price-amount',
                ];
                for (const sel of selectors) {
                    const el = document.querySelector(sel);
                    if (!el) continue;
                    const text = el.textContent.trim();
                    if (text.includes('$') || text.toUpperCase().includes('USD')) continue;
                    const p = parseINR(text);
                    if (p) {
                        // Try to find MRP near this element
                        const parent = el.closest('[class*="price"], [class*="Price"]') || el.parentElement;
                        let mrp = null;
                        if (parent) {
                            const mrpEl = parent.querySelector('[class*="mrp"], [class*="strike"], [class*="original"], s, del');
                            if (mrpEl) mrp = parseINR(mrpEl.textContent);
                        }
                        return { price: p, mrp: mrp && mrp > p ? mrp : null, title: document.title };
                    }
                }

                return null;
            }""")

            result["html"] = await page.content()
            result["title"] = await page.title()

            if price_data and price_data.get("price"):
                result["price"] = price_data["price"]
                result["mrp"] = price_data.get("mrp")
                result["title"] = price_data.get("title") or result["title"]
                result["success"] = True
                print(f"[Browser] ✅ {url[:65]} → ₹{result['price']}")
                return result

            # No price extracted. Distinguish "genuinely no price on page"
            # from "CAPTCHA / block page" so we only burn a proxy on the
            # latter.
            if proxy_manager.is_blocked(200, result["html"]):
                print(f"[Browser] 🛡️ CAPTCHA detected: {url[:65]} — will retry with proxy")
                pp.is_broken = True   # recycle context — it may carry CAPTCHA cookies
                blocked_after_main = True
            else:
                result["success"] = False
                print(f"[Browser] ⚠️ {url[:65]} → no price found")
                return result

    except Exception as e:
        result["error"] = str(e)[:200]
        # Capture partial HTML even on timeout — the page likely has
        # JSON-LD / meta tags already. The TS-side extractPriceFromProductPage
        # will pick up prices from this partial HTML.
        try:
            result["html"] = await page.content()
            result["title"] = await page.title()
            html_len = len(result["html"])
            if html_len > 500:
                print(f"[Browser] ❌ {url[:65]} → timeout but captured {html_len} chars HTML")
            else:
                print(f"[Browser] ❌ {url[:65]} → {str(e)[:80]}")
        except Exception:
            print(f"[Browser] ❌ {url[:65]} → {str(e)[:80]}")
        return result

    # ── Tier 2: proxy retry (only on detected block) ─────────────────────
    if blocked_after_main:
        proxy_url_str = proxy_manager.get_next_proxy()
        if proxy_url_str:
            proxy_result = await _browser_scrape_with_proxy(url, timeout, proxy_url_str)
            if proxy_result["success"]:
                proxy_manager.report_success(proxy_url_str)
                return proxy_result
            proxy_manager.report_failure(proxy_url_str)
            return proxy_result

    return result


async def _browser_scrape_with_proxy(url: str, timeout: int, proxy_url: str) -> dict:
    """
    Retry a browser scrape using a proxy. Uses the pool's persistent browser
    but creates a one-off context (proxy must be set at context construction
    time, so it can't be reused across calls).
    """
    result = {
        "url": url,
        "price": None,
        "mrp": None,
        "title": None,
        "html": "",
        "success": False,
        "error": None,
        "used_proxy": True,
    }

    pool = browser_pool.get_pool()
    browser = pool._browser  # type: ignore[attr-defined]
    if browser is None:
        result["error"] = "Browser pool not initialised"
        return result

    context = None
    try:
        # Parse proxy URL into Playwright proxy config
        from urllib.parse import urlparse as parse_url
        parsed = parse_url(proxy_url)
        pw_proxy = {
            "server": f"http://{parsed.hostname}:{parsed.port}",
            "username": parsed.username or "",
            "password": parsed.password or "",
        }

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            extra_http_headers={"Accept-Language": "en-IN,en;q=0.9"},
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            proxy=pw_proxy,
        )

        page = await context.new_page()
        await page.route(
            "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,eot,ico,mp4,mp3}",
            lambda route: route.abort(),
        )

        # Phase 2.4: per-domain goto cap also applies to the proxy retry
        # path so a known-slow domain on a slow proxy doesn't compound.
        proxy_hostname = urlparse(url).netloc
        proxy_goto_ms = get_site_timeout_ms(proxy_hostname, timeout)
        await page.goto(url, wait_until="domcontentloaded", timeout=proxy_goto_ms)
        # Phase 2.1: race-based wait instead of blind 3 s sleep.
        await smart_wait_for_price(page, hard_timeout_ms=3000)

        result["html"] = await page.content()
        result["title"] = await page.title()

        # Quick price check from page title or content
        if not proxy_manager.is_blocked(200, result["html"]):
            result["success"] = True
            print(f"[Browser] ✅ Proxy success: {url[:65]}")
        else:
            print(f"[Browser] 🛡️ Proxy also blocked: {url[:65]}")

    except Exception as e:
        result["error"] = str(e)[:200]
        print(f"[Browser] ❌ Proxy error: {url[:65]} → {str(e)[:80]}")
    finally:
        if context is not None:
            try:
                await context.close()
            except Exception:
                pass

    return result