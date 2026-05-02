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

    # ── Tier 1: pool lease ───────────────────────────────────────────────
    try:
        async with pool.lease() as pp:
            page = pp.page

            # Navigate. UA, viewport, locale, route handlers are all already
            # set on the pooled context — no per-request setup overhead.
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)

            # Site-specific waits before price extraction
            hostname = urlparse(url).netloc

            if "myntra" in hostname:
                try:
                    await page.wait_for_function(
                        "() => window.__myx !== undefined || document.querySelector('.pdp-price, .pdp-discount-container') !== null",
                        timeout=7000,
                    )
                except Exception:
                    await page.wait_for_timeout(3000)

            elif "bigbasket" in hostname:
                try:
                    await page.wait_for_function(
                        "() => document.querySelector('[qa=\"product-price\"]') !== null || document.querySelector('[class*=\"PriceWidget\"]') !== null",
                        timeout=7000,
                    )
                except Exception:
                    await page.wait_for_timeout(3000)

            elif "jiomart" in hostname:
                try:
                    await page.wait_for_function(
                        "() => (window.__NEXT_DATA__ && window.__NEXT_DATA__.props) || document.querySelector('[class*=\"Price_sp\"]') !== null",
                        timeout=7000,
                    )
                except Exception:
                    await page.wait_for_timeout(3000)

            elif "meesho" in hostname:
                try:
                    await page.wait_for_selector("[class*='ProductCard'], [class*='product-card']", timeout=7000)
                except Exception:
                    await page.wait_for_timeout(3000)

            elif "flipkart" in hostname:
                try:
                    await page.wait_for_selector(".Nx9bqj, ._30jeq3, [class*='pdp-price']", timeout=7000)
                except Exception:
                    await page.wait_for_timeout(2000)

            elif "nykaa" in hostname:
                try:
                    await page.wait_for_selector("[class*='offer-price'], [class*='selling-price']", timeout=7000)
                except Exception:
                    await page.wait_for_timeout(2500)

            else:
                # Generic: wait for any price indicator
                try:
                    await page.wait_for_selector(
                        ", ".join([
                            "[class*='price']:not([class*='label']):not([class*='range'])",
                            "script[type='application/ld+json']",
                            "[itemprop='price']",
                            "[data-testid*='price']",
                            "meta[property*='price:amount']",
                        ]),
                        timeout=6000,
                        state="attached",
                    )
                except Exception:
                    await page.wait_for_timeout(2500)

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

        await page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)
        await page.wait_for_timeout(3000)  # generic wait for proxy scrape

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