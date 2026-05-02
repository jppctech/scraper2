"""
Amazon PA API v5 — Product Advertising API client.

Provides price/title lookup by ASIN as a fallback when direct scraping is blocked.
Uses AWS Signature V4 signing (no external dependencies).

Env vars required:
  AMAZON_PA_ACCESS_KEY
  AMAZON_PA_SECRET_KEY
  AMAZON_PA_PARTNER_TAG
  AMAZON_PA_HOST (optional, defaults to webservices.amazon.in)
  AMAZON_PA_REGION (optional, defaults to eu-west-1)
"""

import datetime
import hashlib
import hmac
import json
import os
import re
import urllib.request
from typing import Optional
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ─── Config ──────────────────────────────────────────────────────────────────

_ACCESS_KEY = os.getenv("AMAZON_PA_ACCESS_KEY", "")
_SECRET_KEY = os.getenv("AMAZON_PA_SECRET_KEY", "")
_PARTNER_TAG = os.getenv("AMAZON_PA_PARTNER_TAG", "")
_HOST = os.getenv("AMAZON_PA_HOST", "webservices.amazon.in")
_REGION = os.getenv("AMAZON_PA_REGION", "eu-west-1")
_SERVICE = "ProductAdvertisingAPI"
_ENDPOINT = f"https://{_HOST}/paapi5/getitems"

# ─── ASIN Extraction ────────────────────────────────────────────────────────

# Patterns to extract ASIN from Amazon URLs
_ASIN_PATTERNS = [
    r"/dp/([A-Z0-9]{10})",
    r"/gp/product/([A-Z0-9]{10})",
    r"/product/([A-Z0-9]{10})",
    r"/ASIN/([A-Z0-9]{10})",
    r"[?&]asin=([A-Z0-9]{10})",
]


@dataclass
class AmazonProduct:
    asin: str
    title: str
    price: Optional[float]
    mrp: Optional[float]
    currency: str
    url: str


def is_configured() -> bool:
    """Check if PA API credentials are set."""
    return bool(_ACCESS_KEY and _SECRET_KEY and _PARTNER_TAG)


def is_amazon_url(url: str) -> bool:
    """Check if a URL is an Amazon product page."""
    return bool(re.search(r"amazon\.(in|com|co\.uk|de|fr|es|it|co\.jp|ca|com\.au)", url))


def extract_asin(url: str) -> Optional[str]:
    """Extract ASIN from an Amazon product URL."""
    for pattern in _ASIN_PATTERNS:
        match = re.search(pattern, url, re.IGNORECASE)
        if match:
            return match.group(1).upper()
    return None


# ─── AWS Signature V4 ───────────────────────────────────────────────────────

def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _get_signature_key(key: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = hmac.new(("AWS4" + key).encode("utf-8"), date_stamp.encode("utf-8"), hashlib.sha256).digest()
    k_region = _sign(k_date, region)
    k_service = _sign(k_region, service)
    k_signing = _sign(k_service, "aws4_request")
    return k_signing


def _create_signed_headers(payload: str) -> dict:
    """Create AWS Signature V4 signed headers for PA API request."""
    now = datetime.datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    # Target header for GetItems (original casing for HTTP header)
    amz_target = "com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems"

    # Canonical headers — header VALUES must match exactly as sent,
    # but AWS Sig V4 requires header NAMES to be lowercase (they already are)
    canonical_headers = (
        f"content-encoding:amz-1.0\n"
        f"content-type:application/json; charset=utf-8\n"
        f"host:{_HOST}\n"
        f"x-amz-date:{amz_date}\n"
        f"x-amz-target:{amz_target}\n"
    )
    signed_headers = "content-encoding;content-type;host;x-amz-date;x-amz-target"

    # Hash payload
    payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    # Canonical request per AWS SigV4 spec:
    # HTTPMethod\nCanonicalURI\nCanonicalQueryString\nCanonicalHeaders\nSignedHeaders\nHashedPayload
    # NOTE: canonical_headers already ends with \n (after last header line)
    canonical_request = (
        "POST\n"
        "/paapi5/getitems\n"
        "\n"                       # empty query string
        f"{canonical_headers}"     # each header ends with \n, last one included
        f"{signed_headers}\n"
        f"{payload_hash}"
    )

    # String to sign
    credential_scope = f"{date_stamp}/{_REGION}/{_SERVICE}/aws4_request"
    string_to_sign = (
        f"AWS4-HMAC-SHA256\n"
        f"{amz_date}\n"
        f"{credential_scope}\n"
        f"{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
    )

    # Calculate signature
    signing_key = _get_signature_key(_SECRET_KEY, date_stamp, _REGION, _SERVICE)
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    # Authorization header
    authorization = (
        f"AWS4-HMAC-SHA256 "
        f"Credential={_ACCESS_KEY}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, "
        f"Signature={signature}"
    )

    return {
        "Content-Encoding": "amz-1.0",
        "Content-Type": "application/json; charset=utf-8",
        "Host": _HOST,
        "X-Amz-Date": amz_date,
        "X-Amz-Target": amz_target,
        "Authorization": authorization,
    }


# ─── PA API Call ─────────────────────────────────────────────────────────────

def get_item_by_asin(asin: str) -> Optional[AmazonProduct]:
    """
    Look up a product by ASIN using Amazon PA API v5.
    Returns AmazonProduct with price/title or None on failure.
    """
    if not is_configured():
        return None

    payload = json.dumps({
        "ItemIds": [asin],
        "ItemIdType": "ASIN",
        "Resources": [
            "ItemInfo.Title",
            "Offers.Listings.Price",
            "Offers.Listings.SavingBasis",
        ],
        "PartnerTag": _PARTNER_TAG,
        "PartnerType": "Associates",
        "Marketplace": "www.amazon.in",
    })

    try:
        headers = _create_signed_headers(payload)
        req = urllib.request.Request(
            _ENDPOINT,
            data=payload.encode("utf-8"),
            headers=headers,
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        items = data.get("ItemsResult", {}).get("Items", [])
        if not items:
            print(f"[Amazon PA] No results for ASIN {asin}")
            return None

        item = items[0]

        # Extract title
        title = item.get("ItemInfo", {}).get("Title", {}).get("DisplayValue", "")

        # Extract price from offers
        price = None
        mrp = None
        currency = "INR"

        listings = item.get("Offers", {}).get("Listings", [])
        if listings:
            listing = listings[0]
            price_info = listing.get("Price", {})
            price = price_info.get("Amount")
            currency = price_info.get("Currency", "INR")

            # MRP from SavingBasis
            saving_basis = listing.get("SavingBasis", {})
            if saving_basis:
                mrp = saving_basis.get("Amount")

        if price and price > 10:
            product = AmazonProduct(
                asin=asin,
                title=title,
                price=price,
                mrp=mrp if mrp and mrp > price else None,
                currency=currency,
                url=f"https://www.amazon.in/dp/{asin}",
            )
            print(f"[Amazon PA] ✅ {asin} → ₹{price} | {title[:60]}")
            return product
        else:
            print(f"[Amazon PA] ⚠️ No valid price for ASIN {asin}")
            return None

    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="ignore")[:200]
        print(f"[Amazon PA] ❌ HTTP {e.code}: {error_body}")
        return None
    except Exception as e:
        print(f"[Amazon PA] ❌ Error: {str(e)[:100]}")
        return None


def lookup_from_url(url: str) -> Optional[AmazonProduct]:
    """
    Convenience: extract ASIN from URL, then call PA API.
    Returns AmazonProduct or None.
    """
    asin = extract_asin(url)
    if not asin:
        print(f"[Amazon PA] Could not extract ASIN from: {url[:80]}")
        return None
    return get_item_by_asin(asin)
