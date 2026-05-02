"""
Fingerprint Rotator — randomises curl_cffi `impersonate` profile and
Accept-Language headers per request to make our outbound traffic look like
many users instead of one bot.

Why we care
===========
Even with curl_cffi's TLS impersonation, sending exactly the same
`impersonate=chrome` + identical Accept-Language on every request from the
same IP within minutes is a strong fingerprint. Sites with bot-detection
("Akamai", "DataDome", "Imperva", store-side rate limiters) cluster traffic
on these signals and start serving 403/CAPTCHA after ~20 requests/min.

Rotating across {chrome124, chrome120, chrome116, chrome110, edge101} plus
4 realistic Indian Accept-Language combos gives us ~20 distinct fingerprints
without changing any other code.

Why ONLY Chrome + Edge (no Safari)
==================================
Our existing static `BROWSER_HEADERS` in main.py declares
`Sec-Ch-Ua-Platform="Windows"`. Safari profiles emit Mac-shaped Sec-CH-UA,
so mixing them would create a self-contradicting fingerprint that is itself
a strong bot signal. Once we move to per-profile header sets we can add
Safari back; for now, Chrome+Edge on Windows keeps everything internally
consistent.

Rollback
========
Set env `DISABLE_FINGERPRINT_ROTATION=true` to fall back to the legacy
fixed `chrome` profile + `BROWSER_HEADERS` baseline.
"""
from __future__ import annotations

import os
import random
from typing import Optional

# ─── Configuration ──────────────────────────────────────────────────────────

# All values below MUST be valid identifiers in curl_cffi's BrowserType enum.
# Verified against curl_cffi==0.7.4 — see `python -c "from curl_cffi.requests
# import BrowserType; print([b.value for b in BrowserType])"`.
_PROFILES: list[str] = [
    "chrome124",   # most common modern Chrome on Windows
    "chrome120",
    "chrome116",
    "chrome110",
    "edge101",     # Microsoft Edge — Windows-shaped Sec-CH-UA, same as Chrome
]

# Realistic Indian browser Accept-Language values. Order matters — these
# represent how Chrome serialises the language preference when the user has
# multiple installed.
_ACCEPT_LANGS: list[str] = [
    "en-IN,en;q=0.9",
    "en-IN,en-US;q=0.9,en;q=0.8",
    "en-IN,hi;q=0.8,en;q=0.7",
    "en-US,en;q=0.9,en-IN;q=0.8",
]


def is_disabled() -> bool:
    """True when the operator has pinned the legacy single-profile path."""
    return os.getenv("DISABLE_FINGERPRINT_ROTATION", "").lower() in ("1", "true", "yes")


# ─── Public API ─────────────────────────────────────────────────────────────


def pick_profile(default: str = "chrome124") -> str:
    """
    Return a random curl_cffi `impersonate` profile name.
    Returns `default` when rotation is disabled.
    """
    if is_disabled():
        return default
    return random.choice(_PROFILES)


def pick_headers(base: Optional[dict] = None) -> dict:
    """
    Return a fresh headers dict suitable for `curl_requests.get(..., headers=)`.

    `base` is an optional dict whose keys we copy through unchanged EXCEPT
    `Accept-Language`, which we always overwrite with a random pick. This
    lets callers keep their existing `Sec-Ch-Ua-*` set on the dict — those
    are kept consistent because the profile pool stays Windows-only.
    """
    headers = dict(base) if base else {}
    if is_disabled():
        # Keep whatever the caller passed.
        return headers
    headers["Accept-Language"] = random.choice(_ACCEPT_LANGS)
    return headers


def profile_count() -> int:
    """Number of profiles in the rotation pool — exposed for /health stats."""
    return 1 if is_disabled() else len(_PROFILES)
