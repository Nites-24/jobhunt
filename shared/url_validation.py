"""
Canonical URL validation utilities for job search pipeline.

Enforces strict source URL requirements so that unverifiable leads, placeholder
values (e.g. 'Unavailable', 'N/A'), malformed links, or internal addresses
never enter the review, draft, or application pipelines.
"""

from typing import Any
import urllib.parse

BLOCKED_DOMAINS = {
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "169.254.169.254",
}

BLOCKED_SUFFIXES = (
    ".local",
    ".internal",
    ".arpa",
    ".localhost",
    ".lan",
    ".home",
    ".corp",
)

INVALID_LITERALS = {
    "unavailable",
    "none",
    "null",
    "n/a",
    "undefined",
    "no link",
    "unknown",
    "missing",
    "false",
    "true",
    "",
}


def is_valid_canonical_url(url: Any) -> bool:
    """Validate that a URL is a well-formed, non-internal HTTP/HTTPS web address."""
    if not url or not isinstance(url, str):
        return False

    raw = url.strip()
    if not raw or raw.lower() in INVALID_LITERALS:
        return False

    if not (raw.startswith("http://") or raw.startswith("https://")):
        return False

    try:
        parsed = urllib.parse.urlparse(raw)
    except Exception:
        return False

    if parsed.scheme.lower() not in ("http", "https"):
        return False

    netloc = (parsed.netloc or "").strip().lower()
    if not netloc:
        return False

    hostname = netloc.split(":")[0].strip()
    if not hostname:
        return False

    if hostname in BLOCKED_DOMAINS or hostname.endswith(BLOCKED_SUFFIXES):
        return False

    if "." not in hostname:
        return False

    return True


def get_canonical_job_url(job: dict) -> str:
    """Extract and validate the canonical source or application URL from a job dict.

    Returns the valid URL string, or empty string if invalid or missing.
    """
    if not isinstance(job, dict):
        return ""

    for field in ("url", "apply_url"):
        candidate = str(job.get(field) or "").strip()
        if is_valid_canonical_url(candidate):
            return candidate

    return ""
