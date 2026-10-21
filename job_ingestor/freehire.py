"""Opt-in FreeHire read-only source for the existing JobHunt ingestion pipeline.

Source: https://freehire.me/docs/api
Only public API calls; no account, application submission, or credentials.
"""
from __future__ import annotations

import hashlib
import json
import logging
import urllib.parse
import urllib.request

log = logging.getLogger(__name__)
BASE = "https://freehire.me/api/v1/jobs/search"


def normalize(raw: dict) -> dict | None:
    if not isinstance(raw, dict):
        return None
    title = str(raw.get("title") or "").strip()
    company = str(raw.get("company") or "").strip()
    url = str(raw.get("url") or "").strip()
    countries = raw.get("countries") or []
    # No guessing: discard explicit non-US jobs, retain ambiguous only when country is absent.
    if countries and "US" not in countries:
        return None
    if not title or not company or not url.startswith("https://"):
        return None
    if not any(token in title.lower() for token in (
        "ai", "machine learning", "ml ", "llm", "applied scientist",
        "research engineer", "inference", "mlops",
    )):
        return None
    slug = str(raw.get("public_slug") or "")
    stable = slug or hashlib.sha256(url.encode()).hexdigest()
    mode = str(raw.get("work_mode") or "").lower()
    return {
        "job_id": "freehire_" + stable,
        "title": title[:120], "company": company[:120],
        "location": str(raw.get("location") or "Not specified")[:120],
        "description": str(raw.get("description") or "")[:30000],
        "url": url, "apply_url": url, "source": "freehire",
        "source_provider": str(raw.get("source") or "FreeHire API"),
        "source_origin": slug,
        "source_job_id": str(raw.get("external_id") or slug),
        "source_search_query": "category:ai_engineering;countries:US",
        "work_mode": mode, "is_remote": mode == "remote",
        "work_mode_source": "FreeHire API",
        "posted_at": str(raw.get("posted_at") or ""),
        "freehire_first_seen_at": str(raw.get("created_at") or ""),
        "employment_type": str((raw.get("enrichment") or {}).get("employment_type") or ""),
        "recruiter_email": "", "recruiter_name": "",
    }


def fetch_jobs(*, max_jobs: int = 100, opener=None) -> list[dict]:
    """Bounded request; fail closed on ignored filters, errors or malformed response."""
    max_jobs = max(1, min(int(max_jobs), 100))
    params = {
        "countries": "US", "category": "ai_engineering",
        "open_within_days": "7", "limit": str(max_jobs), "offset": "0",
        "sort": "posted_at", "order": "desc",
    }
    url = BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "User-Agent": "Nites-24/jobhunt (+https://github.com/Nites-24/jobhunt)",
    })
    client = opener or urllib.request.urlopen
    try:
        with client(req, timeout=15) as response:
            data = json.loads(response.read(2_500_001).decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("FreeHire response not an object")
        meta = data.get("meta") or {}
        if meta.get("ignored_params"):
            raise ValueError("FreeHire ignored filters: " + repr(meta["ignored_params"]))
        rows = data.get("data")
        if not isinstance(rows, list):
            raise ValueError("FreeHire missing job list")
        result = [job for row in rows if (job := normalize(row))]
        log.info("FREEHIRE_FETCH | returned=%d normalized=%d", len(rows), len(result))
        return result
    except Exception as exc:
        log.warning("FREEHIRE_FETCH_FAILED | %s", exc)
        return []  # Other ingestion sources remain available.
