import json
import os
import urllib.error
import urllib.parse
import urllib.request
import re
import uuid
import logging
import time
import boto3
import html

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs", region_name="us-east-1")
secretsmanager = boto3.client("secretsmanager", region_name="us-east-1")
QUEUE_URL = os.environ.get("RAW_QUEUE_URL")
JSEARCH_SECRET_NAME = os.environ.get("JSEARCH_SECRET_NAME", "jobserch/jsearch")
JSEARCH_ENABLED = os.environ.get("JSEARCH_ENABLED", "true").strip().lower() == "true"
JSEARCH_MAX_PER_QUERY = int(os.environ.get("JSEARCH_MAX_PER_QUERY", "10"))
JSEARCH_QUERIES = [
    q.strip()
    for q in os.environ.get(
        "JSEARCH_QUERIES",
        "AI Engineer San Francisco",
    ).split(",")
    if q.strip()
]
GOOGLE_CSE_SECRET_NAME = os.environ.get("GOOGLE_CSE_SECRET_NAME", "jobsearch/google_cse")
GOOGLE_CSE_ENABLED = os.environ.get("GOOGLE_CSE_ENABLED", "true").strip().lower() == "true"
GOOGLE_CSE_MAX_RESULTS = int(os.environ.get("GOOGLE_CSE_MAX_RESULTS", "10"))
GOOGLE_CSE_QUERIES = [
    q.strip()
    for q in os.environ.get(
        "GOOGLE_CSE_QUERIES",
        '("AI Engineer" OR "LLM Engineer" OR "RAG Engineer") ("San Francisco" OR "Bay Area") '
        '(site:greenhouse.io OR site:lever.co OR site:ashbyhq.com OR site:workdayjobs.com OR site:jobs.ashbyhq.com)',
    ).split(";;")
    if q.strip()
]
ADZUNA_SECRET_NAME = os.environ.get("ADZUNA_SECRET_NAME", "adzuna/jobsearch")
ADZUNA_ENABLED = os.environ.get("ADZUNA_ENABLED", "true").strip().lower() == "true"
ADZUNA_MAX_RESULTS = int(os.environ.get("ADZUNA_MAX_RESULTS", "20"))
ADZUNA_QUERIES = [
    q.strip()
    for q in os.environ.get("ADZUNA_QUERIES", "AI Engineer;;LLM Engineer;;Machine Learning Engineer").split(";;")
    if q.strip()
]
ADZUNA_WHERE = os.environ.get("ADZUNA_WHERE", "San Francisco")
ADZUNA_MAX_DAYS_OLD = int(os.environ.get("ADZUNA_MAX_DAYS_OLD", "14"))
HN_MAX_COMMENTS = int(os.environ.get("HN_MAX_COMMENTS", "800"))

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
URL_REGEX = re.compile(r'https?://[^\s"<>]+')
ROLE_PATTERN = re.compile(
    r"\b((senior|staff|principal|lead|founding)\s+)?"
    r"(ai|ml|llm|rag|nlp|machine learning|data|research)\s+"
    r"(engineer|scientist|researcher|architect|developer)\b",
    re.IGNORECASE,
)
ROLE_WORD_PATTERN = re.compile(
    r"\b(engineer|scientist|researcher|architect|developer|applied scientist)\b",
    re.IGNORECASE,
)
LOCATION_PATTERN = re.compile(
    r"\b(remote|hybrid|onsite|on-site|san francisco|sf|bay area|south san francisco|"
    r"palo alto|mountain view|san mateo|san jose|sunnyvale|new york|nyc|austin|seattle|"
    r"boston|california|ca|us|usa)\b",
    re.IGNORECASE,
)
NON_ROLE_SEGMENTS = re.compile(
    r"\b(yc\s*[swf]?\d{2}|seed|series\s+[abc]|venture|funded|salary|visa|h1b)\b",
    re.IGNORECASE,
)

_cached_jsearch_key = None
_cached_secret_payloads: dict[str, dict] = {}
SOURCE_ERRORS: list[dict[str, str]] = []


def _record_source_error(source: str, detail: str) -> None:
    SOURCE_ERRORS.append({"source": source, "detail": detail[:300]})

def _get_secret_payload(secret_name: str) -> dict:
    if not secret_name:
        return {}
    if secret_name in _cached_secret_payloads:
        return _cached_secret_payloads[secret_name]
    try:
        raw = secretsmanager.get_secret_value(SecretId=secret_name)["SecretString"]
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            payload = {}
        payload = {str(key).strip(): value for key, value in payload.items()}
    except Exception as e:
        logger.warning("SECRET_READ_SKIP | secret=%s | error=%s", secret_name, str(e))
        _record_source_error(f"secret:{secret_name}", str(e))
        payload = {}
    _cached_secret_payloads[secret_name] = payload
    return payload


def _extract_email_from_description(description: str) -> str:
    if not description: return ""
    matches = EMAIL_REGEX.findall(description)
    skip_patterns = {"noreply", "no-reply", "donotreply", "support@", "info@", "jobs@", "careers@", "recruiting@", "hiring@", "apply@", "hr@"}
    for email in matches:
        if not any(p in email.lower() for p in skip_patterns):
            return email
    return ""

def _clean_hn_text(raw_text: str) -> str:
    raw_text = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", raw_text or "")
    raw_text = re.sub(r"(?i)</\s*(p|div|li)\s*>", "\n", raw_text)
    text = re.sub(r"<[^>]+>", " ", raw_text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def _extract_urls(raw_text: str) -> list[str]:
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', raw_text or "", re.IGNORECASE)
    direct = URL_REGEX.findall(html.unescape(raw_text or ""))
    urls = []
    for url in hrefs + direct:
        clean = html.unescape(url).strip().rstrip(").,")
        if clean.startswith("http") and clean not in urls:
            urls.append(clean)
    return urls[:5]


def _looks_like_role(segment: str) -> bool:
    s = segment.strip()
    if not s or NON_ROLE_SEGMENTS.search(s):
        return False
    return bool(ROLE_WORD_PATTERN.search(s) and re.search(r"\b(ai|ml|llm|rag|machine learning|data|research)\b", s, re.IGNORECASE))


def _looks_like_location(segment: str) -> bool:
    return bool(LOCATION_PATTERN.search(segment or ""))


def _normalize_company(segment: str) -> str:
    company = re.sub(r"\bis hiring\b.*", "", segment or "", flags=re.IGNORECASE)
    company = re.sub(r"\((?:YC\s*[SWF]?\d{2}|Series\s+[ABC]|seed[^)]*)\)", "", company, flags=re.IGNORECASE)
    company = re.sub(r"\([^)]*https?://[^)]*\)", "", company, flags=re.IGNORECASE)
    company = re.sub(r"https?://\S+", "", company, flags=re.IGNORECASE)
    company = re.sub(r"\s+", " ", company).strip(" -|")
    return company or "Unknown"


def _clean_role_title(segment: str, full_text: str) -> str:
    title = re.sub(r"\s+", " ", segment or "").strip(" -|")
    if not title or len(title) > 70:
        return _infer_title_from_text(full_text)
    match = ROLE_PATTERN.search(title)
    if match and len(title) > 45:
        return re.sub(r"\s+", " ", match.group(0)).strip()
    return title


def _clean_location_segment(segment: str, company: str = "") -> str:
    location = re.sub(r"\s+", " ", segment or "").strip(" -|")
    if not location:
        return ""

    lowered = location.lower()
    company_lower = (company or "").lower()
    if company_lower and company_lower in lowered[5:]:
        idx = lowered.find(company_lower, 5)
        location = location[:idx].strip(" -|")

    for marker in (" we ", " we're ", " we are ", " i ", " our ", " build", " building", " helps", " is building"):
        idx = location.lower().find(marker)
        if idx > 5:
            location = location[:idx].strip(" -|")
            break

    if len(location) > 70:
        location = location[:70].rsplit(" ", 1)[0].strip(" -|")
    return location or ""


def _infer_title_from_text(text: str) -> str:
    match = ROLE_PATTERN.search(text or "")
    if match:
        return re.sub(r"\s+", " ", match.group(0)).strip()
    if re.search(r"\b(ai|llm|rag|agentic|machine learning)\b", text or "", re.IGNORECASE):
        return "AI Engineer"
    return "Software Engineer"


def _parse_hn_comment(comment: dict) -> dict | None:
    raw_text = comment.get("text", "") or ""
    clean_text = _clean_hn_text(raw_text)
    if not re.search(r"\b(AI|ML|LLM|Machine Learning|Generative|RAG|NLP|agentic)\b", clean_text, re.IGNORECASE):
        return None

    lines = [line.strip() for line in clean_text.split("\n") if line.strip()]
    header = lines[0] if lines else ""
    parts = [p.strip() for p in header.split("|") if p.strip()]

    company = _normalize_company(parts[0] if parts else "Unknown")
    title = next((p for p in parts[1:] if _looks_like_role(p)), "")
    if not title:
        title = _infer_title_from_text(clean_text)
    else:
        title = _clean_role_title(title, clean_text)

    location = next((p for p in parts[1:] if _looks_like_location(p)), "")
    if not location:
        location = "Remote" if re.search(r"\bremote\b", clean_text, re.IGNORECASE) else "Not specified"
    else:
        location = _clean_location_segment(location, company)

    urls = _extract_urls(raw_text)
    kid_id = comment.get("id")
    source_url = f"https://news.ycombinator.com/item?id={kid_id}"
    email = _extract_email_from_description(clean_text)

    return {
        "job_id": f"hn_{kid_id}",
        "title": title[:100],
        "company": company[:100],
        "location": location[:120],
        "description": clean_text,
        "url": source_url,
        "apply_url": source_url,
        "source": "hackernews",
        "source_provider": "Hacker News Firebase API",
        "source_origin": "Ask HN: Who is hiring?",
        "source_comment_id": str(kid_id),
        "external_links": urls,
        "salary": "",
        "is_remote": "remote" in location.lower() and "onsite" not in location.lower() and "on-site" not in location.lower(),
        "employment_type": "FULL_TIME",
        "posted_at": str(comment.get("time", "")),
        "employer_website": urls[0] if urls else "",
        "recruiter_email": email,
        "recruiter_name": "",
    }


def fetch_hacker_news_jobs() -> list:
    logger.info("Fetching Hacker News thread...")
    try:
        user_url = 'https://hacker-news.firebaseio.com/v0/user/whoishiring.json'
        req = urllib.request.Request(user_url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            user_data = json.loads(resp.read().decode())
        
        recent = user_data.get('submitted', [])[:5]
        target_id = None
        for item_id in recent:
            item_url = f'https://hacker-news.firebaseio.com/v0/item/{item_id}.json'
            req = urllib.request.Request(item_url)
            with urllib.request.urlopen(req, timeout=5) as resp:
                item_data = json.loads(resp.read().decode())
                if item_data and "Who is hiring" in item_data.get("title", ""):
                    target_id = item_data.get("id")
                    break
        
        if not target_id:
            logger.error("No HN thread found.")
            return []
            
        thread_url = f'https://hacker-news.firebaseio.com/v0/item/{target_id}.json'
        req2 = urllib.request.Request(thread_url)
        with urllib.request.urlopen(req2, timeout=5) as resp:
            thread_data = json.loads(resp.read().decode())
        
        kids = thread_data.get('kids', [])
        logger.info("HN Thread found. Checking top %d comments for AI roles...", HN_MAX_COMMENTS)
        
        hn_jobs = []
        for kid_id in kids[:HN_MAX_COMMENTS]:
            try:
                comment_url = f'https://hacker-news.firebaseio.com/v0/item/{kid_id}.json'
                req3 = urllib.request.Request(comment_url)
                with urllib.request.urlopen(req3, timeout=3) as resp:
                    comment = json.loads(resp.read().decode())
                    parsed = _parse_hn_comment(comment)
                    if parsed:
                        hn_jobs.append(parsed)
            except Exception as e:
                logger.warning("HN_COMMENT_SKIP | kid_id=%s | error=%s", kid_id, str(e))
                
        return hn_jobs
    except Exception as e:
        logger.error("HN_ERROR: %s", str(e))
        return []


def _get_jsearch_key() -> str:
    global _cached_jsearch_key
    if _cached_jsearch_key is not None:
        return _cached_jsearch_key

    env_key = os.environ.get("JSEARCH_API_KEY") or os.environ.get("RAPIDAPI_KEY")
    if env_key:
        _cached_jsearch_key = env_key
        return _cached_jsearch_key

    payload = _get_secret_payload(JSEARCH_SECRET_NAME)
    _cached_jsearch_key = (
        payload.get("JSEARCH_API_KEY")
        or payload.get("api_key")
        or payload.get("RAPIDAPI_KEY")
        or payload.get("key")
        or ""
    )
    return _cached_jsearch_key


def _normalize_jsearch_job(raw: dict, query: str) -> dict | None:
    title = (raw.get("job_title") or "").strip()
    company = (raw.get("employer_name") or "").strip()
    apply_link = (raw.get("job_apply_link") or raw.get("job_google_link") or "").strip()
    if not title or not company or not apply_link:
        return None

    city = (raw.get("job_city") or "").strip()
    state = (raw.get("job_state") or "").strip()
    country = (raw.get("job_country") or "").strip()
    location = ", ".join([part for part in (city, state or country) if part]) or ("Remote" if raw.get("job_is_remote") else "Not specified")

    return {
        "job_id": raw.get("job_id") or f"jsearch_{uuid.uuid4().hex}",
        "title": title[:120],
        "company": company[:120],
        "location": location[:120],
        "description": raw.get("job_description") or "",
        "url": apply_link,
        "apply_url": apply_link,
        "source": "jsearch",
        "source_provider": "JSearch RapidAPI",
        "source_origin": raw.get("job_publisher") or "JSearch",
        "source_search_query": query,
        "source_google_link": raw.get("job_google_link") or "",
        "salary": raw.get("job_salary") or "",
        "is_remote": bool(raw.get("job_is_remote")),
        "employment_type": raw.get("job_employment_type") or "",
        "posted_at": raw.get("job_posted_at_datetime_utc") or "",
        "employer_website": raw.get("employer_website") or "",
        "recruiter_email": "",
        "recruiter_name": "",
    }


def fetch_jsearch_jobs() -> list:
    if not JSEARCH_ENABLED:
        logger.info("JSEARCH_DISABLED")
        return []

    api_key = _get_jsearch_key()
    if not api_key:
        logger.info("JSEARCH_SKIP: no API key configured")
        return []

    jobs = []
    for query in JSEARCH_QUERIES:
        url = (
            "https://jsearch.p.rapidapi.com/search"
            f"?query={urllib.request.quote(query)}"
            "&num_pages=1&date_posted=week&country=us"
        )
        req = urllib.request.Request(
            url,
            headers={
                "X-RapidAPI-Key": api_key,
                "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                data = json.loads(response.read().decode("utf-8"))
            raw_jobs = data.get("data", [])[:JSEARCH_MAX_PER_QUERY]
            for raw in raw_jobs:
                job = _normalize_jsearch_job(raw, query)
                if job and re.search(r"\b(ai|ml|llm|rag|machine learning|nlp|agent)\b", f"{job['title']} {job['description']}", re.IGNORECASE):
                    jobs.append(job)
            logger.info("JSEARCH_QUERY | query=%s | raw=%d | accepted_so_far=%d", query, len(raw_jobs), len(jobs))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                logger.warning("JSEARCH_RATE_LIMITED | query=%s", query)
                _record_source_error("jsearch", f"HTTP 429 rate limited for query={query}")
                break
            logger.warning("JSEARCH_QUERY_ERROR | query=%s | error=%s", query, str(e))
            _record_source_error("jsearch", f"HTTP {e.code} for query={query}")
        except Exception as e:
            logger.warning("JSEARCH_QUERY_ERROR | query=%s | error=%s", query, str(e))
            _record_source_error("jsearch", f"{type(e).__name__}: {e}")

    return jobs


def _get_google_cse_credentials() -> tuple[str, str]:
    payload = _get_secret_payload(GOOGLE_CSE_SECRET_NAME)
    api_key = payload.get("GOOGLE_API_KEY") or payload.get("api_key") or payload.get("key") or ""
    cx = payload.get("GOOGLE_CX") or payload.get("cx") or payload.get("search_engine_id") or ""
    return api_key, cx


def _normalize_google_cse_item(item: dict, query: str) -> dict | None:
    title = re.sub(r"\s+", " ", (item.get("title") or "")).strip()
    link = (item.get("link") or "").strip()
    snippet = re.sub(r"\s+", " ", (item.get("snippet") or "")).strip()
    display_link = item.get("displayLink") or ""
    if not title or not link:
        return None

    title_without_company = re.split(r"\s[-|]\s", title)[0].strip()
    inferred_title = _infer_title_from_text(f"{title_without_company} {snippet}")
    company = display_link.split(".")[0].replace("jobs", "").replace("boards", "").strip("- ") or "Unknown"

    return {
        "job_id": f"google_cse_{uuid.uuid5(uuid.NAMESPACE_URL, link).hex}",
        "title": title_without_company[:120] if ROLE_WORD_PATTERN.search(title_without_company) else inferred_title[:120],
        "company": company[:120],
        "location": "San Francisco Bay Area",
        "description": f"{title}\n\n{snippet}",
        "url": link,
        "apply_url": link,
        "source": "google_cse",
        "source_provider": "Google Custom Search JSON API",
        "source_origin": display_link,
        "source_search_query": query,
        "salary": "",
        "is_remote": "remote" in f"{title} {snippet}".lower(),
        "employment_type": "",
        "posted_at": "",
        "employer_website": f"https://{display_link}" if display_link else "",
        "recruiter_email": "",
        "recruiter_name": "",
    }


def fetch_google_cse_jobs() -> list:
    if not GOOGLE_CSE_ENABLED:
        logger.info("GOOGLE_CSE_DISABLED")
        return []

    api_key, cx = _get_google_cse_credentials()
    if not api_key or not cx:
        logger.info("GOOGLE_CSE_SKIP: missing key or cx")
        return []

    jobs = []
    for query in GOOGLE_CSE_QUERIES:
        params = urllib.parse.urlencode({
            "key": api_key,
            "cx": cx,
            "q": query,
            "num": min(10, GOOGLE_CSE_MAX_RESULTS),
            "dateRestrict": "m1",
        })
        req = urllib.request.Request(f"https://customsearch.googleapis.com/customsearch/v1?{params}")
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                data = json.loads(response.read().decode("utf-8"))
            raw_items = data.get("items", [])[:GOOGLE_CSE_MAX_RESULTS]
            for item in raw_items:
                job = _normalize_google_cse_item(item, query)
                if job and re.search(r"\b(ai|ml|llm|rag|machine learning|nlp|agent)\b", f"{job['title']} {job['description']}", re.IGNORECASE):
                    jobs.append(job)
            logger.info("GOOGLE_CSE_QUERY | raw=%d | accepted_so_far=%d", len(raw_items), len(jobs))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")[:200]
            logger.warning("GOOGLE_CSE_QUERY_ERROR | status=%s | error=%s | body=%s", e.code, str(e), body)
            _record_source_error("google_cse", f"HTTP {e.code}: {body}")
        except Exception as e:
            logger.warning("GOOGLE_CSE_QUERY_ERROR | error=%s", str(e))
            _record_source_error("google_cse", f"{type(e).__name__}: {e}")
    return jobs


def _get_adzuna_credentials() -> tuple[str, str]:
    payload = _get_secret_payload(ADZUNA_SECRET_NAME)
    app_id = payload.get("app_id") or payload.get("ADZUNA_APP_ID") or payload.get("APP_ID") or ""
    app_key = payload.get("app_key") or payload.get("ADZUNA_APP_KEY") or payload.get("APP_KEY") or payload.get("api_key") or ""
    return app_id, app_key


def _normalize_adzuna_job(raw: dict) -> dict | None:
    title = (raw.get("title") or "").strip()
    company = (raw.get("company") or {}).get("display_name", "").strip()
    source_url = (raw.get("redirect_url") or "").strip()
    if not title or not company or not source_url:
        return None

    location = (raw.get("location") or {}).get("display_name", "") or "Not specified"
    salary_min = raw.get("salary_min")
    salary_max = raw.get("salary_max")
    salary = ""
    if salary_min or salary_max:
        salary = f"{salary_min or '?'} - {salary_max or '?'}"

    return {
        "job_id": f"adzuna_{raw.get('id') or uuid.uuid5(uuid.NAMESPACE_URL, source_url).hex}",
        "title": title[:120],
        "company": company[:120],
        "location": location[:120],
        "description": raw.get("description") or "",
        "url": source_url,
        "apply_url": source_url,
        "source": "adzuna",
        "source_provider": "Adzuna Jobs API",
        "source_origin": raw.get("adref") or "Adzuna",
        "salary": salary,
        "is_remote": "remote" in f"{title} {location} {raw.get('description', '')}".lower(),
        "employment_type": raw.get("contract_time") or "",
        "posted_at": raw.get("created") or "",
        "employer_website": "",
        "recruiter_email": "",
        "recruiter_name": "",
    }


def fetch_adzuna_jobs() -> list:
    if not ADZUNA_ENABLED:
        logger.info("ADZUNA_DISABLED")
        return []

    app_id, app_key = _get_adzuna_credentials()
    if not app_id or not app_key:
        logger.info("ADZUNA_SKIP: missing app_id or app_key")
        return []

    jobs = []
    per_query = max(1, min(50, ADZUNA_MAX_RESULTS))
    for query in ADZUNA_QUERIES:
        params = urllib.parse.urlencode({
            "app_id": app_id,
            "app_key": app_key,
            "results_per_page": per_query,
            "what": query,
            "where": ADZUNA_WHERE,
            "max_days_old": ADZUNA_MAX_DAYS_OLD,
            "content-type": "application/json",
        })
        url = f"https://api.adzuna.com/v1/api/jobs/us/search/1?{params}"
        try:
            with urllib.request.urlopen(urllib.request.Request(url), timeout=15) as response:
                data = json.loads(response.read().decode("utf-8"))
            accepted_before = len(jobs)
            for raw in data.get("results", [])[:per_query]:
                job = _normalize_adzuna_job(raw)
                if job and re.search(r"\b(ai|ml|llm|rag|machine learning|nlp|agent)\b", f"{job['title']} {job['description']}", re.IGNORECASE):
                    job["source_search_query"] = query
                    jobs.append(job)
            logger.info(
                "ADZUNA_QUERY | query=%s | raw=%d | accepted=%d | total=%d",
                query, len(data.get("results", [])), len(jobs) - accepted_before, len(jobs),
            )
        except urllib.error.HTTPError as e:
            logger.warning("ADZUNA_QUERY_ERROR | query=%s | status=%s | error=%s", query, e.code, str(e))
            _record_source_error("adzuna", f"HTTP {e.code} for query={query}")
        except Exception as e:
            logger.warning("ADZUNA_QUERY_ERROR | query=%s | error=%s", query, str(e))
            _record_source_error("adzuna", f"{type(e).__name__}: {e}")
    return jobs


def _dedupe_batch(jobs: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for job in jobs:
        key = job.get("job_id") or job.get("url") or f"{job.get('company')}|{job.get('title')}"
        if key in seen:
            continue
        seen.add(key)
        unique.append(job)
    return unique

def _send_to_sqs(jobs: list) -> int:
    sent = 0
    batch = []
    for job in jobs:
        safe_id = str(uuid.uuid4()).replace("-", "")
        batch.append({"Id": safe_id, "MessageBody": json.dumps(job)})
        if len(batch) == 10:
            try:
                result = sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=batch)
                sent += len(result.get("Successful", []))
            except Exception as e:
                logger.error("SQS_BATCH_ERROR: %s", str(e))
            batch = []
    if batch:
        try:
            result = sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=batch)
            sent += len(result.get("Successful", []))
        except Exception as e:
            logger.error("SQS_BATCH_ERROR: %s", str(e))
    return sent

def lambda_handler(event, context):
    global SOURCE_ERRORS
    SOURCE_ERRORS = []
    logger.info("INGESTOR_START | Using HN + Google CSE + Adzuna + JSearch")
    hn_jobs = fetch_hacker_news_jobs()
    google_cse_jobs = fetch_google_cse_jobs()
    adzuna_jobs = fetch_adzuna_jobs()
    jsearch_jobs = fetch_jsearch_jobs()
    all_jobs = _dedupe_batch(hn_jobs + google_cse_jobs + adzuna_jobs + jsearch_jobs)
    
    if not all_jobs:
        return {"status": "success", "message": "No jobs found.", "jobs_ingested": 0}
        
    sent_count = _send_to_sqs(all_jobs)
    logger.info(
        "INGESTOR_COMPLETE | hn=%d | google_cse=%d | adzuna=%d | jsearch=%d | unique=%d | queued=%d",
        len(hn_jobs), len(google_cse_jobs), len(adzuna_jobs), len(jsearch_jobs), len(all_jobs), sent_count,
    )
    return {
        "status": "success",
        "jobs_fetched": len(all_jobs),
        "jobs_queued": sent_count,
        "source_errors": SOURCE_ERRORS[:10],
        "sources": {
            "hackernews": len(hn_jobs),
            "google_cse": len(google_cse_jobs),
            "adzuna": len(adzuna_jobs),
            "jsearch": len(jsearch_jobs),
        },
    }
