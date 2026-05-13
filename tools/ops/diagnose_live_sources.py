"""Diagnose live source APIs without printing secret values."""

import json
import os
import urllib.error
import urllib.parse
import urllib.request

import boto3


REGION = "us-east-1"
GOOGLE_SECRET = "jobsearch/google_cse"
ADZUNA_SECRET = "adzuna/jobsearch"
GOOGLE_QUERIES = [
    '("AI Engineer" OR "LLM Engineer" OR "RAG Engineer") ("San Francisco" OR "Bay Area") '
    "(site:greenhouse.io OR site:lever.co OR site:ashbyhq.com OR site:workdayjobs.com OR site:jobs.ashbyhq.com)",
    '"AI Engineer" "San Francisco" site:greenhouse.io',
    '"LLM Engineer" "San Francisco" site:jobs.ashbyhq.com',
    "AI Engineer San Francisco",
]


def _load_env() -> None:
    if not os.path.exists(".env"):
        return
    with open(".env", encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())


def _session() -> boto3.Session:
    _load_env()
    return boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=REGION,
    )


def _secret_payload(secrets, name: str) -> dict:
    raw = secrets.get_secret_value(SecretId=name)["SecretString"]
    payload = json.loads(raw)
    return {str(key).strip(): value for key, value in payload.items()} if isinstance(payload, dict) else {}


def _google_credentials(payload: dict) -> tuple[str, str]:
    api_key = payload.get("GOOGLE_API_KEY") or payload.get("api_key") or payload.get("key") or ""
    cx = payload.get("GOOGLE_CX") or payload.get("cx") or payload.get("search_engine_id") or ""
    return str(api_key), str(cx)


def _call_google(api_key: str, cx: str, query: str) -> dict:
    params = urllib.parse.urlencode(
        {
            "key": api_key,
            "cx": cx,
            "q": query,
            "num": 5,
            "dateRestrict": "m1",
        }
    )
    request = urllib.request.Request(f"https://customsearch.googleapis.com/customsearch/v1?{params}")
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    session = _session()
    secrets = session.client("secretsmanager")

    google = _secret_payload(secrets, GOOGLE_SECRET)
    adzuna = _secret_payload(secrets, ADZUNA_SECRET)

    print("Secret shapes")
    print("-------------")
    print(f"{GOOGLE_SECRET}: keys={sorted(google.keys())}")
    print(f"{ADZUNA_SECRET}: keys={sorted(adzuna.keys())}")
    print()

    api_key, cx = _google_credentials(google)
    print("Google CSE direct checks")
    print("------------------------")
    print(f"has_api_key={bool(api_key)} has_cx={bool(cx)}")
    if not api_key or not cx:
        return 0

    for query in GOOGLE_QUERIES:
        try:
            data = _call_google(api_key, cx, query)
            items = data.get("items", [])
            total = data.get("searchInformation", {}).get("totalResults", "0")
            print(f"query={query[:90]}... total={total} returned={len(items)}")
            for item in items[:2]:
                print(f"  - {item.get('title', '')[:80]} | {item.get('link', '')[:120]}")
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="ignore")[:300]
            print(f"query={query[:90]}... HTTP {error.code}: {body}")
        except Exception as error:
            print(f"query={query[:90]}... ERROR: {error}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
