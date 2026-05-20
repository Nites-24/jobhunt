"""Clean old pending rows that no longer meet the production review policy.

Default mode is dry-run. Use --apply to update DynamoDB.
"""

import argparse
import os
import re
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REGION = "us-east-1"
TABLE_NAME = "Jobsearch"
SCORE_THRESHOLD = int(os.environ.get("SCORE_THRESHOLD", "80"))
MAX_JOB_AGE_DAYS = int(os.environ.get("MAX_JOB_AGE_DAYS", "14"))
ALLOWED_SOURCES = {
    source.strip().lower()
    for source in os.environ.get("LIST_ALLOWED_SOURCES", "hackernews,hn,jsearch,google_cse,cse,adzuna,tejajob").split(",")
    if source.strip()
}


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


def _as_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _source(job: dict) -> str:
    return str(job.get("source") or job.get("source_provider") or "").strip().lower()


def _source_url(job: dict) -> str:
    return str(job.get("url") or job.get("apply_url") or "").strip()


def _parse_posted_at(value) -> datetime | None:
    if value in (None, ""):
        return None
    raw = str(value).strip()
    try:
        if raw.isdigit():
            return datetime.fromtimestamp(int(raw), tz=timezone.utc)
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _age_days(job: dict) -> int | None:
    posted_at = _parse_posted_at(job.get("posted_at"))
    if posted_at is None:
        return None
    return int(max(0, (datetime.now(timezone.utc) - posted_at).total_seconds()) // 86400)


def _reason_to_hide(job: dict) -> str:
    reasons = []
    if _as_int(job.get("match_score")) < SCORE_THRESHOLD:
        reasons.append(f"below {SCORE_THRESHOLD}% review threshold")
    if _source(job) not in ALLOWED_SOURCES:
        reasons.append("source not allowed")
    source_url = _source_url(job)
    if not re.match(r"^https?://", source_url):
        reasons.append("missing verifiable source link")
    age = _age_days(job)
    if age is None:
        reasons.append("missing posting date")
    elif age > MAX_JOB_AGE_DAYS:
        reasons.append(f"posted {age} days ago")
    return ", ".join(reasons)


def _pending_jobs(table) -> list[dict]:
    jobs = []
    query_args = {
        "IndexName": "status-index",
        "KeyConditionExpression": Key("status").eq("PENDING_APPROVAL"),
    }
    while True:
        response = table.query(**query_args)
        jobs.extend(response.get("Items", []))
        if "LastEvaluatedKey" not in response:
            return jobs
        query_args["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def _reject(table, job: dict, reason: str) -> None:
    table.update_item(
        Key={"job_id": job["job_id"]},
        UpdateExpression=(
            "SET #s = :status, review_reconciled_at = :now, "
            "review_reconcile_reason = :reason"
        ),
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":status": "REJECTED",
            ":now": Decimal(str(int(time.time()))),
            ":reason": reason,
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile stale/invalid pending jobs.")
    parser.add_argument("--apply", action="store_true", help="Update DynamoDB.")
    args = parser.parse_args()

    table = _session().resource("dynamodb").Table(TABLE_NAME)
    pending = _pending_jobs(table)
    to_reject = [(job, _reason_to_hide(job)) for job in pending]
    to_reject = [(job, reason) for job, reason in to_reject if reason]

    print(f"Pending rows scanned: {len(pending)}")
    print(f"Rows that no longer meet review policy: {len(to_reject)}")
    for job, reason in to_reject[:20]:
        print(
            f"- [{job.get('match_score', '?')}%] {job.get('title', 'Unknown')} @ "
            f"{job.get('company', 'Unknown')} | {reason}"
        )

    if args.apply:
        for job, reason in to_reject:
            _reject(table, job, reason)
        print(f"Updated {len(to_reject)} row(s) to REJECTED.")
    else:
        print("Dry-run only. Re-run with --apply to update DynamoDB.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
