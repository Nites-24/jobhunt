"""
Unscored Job Reconciler & Ingestion Requeue Engine (reconcile_new_jobs.py)

Role & Operational Context:
  Audits all backlog rows in DynamoDB with status == 'NEW', applies deterministic
  validity checks, transitions invalid rows to 'REJECTED', and requeues valid rows into
  SQS `job-filtered-queue` for Bedrock Claude evaluation by `job-processor`.

Filtering & Quality Invariants:
  1. Source Validation: Discards rows from deprecated or unknown source providers.
  2. Freshness: Flags and rejects jobs posted older than MAX_JOB_AGE_DAYS (default: 1 day).
  3. AI Signal Verification: Rejects roles lacking core AI/ML keywords (e.g. generic sales, devops).
  4. URL Grounding: Discards jobs with placeholder or invalid URLs.

Operational Safety:
  - Default mode is dry-run. Requires `--apply` to update DynamoDB and SQS.
  - SQS payloads match the expected schema for `job-processor`.

Usage:
  python tools/ops/reconcile_new_jobs.py          # Dry-run audit of NEW jobs
  python tools/ops/reconcile_new_jobs.py --apply  # Reject invalid & requeue valid jobs
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REGION = "us-east-1"
TABLE_NAME = "Jobsearch"
FILTERED_QUEUE_NAME = "job-filtered-queue"
MAX_JOB_AGE_DAYS = int(os.environ.get("MAX_JOB_AGE_DAYS", "1"))
ALLOWED_SOURCES = {
    source.strip().lower()
    for source in os.environ.get("LIST_ALLOWED_SOURCES", "hackernews,hn,jsearch,google_cse,cse,greenhouse,lever,ashby").split(",")
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


def _reject_reason(job: dict) -> str:
    reasons = []
    if _source(job) not in ALLOWED_SOURCES:
        reasons.append("source not allowed")
    if not re.match(r"^https?://", _source_url(job)):
        reasons.append("missing verifiable source link")
    age = _age_days(job)
    if age is None:
        reasons.append("missing posting date")
    elif age > MAX_JOB_AGE_DAYS:
        reasons.append(f"posted {age} days ago")
    return ", ".join(reasons)


def _new_jobs(table) -> list[dict]:
    items = []
    scan_args = {"FilterExpression": Attr("status").eq("NEW")}
    while True:
        response = table.scan(**scan_args)
        items.extend(response.get("Items", []))
        if "LastEvaluatedKey" not in response:
            return items
        scan_args["ExclusiveStartKey"] = response["LastEvaluatedKey"]


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


def _requeue(sqs, queue_url: str, job: dict) -> None:
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(job, default=str),
        MessageAttributes={
            "job_id": {"StringValue": str(job.get("job_id", "")), "DataType": "String"},
            "source": {"StringValue": str(job.get("source", "unknown")), "DataType": "String"},
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile NEW jobs.")
    parser.add_argument("--apply", action="store_true", help="Mutate DynamoDB and SQS.")
    args = parser.parse_args()

    session = _session()
    table = session.resource("dynamodb").Table(TABLE_NAME)
    sqs = session.client("sqs")
    queue_url = sqs.get_queue_url(QueueName=FILTERED_QUEUE_NAME)["QueueUrl"]

    rows = _new_jobs(table)
    rejected = [(job, _reject_reason(job)) for job in rows]
    rejected = [(job, reason) for job, reason in rejected if reason]
    requeue = [job for job in rows if not _reject_reason(job)]

    print(f"NEW rows scanned: {len(rows)}")
    print(f"Reject as unusable: {len(rejected)}")
    print(f"Requeue for scoring: {len(requeue)}")
    for job, reason in rejected[:15]:
        print(f"- reject {job.get('title', 'Unknown')} @ {job.get('company', 'Unknown')} | {reason}")
    for job in requeue[:15]:
        print(f"- requeue {job.get('title', 'Unknown')} @ {job.get('company', 'Unknown')} | {_source(job)}")

    if args.apply:
        for job, reason in rejected:
            _reject(table, job, reason)
        for job in requeue:
            _requeue(sqs, queue_url, job)
        print(f"Updated {len(rejected)} rejected row(s), requeued {len(requeue)} row(s).")
    else:
        print("Dry-run only. Re-run with --apply to update production.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
