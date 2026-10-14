import os
import boto3
import json

if os.path.exists(".env"):
    with open(".env") as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

import sys
sys.path.insert(0, ".")
import job_router.lambda_function as router

print(f"SCORE_THRESHOLD: {router.SCORE_THRESHOLD}")
print(f"MAX_JOB_AGE_DAYS: {router.MAX_JOB_AGE_DAYS}")
print(f"HN_MAX_JOB_AGE_DAYS: {router.HN_MAX_JOB_AGE_DAYS}")
print(f"LIST_ALLOWED_SOURCES: {router.LIST_ALLOWED_SOURCES}")

pending = router._get_pending_jobs(limit=10)
print(f"\nActive reviewable pending jobs count: {len(pending)}")
for p in pending:
    print(f"  - [{p.get('source')}] {p.get('company')} - {p.get('title')} (Score: {p.get('match_score')}, Posted: {p.get('posted_at')})")

scored = router._get_scored_pending_jobs(limit=10)
print(f"\nScored pending jobs count (backlog): {len(scored)}")
for s in scored:
    print(f"  - [{s.get('source')}] {s.get('company')} - {s.get('title')} (Score: {s.get('match_score')}, Posted: {s.get('posted_at')})")

# Check today's jobs specifically
OCT_12_START = 1791781200
table = router.table
resp = table.scan(
    FilterExpression="evaluated_at >= :t",
    ExpressionAttributeValues={":t": OCT_12_START}
)
today_items = resp.get("Items", [])
print(f"\nTotal items evaluated today: {len(today_items)}")
for it in today_items:
    if it.get("status") == "PENDING_APPROVAL":
        age = router._job_age_days(it)
        hidden = router._hidden_pending_reason(it)
        is_rev = router._is_reviewable_job(it)
        print(f"\n[{it.get('company')} - {it.get('title')}]")
        print(f"  Score: {it.get('match_score')} | Source: {it.get('source')}")
        print(f"  Posted At: {it.get('posted_at')}")
        print(f"  Job Age Days: {age}")
        print(f"  Is Reviewable: {is_rev}")
        print(f"  Hidden Reason: {hidden}")

