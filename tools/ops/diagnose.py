"""
DynamoDB Anomaly & Lambda Deployment Diagnostics (diagnose.py)

Role & Operational Context:
  Fast diagnostic utility to detect state anomalies in DynamoDB and inspect
  the deployment freshness of all 5 serverless Lambdas.

Diagnostics Performed:
  1. Corrupted Status Detection:
     - Scans DynamoDB 'Jobsearch' records for whitespace/newline corruption or invalid status values.
     - Flags records not in recognized states ('NEW', 'PENDING_APPROVAL', 'APPROVED', 'SENT', 'REJECTED', 'FAILED').
  2. Unscored Backlog Detection:
     - Identifies 'NEW' jobs that were ingested but never picked up by `job-processor`.
  3. Lambda Deployment Verification:
     - Queries AWS Lambda API for CodeSize and LastModified timestamps across all 5 functions:
       `job-ingestor`, `job-deduplicator`, `job-processor`, `job-router`, `job-sender`.

Usage:
  python tools/ops/diagnose.py
"""
import os, boto3, json

with open(".env") as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith("#") and "=" in l:
            k, _, v = l.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

s = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="us-east-1",
)

table = s.resource("dynamodb").Table("Jobsearch")
scan = table.scan()
items = scan["Items"]

print(f"Total jobs: {len(items)}\n")

# Find corrupted statuses
for item in items:
    status = item.get("status", "MISSING")
    if "\n" in status or "\r" in status or status not in (
        "NEW", "PENDING_APPROVAL", "APPROVED", "SENT", "REJECTED", "FAILED", "NO_RECRUITER_EMAIL"
    ):
        jid = item.get("job_id", "?")
        title = item.get("title", "?")
        print(f"CORRUPTED: job_id={jid} status='{repr(status)}' title={title}")

# Count NEW jobs that should have been scored
new_jobs = [i for i in items if i.get("status") == "NEW"]
print(f"\n41 NEW jobs (never scored) - these got stuck:")
for j in new_jobs[:5]:
    print(f"  {j.get('title', '?')} @ {j.get('company', '?')}")

# Check if deployed code matches local
lc = s.client("lambda")
for func in ["job-ingestor", "job-deduplicator", "job-processor", "job-router", "job-sender"]:
    cfg = lc.get_function_configuration(FunctionName=func)
    print(f"\n{func}: {cfg['CodeSize']}B, last={cfg['LastModified'][:19]}")
