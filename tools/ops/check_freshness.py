"""
Pending Job Freshness & Ingestor Configuration Auditor (check_freshness.py)

Role & Operational Context:
  Audits the posting freshness of all opportunities in 'PENDING_APPROVAL' status
  and inspects the current date/freshness configuration of the `job-ingestor` Lambda.

Workflow:
  1. Computes current UTC timestamp.
  2. Scans DynamoDB 'Jobsearch' table for all pending jobs.
  3. Parses each job's 'posted_at' timestamp and calculates elapsed age in hours/days.
  4. Prints human-readable age breakdowns sorted newest-first.
  5. Queries the AWS Lambda API for `job-ingestor` environment variables:
     - DATE_POSTED
     - FRESHNESS_HOURS
     - SEARCH_QUERIES
     - NUM_PAGES

Usage:
  python tools/ops/check_freshness.py
"""
import os, boto3
from datetime import datetime, timezone

with open(".env") as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith("#") and "=" in l:
            k, _, v = l.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="us-east-1",
)
table = session.resource("dynamodb").Table("Jobsearch")
scan = table.scan()
items = scan["Items"]

now = datetime.now(timezone.utc)
print(f"Current UTC: {now.isoformat()[:19]}")
print()

pending = [i for i in items if i.get("status") == "PENDING_APPROVAL"]
print(f"PENDING_APPROVAL jobs ({len(pending)}):")
print()

for j in sorted(pending, key=lambda x: x.get("posted_at", ""), reverse=True):
    posted = j.get("posted_at", "")
    title = j.get("title", "?")
    company = j.get("company", "?")
    score = j.get("match_score", 0)

    age = "unknown"
    if posted:
        try:
            clean = posted.replace("Z", "+00:00")
            if "." in clean:
                dt = datetime.fromisoformat(clean)
            else:
                dt = datetime.fromisoformat(clean)
            hours = int((now - dt).total_seconds() / 3600)
            if hours < 1:
                age = "just now"
            elif hours < 24:
                age = f"{hours}h ago"
            elif hours < 48:
                age = "yesterday"
            else:
                age = f"{hours // 24}d ago"
        except Exception:
            age = posted[:10]

    print(f"  [{score}%] {title}")
    print(f"       @ {company} | posted: {age}")
    print()

# Also check the ingestor Lambda config
lc = session.client("lambda")
cfg = lc.get_function_configuration(FunctionName="job-ingestor")
env = cfg.get("Environment", {}).get("Variables", {})
print("=" * 50)
print("INGESTOR CONFIG:")
print(f"  DATE_POSTED:     {env.get('DATE_POSTED', 'today (default)')}")
print(f"  FRESHNESS_HOURS: {env.get('FRESHNESS_HOURS', 'not set')}")
print(f"  SEARCH_QUERIES:  {env.get('SEARCH_QUERIES', 'default in code')}")
print(f"  NUM_PAGES:       {env.get('NUM_PAGES', '2 (default)')}")
