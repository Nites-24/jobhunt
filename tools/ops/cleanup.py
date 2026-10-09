"""
Threshold Enforcement & Queue Cleanup Utility (cleanup.py)

Role & Operational Context:
  Enforces the high-match threshold policy (match_score >= 80) across all jobs
  currently in 'PENDING_APPROVAL' status in DynamoDB.

Workflow:
  1. Scans the 'Jobsearch' table for all items with status == 'PENDING_APPROVAL'.
  2. Displays candidate jobs with their match scores, titles, and companies.
  3. Identifies any pending jobs with match_score < 80 (e.g. from legacy scoring runs or lower thresholds).
  4. Automatically transitions sub-80 jobs to 'REJECTED' status in DynamoDB.
  5. Leaves only high-confidence (80%+) opportunities in the candidate's review queue.

Usage:
  python tools/ops/cleanup.py

Post-Execution:
  After running, using the Telegram 'list' command will display only verified >= 80% matches.
"""
import os, boto3

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
pending = [i for i in scan["Items"] if i.get("status") == "PENDING_APPROVAL"]

print(f"{len(pending)} pending jobs:\n")
for i, j in enumerate(pending, 1):
    score = int(j.get("match_score", 0))
    title = j.get("title", "?")
    company = j.get("company", "?")
    print(f"  {i}. [{score}%] {title} @ {company}")

# Auto-reject anything under 80%
rejected = 0
for j in pending:
    score = int(j.get("match_score", 0))
    if score < 80:
        table.update_item(
            Key={"job_id": j["job_id"]},
            UpdateExpression="SET #s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "REJECTED"},
        )
        rejected += 1
        title = j.get("title", "?")
        company = j.get("company", "?")
        print(f"  REJECTED: [{score}%] {title} @ {company}")

remaining = len(pending) - rejected
print(f"\nAuto-rejected {rejected} jobs under 80%")
print(f"Remaining pending (80%+): {remaining}")
print(f"\nNow type 'list' in Telegram to see only strong matches.")
