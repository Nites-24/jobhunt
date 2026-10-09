"""
Pending Queue Reset Utility (clean_db.py)

Role & Operational Context:
  Clears the candidate's pending review queue in DynamoDB by transitioning all
  'PENDING_APPROVAL' jobs to 'REJECTED_OLD'.

When to Use:
  - When the candidate evaluation criteria or target role changes significantly.
  - When old pending jobs accumulate and an operator wants to start fresh with newly ingested,
    high-freshness opportunities.

Usage:
  python tools/ops/clean_db.py

Safety & Invariants:
  - Does NOT delete DynamoDB items; only updates the status field to 'REJECTED_OLD'.
  - Preserves history, URLs, and evaluation scores for auditability.
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
items = scan["Items"]

removed = 0
for item in items:
    if item.get("status") == "PENDING_APPROVAL":
        table.update_item(
            Key={"job_id": item["job_id"]},
            UpdateExpression="SET #s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "REJECTED_OLD"},
        )
        removed += 1
        print(f"Cleared old job: {item.get('title')} ({item.get('source')})")

print(f"\nCleared {removed} old jobs from pending to start fresh.")
