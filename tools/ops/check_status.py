import os, json, sys, boto3
from decimal import Decimal

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

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
items = []
scan = table.scan()
items.extend(scan["Items"])
while "LastEvaluatedKey" in scan:
    scan = table.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])
    items.extend(scan["Items"])
print(f"Total jobs: {len(items)}\n")

by_status = {}
for i in items:
    st = i.get("status", "?")
    by_status.setdefault(st, []).append(i)

for status in ["PENDING_APPROVAL", "NEW", "APPROVED", "SENT", "REJECTED", "FAILED"]:
    jobs = by_status.get(status, [])
    if jobs:
        print(f"{status}: {len(jobs)}")
        for j in jobs[:8]:
            score = int(j.get("match_score", 0))
            email = j.get("recruiter_email", "")
            title = j.get("title", "?")
            company = j.get("company", "?")
            print(f"  [{score}%] {title} @ {company} | email: {email or 'none'}")
        print()
