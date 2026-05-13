import os, json, boto3
from decimal import Decimal

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
