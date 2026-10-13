import os
import boto3
from collections import defaultdict
from datetime import datetime

if os.path.exists(".env"):
    with open(".env") as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                k, _, v = line.strip().partition("=")
                os.environ.setdefault(k.strip(), v.strip())

session = boto3.Session(
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-1"
)
client = session.resource("dynamodb")
table = client.Table("Jobsearch")

resp = table.scan(
    ProjectionExpression="job_id, company, title, #s, evaluated_at, #src, #u",
    ExpressionAttributeNames={"#s": "status", "#src": "source", "#u": "url"}
)
items = resp.get("Items", [])
while "LastEvaluatedKey" in resp:
    resp = table.scan(
        ProjectionExpression="job_id, company, title, #s, evaluated_at, #src, #u",
        ExpressionAttributeNames={"#s": "status", "#src": "source", "#u": "url"},
        ExclusiveStartKey=resp["LastEvaluatedKey"]
    )
    items.extend(resp.get("Items", []))

print("Total items in Jobsearch table:", len(items))

groups = defaultdict(list)
for it in items:
    comp = str(it.get("company", "")).strip().lower()
    tit = str(it.get("title", "")).strip().lower()
    groups[(comp, tit)].append(it)

duplicates = {k: v for k, v in groups.items() if len(v) > 1}
print(f"Total duplicate (company, title) groups: {len(duplicates)}")

# Sort by group with most duplicates
sorted_dups = sorted(duplicates.items(), key=lambda kv: len(kv[1]), reverse=True)

for (comp, tit), group in sorted_dups[:10]:
    print(f"\nDuplicate: {comp} - {tit} ({len(group)} copies):")
    for g in group:
        jid = str(g.get("job_id", ""))
        short_id = jid[:35]
        eval_at = g.get("evaluated_at")
        eval_dt = datetime.fromtimestamp(int(eval_at)).strftime("%Y-%m-%d %H:%M") if eval_at else "unscored"
        print(f"   ID: {short_id}... | Status: {g.get('status')} | Source: {g.get('source')} | Eval: {eval_dt}")
