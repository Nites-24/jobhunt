import os
import boto3
from datetime import datetime
from collections import Counter

if os.path.exists(".env"):
    with open(".env") as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

session = boto3.Session(
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-1"
)
dynamo = session.resource("dynamodb")
table = dynamo.Table("Jobsearch")

resp = table.scan(
    ProjectionExpression="job_id, company, title, #s, evaluated_at, match_score",
    ExpressionAttributeNames={"#s": "status"}
)
items = resp.get("Items", [])
while "LastEvaluatedKey" in resp:
    resp = table.scan(
        ProjectionExpression="job_id, company, title, #s, evaluated_at, match_score",
        ExpressionAttributeNames={"#s": "status"},
        ExclusiveStartKey=resp["LastEvaluatedKey"]
    )
    items.extend(resp.get("Items", []))

OCT_12_START = 1791781200 # roughly Oct 12 00:00 CDT
today_items = [it for it in items if int(it.get("evaluated_at") or 0) >= OCT_12_START]

print(f"Total jobs evaluated today: {len(today_items)}")
approved_today = [it for it in today_items if it.get("status") == "PENDING_APPROVAL"]
print(f"Total PENDING_APPROVAL notifications generated today: {len(approved_today)}\n")

for it in sorted(approved_today, key=lambda x: int(x.get("evaluated_at", 0))):
    eval_at = int(it.get("evaluated_at", 0))
    dt = datetime.fromtimestamp(eval_at).strftime("%H:%M:%S CDT")
    jid = str(it.get("job_id", ""))[:32]
    print(f"[{dt}] Score: {it.get('match_score')} | {it.get('company')} - {it.get('title')} | ID: {jid}...")

