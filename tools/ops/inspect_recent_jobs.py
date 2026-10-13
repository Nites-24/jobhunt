import os
import boto3
import json
from datetime import datetime

if os.path.exists(".env"):
    with open(".env") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

session = boto3.Session(
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-1"
)

dynamo = session.resource("dynamodb")
table = dynamo.Table("Jobsearch")

print("Querying all PENDING_APPROVAL jobs...")
resp = table.query(
    IndexName="status-index",
    KeyConditionExpression="#s = :p",
    ExpressionAttributeNames={"#s": "status"},
    ExpressionAttributeValues={":p": "PENDING_APPROVAL"}
)
items = resp.get("Items", [])
print(f"Total PENDING_APPROVAL jobs: {len(items)}")

def get_time(item):
    return str(item.get("evaluated_at") or item.get("ingested_at") or "")

items.sort(key=get_time, reverse=True)
for i, it in enumerate(items[:6]):
    jid = str(it.get("job_id", ""))
    eval_at = it.get("evaluated_at")
    eval_dt = datetime.fromtimestamp(int(eval_at), tz=datetime.now().astimezone().tzinfo) if eval_at else "none"
    print(f"\n{i+1}. [{it.get('source')}] {it.get('company')} - {it.get('title')}")
    print(f"   Score: {it.get('match_score')} | Evaluated: {eval_at} ({eval_dt}) | ID: {jid}")
    print(f"   URL: {it.get('url')}")
    print(f"   Reason: {it.get('match_reason')}")
    print(f"   Raw Dict Keys: {list(it.keys())}")


