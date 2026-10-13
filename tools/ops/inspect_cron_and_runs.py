import os
import boto3
from datetime import datetime, timezone

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

# 1. EventBridge rules
events = session.client("events")
rules = events.list_rules().get("Rules", [])
print("=== EVENTBRIDGE RULES ===")
for r in rules:
    name = r["Name"]
    schedule = r.get("ScheduleExpression")
    state = r.get("State")
    targets = events.list_targets_by_rule(Rule=name).get("Targets", [])
    target_arns = [t["Arn"].split(":")[-1] for t in targets]
    print(f"Rule: {name} | Schedule: {schedule} | State: {state} | Targets: {target_arns}")

# 2. Jobs evaluated today (Oct 12, 2026)
dynamo = session.resource("dynamodb")
table = dynamo.Table("Jobsearch")

resp = table.scan(
    ProjectionExpression="job_id, company, title, #s, evaluated_at, ingested_at, match_score, #src, #u",
    ExpressionAttributeNames={"#s": "status", "#src": "source", "#u": "url"}
)
items = resp.get("Items", [])
while "LastEvaluatedKey" in resp:
    resp = table.scan(
        ProjectionExpression="job_id, company, title, #s, evaluated_at, ingested_at, match_score, #src, #u",
        ExpressionAttributeNames={"#s": "status", "#src": "source", "#u": "url"},
        ExclusiveStartKey=resp["LastEvaluatedKey"]
    )
    items.extend(resp.get("Items", []))

# Filter items evaluated or ingested since Oct 12 00:00 UTC (1791763200) or Oct 12 local
# Local CDT is UTC-5. Oct 12 00:00 CDT is timestamp 1791781200
OCT_12_START = 1791781200 # roughly today
today_items = []
for it in items:
    eval_at = it.get("evaluated_at")
    try:
        val = int(eval_at) if eval_at else 0
        if val >= OCT_12_START:
            today_items.append(it)
    except Exception:
        pass

print(f"\n=== JOBS EVALUATED TODAY (Oct 12) === Total: {len(today_items)}")
today_items.sort(key=lambda x: int(x.get("evaluated_at", 0)))
for it in today_items:
    eval_at = int(it.get("evaluated_at", 0))
    dt = datetime.fromtimestamp(eval_at).strftime("%Y-%m-%d %H:%M:%S")
    jid = str(it.get("job_id", ""))[:30]
    print(f"[{dt}] Score: {it.get('match_score')} | Status: {it.get('status')} | Source: {it.get('source')} | {it.get('company')} - {it.get('title')} | ID: {jid}")

