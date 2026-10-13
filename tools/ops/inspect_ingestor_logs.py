import os
import boto3
import json

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
logs = session.client("logs")
streams = logs.describe_log_streams(
    logGroupName="/aws/lambda/job-ingestor",
    orderBy="LastEventTime",
    descending=True,
    limit=3
).get("logStreams", [])

for s in streams:
    sname = s["logStreamName"]
    print(f"\n=== STREAM: {sname} ===")
    events = logs.get_log_events(
        logGroupName="/aws/lambda/job-ingestor",
        logStreamName=sname,
        limit=25
    ).get("events", [])
    for e in events:
        msg = e["message"].strip()
        if any(w in msg for w in ["START", "INVOCATION", "Fetched", "sources", "trigger", "Event", "END", "REPORT", "error", "ERROR"]):
            print(f"  {msg}")
        elif len(msg) < 150:
            print(f"  {msg}")
