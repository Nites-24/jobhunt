"""
CloudWatch Multi-Lambda Log Stream Inspector (check_logs.py)

Role & Operational Context:
  Fetches and prints recent execution logs from AWS CloudWatch for the primary
  production Lambda functions in the JobHunt pipeline:
  - /aws/lambda/job-router (Telegram bot webhook & command processing)
  - /aws/lambda/job-ingestor (Job fetching from Hacker News, Google CSE, and ATS boards)
  - /aws/lambda/job-processor (Bedrock LLM scoring and DynamoDB persistence)

Features:
  - Queries logs over a configurable sliding time window (default: last 30 minutes).
  - Automatically suppresses boilerplate AWS Lambda START, END, and REPORT telemetry lines
    to highlight application logic, errors, and LLM telemetry.

Usage:
  python tools/ops/check_logs.py
"""

import os, boto3, time
from datetime import datetime, timedelta

with open(".env") as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith("#") and "=" in l:
            k, _, v = l.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

logs = boto3.client("logs", region_name="us-east-1")

def get_latest_logs(log_group, minutes=60):
    print(f"\n{'='*60}\nChecking logs for {log_group} (Last {minutes} mins)\n{'='*60}")
    start_time = int((time.time() - (minutes * 60)) * 1000)
    
    try:
        response = logs.filter_log_events(
            logGroupName=log_group,
            startTime=start_time,
            limit=50
        )
        events = response.get("Events", [])
        if not events:
            print("  No logs found in this time frame.")
        for e in events:
            msg = e['message'].strip()
            if not msg.startswith("START") and not msg.startswith("END") and not msg.startswith("REPORT"):
                print(f"  {msg}")
    except Exception as ex:
        print(f"  Error fetching logs: {ex}")

get_latest_logs("/aws/lambda/job-router", 30)
get_latest_logs("/aws/lambda/job-ingestor", 30)
get_latest_logs("/aws/lambda/job-processor", 30)
