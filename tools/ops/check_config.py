"""
Lambda Configuration Inspector (check_config.py)

Role & Operational Context:
  Inspects the active configuration of deployed AWS Lambda functions in the JobHunt pipeline,
  specifically checking environment variables like SEARCH_QUERIES on the `job-ingestor` function.

Usage:
  python tools/ops/check_config.py

Outputs:
  Prints current SEARCH_QUERIES configured in AWS Lambda, allowing operators to verify
  that production Lambdas are using the correct queries for Hacker News, Google CSE, and ATS boards.
"""

import os, boto3

with open(".env") as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith("#") and "=" in l:
            k, _, v = l.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

lc = boto3.client("lambda", region_name="us-east-1")
cfg = lc.get_function_configuration(FunctionName="job-ingestor")
env = cfg.get("Environment", {}).get("Variables", {})
print(f"SEARCH_QUERIES: {env.get('SEARCH_QUERIES', 'NOT SET')}")
