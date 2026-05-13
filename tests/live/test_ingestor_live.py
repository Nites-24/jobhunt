import os
import pytest

if os.environ.get("RUN_LIVE_TESTS") != "1":
    pytest.skip("Live AWS Lambda smoke test. Set RUN_LIVE_TESTS=1 to run.", allow_module_level=True)

import boto3, json

with open(".env") as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith("#") and "=" in l:
            k, _, v = l.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

lc = boto3.client("lambda", region_name="us-east-1")
try:
    print("Invoking job-ingestor...")
    resp = lc.invoke(FunctionName="job-ingestor", InvocationType="RequestResponse", Payload=json.dumps({}))
    res = resp["Payload"].read().decode("utf-8")
    print("Result:", res)
except Exception as e:
    print("Error:", e)
