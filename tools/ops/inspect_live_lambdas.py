import os
import boto3
import json

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
client = session.client("lambda")
lambdas = ["job-ingestor", "job-deduplicator", "job-processor", "job-router", "job-sender"]

for fn in lambdas:
    try:
        conf = client.get_function_configuration(FunctionName=fn)
        env = conf.get("Environment", {}).get("Variables", {})
        print(f"\n=== {fn} ===")
        print(f"LastModified: {conf.get('LastModified')}")
        print(f"CodeSize:     {conf.get('CodeSize')}")
        print(f"Runtime:      {conf.get('Runtime')}")
        adzuna_vars = {k: v for k, v in env.items() if "ADZUNA" in k}
        jsearch_vars = {k: v for k, v in env.items() if "JSEARCH" in k}
        print(f"ADZUNA env:   {adzuna_vars}")
        print(f"JSEARCH env:  {jsearch_vars}")
        if "LIST_ALLOWED_SOURCES" in env:
            print(f"ALLOWED_SRCS: {env['LIST_ALLOWED_SOURCES']}")
        if "EVAL_PROFILE_VERSION" in env:
            print(f"PROFILE_VER:  {env['EVAL_PROFILE_VERSION']}")
    except Exception as e:
        print(f"{fn}: Error - {e}")
