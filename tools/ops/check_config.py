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
