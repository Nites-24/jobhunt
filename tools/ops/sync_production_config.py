import os
import boto3
import json

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
client = session.client("lambda")

print("=== 1. SYNCING JOB-INGESTOR CONFIGURATION ===")
ingestor_conf = client.get_function_configuration(FunctionName="job-ingestor")
ingestor_env = ingestor_conf.get("Environment", {}).get("Variables", {})

# Clean out Adzuna keys
new_ingestor_env = {k: v for k, v in ingestor_env.items() if not k.startswith("ADZUNA_")}
new_ingestor_env["GOOGLE_CSE_ENABLED"] = "false"
new_ingestor_env["INGESTION_SOURCES"] = "hackernews,greenhouse,lever,ashby,jsearch"
new_ingestor_env["JSEARCH_QUERIES"] = "AI Engineer,LLM Engineer,RAG Engineer,Machine Learning Engineer"

client.update_function_configuration(
    FunctionName="job-ingestor",
    Timeout=300,
    MemorySize=512,
    Environment={"Variables": new_ingestor_env}
)
print("  job-ingestor: Timeout set to 300s, Memory 512MB, Adzuna removed, ATS boards + clean queries enabled.")

print("\n=== 2. SYNCING JOB-ROUTER CONFIGURATION ===")
router_conf = client.get_function_configuration(FunctionName="job-router")
router_env = router_conf.get("Environment", {}).get("Variables", {})
router_env["LIST_ALLOWED_SOURCES"] = "hackernews,hn,jsearch,google_cse,cse,greenhouse,lever,ashby,showcase,practice,signal_test"

client.update_function_configuration(
    FunctionName="job-router",
    Environment={"Variables": router_env}
)
print("  job-router: Allowed sources updated to include greenhouse, lever, ashby.")

print("\n=== CONFIGURATION SYNC COMPLETE ===")
