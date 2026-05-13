"""
Deploy all Lambda functions to AWS.
Zips each Lambda directory and uploads to the corresponding Lambda function.
"""

import os
import sys
import json
import zipfile
import io
import boto3

# Load .env
with open(".env") as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="us-east-1",
)
lambda_client = session.client("lambda")

# Map local directories to AWS Lambda function names
LAMBDAS = {
    "job_ingestor": "job-ingestor",
    "job_deduplicator": "job-deduplicator",
    "job_processor": "job-processor",
    "job_router": "job-router",
    "job_sender": "job-sender",
}

SHARED_DIR = "shared"


def zip_lambda(directory):
    """Create a zip buffer of a Lambda directory."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith(".py"):
                    filepath = os.path.join(root, file)
                    arcname = os.path.relpath(filepath, directory)
                    zf.write(filepath, arcname)
        if os.path.exists(SHARED_DIR):
            for root, dirs, files in os.walk(SHARED_DIR):
                for file in files:
                    if file.endswith(".py"):
                        filepath = os.path.join(root, file)
                        arcname = os.path.relpath(filepath, ".")
                        zf.write(filepath, arcname)
    buf.seek(0)
    return buf.read()


def get_existing_lambdas():
    """Get list of existing Lambda function names."""
    try:
        response = lambda_client.list_functions(MaxItems=50)
        return [f["FunctionName"] for f in response["Functions"]]
    except Exception as e:
        print(f"  Error listing functions: {e}")
        return []


def deploy():
    print("\n" + "=" * 60)
    print("  DEPLOYING LAMBDA FUNCTIONS")
    print("=" * 60)

    existing = get_existing_lambdas()
    print(f"\n  Existing Lambdas: {existing}\n")

    for local_dir, func_name in LAMBDAS.items():
        if not os.path.exists(local_dir):
            print(f"  SKIP: {local_dir}/ not found")
            continue

        if func_name not in existing:
            print(f"  SKIP: {func_name} not found in AWS (check the name)")
            continue

        print(f"  Deploying {local_dir}/ -> {func_name}...", end=" ")

        try:
            zip_bytes = zip_lambda(local_dir)
            lambda_client.update_function_code(
                FunctionName=func_name,
                ZipFile=zip_bytes,
            )
            print(f"OK ({len(zip_bytes)} bytes)")
        except Exception as e:
            print(f"FAILED: {e}")

    print(f"\n{'='*60}")
    print("  DEPLOYMENT COMPLETE")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    deploy()
