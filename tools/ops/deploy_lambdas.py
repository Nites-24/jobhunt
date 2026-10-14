"""
Deploy Lambdas to AWS (deploy_lambdas.py)

Packages local Lambda directories and shared/ utilities into zip archives in-memory
and updates AWS Lambda function code using boto3.
"""

import os
import sys
import io
import time
import zipfile
import boto3

if os.path.exists(".env"):
    with open(".env", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
session = boto3.Session(
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name=REGION,
)
client = session.client("lambda")

LAMBDA_MAP = {
    "job_ingestor": "job-ingestor",
    "job_deduplicator": "job-deduplicator",
    "job_processor": "job-processor",
    "job_router": "job-router",
    "job_sender": "job-sender",
}

SHARED_DIR = "shared"


def package_lambda(local_dir: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(local_dir):
            for f in files:
                if f.endswith(".py"):
                    full_path = os.path.join(root, f)
                    rel_path = os.path.relpath(full_path, local_dir)
                    zf.write(full_path, rel_path)
        if os.path.exists(SHARED_DIR):
            for root, dirs, files in os.walk(SHARED_DIR):
                for f in files:
                    if f.endswith(".py"):
                        full_path = os.path.join(root, f)
                        rel_path = os.path.join("shared", os.path.relpath(full_path, SHARED_DIR))
                        zf.write(full_path, rel_path)
    return buf.getvalue()


def deploy_function(local_dir: str, func_name: str) -> None:
    print(f"\nPackaging {local_dir} + {SHARED_DIR}...")
    zip_bytes = package_lambda(local_dir)
    print(f"Uploading {len(zip_bytes)} bytes to AWS Lambda '{func_name}'...")

    resp = client.update_function_code(
        FunctionName=func_name,
        ZipFile=zip_bytes,
    )
    print(f"Upload complete. FunctionArn: {resp.get('FunctionArn')}")

    # Wait for completion
    waiter = client.get_waiter("function_updated")
    print(f"Waiting for '{func_name}' to report Successful status...")
    waiter.wait(FunctionName=func_name)
    print(f"[OK] '{func_name}' successfully updated and Active.")


def main():
    targets = sys.argv[1:] if len(sys.argv) > 1 else list(LAMBDA_MAP.keys())
    for target in targets:
        if target in LAMBDA_MAP:
            deploy_function(target, LAMBDA_MAP[target])
        else:
            print(f"Unknown target: {target}")


if __name__ == "__main__":
    main()
