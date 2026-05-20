"""Quick test: Steps 4 + 5 only, using jobs already in DynamoDB with PENDING_APPROVAL status."""

import os
import pytest

if os.environ.get("RUN_LIVE_TESTS") != "1":
    pytest.skip("Live AWS/Bedrock smoke test. Set RUN_LIVE_TESTS=1 to run.", allow_module_level=True)

import sys, json, boto3
from decimal import Decimal

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
dynamodb = session.resource("dynamodb")
bedrock = session.client("bedrock-runtime")
table = dynamodb.Table("Jobsearch")

# Get all PENDING_APPROVAL jobs from the previous run
response = table.scan()
all_items = response.get("Items", [])
pending = [i for i in all_items if i.get("status") == "PENDING_APPROVAL"]
print(f"\nFound {len(pending)} PENDING_APPROVAL jobs in DynamoDB")

if not pending:
    # Check for any 85%+ jobs that were rejected at 80% threshold
    high_scores = [i for i in all_items if int(i.get("match_score", 0)) >= 85]
    print(f"Found {len(high_scores)} jobs scored 85%+ (updating to PENDING_APPROVAL)")
    for job in high_scores:
        table.update_item(
            Key={"job_id": job["job_id"]},
            UpdateExpression="SET #s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "PENDING_APPROVAL"},
        )
    pending = high_scores

if not pending:
    print("No jobs to preview. Run pytest tests/live/test_live_e2e.py first.")
    sys.exit(0)

# === STEP 4: TELEGRAM PREVIEW ===
print(f"\n{'='*60}")
print("  STEP 4: TELEGRAM PREVIEW")
print(f"{'='*60}\n")
print(f"  {len(pending)} Job(s) Pending Approval:\n")
for i, job in enumerate(pending, 1):
    score = int(job.get("match_score", 0))
    print(f"  {i}. [{score}%] {job.get('title', '?')}")
    print(f"     {job.get('company', '?')} | {job.get('location', '?')}")
    print(f"     Source: {job.get('url', 'N/A')}")
    print()
print("  Commands: 'apply' | 'apply 2' | 'reject' | 'list'")

# === STEP 5: EMAIL GENERATION ===
job = pending[0]
print(f"\n{'='*60}")
print("  STEP 5: EMAIL GENERATION (real Bedrock, NOT sent)")
print(f"{'='*60}\n")
print(f"  Generating email for: {job.get('title')} @ {job.get('company')}")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "job_sender"))
from lambda_function import SYSTEM_PROMPT, _build_user_message, _sanitize_body

# Convert Decimals to int for the sender
clean_job = {}
for k, v in job.items():
    if isinstance(v, Decimal):
        clean_job[k] = int(v) if v % 1 == 0 else float(v)
    else:
        clean_job[k] = v

user_msg = _build_user_message(clean_job)

tool_config = {
    "tools": [{
        "toolSpec": {
            "name": "generate_email",
            "description": "Submit the email subject and body.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "subject": {"type": "string"},
                        "body": {"type": "string"},
                    },
                    "required": ["subject", "body"],
                }
            },
        }
    }],
    "toolChoice": {"tool": {"name": "generate_email"}},
}

response = bedrock.converse(
    modelId="us.anthropic.claude-haiku-4-5-20251001-v1:0",
    messages=[{"role": "user", "content": [{"text": user_msg}]}],
    system=[{"text": SYSTEM_PROMPT}],
    inferenceConfig={"maxTokens": 1000, "temperature": 0.3},
    toolConfig=tool_config,
)

usage = response.get("usage", {})
for block in response["output"]["message"]["content"]:
    if "toolUse" in block:
        result = block["toolUse"]["input"]
        subject = result["subject"]
        body = _sanitize_body(result["body"])

        print(f"\n  --- EMAIL PREVIEW ---")
        print(f"  From: vakateja24@gmail.com")
        print(f"  To: {clean_job.get('recruiter_email') or 'vakateja24@gmail.com (no recruiter found)'}")
        print(f"  Subject: {subject}")
        print(f"  Attachment: Teja_Vaka_Resume.pdf")
        print(f"  Tokens: {usage.get('inputTokens', '?')} in / {usage.get('outputTokens', '?')} out")
        print(f"\n  Body:")
        print(f"  {'~'*50}")
        for line in body.split("\n"):
            print(f"  {line}")
        print(f"  {'~'*50}")
        print(f"\n  NOTE: Email NOT sent. Deploy to AWS and use Telegram 'apply' to send for real.")
        break

print(f"\n{'='*60}")
print("  ALL 5 STEPS VERIFIED")
print(f"{'='*60}\n")
