"""
Pipeline Recovery & Remediation Utility (fix_pipeline.py)

Role & Operational Context:
  Diagnoses and resolves state divergence and backlogged records in the live AWS
  pipeline. Used by operators when DynamoDB records have corrupted status attributes
  or when newly ingested jobs remain in 'NEW' status without being processed by
  `job-processor`.

Remediation Workflow:
  1. Status Sanitization:
     - Scans the DynamoDB 'Jobsearch' table for whitespace/newline corruption in status fields.
     - Strips trailing/leading newlines or carriage returns (e.g., 'NEW\\n' -> 'NEW').
     - Resets 'MISSING', 'UNKNOWN', or empty statuses back to 'NEW' for evaluation.
  2. Scoring Requeue:
     - Discovers unscored 'NEW' records in DynamoDB.
     - Constructs standard pipeline payloads and re-enqueues them to `job-filtered-queue`.
     - Triggers asynchronous processing by `job-processor` via Bedrock Claude scoring.
  3. Queue Health Verification:
     - Inspects SQS depths for `job-raw-queue` and `job-filtered-queue` (waiting vs. in-flight).

Usage:
  python tools/ops/fix_pipeline.py

Safety & Invariants:
  - Requires valid AWS credentials in .env.
  - Idempotent: Does not mutate scored jobs (PENDING_APPROVAL, APPROVED, REJECTED, SENT).
  - Only unscored records are re-queued.
"""
import os, json, boto3, time

with open(".env") as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith("#") and "=" in l:
            k, _, v = l.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="us-east-1",
)

table = session.resource("dynamodb").Table("Jobsearch")
sqs = session.client("sqs")
FILTERED_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/827949090396/job-filtered-queue"

# =====================================================
# STEP 1: Fix corrupted status entries
# =====================================================
print("=" * 60)
print("  STEP 1: Fix corrupted DynamoDB records")
print("=" * 60)

scan = table.scan()
items = scan["Items"]
fixed = 0

for item in items:
    status = item.get("status", "")
    job_id = item.get("job_id", "")
    
    # Fix newline in status
    if "\n" in status or "\r" in status:
        clean_status = status.strip()
        print(f"  FIX: '{repr(status)}' -> '{clean_status}' (job_id={job_id})")
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="SET #s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": clean_status},
        )
        fixed += 1

    # Fix MISSING / UNKNOWN status -> set to NEW for re-processing
    if status in ("MISSING", "UNKNOWN", ""):
        print(f"  FIX: '{status}' -> 'NEW' (job_id={job_id})")
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="SET #s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "NEW"},
        )
        fixed += 1

print(f"\n  Fixed {fixed} corrupted records\n")


# =====================================================
# STEP 2: Re-queue stuck NEW jobs for scoring
# =====================================================
print("=" * 60)
print("  STEP 2: Re-queue stuck NEW jobs for AI scoring")
print("=" * 60)

# Re-scan to get updated data
scan = table.scan()
items = scan["Items"]
new_jobs = [i for i in items if i.get("status") == "NEW"]

print(f"  Found {len(new_jobs)} NEW (unscored) jobs")
print(f"  Sending to filtered queue for Bedrock scoring...\n")

sent = 0
for job in new_jobs:
    # Build payload for the processor
    payload = {
        "job_id": job["job_id"],
        "title": job.get("title", ""),
        "company": job.get("company", ""),
        "location": job.get("location", ""),
        "description": job.get("description", ""),
        "url": job.get("url", ""),
        "apply_url": job.get("apply_url", ""),
        "source": job.get("source", "jsearch"),
        "is_remote": job.get("is_remote", False),
        "employment_type": job.get("employment_type", ""),
        "recruiter_email": job.get("recruiter_email", ""),
        "recruiter_name": job.get("recruiter_name", ""),
        "posted_at": job.get("posted_at", ""),
    }

    try:
        sqs.send_message(
            QueueUrl=FILTERED_QUEUE_URL,
            MessageBody=json.dumps(payload, default=str),
        )
        sent += 1
        title = job.get("title", "?")
        company = job.get("company", "?")
        print(f"  [{sent}] Queued: {title} @ {company}")
    except Exception as e:
        print(f"  ERROR: {e}")

print(f"\n  Sent {sent} jobs to scoring queue")
print(f"  The job-processor Lambda will score these in ~2-3 minutes")
print(f"  New 72%+ matches will appear in Telegram via 'list' command\n")


# =====================================================
# STEP 3: Verify queue state
# =====================================================
print("=" * 60)
print("  STEP 3: Verify queue state")
print("=" * 60)

time.sleep(2)

for queue_name in ["job-raw-queue", "job-filtered-queue"]:
    url = f"https://sqs.us-east-1.amazonaws.com/827949090396/{queue_name}"
    attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    depth = attrs.get("ApproximateNumberOfMessages", "0")
    inflight = attrs.get("ApproximateNumberOfMessagesNotVisible", "0")
    print(f"  {queue_name}: {depth} waiting, {inflight} in-flight")

print(f"\n{'='*60}")
print("  DONE — Go test in Telegram:")
print("  1. Wait 2-3 min for scoring to complete")
print("  2. Send 'list' to see new scored matches")
print("  3. Send 'apply 1' to test email sending")
print("  4. Send 'status' to see pipeline stats")
print(f"{'='*60}\n")
