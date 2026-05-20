"""
⚠️  DEPRECATED — Merged into job-processor (v2.0)

This Lambda's logic (S3 context assembly + Bedrock scoring) has been
consolidated into the unified job-processor Lambda.

ACTION REQUIRED:
  Remove the SQS trigger from this Lambda in the AWS Console to prevent
  double-processing and wasted Bedrock costs.

  The job-processor now handles:
    - S3 memory context (config/, data/, prompts/) with warm-start caching
    - Bedrock Converse API with forced tool-calling for deterministic scores
    - SNS notifications for 80%+ matches
    - Proper batchItemFailures for SQS retry

This file is kept for reference only.
"""

# ---------- ORIGINAL CODE BELOW (for reference) ----------

import json
import os
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sns = boto3.client("sns", region_name="us-east-1")
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb")

S3_BUCKET = os.environ.get("S3_BUCKET", "job-search-pipeline-memory")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:827949090396:jobsearch-approvals")
PASS_THRESHOLD = int(os.environ.get("PASS_THRESHOLD", "85"))
MODEL_ID = os.environ.get("MODEL_ID", "anthropic.claude-haiku-4-5-20251001-v1:0")


def lambda_handler(event, context):
    logger.warning("DEPRECATED: This Lambda has been merged into job-processor. "
                    "Remove the SQS trigger to stop double-processing.")
    return {"status": "DEPRECATED", "message": "Use job-processor instead"}
