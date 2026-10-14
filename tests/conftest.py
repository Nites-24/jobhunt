"""
Shared test fixtures for all Lambda function tests.

Uses moto to mock AWS services (DynamoDB, SQS, S3, SNS, Secrets Manager, Lambda).
All tests run entirely locally — no real AWS calls.
"""

import json
import os
import sys
import pytest
import boto3
from datetime import datetime, timezone, timedelta
from moto import mock_aws
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Add each Lambda directory to sys.path so we can import lambda_function
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

LAMBDA_DIRS = [
    "job_ingestor",
    "job_deduplicator",
    "job_processor",
    "job_router",
    "job_sender",
]

for d in LAMBDA_DIRS:
    path = os.path.join(PROJECT_ROOT, d)
    if path not in sys.path:
        sys.path.insert(0, path)


# ---------------------------------------------------------------------------
# Environment variables — set BEFORE importing Lambda modules
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def set_env_vars(monkeypatch):
    """Set all required environment variables for Lambda functions."""
    env = {
        # Common
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SECURITY_TOKEN": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "DYNAMODB_TABLE": "Jobsearch",
        # Ingestor
        "RAW_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/job-raw-queue",
        "JSEARCH_SECRET_NAME": "jobserch/jsearch",
        "HUNTER_SECRET_NAME": "jobsearch/hunterio",
        "SEARCH_QUERIES": "AI Engineer",
        "NUM_PAGES": "1",
        # Deduplicator
        "FILTERED_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/job-filtered-queue",
        # Processor
        "BEDROCK_MODEL_ID": "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
        "FILTER_CITIZENSHIP_RESTRICTIONS": "true",
        "SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:jobsearch-approvals",
        "S3_BUCKET_NAME": "job-search-pipeline-memory",
        "RESUME_KEY": "data/resume/resume.txt",
        "SCORE_THRESHOLD": "80",
        "CONTEXT_FOLDERS": "config/,data/",
        # Router
        "TELEGRAM_SECRET_NAME": "telegram",
        "SENDER_LAMBDA_NAME": "job-sender",
        "INGESTOR_LAMBDA_NAME": "job-ingestor",
        "REQUIRE_TELEGRAM_WEBHOOK_SECRET": "false",
        # Candidate Identity Decoupling
        "CANDIDATE_NAME": "Alex Hunter",
        "TARGET_ROLE": "AI Engineer",
        "CANDIDATE_LOCATION": "open to remote, hybrid, or onsite",
        "SENDER_EMAIL": "candidate@example.com",
        "SECRETS_NAME": "jobserch_gmail",
        "RESUME_BUCKET": "job-search-pipeline-memory",
        "RETIREIQ_URL": "https://huggingface.co/spaces/demo/RetireIQ",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)


# ---------------------------------------------------------------------------
# Mock AWS Resources
# ---------------------------------------------------------------------------
@pytest.fixture
def aws_credentials():
    """Mock AWS credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def dynamodb_table():
    """Create a mock DynamoDB Jobsearch table."""
    with mock_aws():
        client = boto3.resource("dynamodb", region_name="us-east-1")
        table = client.create_table(
            TableName="Jobsearch",
            KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "job_id", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "status-index",
                    "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName="Jobsearch")
        yield table


@pytest.fixture
def sqs_queues():
    """Create mock SQS queues."""
    with mock_aws():
        client = boto3.client("sqs", region_name="us-east-1")
        raw_q = client.create_queue(QueueName="job-raw-queue")
        filtered_q = client.create_queue(QueueName="job-filtered-queue")
        yield {
            "raw_url": raw_q["QueueUrl"],
            "filtered_url": filtered_q["QueueUrl"],
            "client": client,
        }


@pytest.fixture
def s3_bucket():
    """Create mock S3 bucket with resume."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="job-search-pipeline-memory")
        # Upload a text resume
        resume_text = (
            "Alex Hunter — Senior AI Engineer\n"
            "Skills: Python, PyTorch, AWS, LangChain, RAG, LLMs, Vector DBs\n"
            "Experience: 33% GPU cost reduction, 87% ticket resolution\n"
            "Projects: RetireIQ (retirement planning and financial decision support), Rivulet (personal life management)\n"
            "Location: San Francisco, CA. Open to relocate. Open to remote, hybrid, or onsite roles in any location.\n"
        )
        client.put_object(
            Bucket="job-search-pipeline-memory",
            Key="data/resume/resume.txt",
            Body=resume_text.encode("utf-8"),
        )
        client.put_object(
            Bucket="job-search-pipeline-memory",
            Key="data/resume/resume.pdf",
            Body=b"%PDF-1.4 fake resume pdf bytes",
        )
        yield client


@pytest.fixture
def sns_topic():
    """Create mock SNS topic."""
    with mock_aws():
        client = boto3.client("sns", region_name="us-east-1")
        topic = client.create_topic(Name="jobsearch-approvals")
        yield {"arn": topic["TopicArn"], "client": client}


@pytest.fixture
def secrets():
    """Create mock secrets."""
    with mock_aws():
        client = boto3.client("secretsmanager", region_name="us-east-1")
        client.create_secret(
            Name="jobserch/jsearch",
            SecretString=json.dumps({"JSEARCH_API_KEY": "test-jsearch-key"}),
        )
        client.create_secret(
            Name="jobsearch/hunterio",
            SecretString=json.dumps({"HUNTER_API_KEY": "test-hunter-key"}),
        )
        client.create_secret(
            Name="telegram",
            SecretString=json.dumps({
                "TELEGRAM_BOT_TOKEN": "123456:ABC-DEF",
                "TELEGRAM_CHAT_ID": "99999",
            }),
        )
        client.create_secret(
            Name="jobserch_gmail",
            SecretString=json.dumps({"APP_PASSWORD": "test-app-password"}),
        )
        yield client


# ---------------------------------------------------------------------------
# Shared Test Data
# ---------------------------------------------------------------------------
SAMPLE_JOB = {
    "job_id": "test-job-001",
    "title": "AI Engineer",
    "company": "Anthropic",
    "location": "San Francisco, CA",
    "description": "Build next-gen LLM infrastructure. Requires Python, PyTorch, RAG experience.",
    "url": "https://anthropic.com/careers/ai-engineer",
    "apply_url": "https://anthropic.com/apply",
    "source": "jsearch",
    "salary": "$180,000 - $250,000",
    "is_remote": False,
    "employment_type": "FULLTIME",
    "posted_at": (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "recruiter_email": "hiring@anthropic.com",
    "recruiter_name": "Sarah Chen",
}

SAMPLE_SQS_RECORD = {
    "messageId": "msg-001",
    "body": json.dumps(SAMPLE_JOB),
}

SAMPLE_SQS_EVENT = {
    "Records": [SAMPLE_SQS_RECORD],
}


def make_sqs_event(jobs: list) -> dict:
    """Helper to create an SQS event with multiple records."""
    return {
        "Records": [
            {
                "messageId": f"msg-{i:03d}",
                "body": json.dumps(job),
            }
            for i, job in enumerate(jobs)
        ]
    }


class MockContext:
    """Fake Lambda context object."""
    function_name = "test-lambda"
    memory_limit_in_mb = 256
    invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-lambda"
    aws_request_id = "test-request-id"
