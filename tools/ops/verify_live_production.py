"""Verify live AWS pipeline configuration without printing secrets.

Default mode is read-only. Use --invoke-ingestor to run one live ingestion smoke
test; that will enqueue jobs into the normal production pipeline.
"""

import argparse
import json
import os

import boto3


REGION = "us-east-1"
LAMBDA_FUNCTIONS = [
    "job-ingestor",
    "job-deduplicator",
    "job-processor",
    "job-router",
    "job-sender",
]
QUEUE_NAMES = [
    "job-raw-queue",
    "job-filtered-queue",
    "job-raw-queue-dlq",
    "job-filtered-queue-dlq",
]


def _load_env() -> None:
    if not os.path.exists(".env"):
        return
    with open(".env", encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())


def _session() -> boto3.Session:
    _load_env()
    return boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=REGION,
    )


def _queue_url(sqs, queue_name: str) -> str:
    return sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]


def verify_lambdas(lambda_client) -> None:
    print("Lambda functions")
    print("----------------")
    for function_name in LAMBDA_FUNCTIONS:
        cfg = lambda_client.get_function_configuration(FunctionName=function_name)
        print(
            f"{function_name}: state={cfg.get('State')} "
            f"last_update={cfg.get('LastUpdateStatus')} modified={cfg.get('LastModified')}"
        )
    print()


def verify_queues(sqs) -> None:
    print("SQS queues")
    print("----------")
    for queue_name in QUEUE_NAMES:
        try:
            url = _queue_url(sqs, queue_name)
        except sqs.exceptions.QueueDoesNotExist:
            print(f"{queue_name}: MISSING")
            continue
        attrs = sqs.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "RedrivePolicy",
            ],
        )["Attributes"]
        redrive = attrs.get("RedrivePolicy")
        if redrive:
            redrive = json.loads(redrive).get("deadLetterTargetArn", "configured")
        else:
            redrive = "none"
        print(
            f"{queue_name}: visible={attrs.get('ApproximateNumberOfMessages')} "
            f"inflight={attrs.get('ApproximateNumberOfMessagesNotVisible')} dlq={redrive}"
        )
    print()


def verify_alarms(cloudwatch) -> None:
    print("CloudWatch alarms")
    print("-----------------")
    prefixes = [f"{name}-" for name in LAMBDA_FUNCTIONS] + [f"{name}-" for name in QUEUE_NAMES]
    names = set()
    for prefix in prefixes:
        paginator = cloudwatch.get_paginator("describe_alarms")
        for page in paginator.paginate(AlarmNamePrefix=prefix):
            for alarm in page.get("MetricAlarms", []):
                names.add(alarm["AlarmName"])
    print(f"Configured alarms: {len(names)}")
    for name in sorted(names):
        print(f"- {name}")
    print()


def invoke_ingestor(lambda_client) -> None:
    print("Ingestor smoke test")
    print("-------------------")
    response = lambda_client.invoke(
        FunctionName="job-ingestor",
        InvocationType="RequestResponse",
        Payload=json.dumps({}),
    )
    payload = response["Payload"].read().decode("utf-8")
    print(payload)
    print()


def verify_schedule(events_client, lambda_client, rule_name: str = "job-search-daily-trigger") -> None:
    print("EventBridge ingestion schedule")
    print("------------------------------")
    try:
        from tools.ops.setup_cron import verify_ingestor_schedule
        status = verify_ingestor_schedule(events_client, lambda_client, rule_name=rule_name)
        if not status["rule_exists"]:
            print(f"Rule '{rule_name}': MISSING")
        else:
            print(f"Rule:         {status['rule_name']}")
            print(f"State:        {status['state']}")
            print(f"Schedule:     {status['schedule_expression']}")
            print(f"Target:       {status['target_arn'] or 'NOT CONFIGURED'}")
            print(f"Target Input: {status['target_input'] or 'DEFAULT'}")
            print(f"Permission:   {'CONFIGURED' if status['permission_configured'] else 'UNVERIFIED'}")
            print(f"Healthy:      {'YES' if status['healthy'] else 'NO'}")
    except Exception as e:
        print(f"Rule check error: {e}")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the live AWS job pipeline.")
    parser.add_argument(
        "--invoke-ingestor",
        action="store_true",
        help="Run one live ingestion smoke test. This queues production jobs.",
    )
    args = parser.parse_args()

    session = _session()
    lambda_client = session.client("lambda")
    sqs = session.client("sqs")
    cloudwatch = session.client("cloudwatch")
    events_client = session.client("events")

    verify_lambdas(lambda_client)
    verify_queues(sqs)
    verify_alarms(cloudwatch)
    verify_schedule(events_client, lambda_client)
    if args.invoke_ingestor:
        invoke_ingestor(lambda_client)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
