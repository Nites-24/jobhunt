"""Create production safety rails: SQS DLQs and CloudWatch alarms.

Default mode is dry-run. Use --apply to mutate AWS.
"""

import argparse
import json
import os

import boto3


REGION = "us-east-1"
ACCOUNT_ID = "827949090396"
LAMBDA_FUNCTIONS = [
    "job-ingestor",
    "job-deduplicator",
    "job-processor",
    "job-router",
    "job-sender",
]
QUEUE_REDLINE = {
    "job-raw-queue": 100,
    "job-filtered-queue": 50,
}


def _load_env() -> None:
    if not os.path.exists(".env"):
        return
    with open(".env", encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())


def _session():
    _load_env()
    return boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=REGION,
    )


def _queue_url(sqs, queue_name: str) -> str:
    return sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]


def _queue_arn(sqs, queue_url: str) -> str:
    return sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]


def ensure_dlq(sqs, queue_name: str, apply: bool) -> None:
    source_url = _queue_url(sqs, queue_name)
    dlq_name = f"{queue_name}-dlq"

    if apply:
        try:
            dlq_url = _queue_url(sqs, dlq_name)
        except sqs.exceptions.QueueDoesNotExist:
            dlq_url = sqs.create_queue(QueueName=dlq_name)["QueueUrl"]

        dlq_arn = _queue_arn(sqs, dlq_url)
        redrive_policy = {
            "deadLetterTargetArn": dlq_arn,
            "maxReceiveCount": "5",
        }
        sqs.set_queue_attributes(
            QueueUrl=source_url,
            Attributes={"RedrivePolicy": json.dumps(redrive_policy)},
        )
        print(f"DLQ configured: {queue_name} -> {dlq_name}")
    else:
        print(f"DRY-RUN DLQ: would create/configure {dlq_name} for {queue_name}")


def put_lambda_alarms(cloudwatch, topic_arn: str | None, apply: bool) -> None:
    actions = [topic_arn] if topic_arn else []
    for function_name in LAMBDA_FUNCTIONS:
        for metric_name in ("Errors", "Throttles"):
            alarm_name = f"{function_name}-{metric_name.lower()}-alarm"
            if apply:
                cloudwatch.put_metric_alarm(
                    AlarmName=alarm_name,
                    Namespace="AWS/Lambda",
                    MetricName=metric_name,
                    Dimensions=[{"Name": "FunctionName", "Value": function_name}],
                    Statistic="Sum",
                    Period=300,
                    EvaluationPeriods=1,
                    Threshold=0,
                    ComparisonOperator="GreaterThanThreshold",
                    TreatMissingData="notBreaching",
                    AlarmActions=actions,
                )
                print(f"Alarm configured: {alarm_name}")
            else:
                print(f"DRY-RUN alarm: {alarm_name}")


def put_queue_alarms(cloudwatch, sqs, topic_arn: str | None, apply: bool) -> None:
    actions = [topic_arn] if topic_arn else []
    for queue_name, threshold in QUEUE_REDLINE.items():
        for metric_name, alarm_threshold in (
            ("ApproximateNumberOfMessagesVisible", threshold),
            ("ApproximateNumberOfMessagesNotVisible", threshold),
        ):
            alarm_name = f"{queue_name}-{metric_name}-alarm"
            if apply:
                cloudwatch.put_metric_alarm(
                    AlarmName=alarm_name,
                    Namespace="AWS/SQS",
                    MetricName=metric_name,
                    Dimensions=[{"Name": "QueueName", "Value": queue_name}],
                    Statistic="Maximum",
                    Period=300,
                    EvaluationPeriods=2,
                    Threshold=alarm_threshold,
                    ComparisonOperator="GreaterThanThreshold",
                    TreatMissingData="notBreaching",
                    AlarmActions=actions,
                )
                print(f"Alarm configured: {alarm_name}")
            else:
                print(f"DRY-RUN alarm: {alarm_name}")

        dlq_name = f"{queue_name}-dlq"
        if apply:
            try:
                _queue_url(sqs, dlq_name)
            except sqs.exceptions.QueueDoesNotExist:
                continue
        alarm_name = f"{dlq_name}-messages-visible-alarm"
        if apply:
            cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                Namespace="AWS/SQS",
                MetricName="ApproximateNumberOfMessagesVisible",
                Dimensions=[{"Name": "QueueName", "Value": dlq_name}],
                Statistic="Maximum",
                Period=300,
                EvaluationPeriods=1,
                Threshold=0,
                ComparisonOperator="GreaterThanThreshold",
                TreatMissingData="notBreaching",
                AlarmActions=actions,
            )
            print(f"Alarm configured: {alarm_name}")
        else:
            print(f"DRY-RUN alarm: {alarm_name}")


def main() -> int:
    _load_env()
    parser = argparse.ArgumentParser(description="Configure DLQs and CloudWatch alarms.")
    parser.add_argument("--apply", action="store_true", help="Actually mutate AWS.")
    parser.add_argument(
        "--alarm-topic-arn",
        default=os.environ.get("ALARM_TOPIC_ARN") or os.environ.get("SNS_TOPIC_ARN"),
        help="Optional SNS topic ARN for alarm notifications.",
    )
    args = parser.parse_args()

    session = _session()
    sqs = session.client("sqs")
    cloudwatch = session.client("cloudwatch")

    for queue_name in QUEUE_REDLINE:
        ensure_dlq(sqs, queue_name, args.apply)

    put_lambda_alarms(cloudwatch, args.alarm_topic_arn, args.apply)
    put_queue_alarms(cloudwatch, sqs, args.alarm_topic_arn, args.apply)
    print("Done." if args.apply else "Dry-run complete. Re-run with --apply to create resources.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
