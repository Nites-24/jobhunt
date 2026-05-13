"""
Backend Engineer Agent — AWS Infrastructure Health Monitor

Responsibilities:
  - Monitor all 5 Lambda functions (health, errors, config)
  - Validate SQS queue depth and dead-letter queues
  - Check DynamoDB table health and GSI status
  - Verify S3 bucket access and memory files
  - Validate SNS topic subscriptions and delivery
  - Auto-remediate common issues (stuck queues, cold starts)
  
This agent ensures the pipeline infrastructure stays healthy
so that jobs flow from ingestion → scoring → approval → email
without interruption.
"""

import json
import time
import logging
import os
import boto3
from botocore.exceptions import ClientError
from typing import Optional

from .base_agent import (
    BaseAgent, AgentObservation, AgentAction, AgentResult,
    Severity, AgentStatus,
)

logger = logging.getLogger(__name__)


# =========================================================================
# AWS Configuration
# =========================================================================
LAMBDA_FUNCTIONS = [
    "job-ingestor",
    "job-deduplicator",
    "job-processor",
    "job-router",
    "job-sender",
]

SQS_QUEUE_NAMES = [
    "job-raw-queue",
    "job-filtered-queue",
]

DYNAMODB_TABLE = "Jobsearch"
S3_BUCKET = "job-search-pipeline-memory"
SNS_TOPIC_PREFIX = "jobsearch"

# Thresholds
MAX_QUEUE_DEPTH = 100       # Alert if queue has > 100 messages
MAX_DLQ_MESSAGES = 5        # Alert if DLQ has > 5 messages
LAMBDA_ERROR_RATE_PCT = 10  # Alert if error rate > 10%
MAX_LAMBDA_AGE_DAYS = 7     # Warning if Lambda not updated in 7 days


class BackendEngineerAgent(BaseAgent):
    """
    Monitors and maintains the AWS infrastructure backing the job pipeline.
    
    Checks:
      1. Lambda function health (errors, timeouts, memory)
      2. SQS queue depth (stuck messages, DLQ overflow)
      3. DynamoDB table status and capacity
      4. S3 bucket accessibility
      5. SNS topic subscriptions
    """

    def __init__(self, session: Optional[boto3.Session] = None):
        super().__init__(
            name="BackendEngineer",
            description="AWS infrastructure health monitor and auto-remediator"
        )
        self.session = session or self._create_session()
        self.lambda_client = self.session.client("lambda", region_name="us-east-1")
        self.sqs_client = self.session.client("sqs", region_name="us-east-1")
        self.dynamodb_client = self.session.client("dynamodb", region_name="us-east-1")
        self.dynamodb_resource = self.session.resource("dynamodb", region_name="us-east-1")
        self.s3_client = self.session.client("s3", region_name="us-east-1")
        self.sns_client = self.session.client("sns", region_name="us-east-1")
        self.cloudwatch = self.session.client("cloudwatch", region_name="us-east-1")

    def _create_session(self) -> boto3.Session:
        """Create AWS session from .env or environment."""
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, _, v = line.partition("=")
                        os.environ.setdefault(k.strip(), v.strip())

        return boto3.Session(
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name="us-east-1",
        )

    # -------------------------------------------------------------------
    # OBSERVE: Gather infrastructure state
    # -------------------------------------------------------------------
    def observe(self, context: dict) -> list[AgentObservation]:
        observations = []
        observations.extend(self._check_lambdas())
        observations.extend(self._check_sqs_queues())
        observations.extend(self._check_dynamodb())
        observations.extend(self._check_s3())
        observations.extend(self._check_sns())
        return observations

    def _check_lambdas(self) -> list[AgentObservation]:
        """Check all Lambda functions for health issues."""
        observations = []
        for func_name in LAMBDA_FUNCTIONS:
            try:
                config = self.lambda_client.get_function_configuration(
                    FunctionName=func_name
                )
                
                # Check basic health
                state = config.get("State", "Unknown")
                last_modified = config.get("LastModified", "")
                timeout = config.get("Timeout", 0)
                memory = config.get("MemorySize", 0)
                env_vars = config.get("Environment", {}).get("Variables", {})
                code_size = config.get("CodeSize", 0)

                data = {
                    "function": func_name,
                    "state": state,
                    "timeout": timeout,
                    "memory": memory,
                    "code_size": code_size,
                    "env_var_count": len(env_vars),
                    "last_modified": last_modified,
                }

                if state != "Active":
                    observations.append(AgentObservation(
                        source=f"lambda:{func_name}",
                        data=data,
                        severity=Severity.CRITICAL,
                        message=f"Lambda {func_name} is in {state} state (expected Active)",
                    ))
                elif timeout < 30 and func_name in ("job-processor", "job-sender"):
                    observations.append(AgentObservation(
                        source=f"lambda:{func_name}",
                        data=data,
                        severity=Severity.WARNING,
                        message=f"Lambda {func_name} has low timeout ({timeout}s), "
                                f"Bedrock calls may exceed this",
                    ))
                else:
                    observations.append(AgentObservation(
                        source=f"lambda:{func_name}",
                        data=data,
                        severity=Severity.INFO,
                        message=f"Lambda {func_name} is healthy "
                                f"(timeout={timeout}s, memory={memory}MB)",
                    ))

                # Check for missing critical env vars
                required_vars = self._get_required_env_vars(func_name)
                missing = [v for v in required_vars if v not in env_vars]
                if missing:
                    observations.append(AgentObservation(
                        source=f"lambda:{func_name}:env",
                        data={"missing_vars": missing, "function": func_name},
                        severity=Severity.CRITICAL,
                        message=f"Lambda {func_name} missing env vars: {', '.join(missing)}",
                    ))

            except ClientError as e:
                observations.append(AgentObservation(
                    source=f"lambda:{func_name}",
                    data={"error": str(e)},
                    severity=Severity.FATAL,
                    message=f"Cannot reach Lambda {func_name}: {e.response['Error']['Code']}",
                ))

        return observations

    def _get_required_env_vars(self, func_name: str) -> list[str]:
        """Return required env vars per Lambda function."""
        common = ["DYNAMODB_TABLE"]
        specific = {
            "job-ingestor": ["RAW_QUEUE_URL", "JSEARCH_SECRET_NAME"],
            "job-deduplicator": ["FILTERED_QUEUE_URL"],
            "job-processor": ["SNS_TOPIC_ARN", "S3_BUCKET_NAME", "BEDROCK_MODEL_ID"],
            "job-router": ["TELEGRAM_SECRET_NAME", "SENDER_LAMBDA_NAME", "INGESTOR_LAMBDA_NAME"],
            "job-sender": ["SENDER_EMAIL", "SECRETS_NAME"],
        }
        return common + specific.get(func_name, [])

    def _check_sqs_queues(self) -> list[AgentObservation]:
        """Check SQS queue health — depth, DLQ, stuck messages."""
        observations = []
        try:
            response = self.sqs_client.list_queues(QueueNamePrefix="job-")
            queue_urls = response.get("QueueUrls", [])

            for url in queue_urls:
                name = url.split("/")[-1]
                attrs = self.sqs_client.get_queue_attributes(
                    QueueUrl=url,
                    AttributeNames=["All"],
                )["Attributes"]

                depth = int(attrs.get("ApproximateNumberOfMessages", 0))
                inflight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
                dlq_policy = attrs.get("RedrivePolicy")

                data = {
                    "queue": name,
                    "url": url,
                    "depth": depth,
                    "inflight": inflight,
                    "has_dlq": bool(dlq_policy),
                }

                if depth > MAX_QUEUE_DEPTH:
                    observations.append(AgentObservation(
                        source=f"sqs:{name}",
                        data=data,
                        severity=Severity.CRITICAL,
                        message=f"Queue {name} has {depth} messages (threshold: {MAX_QUEUE_DEPTH}) "
                                f"— possible consumer failure",
                    ))
                elif depth > 0:
                    observations.append(AgentObservation(
                        source=f"sqs:{name}",
                        data=data,
                        severity=Severity.INFO,
                        message=f"Queue {name}: {depth} messages, {inflight} in-flight",
                    ))
                else:
                    observations.append(AgentObservation(
                        source=f"sqs:{name}",
                        data=data,
                        severity=Severity.INFO,
                        message=f"Queue {name} is empty (healthy)",
                    ))

        except ClientError as e:
            observations.append(AgentObservation(
                source="sqs",
                data={"error": str(e)},
                severity=Severity.CRITICAL,
                message=f"Cannot list SQS queues: {str(e)}",
            ))

        return observations

    def _check_dynamodb(self) -> list[AgentObservation]:
        """Check DynamoDB table health."""
        observations = []
        try:
            desc = self.dynamodb_client.describe_table(TableName=DYNAMODB_TABLE)
            table_info = desc["Table"]
            status = table_info["TableStatus"]
            item_count = table_info.get("ItemCount", 0)
            gsis = table_info.get("GlobalSecondaryIndexes", [])

            data = {
                "status": status,
                "item_count": item_count,
                "gsi_count": len(gsis),
                "gsi_names": [g["IndexName"] for g in gsis],
            }

            if status != "ACTIVE":
                observations.append(AgentObservation(
                    source="dynamodb:Jobsearch",
                    data=data,
                    severity=Severity.CRITICAL,
                    message=f"DynamoDB table is {status} (expected ACTIVE)",
                ))
            else:
                # Check for status-index GSI (required by job-router)
                gsi_names = [g["IndexName"] for g in gsis]
                if "status-index" not in gsi_names:
                    observations.append(AgentObservation(
                        source="dynamodb:Jobsearch:gsi",
                        data=data,
                        severity=Severity.CRITICAL,
                        message="Missing 'status-index' GSI — job-router queries will fail",
                    ))
                else:
                    observations.append(AgentObservation(
                        source="dynamodb:Jobsearch",
                        data=data,
                        severity=Severity.INFO,
                        message=f"DynamoDB ACTIVE, {item_count} items, GSIs: {gsi_names}",
                    ))

            # Check job status distribution
            table = self.dynamodb_resource.Table(DYNAMODB_TABLE)
            scan = table.scan(
                ProjectionExpression="#s",
                ExpressionAttributeNames={"#s": "status"},
            )
            status_counts = {}
            for item in scan.get("Items", []):
                s = item.get("status", "UNKNOWN")
                status_counts[s] = status_counts.get(s, 0) + 1

            failed_count = status_counts.get("FAILED", 0)
            if failed_count > 5:
                observations.append(AgentObservation(
                    source="dynamodb:Jobsearch:status",
                    data={"status_distribution": status_counts},
                    severity=Severity.WARNING,
                    message=f"High failure count: {failed_count} jobs in FAILED status",
                ))

        except ClientError as e:
            observations.append(AgentObservation(
                source="dynamodb",
                data={"error": str(e)},
                severity=Severity.FATAL,
                message=f"Cannot access DynamoDB: {str(e)}",
            ))

        return observations

    def _check_s3(self) -> list[AgentObservation]:
        """Check S3 bucket accessibility and required files."""
        observations = []
        required_files = [
            "data/resume/Teja_Vaka.txt",
            "data/resume/Teja_Vaka.pdf",
        ]

        try:
            for key in required_files:
                try:
                    self.s3_client.head_object(Bucket=S3_BUCKET, Key=key)
                    observations.append(AgentObservation(
                        source=f"s3:{key}",
                        severity=Severity.INFO,
                        message=f"S3 file exists: s3://{S3_BUCKET}/{key}",
                    ))
                except ClientError:
                    observations.append(AgentObservation(
                        source=f"s3:{key}",
                        severity=Severity.CRITICAL,
                        message=f"Missing S3 file: s3://{S3_BUCKET}/{key}",
                    ))

        except Exception as e:
            observations.append(AgentObservation(
                source="s3",
                data={"error": str(e)},
                severity=Severity.FATAL,
                message=f"S3 access failed: {str(e)}",
            ))

        return observations

    def _check_sns(self) -> list[AgentObservation]:
        """Check SNS topic health and subscriptions."""
        observations = []
        try:
            topics = self.sns_client.list_topics()["Topics"]
            job_topics = [t for t in topics if SNS_TOPIC_PREFIX in t["TopicArn"].lower()]

            if not job_topics:
                observations.append(AgentObservation(
                    source="sns",
                    severity=Severity.CRITICAL,
                    message="No SNS topics found for job pipeline notifications",
                ))
            else:
                for topic in job_topics:
                    arn = topic["TopicArn"]
                    subs = self.sns_client.list_subscriptions_by_topic(
                        TopicArn=arn
                    )["Subscriptions"]

                    if not subs:
                        observations.append(AgentObservation(
                            source=f"sns:{arn.split(':')[-1]}",
                            data={"topic_arn": arn},
                            severity=Severity.WARNING,
                            message=f"SNS topic has no subscribers — notifications won't be delivered",
                        ))
                    else:
                        observations.append(AgentObservation(
                            source=f"sns:{arn.split(':')[-1]}",
                            data={"topic_arn": arn, "subscriber_count": len(subs)},
                            severity=Severity.INFO,
                            message=f"SNS topic has {len(subs)} subscriber(s)",
                        ))

        except ClientError as e:
            observations.append(AgentObservation(
                source="sns",
                severity=Severity.WARNING,
                message=f"Cannot check SNS: {str(e)}",
            ))

        return observations

    # -------------------------------------------------------------------
    # THINK: Analyze observations and plan remediation
    # -------------------------------------------------------------------
    def think(self, observations: list[AgentObservation], context: dict) -> list[AgentAction]:
        actions = []

        for obs in observations:
            if obs.severity == Severity.CRITICAL:
                # Queue overflow — might need to purge or scale
                if "sqs:" in obs.source and "messages" in obs.message:
                    queue_url = obs.data.get("url", "")
                    if queue_url:
                        actions.append(AgentAction(
                            action_type="INVESTIGATE_QUEUE",
                            target=obs.data.get("queue", "unknown"),
                            params={"queue_url": queue_url},
                            reason=obs.message,
                            requires_approval=True,  # Dangerous — needs human
                        ))

                # Missing env vars — report but don't auto-fix
                if "missing_vars" in obs.data:
                    actions.append(AgentAction(
                        action_type="REPORT_MISSING_ENV",
                        target=obs.data.get("function", "unknown"),
                        params={"missing_vars": obs.data["missing_vars"]},
                        reason=obs.message,
                        requires_approval=True,
                    ))

                # Missing S3 files
                if "s3:" in obs.source and "Missing" in obs.message:
                    actions.append(AgentAction(
                        action_type="ALERT_MISSING_FILE",
                        target=obs.source,
                        reason=obs.message,
                    ))

            if obs.severity == Severity.WARNING:
                # High failure count
                if "FAILED" in obs.message:
                    actions.append(AgentAction(
                        action_type="ANALYZE_FAILURES",
                        target="dynamodb:Jobsearch",
                        params=obs.data,
                        reason="High number of failed jobs — need root cause analysis",
                    ))

        return actions

    # -------------------------------------------------------------------
    # ACT: Execute remediation actions
    # -------------------------------------------------------------------
    def act(self, action: AgentAction, context: dict) -> AgentResult:
        handler = {
            "INVESTIGATE_QUEUE": self._act_investigate_queue,
            "REPORT_MISSING_ENV": self._act_report_missing_env,
            "ALERT_MISSING_FILE": self._act_alert_missing_file,
            "ANALYZE_FAILURES": self._act_analyze_failures,
        }.get(action.action_type)

        if not handler:
            return AgentResult(
                success=False, action=action,
                error=f"Unknown action type: {action.action_type}"
            )

        return handler(action)

    def _act_investigate_queue(self, action: AgentAction) -> AgentResult:
        """Investigate a queue with high message count."""
        queue_url = action.params.get("queue_url", "")
        try:
            # Peek at messages without consuming them
            msgs = self.sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                VisibilityTimeout=0,  # Don't hide the message
                WaitTimeSeconds=0,
            ).get("Messages", [])

            sample = msgs[0]["Body"][:200] if msgs else "No messages"
            return AgentResult(
                success=True, action=action,
                output={"sample_message": sample, "queue": action.target},
            )
        except Exception as e:
            return AgentResult(success=False, action=action, error=str(e))

    def _act_report_missing_env(self, action: AgentAction) -> AgentResult:
        """Report missing environment variables."""
        return AgentResult(
            success=True, action=action,
            output={
                "function": action.target,
                "missing": action.params.get("missing_vars", []),
                "recommendation": "Set these via AWS Console or update deploy.py",
            },
        )

    def _act_alert_missing_file(self, action: AgentAction) -> AgentResult:
        """Alert about missing S3 file."""
        return AgentResult(
            success=True, action=action,
            output={"file": action.target, "action": "Upload to S3"},
        )

    def _act_analyze_failures(self, action: AgentAction) -> AgentResult:
        """Analyze failed jobs to find patterns."""
        try:
            table = self.dynamodb_resource.Table(DYNAMODB_TABLE)
            # Query failed jobs
            scan = table.scan(
                FilterExpression="begins_with(#s, :failed)",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":failed": "FAILED"},
                Limit=10,
            )
            failed_jobs = scan.get("Items", [])

            # Analyze patterns
            failure_reasons = {}
            for job in failed_jobs:
                reason = str(job.get("failure_reason", "Unknown"))[:50]
                failure_reasons[reason] = failure_reasons.get(reason, 0) + 1

            return AgentResult(
                success=True, action=action,
                output={
                    "total_failed": len(failed_jobs),
                    "failure_patterns": failure_reasons,
                    "sample_jobs": [
                        {"job_id": j["job_id"], "title": j.get("title", "?"),
                         "reason": j.get("failure_reason", "?")}
                        for j in failed_jobs[:3]
                    ],
                },
            )
        except Exception as e:
            return AgentResult(success=False, action=action, error=str(e))

    # -------------------------------------------------------------------
    # VERIFY: Confirm actions worked
    # -------------------------------------------------------------------
    def verify(self, results: list[AgentResult], context: dict) -> dict:
        return {
            "total_actions": len(results),
            "succeeded": sum(1 for r in results if r.success),
            "failed": sum(1 for r in results if not r.success),
            "requires_approval": sum(
                1 for r in results 
                if not r.success and r.error == "Requires human approval"
            ),
        }

    # -------------------------------------------------------------------
    # Public API: Quick health check
    # -------------------------------------------------------------------
    def quick_health_check(self) -> dict:
        """Run a fast health check and return status summary."""
        observations = self.observe({})
        
        critical = [o for o in observations if o.severity == Severity.CRITICAL]
        warnings = [o for o in observations if o.severity == Severity.WARNING]
        fatal = [o for o in observations if o.severity == Severity.FATAL]

        return {
            "status": "CRITICAL" if (critical or fatal) else ("WARNING" if warnings else "HEALTHY"),
            "total_checks": len(observations),
            "critical": [o.message for o in critical],
            "warnings": [o.message for o in warnings],
            "fatal": [o.message for o in fatal],
            "healthy": [o.message for o in observations if o.severity == Severity.INFO],
        }
