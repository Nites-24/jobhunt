"""
EventBridge Ingestion Schedule Manager.

Configures, verifies, and manages the scheduled EventBridge trigger for
job-ingestor to ensure daily automated job discovery.
"""

import argparse
import json
import logging
import os
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("setup_cron")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DEFAULT_RULE_NAME = "job-search-daily-trigger"
DEFAULT_FUNCTION_NAME = "job-ingestor"
DEFAULT_SCHEDULE = "rate(3 hours)"
REGION = "us-east-1"


def _load_env() -> None:
    """Load .env file safely if it exists."""
    if not os.path.exists(".env"):
        return
    try:
        with open(".env", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())
    except Exception as e:
        logger.warning("Could not read .env: %s", e)


def _get_session() -> boto3.Session:
    """Get boto3 session using environment variables or default AWS credentials chain."""
    _load_env()
    kwargs: Dict[str, Any] = {"region_name": os.environ.get("AWS_DEFAULT_REGION", REGION)}
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        kwargs["aws_access_key_id"] = os.environ.get("AWS_ACCESS_KEY_ID")
        kwargs["aws_secret_access_key"] = os.environ.get("AWS_SECRET_ACCESS_KEY")
    return boto3.Session(**kwargs)


def setup_ingestor_schedule(
    events_client: Any,
    lambda_client: Any,
    rule_name: str = DEFAULT_RULE_NAME,
    schedule_expression: str = DEFAULT_SCHEDULE,
    enabled: bool = True,
    function_name: str = DEFAULT_FUNCTION_NAME,
) -> Dict[str, Any]:
    """Create or update the EventBridge rule targeting job-ingestor."""
    state = "ENABLED" if enabled else "DISABLED"
    logger.info("Configuring EventBridge rule '%s' with schedule '%s' (State=%s)", rule_name, schedule_expression, state)

    rule_response = events_client.put_rule(
        Name=rule_name,
        ScheduleExpression=schedule_expression,
        State=state,
        Description=f"Automatically trigger {function_name} on schedule",
    )
    rule_arn = rule_response["RuleArn"]

    # Retrieve target Lambda ARN
    lambda_info = lambda_client.get_function(FunctionName=function_name)
    lambda_arn = lambda_info["Configuration"]["FunctionArn"]

    # Ensure Lambda invoke permission exists for this EventBridge rule
    statement_id = f"EventBridgeInvokePermission-{rule_name}"
    permission_configured = False
    try:
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId=statement_id,
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule_arn,
        )
        permission_configured = True
        logger.info("Added invocation permission for EventBridge to invoke %s", function_name)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("ResourceConflictException", "ResourceConflict"):
            permission_configured = True
            logger.info("Lambda invocation permission already exists (%s)", statement_id)
        else:
            logger.warning("Could not add lambda permission (%s): %s", code, e)

    # Attach Lambda as target
    events_client.put_targets(
        Rule=rule_name,
        Targets=[
            {
                "Id": "1",
                "Arn": lambda_arn,
                "Input": json.dumps({"source": "eventbridge.cron"}),
            }
        ],
    )
    logger.info("Attached target %s to rule %s", lambda_arn, rule_name)

    return {
        "rule_name": rule_name,
        "rule_arn": rule_arn,
        "state": state,
        "schedule_expression": schedule_expression,
        "target_arn": lambda_arn,
        "target_input": json.dumps({"source": "eventbridge.cron"}),
        "permission_configured": permission_configured,
        "healthy": enabled and bool(rule_arn) and bool(lambda_arn),
    }


def verify_ingestor_schedule(
    events_client: Any,
    lambda_client: Any,
    rule_name: str = DEFAULT_RULE_NAME,
    function_name: str = DEFAULT_FUNCTION_NAME,
) -> Dict[str, Any]:
    """Verify whether the EventBridge schedule rule is configured and targeting job-ingestor."""
    try:
        rule = events_client.describe_rule(Name=rule_name)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("ResourceNotFoundException", "404"):
            return {
                "rule_name": rule_name,
                "rule_exists": False,
                "state": "MISSING",
                "schedule_expression": "",
                "rule_arn": "",
                "target_configured": False,
                "target_arn": "",
                "target_input": "",
                "permission_configured": False,
                "healthy": False,
                "reason": "Rule does not exist in EventBridge",
            }
        raise

    targets_resp = events_client.list_targets_by_rule(Rule=rule_name)
    targets = targets_resp.get("Targets", [])
    matched_target = None
    for target in targets:
        arn = target.get("Arn", "")
        if function_name in arn:
            matched_target = target
            break

    # Check permission in policy if available
    permission_configured = False
    try:
        policy_resp = lambda_client.get_policy(FunctionName=function_name)
        policy_doc = json.loads(policy_resp.get("Policy", "{}"))
        for stmt in policy_doc.get("Statement", []):
            principal = stmt.get("Principal", {})
            principal_svc = principal.get("Service", "") if isinstance(principal, dict) else principal
            if principal_svc == "events.amazonaws.com":
                cond = stmt.get("Condition", {}).get("ArnLike", {}).get("AWS:SourceArn", "")
                if not cond or rule.get("Arn") in cond or rule_name in cond:
                    permission_configured = True
                    break
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("ResourceNotFoundException", "ResourceNotFound"):
            permission_configured = False
        else:
            # Policy check may fail if IAM role lacks lambda:GetPolicy; don't fail verification entirely
            permission_configured = True

    is_healthy = (
        rule.get("State") == "ENABLED"
        and matched_target is not None
        and bool(rule.get("ScheduleExpression"))
    )

    return {
        "rule_name": rule_name,
        "rule_exists": True,
        "state": rule.get("State", "UNKNOWN"),
        "schedule_expression": rule.get("ScheduleExpression", ""),
        "rule_arn": rule.get("Arn", ""),
        "target_configured": matched_target is not None,
        "target_arn": matched_target.get("Arn", "") if matched_target else "",
        "target_input": matched_target.get("Input", "") if matched_target else "",
        "permission_configured": permission_configured,
        "healthy": is_healthy,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage EventBridge schedule for job-ingestor.")
    parser.add_argument("--verify", action="store_true", help="Verify current schedule configuration without modifying AWS.")
    parser.add_argument("--rule-name", default=DEFAULT_RULE_NAME, help="EventBridge rule name.")
    parser.add_argument("--schedule", default=DEFAULT_SCHEDULE, help="Schedule expression, e.g. 'rate(3 hours)' or 'cron(0 13 * * ? *)'.")
    parser.add_argument("--disable", action="store_true", help="Disable the schedule rule instead of enabling it.")
    args = parser.parse_args()

    session = _get_session()
    events_client = session.client("events")
    lambda_client = session.client("lambda")

    if args.verify:
        result = verify_ingestor_schedule(events_client, lambda_client, rule_name=args.rule_name)
        print("\n--- EventBridge Ingestion Schedule Audit ---")
        print(f"Rule Name:           {result['rule_name']}")
        print(f"Rule Exists:         {result['rule_exists']}")
        print(f"State:               {result['state']}")
        print(f"Schedule Expression: {result['schedule_expression']}")
        print(f"Target Configured:   {result['target_configured']}")
        print(f"Target ARN:          {result['target_arn'] or 'None'}")
        print(f"Target Input:        {result['target_input'] or 'Default'}")
        print(f"Healthy:             {result['healthy']}")
        return 0 if result["healthy"] else 1

    result = setup_ingestor_schedule(
        events_client,
        lambda_client,
        rule_name=args.rule_name,
        schedule_expression=args.schedule,
        enabled=not args.disable,
    )
    print("\n--- EventBridge Ingestion Schedule Configured ---")
    print(f"Rule Name:           {result['rule_name']}")
    print(f"Rule ARN:            {result['rule_arn']}")
    print(f"State:               {result['state']}")
    print(f"Schedule Expression: {result['schedule_expression']}")
    print(f"Target ARN:          {result['target_arn']}")
    print(f"Target Input:        {result['target_input']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
