"""
Unit tests for EventBridge scheduled ingestion configuration and verification.
"""

import io
import json
import zipfile
import boto3
from moto import mock_aws
from tools.ops.setup_cron import setup_ingestor_schedule, verify_ingestor_schedule


def _create_mock_lambda(lambda_client, function_name="job-ingestor"):
    """Create a minimal dummy Lambda function in moto."""
    iam = boto3.client("iam", region_name="us-east-1")
    assume_role_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    })
    try:
        role = iam.create_role(
            RoleName="job-ingestor-role",
            AssumeRolePolicyDocument=assume_role_policy,
        )
        role_arn = role["Role"]["Arn"]
    except Exception:
        role_arn = iam.get_role(RoleName="job-ingestor-role")["Role"]["Arn"]

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", "def lambda_handler(e, c): return {'status': 'ok'}")
    buf.seek(0)

    resp = lambda_client.create_function(
        FunctionName=function_name,
        Runtime="python3.11",
        Role=role_arn,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": buf.read()},
    )
    return resp["FunctionArn"]


@mock_aws
def test_setup_ingestor_schedule_creates_rule_and_target():
    events_client = boto3.client("events", region_name="us-east-1")
    lambda_client = boto3.client("lambda", region_name="us-east-1")
    lambda_arn = _create_mock_lambda(lambda_client, "job-ingestor")

    result = setup_ingestor_schedule(
        events_client,
        lambda_client,
        rule_name="job-search-daily-trigger",
        schedule_expression="rate(3 hours)",
        enabled=True,
        function_name="job-ingestor",
    )

    assert result["rule_name"] == "job-search-daily-trigger"
    assert result["state"] == "ENABLED"
    assert result["schedule_expression"] == "rate(3 hours)"
    assert result["target_arn"] == lambda_arn
    assert json.loads(result["target_input"]) == {"source": "eventbridge.cron"}
    assert result["healthy"] is True

    # Verify directly via AWS clients
    rule = events_client.describe_rule(Name="job-search-daily-trigger")
    assert rule["State"] == "ENABLED"
    assert rule["ScheduleExpression"] == "rate(3 hours)"

    targets = events_client.list_targets_by_rule(Rule="job-search-daily-trigger")["Targets"]
    assert len(targets) == 1
    assert targets[0]["Arn"] == lambda_arn
    assert json.loads(targets[0]["Input"]) == {"source": "eventbridge.cron"}


@mock_aws
def test_setup_ingestor_schedule_is_idempotent():
    """Subsequent runs should update cleanly without failing on permission conflict."""
    events_client = boto3.client("events", region_name="us-east-1")
    lambda_client = boto3.client("lambda", region_name="us-east-1")
    _create_mock_lambda(lambda_client, "job-ingestor")

    res1 = setup_ingestor_schedule(events_client, lambda_client)
    assert res1["healthy"] is True

    # Second invocation updates or re-applies cleanly
    res2 = setup_ingestor_schedule(events_client, lambda_client, schedule_expression="rate(6 hours)")
    assert res2["healthy"] is True
    assert res2["schedule_expression"] == "rate(6 hours)"


@mock_aws
def test_verify_ingestor_schedule_missing():
    events_client = boto3.client("events", region_name="us-east-1")
    lambda_client = boto3.client("lambda", region_name="us-east-1")

    res = verify_ingestor_schedule(events_client, lambda_client, rule_name="non-existent-rule")
    assert res["rule_exists"] is False
    assert res["state"] == "MISSING"
    assert res["healthy"] is False


@mock_aws
def test_verify_ingestor_schedule_healthy():
    events_client = boto3.client("events", region_name="us-east-1")
    lambda_client = boto3.client("lambda", region_name="us-east-1")
    lambda_arn = _create_mock_lambda(lambda_client, "job-ingestor")

    setup_ingestor_schedule(events_client, lambda_client, rule_name="job-search-daily-trigger")

    status = verify_ingestor_schedule(events_client, lambda_client, rule_name="job-search-daily-trigger")
    assert status["rule_exists"] is True
    assert status["state"] == "ENABLED"
    assert status["target_configured"] is True
    assert status["target_arn"] == lambda_arn
    assert status["healthy"] is True


@mock_aws
def test_verify_ingestor_schedule_disabled():
    events_client = boto3.client("events", region_name="us-east-1")
    lambda_client = boto3.client("lambda", region_name="us-east-1")
    _create_mock_lambda(lambda_client, "job-ingestor")

    setup_ingestor_schedule(events_client, lambda_client, enabled=False)

    status = verify_ingestor_schedule(events_client, lambda_client)
    assert status["rule_exists"] is True
    assert status["state"] == "DISABLED"
    assert status["healthy"] is False
