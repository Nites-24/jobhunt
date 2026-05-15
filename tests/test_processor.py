"""
Tests for job-processor Lambda (Unified Scorer).

Tests:
  1. High-scoring job gets PENDING_APPROVAL + SNS notification
  2. Low-scoring job gets REJECTED
  3. Batch processing with partial failures
  4. S3 context caching works
  5. Bedrock tool-call response parsing
  6. Bedrock error handling (graceful degradation)
"""

import json
import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch, MagicMock
from conftest import SAMPLE_JOB, MockContext, make_sqs_event


@mock_aws
class TestProcessor:
    """End-to-end tests for job-processor (unified scorer)."""

    def _setup_infra(self):
        """Create DynamoDB, S3, and SNS resources."""
        import os
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        os.environ["SCORE_THRESHOLD"] = "80"

        dynamo = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamo.create_table(
            TableName="Jobsearch",
            KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName="Jobsearch")

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="job-search-pipeline-memory")
        s3.put_object(
            Bucket="job-search-pipeline-memory",
            Key="data/resume/Teja_Vaka.txt",
            Body=b"Teja Vaka - AI Engineer, Python, PyTorch, RAG, LLMs",
        )

        sns = boto3.client("sns", region_name="us-east-1")
        topic = sns.create_topic(Name="jobsearch-approvals")

        os.environ["SNS_TOPIC_ARN"] = topic["TopicArn"]

        import importlib
        import job_processor.lambda_function as mod
        mod.dynamodb = dynamo
        mod.s3 = s3
        mod.sns = sns
        mod.table = dynamo.Table("Jobsearch")
        mod.SNS_TOPIC_ARN = topic["TopicArn"]
        mod._cached_context = None  # Reset cache

        # Pre-insert the job so update_item works
        table.put_item(Item={"job_id": SAMPLE_JOB["job_id"], "status": "NEW"})

        return mod, table, s3, sns

    def _mock_bedrock_response(self, score, reason):
        """Create a mock Bedrock Converse API response with tool-call."""
        return {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "toolUseId": "test-tool-use-id",
                                "name": "submit_evaluation",
                                "input": {
                                    "match_score": score,
                                    "match_reason": reason,
                                },
                            }
                        }
                    ]
                }
            },
            "usage": {"inputTokens": 500, "outputTokens": 50},
        }

    @patch("job_processor.lambda_function.bedrock")
    def test_high_score_pending_approval(self, mock_bedrock):
        """Job scoring ≥80 should be set to PENDING_APPROVAL with SNS alert."""
        mod, table, s3, sns = self._setup_infra()
        mock_bedrock.converse.return_value = self._mock_bedrock_response(
            95, "Strong match: Python, PyTorch, RAG experience aligns perfectly."
        )

        event = make_sqs_event([SAMPLE_JOB])
        result = mod.lambda_handler(event, MockContext())

        assert result["batchItemFailures"] == []

        # Check DynamoDB
        item = table.get_item(Key={"job_id": "test-job-001"})["Item"]
        assert item["status"] == "PENDING_APPROVAL"
        assert int(item["match_score"]) == 95
        assert "Strong match" in item["match_reason"]
        assert item["evaluation_profile_version"] == mod.JOB_EVAL_PROFILE_VERSION

    @patch("job_processor.lambda_function.bedrock")
    def test_low_score_rejected(self, mock_bedrock):
        """Job scoring <90 should be set to REJECTED."""
        mod, table, s3, sns = self._setup_infra()
        mock_bedrock.converse.return_value = self._mock_bedrock_response(
            45, "Weak match: role is primarily data analysis, not AI engineering."
        )

        event = make_sqs_event([SAMPLE_JOB])
        result = mod.lambda_handler(event, MockContext())

        assert result["batchItemFailures"] == []

        item = table.get_item(Key={"job_id": "test-job-001"})["Item"]
        assert item["status"] == "REJECTED"
        assert int(item["match_score"]) == 45

    @patch("job_processor.lambda_function.bedrock")
    def test_batch_mixed_scores(self, mock_bedrock):
        """Batch with high and low scoring jobs should be processed independently."""
        mod, table, s3, sns = self._setup_infra()

        job_high = {**SAMPLE_JOB, "job_id": "job-high"}
        job_low = {**SAMPLE_JOB, "job_id": "job-low", "title": "Data Analyst"}

        # Pre-insert both
        table.put_item(Item={"job_id": "job-high", "status": "NEW"})
        table.put_item(Item={"job_id": "job-low", "status": "NEW"})

        # Mock returns different scores based on call order
        mock_bedrock.converse.side_effect = [
            self._mock_bedrock_response(95, "Great match"),
            self._mock_bedrock_response(30, "Poor match"),
        ]

        event = make_sqs_event([job_high, job_low])
        result = mod.lambda_handler(event, MockContext())

        assert result["batchItemFailures"] == []

        assert table.get_item(Key={"job_id": "job-high"})["Item"]["status"] == "PENDING_APPROVAL"
        assert table.get_item(Key={"job_id": "job-low"})["Item"]["status"] == "REJECTED"

    @patch("job_processor.lambda_function.bedrock")
    def test_bedrock_error_graceful_degradation(self, mock_bedrock):
        """Non-retryable Bedrock errors should score 0 and REJECT, not crash the batch."""
        mod, table, s3, sns = self._setup_infra()
        mock_bedrock.converse.side_effect = Exception("Bedrock internal error")

        event = make_sqs_event([SAMPLE_JOB])
        result = mod.lambda_handler(event, MockContext())

        # Should NOT be in failures — the error was handled gracefully
        assert result["batchItemFailures"] == []

        # Job should be REJECTED with score 0
        item = table.get_item(Key={"job_id": "test-job-001"})["Item"]
        assert item["status"] == "REJECTED"
        assert int(item["match_score"]) == 0

    @patch("job_processor.lambda_function.bedrock")
    def test_score_clamped_to_valid_range(self, mock_bedrock):
        """Scores outside 0-100 should be clamped."""
        mod, table, s3, sns = self._setup_infra()
        mock_bedrock.converse.return_value = self._mock_bedrock_response(
            150, "Model hallucinated a score above 100"
        )

        event = make_sqs_event([SAMPLE_JOB])
        result = mod.lambda_handler(event, MockContext())

        item = table.get_item(Key={"job_id": "test-job-001"})["Item"]
        assert int(item["match_score"]) == 100  # Clamped

    @patch("job_processor.lambda_function.bedrock")
    def test_stale_remote_job_is_guardrailed(self, mock_bedrock):
        """A stale remote keyword match should not reach the approval queue."""
        mod, table, s3, sns = self._setup_infra()
        stale_remote = {
            **SAMPLE_JOB,
            "job_id": "stale-remote",
            "company": "Dry Ground AI",
            "location": "Brazil, Colombia, Philippines",
            "source": "remotive",
            "is_remote": True,
            "posted_at": "2026-04-16T12:00:00Z",
        }
        table.put_item(Item={"job_id": "stale-remote", "status": "NEW"})

        mock_bedrock.converse.return_value = self._mock_bedrock_response(
            95, "Strong AI keyword overlap."
        )

        result = mod.lambda_handler(make_sqs_event([stale_remote]), MockContext())

        assert result["batchItemFailures"] == []
        item = table.get_item(Key={"job_id": "stale-remote"})["Item"]
        assert item["status"] == "REJECTED"
        assert int(item["match_score"]) <= 55
        assert "Rejected before LLM scoring" in item["match_reason"]
        mock_bedrock.converse.assert_not_called()

    @patch("job_processor.lambda_function.bedrock")
    def test_hacker_news_jobs_older_than_three_days_are_rejected_before_llm(self, mock_bedrock):
        """HN leads must be fresher than generic sources because startup posts age quickly."""
        mod, table, s3, sns = self._setup_infra()
        hn_stale = {
            **SAMPLE_JOB,
            "job_id": "hn-stale",
            "company": "Cora AI",
            "title": "Applied AI Engineer",
            "source": "hackernews",
            "description": "Build AI agents, knowledge graphs, multi-agent systems, evals, and simulations.",
            "url": "https://news.ycombinator.com/item?id=123",
            "apply_url": "https://news.ycombinator.com/item?id=123",
            "posted_at": "2026-05-09T12:00:00Z",
        }
        table.put_item(Item={"job_id": "hn-stale", "status": "NEW"})

        result = mod.lambda_handler(make_sqs_event([hn_stale]), MockContext())

        assert result["batchItemFailures"] == []
        item = table.get_item(Key={"job_id": "hn-stale"})["Item"]
        assert item["status"] == "REJECTED"
        assert int(item["match_score"]) <= 55
        assert "source limit 3 days" in item["match_reason"]
        mock_bedrock.converse.assert_not_called()

    @patch("job_processor.lambda_function.bedrock")
    def test_hard_guardrails_stay_below_lower_threshold(self, mock_bedrock):
        """Hard rejections must not become pending when production threshold is lower."""
        mod, table, s3, sns = self._setup_infra()
        mod.SCORE_THRESHOLD = 72
        missing_source = {
            **SAMPLE_JOB,
            "job_id": "missing-source-low-threshold",
            "location": "Remote, Brazil",
            "is_remote": True,
            "url": "",
            "apply_url": "",
            "posted_at": "2026-05-12T12:00:00Z",
        }
        table.put_item(Item={"job_id": "missing-source-low-threshold", "status": "NEW"})

        result = mod.lambda_handler(make_sqs_event([missing_source]), MockContext())

        assert result["batchItemFailures"] == []
        item = table.get_item(Key={"job_id": "missing-source-low-threshold"})["Item"]
        assert item["status"] == "REJECTED"
        assert int(item["match_score"]) == 40
        assert "missing verifiable source link" in item["match_reason"]
        mock_bedrock.converse.assert_not_called()
        mod.SCORE_THRESHOLD = 80

    @patch("job_processor.lambda_function.bedrock")
    def test_missing_source_link_is_rejected_before_llm(self, mock_bedrock):
        """Jobs without a verifiable source URL should never enter the review queue."""
        mod, table, s3, sns = self._setup_infra()
        no_source = {
            **SAMPLE_JOB,
            "job_id": "no-source",
            "url": "",
            "apply_url": "",
            "posted_at": "2026-05-12T12:00:00Z",
        }
        table.put_item(Item={"job_id": "no-source", "status": "NEW"})

        result = mod.lambda_handler(make_sqs_event([no_source]), MockContext())

        assert result["batchItemFailures"] == []
        item = table.get_item(Key={"job_id": "no-source"})["Item"]
        assert item["status"] == "REJECTED"
        assert int(item["match_score"]) <= 40
        assert "missing verifiable source link" in item["match_reason"]
        mock_bedrock.converse.assert_not_called()

    @patch("job_processor.lambda_function._find_hunter_email", return_value="")
    @patch("job_processor.lambda_function._send_match_notification")
    @patch("job_processor.lambda_function.bedrock")
    def test_signal_test_seed_bypasses_llm_and_notifies(self, mock_bedrock, mock_notify, mock_hunter):
        """Synthetic signal-test jobs should produce a deterministic high-match notification."""
        mod, table, s3, sns = self._setup_infra()
        signal_job = {
            **SAMPLE_JOB,
            "job_id": "signal-test-001",
            "title": "AI Engineer Notification Test",
            "company": "Signal Test AI",
            "location": "Wichita, Kansas",
            "url": "https://example.com/signal-test-ai-engineer",
            "apply_url": "https://example.com/signal-test-ai-engineer",
            "source": "signal_test",
            "work_mode": "onsite",
            "work_mode_source": "location",
            "signal_test": True,
        }
        table.put_item(Item={"job_id": "signal-test-001", "status": "NEW"})

        result = mod.lambda_handler(make_sqs_event([signal_job]), MockContext())

        assert result["batchItemFailures"] == []
        item = table.get_item(Key={"job_id": "signal-test-001"})["Item"]
        assert item["status"] == "PENDING_APPROVAL"
        assert int(item["match_score"]) == 96
        mock_bedrock.converse.assert_not_called()
        mock_notify.assert_called_once()

    @patch("job_processor.lambda_function.bedrock")
    def test_negated_ai_signal_is_rejected_before_llm(self, mock_bedrock):
        """A job saying it has no AI work should not pass keyword matching."""
        mod, table, s3, sns = self._setup_infra()
        frontend_role = {
            **SAMPLE_JOB,
            "job_id": "frontend-no-ai",
            "title": "Frontend Developer",
            "description": "Build React marketing pages and CMS templates. No AI or ML work.",
            "url": "https://jobs.example.com/frontend-no-ai",
            "apply_url": "https://jobs.example.com/frontend-no-ai",
            "posted_at": "2026-05-12T12:00:00Z",
        }
        table.put_item(Item={"job_id": "frontend-no-ai", "status": "NEW"})

        result = mod.lambda_handler(make_sqs_event([frontend_role]), MockContext())

        assert result["batchItemFailures"] == []
        item = table.get_item(Key={"job_id": "frontend-no-ai"})["Item"]
        assert item["status"] == "REJECTED"
        assert int(item["match_score"]) <= 50
        assert "insufficient AI/ML signal" in item["match_reason"]
        mock_bedrock.converse.assert_not_called()

    def test_s3_context_building(self):
        """Processor context should be compacted into a candidate graph."""
        mod, table, s3, sns = self._setup_infra()

        context = mod._build_candidate_graph_context()
        assert "<candidate_graph>" in context
        assert "candidate:" in context
        assert "skills:" in context
        assert "RetireIQ" in context

    @patch("job_processor.lambda_function._send_match_notification")
    @patch("job_processor.lambda_function.bedrock")
    def test_processor_logs_source_query_yield_summary(self, mock_bedrock, mock_notify):
        """Batch completion should log compact per-source/query scoring outcomes."""
        mod, table, s3, sns = self._setup_infra()
        job_high = {**SAMPLE_JOB, "job_id": "job-high", "source": "adzuna", "source_search_query": "AI Engineer"}
        job_low = {**SAMPLE_JOB, "job_id": "job-low", "source": "hackernews", "source_search_query": "hn_who_is_hiring"}
        table.put_item(Item={"job_id": "job-high", "status": "NEW"})
        table.put_item(Item={"job_id": "job-low", "status": "NEW"})
        mock_bedrock.converse.side_effect = [
            self._mock_bedrock_response(95, "Great match"),
            self._mock_bedrock_response(30, "Poor match"),
        ]

        with patch.object(mod.logger, "info") as mock_info:
            mod.lambda_handler(make_sqs_event([job_high, job_low]), MockContext())

        summary_call = next(
            call for call in mock_info.call_args_list
            if call.args and call.args[0] == "PROCESSOR_YIELD_BY_SOURCE_QUERY | %s"
        )
        payload = json.loads(summary_call.args[1])
        assert payload == [
            {"source": "adzuna", "query": "AI Engineer", "evaluated": 1, "matched": 1, "rejected": 0, "failed": 0},
            {"source": "hackernews", "query": "hn_who_is_hiring", "evaluated": 1, "matched": 0, "rejected": 1, "failed": 0},
        ]
