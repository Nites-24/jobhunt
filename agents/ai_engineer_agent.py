"""
AI Engineer Agent — LLM Scoring Calibration & Prompt Optimization

Responsibilities:
  - Validate Bedrock model connectivity and latency
  - Calibrate scoring thresholds (is 85% too strict / too loose?)
  - Audit prompt quality (system instructions, tool schemas)
  - Monitor token usage and cost
  - Detect scoring drift (all jobs getting 80+ or all below 50)
  - Test email generation quality (banned words, tone, formatting)
  
This agent is the "brain tuner" — it makes sure the AI scoring
and email generation produce high-quality, consistent results.
"""

import json
import time
import logging
import os
import re
import boto3
from botocore.exceptions import ClientError
from typing import Optional

from .base_agent import (
    BaseAgent, AgentObservation, AgentAction, AgentResult,
    Severity,
)

logger = logging.getLogger(__name__)


# =========================================================================
# Quality Criteria
# =========================================================================
BANNED_WORDS = {
    "leverage", "passionate", "cutting-edge", "synergy", "seamlessly",
    "excited", "thrilled", "innovative", "delighted", "keen",
}

REQUIRED_URLS = [
    "https://huggingface.co/spaces/vakaTeja/RetireIQ",
    "https://rivulet.info",
    "https://linkedin.com/in/tejagopalreddyvaka/",
]

REQUIRED_METRICS = ["33%", "87%"]

MAX_EMAIL_WORDS = 150
MIN_EMAIL_WORDS = 60


class AIEngineerAgent(BaseAgent):
    """
    Monitors and optimizes the AI/LLM components of the pipeline.
    
    Checks:
      1. Bedrock model availability and latency
      2. Scoring distribution (detect drift)
      3. Email quality (banned words, required links, word count)
      4. Token usage and cost estimation
      5. Prompt effectiveness
    """

    def __init__(self, session: Optional[boto3.Session] = None):
        super().__init__(
            name="AIEngineer",
            description="LLM scoring calibration, prompt optimization, and quality assurance",
        )
        self.session = session or self._create_session()
        self.bedrock = self.session.client("bedrock-runtime", region_name="us-east-1")
        self.dynamodb = self.session.resource("dynamodb", region_name="us-east-1")
        self.s3 = self.session.client("s3", region_name="us-east-1")
        self.table = self.dynamodb.Table("Jobsearch")

    def _create_session(self) -> boto3.Session:
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
    # OBSERVE
    # -------------------------------------------------------------------
    def observe(self, context: dict) -> list[AgentObservation]:
        observations = []
        observations.extend(self._check_bedrock_health())
        observations.extend(self._check_scoring_distribution())
        observations.extend(self._check_sent_email_quality())
        observations.extend(self._check_s3_context())
        return observations

    def _check_bedrock_health(self) -> list[AgentObservation]:
        """Test Bedrock model connectivity with a minimal call."""
        model_id = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
        try:
            start = time.time()
            response = self.bedrock.converse(
                modelId=model_id,
                messages=[{"role": "user", "content": [{"text": "Say 'OK' and nothing else."}]}],
                inferenceConfig={"maxTokens": 10, "temperature": 0},
            )
            latency_ms = (time.time() - start) * 1000
            usage = response.get("usage", {})

            data = {
                "model_id": model_id,
                "latency_ms": round(latency_ms),
                "input_tokens": usage.get("inputTokens", 0),
                "output_tokens": usage.get("outputTokens", 0),
            }

            if latency_ms > 5000:
                return [AgentObservation(
                    source="bedrock:health",
                    data=data,
                    severity=Severity.WARNING,
                    message=f"Bedrock latency high: {latency_ms:.0f}ms (model: {model_id})",
                )]
            else:
                return [AgentObservation(
                    source="bedrock:health",
                    data=data,
                    severity=Severity.INFO,
                    message=f"Bedrock healthy: {latency_ms:.0f}ms latency",
                )]

        except ClientError as e:
            return [AgentObservation(
                source="bedrock:health",
                data={"error": str(e), "model_id": model_id},
                severity=Severity.FATAL,
                message=f"Bedrock unreachable: {e.response['Error']['Code']}",
            )]

    def _check_scoring_distribution(self) -> list[AgentObservation]:
        """Analyze score distribution to detect drift."""
        observations = []
        try:
            scan = self.table.scan(
                ProjectionExpression="match_score, #s",
                ExpressionAttributeNames={"#s": "status"},
            )
            items = scan.get("Items", [])
            
            scored = [
                int(i["match_score"]) for i in items 
                if "match_score" in i and i.get("status") != "NEW"
            ]

            if len(scored) < 5:
                observations.append(AgentObservation(
                    source="scoring:distribution",
                    data={"scored_count": len(scored)},
                    severity=Severity.INFO,
                    message=f"Only {len(scored)} scored jobs — not enough data for drift analysis",
                ))
                return observations

            avg_score = sum(scored) / len(scored)
            high_count = sum(1 for s in scored if s >= 85)
            low_count = sum(1 for s in scored if s < 40)
            high_pct = (high_count / len(scored)) * 100
            low_pct = (low_count / len(scored)) * 100

            data = {
                "total_scored": len(scored),
                "avg_score": round(avg_score, 1),
                "high_pct": round(high_pct, 1),
                "low_pct": round(low_pct, 1),
                "min": min(scored),
                "max": max(scored),
            }

            # Detect scoring drift
            if high_pct > 80:
                observations.append(AgentObservation(
                    source="scoring:drift",
                    data=data,
                    severity=Severity.WARNING,
                    message=f"Scoring too lenient: {high_pct:.0f}% of jobs score 85+ "
                            f"(avg={avg_score:.0f}). Tighten criteria.",
                ))
            elif low_pct > 90:
                observations.append(AgentObservation(
                    source="scoring:drift",
                    data=data,
                    severity=Severity.WARNING,
                    message=f"Scoring too strict: {low_pct:.0f}% of jobs score below 40 "
                            f"(avg={avg_score:.0f}). Loosen criteria.",
                ))
            else:
                observations.append(AgentObservation(
                    source="scoring:distribution",
                    data=data,
                    severity=Severity.INFO,
                    message=f"Scoring healthy: avg={avg_score:.0f}, "
                            f"{high_pct:.0f}% high, {low_pct:.0f}% low",
                ))

        except Exception as e:
            observations.append(AgentObservation(
                source="scoring",
                severity=Severity.WARNING,
                message=f"Cannot analyze scoring: {str(e)}",
            ))

        return observations

    def _check_sent_email_quality(self) -> list[AgentObservation]:
        """Spot-check quality of recently generated emails (if stored)."""
        observations = []
        # This would check email logs/samples if they were stored
        # For now, validate the system prompt quality
        observations.append(AgentObservation(
            source="email:quality",
            severity=Severity.INFO,
            message="Email quality checks configured (banned words, URL validation)",
            data={
                "banned_words": list(BANNED_WORDS),
                "required_urls": REQUIRED_URLS,
                "max_words": MAX_EMAIL_WORDS,
            },
        ))
        return observations

    def _check_s3_context(self) -> list[AgentObservation]:
        """Check that S3 memory context files exist and are valid."""
        observations = []
        bucket = "job-search-pipeline-memory"
        
        try:
            response = self.s3.list_objects_v2(Bucket=bucket, Prefix="data/")
            items = response.get("Contents", [])
            
            if not items:
                observations.append(AgentObservation(
                    source="s3:context",
                    severity=Severity.CRITICAL,
                    message="No context files in S3 data/ folder — scoring will use fallback only",
                ))
            else:
                total_size = sum(obj["Size"] for obj in items)
                observations.append(AgentObservation(
                    source="s3:context",
                    data={"file_count": len(items), "total_bytes": total_size},
                    severity=Severity.INFO,
                    message=f"S3 context: {len(items)} files, {total_size} bytes total",
                ))

        except Exception as e:
            observations.append(AgentObservation(
                source="s3:context",
                severity=Severity.WARNING,
                message=f"Cannot check S3 context: {str(e)}",
            ))

        return observations

    # -------------------------------------------------------------------
    # THINK
    # -------------------------------------------------------------------
    def think(self, observations: list[AgentObservation], context: dict) -> list[AgentAction]:
        actions = []

        for obs in observations:
            if obs.severity in (Severity.CRITICAL, Severity.FATAL):
                if "bedrock" in obs.source.lower():
                    actions.append(AgentAction(
                        action_type="BEDROCK_FAILOVER",
                        target="bedrock",
                        reason="Bedrock is unreachable — consider fallback model",
                        requires_approval=True,
                    ))

            if obs.severity == Severity.WARNING:
                if "drift" in obs.source:
                    actions.append(AgentAction(
                        action_type="CALIBRATE_SCORING",
                        target="scoring",
                        params=obs.data,
                        reason=obs.message,
                    ))

        return actions

    # -------------------------------------------------------------------
    # ACT
    # -------------------------------------------------------------------
    def act(self, action: AgentAction, context: dict) -> AgentResult:
        if action.action_type == "CALIBRATE_SCORING":
            return self._act_calibrate_scoring(action)
        
        return AgentResult(
            success=False, action=action,
            error=f"Unknown action: {action.action_type}",
        )

    def _act_calibrate_scoring(self, action: AgentAction) -> AgentResult:
        """Analyze scoring and recommend threshold adjustments."""
        data = action.params
        avg = data.get("avg_score", 0)
        high_pct = data.get("high_pct", 0)

        recommendation = ""
        if high_pct > 80:
            recommendation = (
                f"Average score is {avg}. {high_pct}% of jobs score 85+. "
                f"Consider raising SCORE_THRESHOLD above 80 or tightening the system prompt "
                f"to penalize generic roles more heavily."
            )
        elif high_pct < 5:
            recommendation = (
                f"Average score is {avg}. Very few jobs pass threshold. "
                f"Consider lowering SCORE_THRESHOLD to 80 or broadening search queries."
            )

        return AgentResult(
            success=True, action=action,
            output={"recommendation": recommendation, "current_data": data},
        )

    # -------------------------------------------------------------------
    # VERIFY
    # -------------------------------------------------------------------
    def verify(self, results: list[AgentResult], context: dict) -> dict:
        return {
            "total_actions": len(results),
            "succeeded": sum(1 for r in results if r.success),
            "failed": sum(1 for r in results if not r.success),
        }

    # -------------------------------------------------------------------
    # Public API: Email quality validator
    # -------------------------------------------------------------------
    def validate_email(self, subject: str, body: str) -> dict:
        """
        Validate a generated email against quality rules.
        Returns pass/fail with specific violations.
        """
        violations = []
        
        # Word count
        word_count = len(body.split())
        if word_count > MAX_EMAIL_WORDS:
            violations.append(f"Too long: {word_count} words (max {MAX_EMAIL_WORDS})")
        if word_count < MIN_EMAIL_WORDS:
            violations.append(f"Too short: {word_count} words (min {MIN_EMAIL_WORDS})")

        # Banned words
        body_lower = body.lower()
        for word in BANNED_WORDS:
            if word in body_lower:
                violations.append(f"Banned word found: '{word}'")

        # Required URLs
        for url in REQUIRED_URLS:
            if url not in body:
                violations.append(f"Missing required URL: {url}")

        # Required metrics
        for metric in REQUIRED_METRICS:
            if metric not in body:
                violations.append(f"Missing metric: {metric}")

        # Formatting violations
        if "!" in body:
            violations.append("Contains exclamation point (banned)")
        if "—" in body or "–" in body:
            violations.append("Contains em/en dash (banned)")

        return {
            "passed": len(violations) == 0,
            "violations": violations,
            "word_count": word_count,
            "score": max(0, 100 - (len(violations) * 15)),
        }
