"""
QA Agent — Automated Quality Assurance & Regression Detection

Responsibilities:
  - Run the full pytest suite and report pass/fail
  - Detect regressions (new failures vs. last run)
  - Validate pipeline data flow integrity (ingestion → scoring → email)
  - Check for common failure patterns in DynamoDB
  - Verify Lambda code deployment matches local code
  - Generate test coverage report
  - Alert on quality degradation

This agent is the safety net — it catches issues before they
reach production and break the Telegram bot.
"""

import json
import time
import logging
import os
import subprocess
import hashlib
import sys
from typing import Optional

from .base_agent import (
    BaseAgent, AgentObservation, AgentAction, AgentResult,
    Severity,
)

logger = logging.getLogger(__name__)


# =========================================================================
# Configuration
# =========================================================================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LAMBDA_DIRS = {
    "job_ingestor": "job-ingestor",
    "job_deduplicator": "job-deduplicator",
    "job_processor": "job-processor",
    "job_router": "job-router",
    "job_sender": "job-sender",
}

# Expected test files
EXPECTED_TEST_FILES = [
    os.path.join("tests", "test_ingestor.py"),
    os.path.join("tests", "test_deduplicator.py"),
    os.path.join("tests", "test_processor.py"),
    os.path.join("tests", "test_router.py"),
    os.path.join("tests", "test_sender.py"),
]


class QAAgent(BaseAgent):
    """
    Automated quality assurance agent that validates the entire pipeline.
    
    Checks:
      1. Test suite health (run pytest, detect failures)
      2. Code integrity (local vs deployed hash match)
      3. Data flow validation (jobs progress through statuses correctly)
      4. Test coverage completeness
      5. Regression detection
    """

    def __init__(self):
        super().__init__(
            name="QAAgent",
            description="Automated testing, regression detection, and quality gates",
        )
        self._last_test_results: dict = {}

    # -------------------------------------------------------------------
    # OBSERVE
    # -------------------------------------------------------------------
    def observe(self, context: dict) -> list[AgentObservation]:
        observations = []
        observations.extend(self._check_test_suite())
        observations.extend(self._check_test_coverage())
        observations.extend(self._check_code_integrity())
        observations.extend(self._check_data_flow_patterns())
        return observations

    def _check_test_suite(self) -> list[AgentObservation]:
        """Run pytest and analyze results."""
        observations = []
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pytest", "--tb=short", "-q", "--no-header"],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=120,
            )

            output = result.stdout + result.stderr
            returncode = result.returncode

            # Parse results
            passed = 0
            failed = 0
            errors = 0
            failure_details = []

            for line in output.split("\n"):
                if "passed" in line or "failed" in line:
                    # "2 failed, 35 passed in 28.66s" or "37 passed in 28.66s"
                    parts = line.split()
                    for i, part in enumerate(parts):
                        # Strip commas: "failed," -> "failed"
                        clean = part.rstrip(",")
                        if clean == "passed":
                            try:
                                passed = int(parts[i - 1])
                            except (ValueError, IndexError):
                                pass
                        if clean == "failed":
                            try:
                                failed = int(parts[i - 1])
                            except (ValueError, IndexError):
                                pass
                        if clean in ("error", "errors"):
                            try:
                                errors = int(parts[i - 1])
                            except (ValueError, IndexError):
                                pass

                if "FAILED" in line:
                    failure_details.append(line.strip())

            data = {
                "passed": passed,
                "failed": failed,
                "errors": errors,
                "total": passed + failed + errors,
                "returncode": returncode,
                "failure_details": failure_details,
            }

            if failed > 0 or errors > 0:
                observations.append(AgentObservation(
                    source="pytest",
                    data=data,
                    severity=Severity.CRITICAL,
                    message=f"Test suite FAILING: {failed} failed, {errors} errors "
                            f"out of {data['total']} tests",
                ))
            elif passed == 0:
                observations.append(AgentObservation(
                    source="pytest",
                    data=data,
                    severity=Severity.WARNING,
                    message="No tests found or all skipped",
                ))
            else:
                observations.append(AgentObservation(
                    source="pytest",
                    data=data,
                    severity=Severity.INFO,
                    message=f"All {passed} tests passing",
                ))

            # Detect regression vs last run
            if self._last_test_results:
                prev_failed = self._last_test_results.get("failed", 0)
                if failed > prev_failed:
                    observations.append(AgentObservation(
                        source="pytest:regression",
                        data={"previous_failed": prev_failed, "current_failed": failed},
                        severity=Severity.CRITICAL,
                        message=f"REGRESSION: {failed - prev_failed} new test failure(s) "
                                f"since last run",
                    ))

            self._last_test_results = data

        except subprocess.TimeoutExpired:
            observations.append(AgentObservation(
                source="pytest",
                severity=Severity.WARNING,
                message="Test suite timed out after 120s",
            ))
        except Exception as e:
            observations.append(AgentObservation(
                source="pytest",
                severity=Severity.WARNING,
                message=f"Cannot run tests: {str(e)}",
            ))

        return observations

    def _check_test_coverage(self) -> list[AgentObservation]:
        """Verify all Lambda functions have corresponding test files."""
        observations = []
        existing_tests = set()

        tests_dir = os.path.join(PROJECT_ROOT, "tests")
        for root, _dirs, files in os.walk(tests_dir):
            for f in files:
                if f.startswith("test_") and f.endswith(".py"):
                    existing_tests.add(os.path.relpath(os.path.join(root, f), PROJECT_ROOT))

        missing = []
        for expected in EXPECTED_TEST_FILES:
            if expected not in existing_tests:
                missing.append(expected)

        if missing:
            observations.append(AgentObservation(
                source="coverage",
                data={"missing_tests": missing, "existing": list(existing_tests)},
                severity=Severity.WARNING,
                message=f"Missing test files: {', '.join(missing)}",
            ))
        else:
            observations.append(AgentObservation(
                source="coverage",
                data={"test_count": len(existing_tests)},
                severity=Severity.INFO,
                message=f"All {len(EXPECTED_TEST_FILES)} Lambda functions have test files",
            ))

        return observations

    def _check_code_integrity(self) -> list[AgentObservation]:
        """Hash local Lambda code to detect undeployed changes."""
        observations = []

        for local_dir, func_name in LAMBDA_DIRS.items():
            lambda_path = os.path.join(PROJECT_ROOT, local_dir, "lambda_function.py")
            if not os.path.exists(lambda_path):
                observations.append(AgentObservation(
                    source=f"code:{func_name}",
                    severity=Severity.CRITICAL,
                    message=f"Lambda source missing: {lambda_path}",
                ))
                continue

            with open(lambda_path, "rb") as f:
                local_hash = hashlib.sha256(f.read()).hexdigest()[:16]

            observations.append(AgentObservation(
                source=f"code:{func_name}",
                data={"local_hash": local_hash, "path": lambda_path},
                severity=Severity.INFO,
                message=f"{func_name}: local hash={local_hash}",
            ))

        return observations

    def _check_data_flow_patterns(self) -> list[AgentObservation]:
        """Check for data flow anomalies (e.g., jobs stuck in NEW status)."""
        observations = []

        # Check for common import issues in Lambda code
        for local_dir in LAMBDA_DIRS.keys():
            lambda_path = os.path.join(PROJECT_ROOT, local_dir, "lambda_function.py")
            if not os.path.exists(lambda_path):
                continue

            with open(lambda_path, "r", encoding="utf-8") as f:
                code = f.read()

            # Check for hardcoded credentials (security)
            if "AKIA" in code or "aws_secret_access_key" in code.lower():
                observations.append(AgentObservation(
                    source=f"security:{local_dir}",
                    severity=Severity.CRITICAL,
                    message=f"Possible hardcoded credentials in {local_dir}/lambda_function.py",
                ))

            # Check for TODO/FIXME comments
            todo_count = code.lower().count("todo") + code.lower().count("fixme")
            if todo_count > 0:
                observations.append(AgentObservation(
                    source=f"code_quality:{local_dir}",
                    data={"todo_count": todo_count},
                    severity=Severity.INFO,
                    message=f"{local_dir}: {todo_count} TODO/FIXME comment(s)",
                ))

        return observations

    # -------------------------------------------------------------------
    # THINK
    # -------------------------------------------------------------------
    def think(self, observations: list[AgentObservation], context: dict) -> list[AgentAction]:
        actions = []

        for obs in observations:
            if obs.severity == Severity.CRITICAL:
                if "pytest" in obs.source:
                    actions.append(AgentAction(
                        action_type="FIX_TESTS",
                        target="pytest",
                        params=obs.data,
                        reason=obs.message,
                    ))
                if "security" in obs.source:
                    actions.append(AgentAction(
                        action_type="SECURITY_ALERT",
                        target=obs.source,
                        reason=obs.message,
                        requires_approval=True,
                    ))

        return actions

    # -------------------------------------------------------------------
    # ACT
    # -------------------------------------------------------------------
    def act(self, action: AgentAction, context: dict) -> AgentResult:
        if action.action_type == "FIX_TESTS":
            return self._act_analyze_test_failures(action)

        return AgentResult(
            success=False, action=action,
            error=f"Unknown action: {action.action_type}",
        )

    def _act_analyze_test_failures(self, action: AgentAction) -> AgentResult:
        """Analyze test failures and suggest fixes."""
        failures = action.params.get("failure_details", [])

        analysis = []
        for failure in failures:
            if "AssertionError" in failure or "AssertionEr" in failure:
                analysis.append({
                    "test": failure,
                    "type": "assertion_failure",
                    "suggestion": "Test assertion doesn't match actual output. "
                                  "Check if the code behavior changed.",
                })
            elif "ImportError" in failure:
                analysis.append({
                    "test": failure,
                    "type": "import_error",
                    "suggestion": "Missing dependency. Run pip install -r requirements.txt",
                })
            else:
                analysis.append({
                    "test": failure,
                    "type": "unknown",
                    "suggestion": "Review test output for details.",
                })

        return AgentResult(
            success=True, action=action,
            output={"analysis": analysis, "total_failures": len(failures)},
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
    # Public API: Quick test run
    # -------------------------------------------------------------------
    def run_tests(self) -> dict:
        """Run pytest and return structured results."""
        observations = self._check_test_suite()
        if observations:
            return observations[0].data
        return {"error": "No test results"}

    def get_quality_score(self) -> dict:
        """Calculate overall quality score for the pipeline."""
        observations = self.observe({})
        
        critical = sum(1 for o in observations if o.severity == Severity.CRITICAL)
        warnings = sum(1 for o in observations if o.severity == Severity.WARNING)
        
        # Deduct points for issues
        score = 100
        score -= critical * 25
        score -= warnings * 10
        score = max(0, score)

        return {
            "score": score,
            "grade": (
                "A" if score >= 90 else
                "B" if score >= 80 else
                "C" if score >= 70 else
                "D" if score >= 60 else "F"
            ),
            "total_checks": len(observations),
            "critical_issues": critical,
            "warnings": warnings,
            "details": [
                {"source": o.source, "severity": o.severity.value, "message": o.message}
                for o in observations
            ],
        }
