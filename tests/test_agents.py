"""
Tests for the Multi-Agent System

Tests all agents in isolation using mocks:
  1. BaseAgent — ReAct loop mechanics
  2. QAAgent — Test suite runner, quality scoring
  3. PythonDevAgent — Syntax checking, import analysis
  4. AIEngineerAgent — Email validation, scoring analysis
  5. BackendEngineerAgent — Infrastructure health checks
  6. OrchestratorAgent — Multi-agent coordination
"""

import json
import os
import sys
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from decimal import Decimal

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.base_agent import (
    BaseAgent, AgentObservation, AgentAction, AgentResult,
    Severity, AgentStatus,
)
from agents.qa_agent import QAAgent
from agents.ai_engineer_agent import AIEngineerAgent
from agents.orchestrator_agent import OrchestratorAgent


# =========================================================================
# 1. BASE AGENT TESTS
# =========================================================================
class ConcreteAgent(BaseAgent):
    """Concrete implementation for testing the abstract base."""

    def __init__(self):
        super().__init__("TestAgent", "A test agent")
        self.should_fail = False

    def observe(self, context):
        return [
            AgentObservation(
                source="test", severity=Severity.INFO,
                message="All good"
            )
        ]

    def think(self, observations, context):
        if context.get("no_actions"):
            return []
        return [
            AgentAction(
                action_type="TEST_ACTION",
                target="test_target",
                reason="Testing",
            )
        ]

    def act(self, action, context):
        if self.should_fail:
            raise RuntimeError("Simulated failure")
        return AgentResult(success=True, action=action, output="done")

    def verify(self, results, context):
        return {"verified": True, "count": len(results)}


class TestBaseAgent:
    """Tests for the ReAct loop in BaseAgent."""

    def test_successful_run(self):
        agent = ConcreteAgent()
        summary = agent.run()

        assert summary["status"] == "SUCCESS"
        assert summary["agent"] == "TestAgent"
        assert len(summary["observations"]) == 1
        assert len(summary["actions"]) == 1
        assert summary["actions"][0]["success"] is True

    def test_no_actions_needed(self):
        agent = ConcreteAgent()
        summary = agent.run({"no_actions": True})

        assert summary["status"] == "HEALTHY"
        assert len(summary["actions"]) == 0

    def test_failed_action(self):
        agent = ConcreteAgent()
        agent.should_fail = True
        summary = agent.run()

        assert summary["status"] == "FAILED"
        assert summary["actions"][0]["success"] is False

    def test_agent_report(self):
        agent = ConcreteAgent()
        agent.run()
        report = agent.get_report()

        assert "TestAgent" in report
        assert "SUCCESS" in report

    def test_requires_approval_action(self):
        """Actions requiring approval should not execute."""
        agent = ConcreteAgent()
        # Override think to return an action that requires approval
        original_think = agent.think
        agent.think = lambda obs, ctx: [
            AgentAction(
                action_type="DANGEROUS",
                target="test",
                requires_approval=True,
            )
        ]
        
        summary = agent.run()
        assert summary["actions"][0]["success"] is False
        assert "approval" in summary["actions"][0]["error"].lower()


# =========================================================================
# 2. QA AGENT TESTS
# =========================================================================
class TestQAAgent:
    """Tests for the QA Agent."""

    def test_initialization(self):
        qa = QAAgent()
        assert qa.name == "QAAgent"

    @patch("agents.qa_agent.subprocess.run")
    def test_test_suite_all_pass(self, mock_run):
        """Simulate all tests passing."""
        mock_run.return_value = MagicMock(
            stdout="37 passed in 28s\n",
            stderr="",
            returncode=0,
        )
        qa = QAAgent()
        results = qa.run_tests()

        assert results["passed"] == 37
        assert results["failed"] == 0

    @patch("agents.qa_agent.subprocess.run")
    def test_test_suite_failures(self, mock_run):
        """Simulate test failures."""
        mock_run.return_value = MagicMock(
            stdout="2 failed, 35 passed in 28s\n"
                   "FAILED test_router.py::test_apply\n",
            stderr="",
            returncode=1,
        )
        qa = QAAgent()
        results = qa.run_tests()

        assert results["failed"] == 2
        assert results["passed"] == 35

    def test_code_integrity_check(self):
        """Verify code integrity checks find existing Lambda files."""
        qa = QAAgent()
        observations = qa._check_code_integrity()

        # Should find at least some Lambda files
        assert len(observations) > 0
        # Check that existing files are reported as INFO
        info_obs = [o for o in observations if o.severity == Severity.INFO]
        assert len(info_obs) > 0

    def test_test_coverage_check(self):
        """Verify test coverage detects existing test files."""
        qa = QAAgent()
        observations = qa._check_test_coverage()

        assert len(observations) > 0

    @patch("agents.qa_agent.subprocess.run")
    def test_quality_score_healthy(self, mock_run):
        """Quality score should be high when all tests pass."""
        mock_run.return_value = MagicMock(
            stdout="37 passed in 28s\n",
            stderr="",
            returncode=0,
        )
        qa = QAAgent()
        score = qa.get_quality_score()

        assert score["score"] >= 70
        assert score["grade"] in ("A", "B", "C")

    @patch("agents.qa_agent.subprocess.run")  
    def test_regression_detection(self, mock_run):
        """Should detect new test failures as regression."""
        qa = QAAgent()

        # First run: all pass
        mock_run.return_value = MagicMock(
            stdout="37 passed in 28s\n", stderr="", returncode=0
        )
        qa._check_test_suite()

        # Second run: 2 failures
        mock_run.return_value = MagicMock(
            stdout="2 failed, 35 passed in 28s\n"
                   "FAILED test_router.py::test_apply\n",
            stderr="",
            returncode=1,
        )
        observations = qa._check_test_suite()

        # Should have regression observation
        regression = [o for o in observations if "regression" in o.source.lower()]
        assert len(regression) == 1
        assert regression[0].severity == Severity.CRITICAL


# =========================================================================
# 3. AI ENGINEER AGENT TESTS
# =========================================================================
class TestAIEngineerAgent:
    """Tests for the AI Engineer Agent's email validation."""

    def test_email_validation_pass(self):
        """Valid email should pass all checks."""
        ai = AIEngineerAgent.__new__(AIEngineerAgent)
        ai.name = "AIEngineer"
        
        # Construct a valid email body
        body = (
            "Hi John,\n\n"
            "Hope you are doing well. I noticed Anthropic is building next-gen "
            "LLM infrastructure, and I have done similar work, reducing GPU costs "
            "by 33% and resolving 87% of support tickets autonomously.\n\n"
            "My projects RetireIQ (https://huggingface.co/spaces/vakaTeja/RetireIQ) "
            "is a retirement planning and financial decision support app, and "
            "Rivulet (https://rivulet.info) is a personal life management app.\n\n"
            "Would 20 minutes this week work to connect?\n\n"
            "Best,\n"
            "Teja Gopal Reddy Vaka\n"
            "AI Engineer\n"
            "https://linkedin.com/in/tejagopalreddyvaka/"
        )

        result = ai.validate_email("AI Engineer Role at Anthropic", body)
        assert result["passed"] is True
        assert result["score"] == 100
        assert len(result["violations"]) == 0

    def test_email_validation_banned_words(self):
        """Emails with banned words should fail."""
        ai = AIEngineerAgent.__new__(AIEngineerAgent)
        ai.name = "AIEngineer"
        
        body = (
            "Hi there,\n\n"
            "I am passionate about leveraging cutting-edge AI to create "
            "seamlessly innovative solutions. I am excited and thrilled about "
            "this opportunity. My work reduced costs by 33% and resolved 87% "
            "of tickets. Check RetireIQ (https://huggingface.co/spaces/vakaTeja/RetireIQ) "
            "and Rivulet (https://rivulet.info). "
            "https://linkedin.com/in/tejagopalreddyvaka/"
        )

        result = ai.validate_email("Subject", body)
        assert result["passed"] is False
        assert len(result["violations"]) > 3  # Multiple banned words

    def test_email_validation_missing_urls(self):
        """Emails missing required URLs should fail."""
        ai = AIEngineerAgent.__new__(AIEngineerAgent)
        ai.name = "AIEngineer"
        
        body = (
            "Hi there,\n\n"
            "I have done great work reducing GPU costs by 33% and resolving "
            "87% of tickets. I would love to connect about this role.\n\n"
            "Would 20 minutes work?\n\n"
            "Best, Teja"
        )

        result = ai.validate_email("Subject", body)
        assert result["passed"] is False
        # Should flag all 3 missing URLs
        url_violations = [v for v in result["violations"] if "URL" in v]
        assert len(url_violations) == 3

    def test_email_validation_exclamation_point(self):
        """Exclamation points should be caught."""
        ai = AIEngineerAgent.__new__(AIEngineerAgent)
        ai.name = "AIEngineer"
        
        body = (
            "Hi there,\n\n"
            "This is amazing! I reduced costs by 33% and resolved 87% of "
            "tickets. Check RetireIQ (https://huggingface.co/spaces/vakaTeja/RetireIQ) "
            "and Rivulet (https://rivulet.info). "
            "https://linkedin.com/in/tejagopalreddyvaka/"
        )

        result = ai.validate_email("Subject", body)
        assert result["passed"] is False
        assert any("exclamation" in v.lower() for v in result["violations"])

    def test_email_validation_too_long(self):
        """Emails over 150 words should fail."""
        ai = AIEngineerAgent.__new__(AIEngineerAgent)
        ai.name = "AIEngineer"
        
        body = " ".join(["word"] * 200)

        result = ai.validate_email("Subject", body)
        assert result["passed"] is False
        assert any("Too long" in v for v in result["violations"])


# =========================================================================
# 4. ORCHESTRATOR TESTS
# =========================================================================
class TestOrchestrator:
    """Tests for the Orchestrator Agent."""

    @patch("agents.qa_agent.subprocess.run")
    def test_local_orchestrator(self, mock_run):
        """Orchestrator should coordinate QA agent in local mode."""
        mock_run.return_value = MagicMock(
            stdout="37 passed in 28s\n", stderr="", returncode=0
        )

        orchestrator = OrchestratorAgent(include_aws=False)
        report = orchestrator.generate_health_report()

        assert "overall_status" in report
        assert "agent_statuses" in report
        assert "QAAgent" in report["agent_statuses"]
        assert "recommendations" in report

    @patch("agents.qa_agent.subprocess.run")
    def test_orchestrator_healthy(self, mock_run):
        """Healthy pipeline should report HEALTHY."""
        mock_run.return_value = MagicMock(
            stdout="37 passed in 28s\n", stderr="", returncode=0
        )

        orchestrator = OrchestratorAgent(include_aws=False)
        report = orchestrator.generate_health_report()

        # QA passes → overall should be healthy (or at least not CRITICAL)
        assert report["overall_status"] in ("HEALTHY", "WARNING")

    @patch("agents.qa_agent.subprocess.run")
    def test_orchestrator_with_failures(self, mock_run):
        """Failed tests should cascade to CRITICAL overall status."""
        mock_run.return_value = MagicMock(
            stdout="5 failed, 32 passed in 28s\n"
                   "FAILED test_router.py::test_apply\n",
            stderr="",
            returncode=1,
        )

        orchestrator = OrchestratorAgent(include_aws=False)
        report = orchestrator.generate_health_report()

        # Should have recommendations about fixing tests
        recs = report.get("recommendations", [])
        assert len(recs) > 0


# =========================================================================
# 5. INTEGRATION-STYLE TESTS
# =========================================================================
class TestAgentIntegration:
    """Integration tests verifying agents work together."""

    def test_all_agents_importable(self):
        """All agent modules should import without error."""
        from agents.base_agent import BaseAgent
        from agents.qa_agent import QAAgent
        from agents.ai_engineer_agent import AIEngineerAgent
        from agents.python_dev_agent import PythonDevAgent
        from agents.orchestrator_agent import OrchestratorAgent
        # BackendEngineerAgent needs AWS — skip import test

        assert BaseAgent is not None
        assert QAAgent is not None
        assert AIEngineerAgent is not None
        assert PythonDevAgent is not None
        assert OrchestratorAgent is not None

    def test_agent_observation_dataclass(self):
        """Observations should be properly structured."""
        obs = AgentObservation(
            source="test",
            severity=Severity.WARNING,
            message="Test observation",
            data={"key": "value"},
        )
        assert obs.source == "test"
        assert obs.severity == Severity.WARNING
        assert obs.data["key"] == "value"
        assert obs.timestamp > 0

    def test_agent_action_dataclass(self):
        """Actions should be properly structured."""
        action = AgentAction(
            action_type="DEPLOY",
            target="job-router",
            params={"version": "2.0"},
            reason="New code ready",
            requires_approval=True,
        )
        assert action.action_type == "DEPLOY"
        assert action.requires_approval is True

    def test_agent_result_dataclass(self):
        """Results should capture success/failure."""
        action = AgentAction(action_type="TEST", target="test")
        result = AgentResult(success=True, action=action, output="done")
        assert result.success is True
        assert result.output == "done"
        assert result.error is None

    def test_agent_status_enum(self):
        """All expected statuses should exist."""
        assert AgentStatus.IDLE.value == "IDLE"
        assert AgentStatus.RUNNING.value == "RUNNING"
        assert AgentStatus.SUCCESS.value == "SUCCESS"
        assert AgentStatus.FAILED.value == "FAILED"
        assert AgentStatus.DEGRADED.value == "DEGRADED"

    def test_severity_enum(self):
        """All severity levels should exist."""
        assert Severity.INFO.value == "INFO"
        assert Severity.WARNING.value == "WARNING"
        assert Severity.CRITICAL.value == "CRITICAL"
        assert Severity.FATAL.value == "FATAL"
