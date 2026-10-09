"""
Orchestrator Agent — Multi-Agent Coordinator

The Orchestrator is the "brain" that coordinates all specialized agents:
  1. PythonDevAgent   → Code quality, syntax, deployments
  2. AIEngineerAgent  → LLM health, scoring calibration, email quality
  3. BackendEngineerAgent → AWS infra, queues, databases
  4. QAAgent          → Testing, regressions, quality gates

Workflow:
  1. Run all agents in sequence (QA first for quick feedback)
  2. Aggregate observations across all agents
  3. Detect cross-agent issues (e.g., code change broke tests)
  4. Generate unified health report
  5. Trigger auto-remediation where safe

This implements the Agentic RAG + Multi-Agent pattern used by
companies like Jeeva.AI for autonomous workflow orchestration.
"""

import json
import time
import logging
from typing import Optional
from datetime import datetime, timezone

from .base_agent import (
    BaseAgent, AgentObservation, AgentAction, AgentResult,
    Severity, AgentStatus,
)
from .qa_agent import QAAgent
from .python_dev_agent import PythonDevAgent
from .security_auditor_agent import SecurityAuditorAgent

logger = logging.getLogger(__name__)


class OrchestratorAgent(BaseAgent):
    """
    Coordinates all specialized agents and generates unified reports.
    
    This is the top-level entry point. Run the orchestrator and it
    will invoke all sub-agents, aggregate results, and produce
    a single health report.
    """

    def __init__(self, include_aws: bool = False, session=None):
        """
        Args:
            include_aws: If True, includes BackendEngineer and AIEngineer agents
                         which require real AWS credentials. Set to False for
                         local-only checks.
            session:     Optional boto3 session for AWS agents.
        """
        super().__init__(
            name="Orchestrator",
            description="Multi-agent coordinator — runs all agents and aggregates results",
        )
        self.include_aws = include_aws
        self.session = session

        # Initialize sub-agents
        self.qa_agent = QAAgent()
        self.security_agent = SecurityAuditorAgent()
        self.python_dev_agent = PythonDevAgent(session=session) if include_aws else None

        self.ai_engineer_agent = None
        self.backend_engineer_agent = None

        if include_aws:
            from .ai_engineer_agent import AIEngineerAgent
            from .backend_engineer_agent import BackendEngineerAgent
            self.ai_engineer_agent = AIEngineerAgent(session=session)
            self.backend_engineer_agent = BackendEngineerAgent(session=session)

        self.agent_results: dict = {}

    # -------------------------------------------------------------------
    # OBSERVE: Run all sub-agents and collect observations
    # -------------------------------------------------------------------
    def observe(self, context: dict) -> list[AgentObservation]:
        observations = []

        # Phase 1: QA Agent (fastest, catches immediate issues)
        self.logger.info("Phase 1: Running QA Agent...")
        try:
            qa_summary = self.qa_agent.run(context)
            self.agent_results["QAAgent"] = qa_summary
            observations.append(AgentObservation(
                source="orchestrator:qa",
                data=qa_summary,
                severity=self._status_to_severity(qa_summary.get("status", "UNKNOWN")),
                message=f"QA Agent: {qa_summary.get('status', 'UNKNOWN')}",
            ))
        except Exception as e:
            observations.append(AgentObservation(
                source="orchestrator:qa",
                severity=Severity.CRITICAL,
                message=f"QA Agent crashed: {str(e)}",
            ))

        # Phase 2: Security & Identity Auditor Agent (zero leaks, zero secrets, zero loops)
        self.logger.info("Phase 2: Running Security Auditor Agent...")
        try:
            sec_summary = self.security_agent.run(context)
            self.agent_results["SecurityAuditor"] = sec_summary
            observations.append(AgentObservation(
                source="orchestrator:security",
                data=sec_summary,
                severity=self._status_to_severity(sec_summary.get("status", "UNKNOWN")),
                message=f"Security Auditor Agent: {sec_summary.get('status', 'UNKNOWN')}",
            ))
        except Exception as e:
            observations.append(AgentObservation(
                source="orchestrator:security",
                severity=Severity.CRITICAL,
                message=f"Security Auditor Agent crashed: {str(e)}",
            ))

        # Phase 3: Python Dev Agent (code quality)
        if self.python_dev_agent:
            self.logger.info("Phase 3: Running Python Dev Agent...")
            try:
                dev_summary = self.python_dev_agent.run(context)
                self.agent_results["PythonDev"] = dev_summary
                observations.append(AgentObservation(
                    source="orchestrator:dev",
                    data=dev_summary,
                    severity=self._status_to_severity(dev_summary.get("status", "UNKNOWN")),
                    message=f"Python Dev Agent: {dev_summary.get('status', 'UNKNOWN')}",
                ))
            except Exception as e:
                observations.append(AgentObservation(
                    source="orchestrator:dev",
                    severity=Severity.WARNING,
                    message=f"Python Dev Agent error: {str(e)}",
                ))

        # Phase 4: Backend Engineer Agent (AWS infra)
        if self.backend_engineer_agent:
            self.logger.info("Phase 4: Running Backend Engineer Agent...")
            try:
                backend_summary = self.backend_engineer_agent.run(context)
                self.agent_results["BackendEngineer"] = backend_summary
                observations.append(AgentObservation(
                    source="orchestrator:backend",
                    data=backend_summary,
                    severity=self._status_to_severity(backend_summary.get("status", "UNKNOWN")),
                    message=f"Backend Engineer: {backend_summary.get('status', 'UNKNOWN')}",
                ))
            except Exception as e:
                observations.append(AgentObservation(
                    source="orchestrator:backend",
                    severity=Severity.WARNING,
                    message=f"Backend Engineer error: {str(e)}",
                ))

        # Phase 5: AI Engineer Agent (LLM health)
        if self.ai_engineer_agent:
            self.logger.info("Phase 5: Running AI Engineer Agent...")
            try:
                ai_summary = self.ai_engineer_agent.run(context)
                self.agent_results["AIEngineer"] = ai_summary
                observations.append(AgentObservation(
                    source="orchestrator:ai",
                    data=ai_summary,
                    severity=self._status_to_severity(ai_summary.get("status", "UNKNOWN")),
                    message=f"AI Engineer: {ai_summary.get('status', 'UNKNOWN')}",
                ))
            except Exception as e:
                observations.append(AgentObservation(
                    source="orchestrator:ai",
                    severity=Severity.WARNING,
                    message=f"AI Engineer error: {str(e)}",
                ))

        return observations

    def _status_to_severity(self, status: str) -> Severity:
        return {
            "SUCCESS": Severity.INFO,
            "HEALTHY": Severity.INFO,
            "DEGRADED": Severity.WARNING,
            "FAILED": Severity.CRITICAL,
            "CRASHED": Severity.FATAL,
        }.get(status, Severity.WARNING)

    # -------------------------------------------------------------------
    # THINK: Cross-agent analysis
    # -------------------------------------------------------------------
    def think(self, observations: list[AgentObservation], context: dict) -> list[AgentAction]:
        actions = []

        # Check for cross-agent issues
        qa_status = self.agent_results.get("QAAgent", {}).get("status", "UNKNOWN")
        
        if qa_status == "FAILED":
            actions.append(AgentAction(
                action_type="NOTIFY_TEAM",
                target="telegram",
                params={"urgency": "high"},
                reason="Test suite is failing — pipeline may be broken",
            ))

        # If tests pass but infra is degraded, deploy might fix it
        if qa_status == "SUCCESS":
            backend_status = self.agent_results.get("BackendEngineer", {}).get("status")
            if backend_status == "DEGRADED":
                actions.append(AgentAction(
                    action_type="SUGGEST_DEPLOY",
                    target="all_lambdas",
                    reason="Tests pass locally but infra is degraded — fresh deploy may help",
                ))

        return actions

    # -------------------------------------------------------------------
    # ACT
    # -------------------------------------------------------------------
    def act(self, action: AgentAction, context: dict) -> AgentResult:
        if action.action_type == "NOTIFY_TEAM":
            return AgentResult(
                success=True, action=action,
                output={"notification": "Pipeline health alert generated"},
            )
        if action.action_type == "SUGGEST_DEPLOY":
            return AgentResult(
                success=True, action=action,
                output={"suggestion": "Run: python deploy.py"},
            )
        return AgentResult(success=False, action=action, error="Unknown action")

    # -------------------------------------------------------------------
    # VERIFY
    # -------------------------------------------------------------------
    def verify(self, results: list[AgentResult], context: dict) -> dict:
        return {
            "total_actions": len(results),
            "succeeded": sum(1 for r in results if r.success),
            "agent_count": len(self.agent_results),
        }

    # -------------------------------------------------------------------
    # Public API: Generate full health report
    # -------------------------------------------------------------------
    def generate_health_report(self, context: dict = None) -> dict:
        """
        Run all agents and generate a comprehensive health report.
        
        Returns a structured report suitable for display or logging.
        """
        summary = self.run(context or {})

        # Calculate overall health
        agent_statuses = {
            name: result.get("status", "UNKNOWN")
            for name, result in self.agent_results.items()
        }

        critical_count = sum(1 for s in agent_statuses.values() if s in ("FAILED", "CRASHED"))
        warning_count = sum(1 for s in agent_statuses.values() if s == "DEGRADED")

        overall = (
            "CRITICAL" if critical_count > 0 else
            "WARNING" if warning_count > 0 else
            "HEALTHY"
        )

        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "overall_status": overall,
            "agent_statuses": agent_statuses,
            "orchestrator_summary": summary,
            "recommendations": self._generate_recommendations(),
        }

        return report

    def _generate_recommendations(self) -> list[str]:
        """Generate actionable recommendations based on all agent results."""
        recs = []

        qa_data = self.agent_results.get("QAAgent", {})
        if qa_data.get("status") == "FAILED":
            recs.append("CRITICAL: Fix failing tests before deploying")
            for obs in qa_data.get("observations", []):
                if obs.get("severity") == "CRITICAL":
                    recs.append(f"  → {obs.get('message', '')}")

        sec_data = self.agent_results.get("SecurityAuditor", {})
        if sec_data.get("status") == "FAILED":
            recs.append("CRITICAL: Resolve security auditor violations (identity leaks, secrets, or unbounded loops)")
            for obs in sec_data.get("observations", []):
                if obs.get("severity") == "CRITICAL":
                    recs.append(f"  → {obs.get('message', '')}")

        dev_data = self.agent_results.get("PythonDev", {})
        if dev_data.get("status") == "FAILED":
            recs.append("FIX: Resolve code quality issues")

        backend_data = self.agent_results.get("BackendEngineer", {})
        if backend_data.get("status") in ("FAILED", "DEGRADED"):
            recs.append("INFRA: Check AWS infrastructure health")

        ai_data = self.agent_results.get("AIEngineer", {})
        if ai_data.get("status") in ("FAILED", "DEGRADED"):
            recs.append("AI: Check Bedrock model availability and scoring calibration")

        if not recs:
            recs.append("All systems healthy. Pipeline is ready for production.")

        return recs

    # -------------------------------------------------------------------
    # Pretty print report
    # -------------------------------------------------------------------
    def print_report(self, report: dict = None):
        """Print a formatted health report to console."""
        if report is None:
            report = self.generate_health_report()

        overall = report["overall_status"]
        icon = {"HEALTHY": "✅", "WARNING": "⚠️", "CRITICAL": "❌"}.get(overall, "❓")

        print(f"\n{'='*65}")
        print(f"  {icon} PIPELINE HEALTH REPORT — {report['timestamp'][:19]}")
        print(f"{'='*65}")
        print(f"\n  Overall Status: {overall}\n")

        print("  Agent Results:")
        for agent, status in report["agent_statuses"].items():
            agent_icon = {"SUCCESS": "✅", "HEALTHY": "✅", "DEGRADED": "⚠️",
                         "FAILED": "❌", "CRASHED": "💀"}.get(status, "❓")
            print(f"    {agent_icon} {agent}: {status}")

        print(f"\n  Recommendations:")
        for rec in report["recommendations"]:
            print(f"    • {rec}")

        print(f"\n{'='*65}\n")
