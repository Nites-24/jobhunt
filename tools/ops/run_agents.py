#!/usr/bin/env python3
"""
Multi-Agent Operational Health Check & Diagnostic CLI (run_agents.py)

Role & Architectural Placement:
  Coordinates the JobHunt local diagnostic agent suite to verify code quality,
  test suite regressions, secret safety, identity decoupling, and live AWS infrastructure.

Diagnostic Agents:
  - QAAgent: Runs offline pytest suites, measures test runtimes, and detects regressions.
  - SecurityAuditorAgent: Enforces zero-trust identity decoupling, scans for secrets/credentials,
    and analyzes Python ASTs for unbounded loops.
  - PythonDevAgent: Inspects code syntax, packaging hygiene, and local file status.
  - OrchestratorAgent: Aggregates observations across agents and synthesizes health grades (A-F).
  - BackendEngineerAgent / AIEngineerAgent (enabled with --full): Verifies live AWS queues,
    DynamoDB tables, Bedrock connectivity, and LLM telemetry.

CLI Usage:
  python tools/ops/run_agents.py              # Local orchestrator check (offline, zero-spend)
  python tools/ops/run_agents.py --qa         # Run QA agent only (pytest suite)
  python tools/ops/run_agents.py --security   # Run Security & Identity Auditor agent only
  python tools/ops/run_agents.py --full       # Include live AWS infrastructure agents
  python tools/ops/run_agents.py --deploy     # Run checks before promoting changes
  python tools/ops/run_agents.py --json       # Output machine-readable JSON health report
"""

import argparse
import json
import logging
import os
import sys


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

from agents.orchestrator_agent import OrchestratorAgent


def _severity_label(severity: str) -> str:
    return {
        "INFO": "INFO",
        "WARNING": "WARN",
        "CRITICAL": "CRITICAL",
        "FATAL": "FATAL",
    }.get(severity, "UNKNOWN")


def _grade_label(grade: str) -> str:
    return {
        "A": "PASS",
        "B": "PASS",
        "C": "WARN",
        "D": "FAIL",
        "F": "FAIL",
    }.get(grade, "UNKNOWN")


def _print_report(report: dict) -> None:
    overall = report["overall_status"]
    print(f"\n{'=' * 65}")
    print(f"  PIPELINE HEALTH REPORT - {report['timestamp'][:19]}")
    print(f"{'=' * 65}")
    print(f"\n  Overall Status: {overall}\n")

    print("  Agent Results:")
    for agent, status in report["agent_statuses"].items():
        print(f"    [{status}] {agent}")

    print("\n  Recommendations:")
    for recommendation in report["recommendations"]:
        print(f"    - {recommendation}")

    print(f"\n{'=' * 65}\n")


def main():
    parser = argparse.ArgumentParser(description="JobHunt multi-agent pipeline health check")
    parser.add_argument("--full", action="store_true", help="Run all agents including AWS checks")
    parser.add_argument("--qa", action="store_true", help="Run QA agent only")
    parser.add_argument("--security", action="store_true", help="Run Security & Identity Auditor agent only")
    parser.add_argument("--deploy", action="store_true", help="Deploy mode after health check")
    parser.add_argument("--json", action="store_true", help="Output report as JSON")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    print("\n" + "=" * 65)
    print("  JOBHUNT MULTI-AGENT SYSTEM")
    print("=" * 65)

    if args.security:
        from agents.security_auditor_agent import SecurityAuditorAgent
        from agents.base_agent import Severity

        sec = SecurityAuditorAgent()
        print("\n  Running Security & Identity Auditor Agent...\n")
        obs = sec.observe()
        critical_count = sum(1 for o in obs if o.severity == Severity.CRITICAL)
        warning_count = sum(1 for o in obs if o.severity == Severity.WARNING)

        status_str = "HEALTHY" if critical_count == 0 else "FAILED"
        print(f"  Status: {status_str}")
        print(f"  Critical Issues: {critical_count}")
        print(f"  Warnings: {warning_count}")

        if obs:
            print("\n  Findings:")
            for o in obs:
                label = _severity_label(o.severity.value)
                print(f"    [{label}] {o.message}")
        else:
            print("\n  Result: Zero identity leaks, zero hardcoded secrets, and zero unbounded loops verified.")

        print(f"\n{'=' * 65}\n")
        return 0 if critical_count == 0 else 1

    include_aws = args.full or args.deploy
    context = {"deploy": True} if args.deploy else {}

    if args.qa:
        from agents.qa_agent import QAAgent

        qa = QAAgent()
        print("\n  Running QA Agent...\n")
        quality = qa.get_quality_score()

        print(f"  [{_grade_label(quality['grade'])}] Quality Grade: {quality['grade']} ({quality['score']}/100)")
        print(f"  Total Checks: {quality['total_checks']}")
        print(f"  Critical: {quality['critical_issues']}")
        print(f"  Warnings: {quality['warnings']}")

        if quality["details"]:
            print("\n  Details:")
            for detail in quality["details"]:
                label = _severity_label(detail["severity"])
                print(f"    [{label}] [{detail['source']}] {detail['message']}")

        print(f"\n{'=' * 65}\n")
        return 0 if quality["grade"] in ("A", "B") else 1

    mode = "FULL (AWS + Local)" if include_aws else "LOCAL (QA + Code Quality)"
    print(f"  Mode: {mode}")

    if include_aws:
        print("  Loading AWS credentials from .env...")

    orchestrator = OrchestratorAgent(include_aws=include_aws)

    report = orchestrator.generate_health_report(context)
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        _print_report(report)

    if report["overall_status"] == "HEALTHY":
        return 0
    if report["overall_status"] == "WARNING":
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
