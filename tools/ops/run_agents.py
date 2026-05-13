#!/usr/bin/env python3
"""
Pipeline health-check CLI.

Usage:
  python tools/ops/run_agents.py              # Local orchestrator check
  python tools/ops/run_agents.py --qa         # Run QA agent only
  python tools/ops/run_agents.py --full       # Include AWS infrastructure agents
  python tools/ops/run_agents.py --deploy     # Run checks in deploy mode
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
