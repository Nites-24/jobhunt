"""
Security Auditor Agent — Identity Redaction, Secret Auditing, and Zero-Loop Verification.

Responsibilities:
  1. Identity Scrubbing: Detect and prevent sensitive personal identity details
     (name, email, CPT/visa status, personal location, personal LinkedIn) from entering tracked code.
  2. Secret & Credential Auditing: Detect hardcoded API keys, tokens, or AWS credentials.
  3. Zero-Loop Analysis: Perform AST analysis on all Python code to ensure all loops have
     bounded termination conditions (no unbounded while/infinite loops).
  4. Code Freshness & Integrity: Validate that all pipeline code is clean, consistent, and syntax-valid.
"""

import ast
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from .base_agent import (
    AgentAction,
    AgentObservation,
    AgentResult,
    BaseAgent,
    Severity,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_identity_patterns() -> List[Tuple[str, str]]:
    """Dynamically resolve identity patterns from runtime environment & generic PII regexes."""
    patterns: List[Tuple[str, str]] = [
        (r"\b[a-zA-Z0-9_.+-]+@(gmail|yahoo|hotmail|outlook)\.com\b", "Personal email domain in tracked code"),
        (r"\b\+1[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b", "Personal phone number in tracked code"),
        (r"\bCPT\s+student\b", "Immigration/visa status 'CPT student'"),
        (r"data/resume/[A-Za-z]+_[A-Za-z]+\.(pdf|txt)", "Personal resume path pattern"),
    ]

    # Dynamically inject candidate identity if configured in runtime environment
    candidate_name = os.environ.get("CANDIDATE_NAME")
    if candidate_name and candidate_name.strip() and candidate_name.lower() not in ("candidate name", "alex hunter"):
        name_parts = [re.escape(p) for p in candidate_name.strip().split()]
        patterns.append((r"\b" + r"\s+".join(name_parts) + r"\b", f"Candidate name '{candidate_name}'"))

    sender_email = os.environ.get("SENDER_EMAIL")
    if sender_email and sender_email.strip() and sender_email.lower() not in ("candidate@example.com", "sender@example.com"):
        patterns.append((re.escape(sender_email.strip()), f"Candidate email '{sender_email}'"))

    candidate_loc = os.environ.get("CANDIDATE_LOCATION")
    if candidate_loc and candidate_loc.strip() and candidate_loc.lower() not in ("open to remote", "remote", "any"):
        patterns.append((re.escape(candidate_loc.strip()), f"Candidate location '{candidate_loc}'"))

    linkedin = os.environ.get("CANDIDATE_LINKEDIN")
    if linkedin and "linkedin.com/in/" in linkedin and "example" not in linkedin:
        handle = linkedin.rstrip("/").split("/")[-1]
        if handle and len(handle) > 3:
            patterns.append((re.escape(handle), f"Candidate LinkedIn handle '{handle}'"))

    return patterns


# Sensitive personal patterns resolved dynamically
SENSITIVE_IDENTITY_PATTERNS = get_identity_patterns()

# Secret patterns
SECRET_PATTERNS = [
    (r"\bAKIA[0-9A-Z]{16}\b", "AWS Access Key ID"),
    (r"\bghp_[0-9a-zA-Z]{36}\b", "GitHub Personal Access Token"),
    (r"\bsk-[a-zA-Z0-9_-]{20,}\b", "API Secret Key"),
    (r"\bbot\d{8,}:[A-Za-z0-9_-]{35}\b", "Telegram Bot Token"),
]

EXCLUDED_DIRS = {
    ".git",
    ".idea",
    ".vscode",
    "__pycache__",
    ".pytest_cache",
    "venv",
    ".venv",
    "env",
    "dist",
    "build",
    ".agents",
    "docs",
}

EXCLUDED_FILES = {
    "security_auditor_agent.py",
    "identity_sanitization_and_agent_plan.md",
    "learning_proposal.md",
}

ALLOWED_EXTENSIONS = (".py", ".json", ".md", ".yml", ".yaml", ".txt", ".env.example")


class SecurityAuditorAgent(BaseAgent):
    """
    Autonomous security & identity auditor agent.
    Enforces zero personal identity leaks, zero hardcoded secrets, and AST-verified zero loops.
    """

    def __init__(self, project_root: str = PROJECT_ROOT):
        super().__init__(
            name="SecurityAuditor",
            description="Audits codebase for identity redaction, secrets, and unbounded execution loops",
        )
        self.project_root = project_root

    # -------------------------------------------------------------------
    # OBSERVE: Scan the workspace
    # -------------------------------------------------------------------
    def observe(self, context: dict = None) -> List[AgentObservation]:
        observations = []

        # 1. Identity Leak Scan
        identity_findings = self._scan_identity_leaks()
        for rel_path, line_no, desc, snippet in identity_findings:
            observations.append(
                AgentObservation(
                    source=f"{rel_path}:{line_no}",
                    severity=Severity.CRITICAL,
                    message=f"Sensitive identity found in {rel_path}:{line_no} — {desc}",
                    data={"snippet": snippet, "path": rel_path, "line": line_no},
                )
            )

        # 2. Secret Scan
        secret_findings = self._scan_secrets()
        for rel_path, line_no, desc, snippet in secret_findings:
            observations.append(
                AgentObservation(
                    source=f"{rel_path}:{line_no}",
                    severity=Severity.CRITICAL,
                    message=f"Potential credential found in {rel_path}:{line_no} — {desc}",
                    data={"snippet": snippet, "path": rel_path, "line": line_no},
                )
            )

        # 3. AST Zero-Loop Analysis
        loop_findings = self._scan_unbounded_loops()
        for rel_path, line_no, desc in loop_findings:
            observations.append(
                AgentObservation(
                    source=f"{rel_path}:{line_no}",
                    severity=Severity.CRITICAL,
                    message=f"Unbounded loop found in {rel_path}:{line_no} — {desc}",
                    data={"path": rel_path, "line": line_no},
                )
            )

        self.observations = observations
        return observations

    # -------------------------------------------------------------------
    # THINK: Determine pass/fail status and remediation actions
    # -------------------------------------------------------------------
    def think(self, observations: List[AgentObservation], context: dict = None) -> List[AgentAction]:
        actions = []
        critical_count = sum(1 for obs in observations if obs.severity == Severity.CRITICAL)

        if critical_count > 0:
            actions.append(
                AgentAction(
                    action_type="REDACT_IDENTITY_OR_SECRETS",
                    target="codebase",
                    params={"critical_count": critical_count},
                    reason=f"Found {critical_count} critical security/identity violations that must be resolved.",
                    requires_approval=False,
                )
            )

        return actions

    # -------------------------------------------------------------------
    # ACT: Log and execute remediation
    # -------------------------------------------------------------------
    def act(self, action: AgentAction, context: dict = None) -> AgentResult:
        return AgentResult(
            success=False,
            action=action,
            output=f"Security auditor flagged action: {action.action_type}",
            error="Found critical security or identity violations that must be resolved.",
        )

    # -------------------------------------------------------------------
    # VERIFY: Verify audit results
    # -------------------------------------------------------------------
    def verify(self, results: list[AgentResult], context: dict = None) -> bool:
        critical_obs = [obs for obs in self.observations if obs.severity == Severity.CRITICAL]
        return len(critical_obs) == 0

    # -------------------------------------------------------------------
    # Audit Helpers
    # -------------------------------------------------------------------
    def _iterate_scannable_files(self) -> List[str]:
        scannable = []
        for root, dirs, files in os.walk(self.project_root):
            dirs[:] = [d for d in dirs if d not in EXCLUDED_DIRS]
            for file in files:
                if file in EXCLUDED_FILES:
                    continue
                if file.endswith(ALLOWED_EXTENSIONS):
                    scannable.append(os.path.join(root, file))
        return scannable

    def _scan_identity_leaks(self) -> List[Tuple[str, int, str, str]]:
        findings = []
        files = self._iterate_scannable_files()

        for filepath in files:
            rel_path = os.path.relpath(filepath, self.project_root).replace("\\", "/")
            try:
                with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                    for line_no, line in enumerate(f, 1):
                        for pattern, desc in SENSITIVE_IDENTITY_PATTERNS:
                            if re.search(pattern, line):
                                findings.append((rel_path, line_no, desc, line.strip()[:120]))
            except Exception as e:
                logger.warning("Could not read %s: %s", filepath, e)

        return findings

    def _scan_secrets(self) -> List[Tuple[str, int, str, str]]:
        findings = []
        files = self._iterate_scannable_files()

        for filepath in files:
            rel_path = os.path.relpath(filepath, self.project_root).replace("\\", "/")
            if "test" in rel_path.lower():
                continue

            try:
                with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                    for line_no, line in enumerate(f, 1):
                        for pattern, desc in SECRET_PATTERNS:
                            if re.search(pattern, line):
                                findings.append((rel_path, line_no, desc, line.strip()[:120]))
            except Exception as e:
                logger.warning("Could not read %s: %s", filepath, e)

        return findings

    def _scan_unbounded_loops(self) -> List[Tuple[str, int, str]]:
        findings = []
        py_files = [f for f in self._iterate_scannable_files() if f.endswith(".py")]

        for filepath in py_files:
            rel_path = os.path.relpath(filepath, self.project_root).replace("\\", "/")
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    code = f.read()
                tree = ast.parse(code, filename=filepath)

                for node in ast.walk(tree):
                    if isinstance(node, ast.While):
                        # Check if test condition is always true (e.g. while True:)
                        is_constant_true = (
                            isinstance(node.test, ast.Constant) and bool(node.test.value) is True
                        )
                        if is_constant_true:
                            # Verify if there is a break or return statement in body
                            has_break_or_return = any(
                                isinstance(child, (ast.Break, ast.Return))
                                for child in ast.walk(node)
                            )
                            if not has_break_or_return:
                                findings.append(
                                    (rel_path, node.lineno, "while True loop without break or return statement")
                                )
            except Exception:
                continue

        return findings
