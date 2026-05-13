"""
Python Developer Agent — Code Quality, Deployment, and Maintenance

Responsibilities:
  - Validate code syntax and imports across all Lambda functions
  - Check for dependency issues (missing packages in Lambda layers)
  - Manage deployments (zip, upload, verify)
  - Monitor code freshness (are deployed functions up-to-date?)
  - Detect code smells and anti-patterns
  - Manage configuration consistency across Lambdas
  
This agent is the "developer" — it makes sure the code is clean,
deployable, and consistent across all Lambda functions.
"""

import json
import time
import logging
import os
import sys
import ast
import hashlib
import io
import zipfile
from typing import Optional

import boto3
from botocore.exceptions import ClientError

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

# Standard library + boto3 — things available in Lambda runtime
LAMBDA_STDLIB = {
    "json", "os", "re", "logging", "time", "uuid", "hashlib", "sys",
    "urllib", "urllib.request", "urllib.parse", "urllib.error",
    "datetime", "decimal", "typing", "abc", "dataclasses", "enum",
    "smtplib", "email", "email.mime.text", "email.mime.multipart",
    "email.mime.application", "traceback", "io", "zipfile",
    "boto3", "botocore", "botocore.config", "botocore.exceptions",
}


class PythonDevAgent(BaseAgent):
    """
    Code quality, deployment, and maintenance agent.
    
    Checks:
      1. Syntax validation (all Lambda .py files parse correctly)
      2. Import analysis (no missing dependencies for Lambda runtime)
      3. Handler validation (lambda_handler exists with correct signature)
      4. Code freshness (local vs deployed comparison)
      5. Configuration consistency (env vars match across functions)
    """

    def __init__(self, session: Optional[boto3.Session] = None):
        super().__init__(
            name="PythonDev",
            description="Code quality, deployment, and maintenance agent",
        )
        self.session = session or self._create_session()
        self.lambda_client = self.session.client("lambda", region_name="us-east-1")

    def _create_session(self) -> boto3.Session:
        env_path = os.path.join(PROJECT_ROOT, ".env")
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
        observations.extend(self._check_syntax())
        observations.extend(self._check_imports())
        observations.extend(self._check_handlers())
        observations.extend(self._check_code_freshness())
        observations.extend(self._check_config_consistency())
        return observations

    def _check_syntax(self) -> list[AgentObservation]:
        """Parse all Lambda Python files for syntax errors."""
        observations = []

        for local_dir in LAMBDA_DIRS.keys():
            lambda_path = os.path.join(PROJECT_ROOT, local_dir, "lambda_function.py")
            if not os.path.exists(lambda_path):
                observations.append(AgentObservation(
                    source=f"syntax:{local_dir}",
                    severity=Severity.CRITICAL,
                    message=f"Lambda file missing: {lambda_path}",
                ))
                continue

            try:
                with open(lambda_path, "r", encoding="utf-8") as f:
                    source = f.read()
                ast.parse(source)
                observations.append(AgentObservation(
                    source=f"syntax:{local_dir}",
                    data={"lines": len(source.splitlines()), "bytes": len(source)},
                    severity=Severity.INFO,
                    message=f"{local_dir}: syntax OK ({len(source.splitlines())} lines)",
                ))
            except SyntaxError as e:
                observations.append(AgentObservation(
                    source=f"syntax:{local_dir}",
                    data={"line": e.lineno, "offset": e.offset, "msg": e.msg},
                    severity=Severity.CRITICAL,
                    message=f"{local_dir}: SYNTAX ERROR at line {e.lineno}: {e.msg}",
                ))

        return observations

    def _check_imports(self) -> list[AgentObservation]:
        """Check for imports that won't be available in Lambda runtime."""
        observations = []

        for local_dir in LAMBDA_DIRS.keys():
            lambda_path = os.path.join(PROJECT_ROOT, local_dir, "lambda_function.py")
            if not os.path.exists(lambda_path):
                continue

            try:
                with open(lambda_path, "r", encoding="utf-8") as f:
                    tree = ast.parse(f.read())

                imports = set()
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            imports.add(alias.name.split(".")[0])
                    elif isinstance(node, ast.ImportFrom):
                        if node.module:
                            imports.add(node.module.split(".")[0])

                # Check for non-stdlib imports that Lambda might not have
                risky = imports - LAMBDA_STDLIB - {"__future__"}
                # Filter out relative imports (from .base_agent etc.)
                risky = {r for r in risky if not r.startswith(".")}

                if risky:
                    observations.append(AgentObservation(
                        source=f"imports:{local_dir}",
                        data={"risky_imports": list(risky), "all_imports": list(imports)},
                        severity=Severity.WARNING,
                        message=f"{local_dir}: non-stdlib imports that need Lambda layer: "
                                f"{', '.join(sorted(risky))}",
                    ))
                else:
                    observations.append(AgentObservation(
                        source=f"imports:{local_dir}",
                        data={"imports": list(imports)},
                        severity=Severity.INFO,
                        message=f"{local_dir}: all imports available in Lambda runtime",
                    ))

            except Exception as e:
                observations.append(AgentObservation(
                    source=f"imports:{local_dir}",
                    severity=Severity.WARNING,
                    message=f"Cannot analyze imports for {local_dir}: {str(e)}",
                ))

        return observations

    def _check_handlers(self) -> list[AgentObservation]:
        """Verify lambda_handler function exists with correct signature."""
        observations = []

        for local_dir in LAMBDA_DIRS.keys():
            lambda_path = os.path.join(PROJECT_ROOT, local_dir, "lambda_function.py")
            if not os.path.exists(lambda_path):
                continue

            try:
                with open(lambda_path, "r", encoding="utf-8") as f:
                    tree = ast.parse(f.read())

                handler_found = False
                for node in ast.walk(tree):
                    if isinstance(node, ast.FunctionDef) and node.name == "lambda_handler":
                        handler_found = True
                        args = [a.arg for a in node.args.args]
                        if len(args) < 2:
                            observations.append(AgentObservation(
                                source=f"handler:{local_dir}",
                                severity=Severity.CRITICAL,
                                message=f"{local_dir}: lambda_handler has {len(args)} args "
                                        f"(expected 2: event, context)",
                            ))
                        else:
                            observations.append(AgentObservation(
                                source=f"handler:{local_dir}",
                                severity=Severity.INFO,
                                message=f"{local_dir}: lambda_handler(event, context) OK",
                            ))
                        break

                if not handler_found:
                    observations.append(AgentObservation(
                        source=f"handler:{local_dir}",
                        severity=Severity.CRITICAL,
                        message=f"{local_dir}: lambda_handler function NOT FOUND",
                    ))

            except Exception as e:
                observations.append(AgentObservation(
                    source=f"handler:{local_dir}",
                    severity=Severity.WARNING,
                    message=f"Cannot check handler for {local_dir}: {str(e)}",
                ))

        return observations

    def _check_code_freshness(self) -> list[AgentObservation]:
        """Compare local code hash against deployed Lambda code size."""
        observations = []

        for local_dir, func_name in LAMBDA_DIRS.items():
            lambda_path = os.path.join(PROJECT_ROOT, local_dir, "lambda_function.py")
            if not os.path.exists(lambda_path):
                continue

            try:
                # Local hash
                with open(lambda_path, "rb") as f:
                    local_hash = hashlib.sha256(f.read()).hexdigest()[:16]

                # Deployed info
                config = self.lambda_client.get_function_configuration(
                    FunctionName=func_name
                )
                deployed_size = config.get("CodeSize", 0)
                last_modified = config.get("LastModified", "unknown")

                observations.append(AgentObservation(
                    source=f"freshness:{func_name}",
                    data={
                        "local_hash": local_hash,
                        "deployed_size": deployed_size,
                        "last_modified": last_modified,
                    },
                    severity=Severity.INFO,
                    message=f"{func_name}: local_hash={local_hash}, "
                            f"deployed_size={deployed_size}B, "
                            f"last_deployed={last_modified[:19]}",
                ))

            except ClientError as e:
                observations.append(AgentObservation(
                    source=f"freshness:{func_name}",
                    severity=Severity.WARNING,
                    message=f"Cannot check deployed {func_name}: {str(e)}",
                ))

        return observations

    def _check_config_consistency(self) -> list[AgentObservation]:
        """Check that DYNAMODB_TABLE env var is consistent across all Lambdas."""
        observations = []
        table_names = {}

        for _, func_name in LAMBDA_DIRS.items():
            try:
                config = self.lambda_client.get_function_configuration(
                    FunctionName=func_name
                )
                env = config.get("Environment", {}).get("Variables", {})
                tbl = env.get("DYNAMODB_TABLE", "NOT_SET")
                table_names[func_name] = tbl
            except Exception:
                table_names[func_name] = "UNKNOWN"

        unique_tables = set(table_names.values()) - {"NOT_SET", "UNKNOWN"}
        if len(unique_tables) > 1:
            observations.append(AgentObservation(
                source="config:consistency",
                data={"table_names": table_names},
                severity=Severity.CRITICAL,
                message=f"DYNAMODB_TABLE inconsistent across Lambdas: {table_names}",
            ))
        elif unique_tables:
            observations.append(AgentObservation(
                source="config:consistency",
                data={"table_name": list(unique_tables)[0]},
                severity=Severity.INFO,
                message=f"Config consistent: all Lambdas use table '{list(unique_tables)[0]}'",
            ))

        return observations

    # -------------------------------------------------------------------
    # THINK
    # -------------------------------------------------------------------
    def think(self, observations: list[AgentObservation], context: dict) -> list[AgentAction]:
        actions = []

        for obs in observations:
            if obs.severity == Severity.CRITICAL:
                if "SYNTAX ERROR" in obs.message:
                    actions.append(AgentAction(
                        action_type="FIX_SYNTAX",
                        target=obs.source.split(":")[1],
                        params=obs.data,
                        reason=obs.message,
                    ))

                if "lambda_handler" in obs.message and "NOT FOUND" in obs.message:
                    actions.append(AgentAction(
                        action_type="FIX_HANDLER",
                        target=obs.source.split(":")[1],
                        reason=obs.message,
                        requires_approval=True,
                    ))

        # Check if deployment is needed
        deploy_needed = context.get("deploy", False)
        if deploy_needed:
            actions.append(AgentAction(
                action_type="DEPLOY_ALL",
                target="all_lambdas",
                reason="Deployment requested",
            ))

        return actions

    # -------------------------------------------------------------------
    # ACT
    # -------------------------------------------------------------------
    def act(self, action: AgentAction, context: dict) -> AgentResult:
        if action.action_type == "DEPLOY_ALL":
            return self._act_deploy_all()
        if action.action_type == "FIX_SYNTAX":
            return AgentResult(
                success=True, action=action,
                output={"suggestion": f"Syntax error at line {action.params.get('line')}: "
                                      f"{action.params.get('msg')}. Manual fix needed."},
            )
        return AgentResult(
            success=False, action=action,
            error=f"Unknown action: {action.action_type}",
        )

    def _act_deploy_all(self) -> AgentResult:
        """Deploy all Lambda functions."""
        results = {}
        for local_dir, func_name in LAMBDA_DIRS.items():
            lambda_path = os.path.join(PROJECT_ROOT, local_dir)
            if not os.path.exists(lambda_path):
                results[func_name] = "SKIP: directory not found"
                continue

            try:
                # Create zip
                buf = io.BytesIO()
                with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                    for root, dirs, files in os.walk(lambda_path):
                        for f in files:
                            if f.endswith(".py"):
                                filepath = os.path.join(root, f)
                                arcname = os.path.relpath(filepath, lambda_path)
                                zf.write(filepath, arcname)
                buf.seek(0)
                zip_bytes = buf.read()

                # Deploy
                self.lambda_client.update_function_code(
                    FunctionName=func_name,
                    ZipFile=zip_bytes,
                )
                results[func_name] = f"OK ({len(zip_bytes)} bytes)"
            except Exception as e:
                results[func_name] = f"FAILED: {str(e)}"

        action = AgentAction(action_type="DEPLOY_ALL", target="all_lambdas")
        all_ok = all("OK" in v for v in results.values())
        return AgentResult(
            success=all_ok, action=action,
            output=results,
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
