"""Offline scoring regression eval for the job processor guardrails.

This does not call Bedrock. Each labeled case provides a mock model score, then the
same deterministic guardrails used in production are applied. The goal is to catch
hallucination-risk regressions before deployment: stale, remote, weak, or uncited
jobs must not enter the review queue.
"""

import argparse
from datetime import datetime, timezone, timedelta
import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
PROCESSOR_DIR = ROOT / "job_processor"
if str(PROCESSOR_DIR) not in sys.path:
    sys.path.insert(0, str(PROCESSOR_DIR))

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")


def _load_cases(path: Path) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8"))


def _evaluate_case(case: dict, threshold: int) -> dict:
    import lambda_function as processor

    processor.SCORE_THRESHOLD = threshold
    job = dict(case["job"])
    # Normalize posted_at relative to eval run time so offline evals do not expire over time
    if case.get("label") == "stale_job":
        job["posted_at"] = (datetime.now(timezone.utc) - timedelta(days=20)).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif job.get("posted_at"):
        job["posted_at"] = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    mock_score = int(case["mock_model_score"])
    mock_reason = f"Mock model reason for {case['id']}."

    prefilter = processor._pre_llm_rejection(job)
    if prefilter:
        final_score, reason = prefilter
        path = "prefilter"
    else:
        final_score, reason = processor._apply_deterministic_guardrails(job, mock_score, mock_reason)
        path = "model_then_guardrail"

    status = "PENDING_APPROVAL" if final_score >= threshold else "REJECTED"
    score_ok = int(case["expected_score_min"]) <= final_score <= int(case["expected_score_max"])
    status_ok = status == case["expected_status"]

    return {
        "id": case["id"],
        "label": case["label"],
        "path": path,
        "score": final_score,
        "status": status,
        "expected_status": case["expected_status"],
        "passed": score_ok and status_ok,
        "reason": reason,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run offline scoring guardrail evals.")
    parser.add_argument("--cases", default="evals/scoring_labeled_set.json")
    parser.add_argument("--threshold", type=int, default=80)
    args = parser.parse_args()

    cases = _load_cases(ROOT / args.cases)
    results = [_evaluate_case(case, args.threshold) for case in cases]

    print(f"Scoring evals: {len(results)} cases, threshold={args.threshold}%")
    print("-" * 72)
    for result in results:
        mark = "PASS" if result["passed"] else "FAIL"
        print(
            f"{mark} {result['id']} | label={result['label']} | "
            f"path={result['path']} | score={result['score']} | status={result['status']}"
        )
        if not result["passed"]:
            print(f"     expected_status={result['expected_status']}")
            print(f"     reason={result['reason']}")

    failures = [result for result in results if not result["passed"]]
    print("-" * 72)
    print(f"Passed: {len(results) - len(failures)} / {len(results)}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
