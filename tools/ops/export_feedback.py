"""Export Telegram feedback rows from DynamoDB into eval-ready JSON or JSONL.

This is intentionally offline-friendly: the shaping logic is pure Python so it
can be tested without AWS, while the CLI can still pull from the live table
when you want a real production export.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TABLE = os.environ.get("DYNAMODB_TABLE", "Jobsearch")


def _load_env() -> None:
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())


def _feedback_label(value: str) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"interested", "applied"}:
        return "positive"
    if raw in {"meh", "neutral", "maybe"}:
        return "neutral"
    return "negative"


def _feedback_stage(item: dict[str, Any]) -> str:
    if item.get("application_outcome"):
        return "application_outcome"
    return "review_feedback"


def _excerpt(text: Any, limit: int = 280) -> str:
    raw = " ".join(str(text or "").split())
    if len(raw) <= limit:
        return raw
    return raw[: limit - 3].rstrip() + "..."


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def build_feedback_case(item: dict[str, Any]) -> dict[str, Any] | None:
    """Convert one DynamoDB job row with feedback into an eval-ready case."""
    feedback = str(item.get("user_feedback") or "").strip().lower()
    if not feedback:
        return None

    feedback_at = _to_int(item.get("user_feedback_at"))
    if not feedback_at:
        return None

    job_id = str(item.get("job_id") or "").strip()
    if not job_id:
        return None

    return {
        "id": f"feedback-{job_id}-{feedback_at}",
        "job_id": job_id,
        "stage": _feedback_stage(item),
        "label": _feedback_label(feedback),
        "user_feedback": feedback,
        "user_interest": item.get("user_interest"),
        "application_outcome": item.get("application_outcome"),
        "reason": str(item.get("user_feedback_reason") or "").strip(),
        "feedback_at": feedback_at,
        "feedback_source": item.get("feedback_source", "unknown"),
        "job": {
            "title": item.get("title"),
            "company": item.get("company"),
            "location": item.get("location"),
            "source": item.get("source"),
            "posted_at": item.get("posted_at"),
            "match_score": _to_int(item.get("match_score"), default=-1),
            "status": item.get("status"),
            "work_mode": item.get("work_mode"),
            "work_mode_source": item.get("work_mode_source"),
            "url": item.get("url"),
            "apply_url": item.get("apply_url"),
            "recruiter_email": item.get("recruiter_email"),
            "source_grounding": item.get("source_grounding"),
            "linked_page_url": item.get("linked_page_url"),
            "linked_page_excerpt": _excerpt(item.get("linked_page_excerpt")),
            "description_excerpt": _excerpt(item.get("description")),
        },
    }


def build_feedback_export(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build a sorted export from raw DynamoDB items."""
    cases = []
    for item in items:
        case = build_feedback_case(item)
        if case:
            cases.append(case)
    return sorted(cases, key=lambda case: case["feedback_at"], reverse=True)


def fetch_feedback_items(table_name: str, limit: int | None = None) -> list[dict[str, Any]]:
    dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    table = dynamodb.Table(table_name)

    items: list[dict[str, Any]] = []
    scan_args: dict[str, Any] = {
        "FilterExpression": Attr("user_feedback").exists() & Attr("user_feedback_at").exists(),
    }

    while True:
        response = table.scan(**scan_args)
        items.extend(response.get("Items", []))
        if limit and len(items) >= limit:
            return items[:limit]
        if "LastEvaluatedKey" not in response:
            return items[:limit] if limit else items
        scan_args["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def write_export(cases: list[dict[str, Any]], output_path: Path, fmt: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "jsonl":
        output_path.write_text(
            "\n".join(json.dumps(case, ensure_ascii=True) for case in cases) + ("\n" if cases else ""),
            encoding="utf-8",
        )
        return
    output_path.write_text(json.dumps(cases, indent=2, ensure_ascii=True), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Telegram feedback rows into eval-ready JSON.")
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--format", choices=("json", "jsonl"), default="json")
    parser.add_argument("--output", default="evals/feedback_export.json")
    args = parser.parse_args()

    _load_env()
    items = fetch_feedback_items(args.table, args.limit)
    cases = build_feedback_export(items)
    output_path = ROOT / args.output
    write_export(cases, output_path, args.format)

    counts = {"positive": 0, "neutral": 0, "negative": 0}
    for case in cases:
        counts[case["label"]] = counts.get(case["label"], 0) + 1

    print(f"Exported {len(cases)} feedback cases to {output_path}")
    print(f"positive={counts.get('positive', 0)} neutral={counts.get('neutral', 0)} negative={counts.get('negative', 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
