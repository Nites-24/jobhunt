"""Summarize exported Telegram feedback into preference signals.

This script is intentionally small and deterministic. It helps turn stored
production feedback into something human-readable before any scoring or prompt
changes are proposed.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from tools.ops.export_feedback import ROOT, build_feedback_export, fetch_feedback_items, _load_env


def load_feedback_cases(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return []
    if suffix == ".jsonl":
        return [json.loads(line) for line in raw.splitlines() if line.strip()]
    return json.loads(raw)


def summarize_feedback(cases: list[dict[str, Any]], top_n: int = 5) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "total_cases": len(cases),
        "by_label": Counter(),
        "by_stage": Counter(),
        "by_source": Counter(),
        "top_reasons": Counter(),
        "top_reasons_by_label": {
            "positive": Counter(),
            "neutral": Counter(),
            "negative": Counter(),
        },
    }

    for case in cases:
        label = str(case.get("label") or "unknown")
        stage = str(case.get("stage") or "unknown")
        source = str(case.get("job", {}).get("source") or "unknown")
        reason = " ".join(str(case.get("reason") or "").strip().lower().split())

        summary["by_label"][label] += 1
        summary["by_stage"][stage] += 1
        summary["by_source"][source] += 1

        if reason:
            summary["top_reasons"][reason] += 1
            summary["top_reasons_by_label"].setdefault(label, Counter())[reason] += 1

    summary["by_label"] = dict(summary["by_label"])
    summary["by_stage"] = dict(summary["by_stage"])
    summary["by_source"] = dict(summary["by_source"].most_common(top_n))
    summary["top_reasons"] = dict(summary["top_reasons"].most_common(top_n))
    summary["top_reasons_by_label"] = {
        label: dict(counter.most_common(top_n))
        for label, counter in summary["top_reasons_by_label"].items()
    }
    return summary


def format_summary(summary: dict[str, Any]) -> str:
    lines = [
        "Feedback Summary",
        "----------------",
        f"Total cases: {summary['total_cases']}",
        "",
        "By label:",
    ]

    for label, count in summary["by_label"].items():
        lines.append(f"- {label}: {count}")

    lines.extend([
        "",
        "By stage:",
    ])
    for stage, count in summary["by_stage"].items():
        lines.append(f"- {stage}: {count}")

    lines.extend([
        "",
        "Top sources:",
    ])
    if summary["by_source"]:
        for source, count in summary["by_source"].items():
            lines.append(f"- {source}: {count}")
    else:
        lines.append("- none")

    lines.extend([
        "",
        "Top reasons:",
    ])
    if summary["top_reasons"]:
        for reason, count in summary["top_reasons"].items():
            lines.append(f"- {reason}: {count}")
    else:
        lines.append("- none")

    for label in ("negative", "neutral", "positive"):
        lines.extend([
            "",
            f"Top {label} reasons:",
        ])
        reasons = summary["top_reasons_by_label"].get(label, {})
        if reasons:
            for reason, count in reasons.items():
                lines.append(f"- {reason}: {count}")
        else:
            lines.append("- none")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize Telegram feedback patterns.")
    parser.add_argument("--input", help="Existing JSON or JSONL export from export_feedback.py")
    parser.add_argument("--table", default="Jobsearch")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--top", type=int, default=5)
    args = parser.parse_args()

    if args.input:
        cases = load_feedback_cases(ROOT / args.input)
    else:
        _load_env()
        items = fetch_feedback_items(args.table, args.limit)
        cases = build_feedback_export(items)

    summary = summarize_feedback(cases, top_n=args.top)
    print(format_summary(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
