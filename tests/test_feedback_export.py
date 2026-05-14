import json
from pathlib import Path

from tools.ops.export_feedback import build_feedback_case, build_feedback_export, write_export


def test_build_feedback_case_from_interested_review():
    case = build_feedback_case(
        {
            "job_id": "job-1",
            "user_feedback": "interested",
            "user_interest": "interested",
            "user_feedback_at": 1715551000,
            "feedback_source": "telegram",
            "title": "Applied AI Engineer",
            "company": "Cora AI",
            "match_score": 94,
            "description": "Strong fit for applied AI product work.",
        }
    )

    assert case is not None
    assert case["label"] == "positive"
    assert case["stage"] == "review_feedback"
    assert case["job"]["title"] == "Applied AI Engineer"


def test_build_feedback_case_from_not_applied_outcome():
    case = build_feedback_case(
        {
            "job_id": "job-2",
            "user_feedback": "not_applied",
            "application_outcome": "not_applied",
            "user_feedback_reason": "too generic",
            "user_feedback_at": 1715552000,
            "feedback_source": "telegram",
            "title": "AI Engineer",
            "company": "Example AI",
        }
    )

    assert case is not None
    assert case["label"] == "negative"
    assert case["stage"] == "application_outcome"
    assert case["reason"] == "too generic"


def test_build_feedback_export_sorts_newest_first_and_skips_missing_feedback():
    export = build_feedback_export(
        [
            {"job_id": "older", "user_feedback": "neutral", "user_feedback_at": 100},
            {"job_id": "skip-me", "title": "No feedback"},
            {"job_id": "newer", "user_feedback": "applied", "user_feedback_at": 200},
        ]
    )

    assert [case["job_id"] for case in export] == ["newer", "older"]
    assert export[0]["label"] == "positive"
    assert export[1]["label"] == "neutral"


def test_write_export_jsonl(tmp_path: Path):
    output = tmp_path / "feedback.jsonl"
    cases = [{"id": "feedback-1", "label": "positive"}, {"id": "feedback-2", "label": "negative"}]

    write_export(cases, output, "jsonl")

    lines = output.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["id"] == "feedback-1"
