import json

from tools.ops.feedback_summary import format_summary, load_feedback_cases, summarize_feedback


def test_summarize_feedback_counts_labels_sources_and_reasons():
    cases = [
        {
            "label": "negative",
            "stage": "review_feedback",
            "reason": "too remote",
            "job": {"source": "hackernews"},
        },
        {
            "label": "negative",
            "stage": "application_outcome",
            "reason": "too remote",
            "job": {"source": "hackernews"},
        },
        {
            "label": "neutral",
            "stage": "review_feedback",
            "reason": "draft generic",
            "job": {"source": "adzuna"},
        },
        {
            "label": "positive",
            "stage": "application_outcome",
            "reason": "",
            "job": {"source": "hackernews"},
        },
    ]

    summary = summarize_feedback(cases, top_n=3)

    assert summary["total_cases"] == 4
    assert summary["by_label"]["negative"] == 2
    assert summary["by_stage"]["review_feedback"] == 2
    assert summary["by_source"]["hackernews"] == 3
    assert summary["top_reasons"]["too remote"] == 2
    assert summary["top_reasons_by_label"]["negative"]["too remote"] == 2


def test_format_summary_includes_sections():
    summary = {
        "total_cases": 2,
        "by_label": {"positive": 1, "negative": 1},
        "by_stage": {"review_feedback": 1, "application_outcome": 1},
        "by_source": {"hackernews": 2},
        "top_reasons": {"too remote": 1},
        "top_reasons_by_label": {
            "positive": {},
            "neutral": {},
            "negative": {"too remote": 1},
        },
    }

    rendered = format_summary(summary)

    assert "Feedback Summary" in rendered
    assert "By label:" in rendered
    assert "Top sources:" in rendered
    assert "Top negative reasons:" in rendered


def test_load_feedback_cases_jsonl(tmp_path):
    path = tmp_path / "feedback.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"id": "a", "label": "positive"}),
                json.dumps({"id": "b", "label": "negative"}),
            ]
        ),
        encoding="utf-8",
    )

    cases = load_feedback_cases(path)

    assert [case["id"] for case in cases] == ["a", "b"]
