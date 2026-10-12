"""
Unit tests for shared/url_validation.py
"""

import pytest
from shared.url_validation import is_valid_canonical_url, get_canonical_job_url


@pytest.mark.parametrize(
    "valid_url",
    [
        "https://anthropic.com/careers/ai-engineer",
        "https://boards.greenhouse.io/openai/jobs/123456",
        "https://jobs.lever.co/company/abc-123",
        "https://news.ycombinator.com/item?id=40201000",
        "http://startup.io/jobs/apply",
        "https://example.com/jobs/ai-lead",
        "https://sub.domain.co.uk/role",
    ],
)
def test_valid_canonical_urls(valid_url):
    assert is_valid_canonical_url(valid_url) is True


@pytest.mark.parametrize(
    "invalid_url",
    [
        "",
        "   ",
        None,
        123,
        "Unavailable",
        "unavailable",
        "Source link: Unavailable",
        "N/A",
        "none",
        "null",
        "undefined",
        "https://",
        "http://",
        "ftp://company.com/job",
        "javascript:alert(1)",
        "https://localhost/job",
        "https://127.0.0.1:8080/careers",
        "https://169.254.169.254/latest/meta-data/",
        "https://10.0.0.1/careers",
        "http://192.168.1.50/role",
        "http://172.16.0.1/jobs",
        "http://100.64.0.1/api",
        "http://[::1]/internal",
        "https://internal-server.local/jobs",
        "https://metadata.internal/role",
        "https://nodotdomain/jobs",
        "company.com/jobs",  # missing scheme
    ],
)
def test_invalid_canonical_urls(invalid_url):
    assert is_valid_canonical_url(invalid_url) is False


def test_get_canonical_job_url_prefers_url_then_apply_url():
    job1 = {"url": "https://company.com/job1", "apply_url": "https://company.com/apply1"}
    assert get_canonical_job_url(job1) == "https://company.com/job1"

    job2 = {"url": "", "apply_url": "https://company.com/apply2"}
    assert get_canonical_job_url(job2) == "https://company.com/apply2"

    job3 = {"url": "Unavailable", "apply_url": "https://company.com/apply3"}
    assert get_canonical_job_url(job3) == "https://company.com/apply3"

    job4 = {"url": "Unavailable", "apply_url": "N/A"}
    assert get_canonical_job_url(job4) == ""

    job5 = {"url": None, "apply_url": None}
    assert get_canonical_job_url(job5) == ""
