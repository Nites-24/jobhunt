# Direct ATS source configuration

The ingestor supports public, read-only job-board endpoints for Greenhouse,
Lever, and Ashby. These sources are opt-in by company registry: there is no
single global endpoint, so add only companies that match the candidate profile.

## Environment variables

```text
GREENHOUSE_ENABLED=true
GREENHOUSE_BOARDS=[{"slug":"acme","company":"Acme AI"}]
LEVER_ENABLED=true
LEVER_SITES=[{"slug":"acme","company":"Acme AI"}]
ASHBY_ENABLED=true
ASHBY_BOARDS=[{"slug":"acme","company":"Acme AI"}]
ATS_TIMEOUT_SECONDS=15
ATS_MAX_JOBS_PER_BOARD=100
```

For a simple registry, pipe-delimited values are also accepted:

```text
GREENHOUSE_BOARDS=acme|Acme AI,other-company|Other Company
LEVER_SITES=acme|Acme AI
ASHBY_BOARDS=acme|Acme AI
```

The adapters normalize all three sources into the existing job schema and
retain `source_provider`, `source_origin`, `source_job_id`, `posted_at`,
`url`, and `apply_url` for deduplication and evaluation.

The endpoints are strictly read-only. Application submission is intentionally not
implemented by these adapters.
