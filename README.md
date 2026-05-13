# JobHunt Agent Pipeline

Telegram-controlled AI job pipeline for sourcing, deduplicating, scoring, reviewing, drafting, and optionally sending recruiter outreach.

## Layout

* `job_ingestor/`, `job_deduplicator/`, `job_processor/`, `job_router/`, `job_sender/` - AWS Lambda source packages.
* `shared/` - shared runtime helpers packaged into Lambda zips.
* `agents/` - local health-check and quality agents, not part of the Lambda runtime path.
* `tests/` - local unit and integration tests that use mocks.
* `tests/live/` - opt-in live AWS smoke tests.
* `tools/ops/` - operational scripts for AWS checks, reconciliation, setup, and diagnostics.
* `docs/` - architecture, implementation notes, and interview material.
* `evals/` - labeled scoring eval data.

## Common Commands

```powershell
pytest -q
python tools/ops/eval_scoring.py
python deploy.py
python tools/ops/verify_live_production.py
```

Live ingestion and live scoring spend API credits. Use them only when deploying or debugging production behavior.
