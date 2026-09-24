# Contributing to JobHunt

## Development Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/Nites-24/jobhunt.git
   cd jobhunt
   ```

2. **Install dependencies**

   ```bash
   pip install pytest moto[all] boto3 ruff
   ```

3. **Set up environment**

   Copy `.env.example` to `.env` and fill in your AWS credentials:

   ```bash
   cp .env.example .env
   ```

4. **Run tests**

   ```bash
   pytest -q
   ```

## Code Organization

Each Lambda function lives in its own directory with a single `lambda_function.py`:

```
job_ingestor/lambda_function.py     # Fetches jobs from sources
job_deduplicator/lambda_function.py # Deduplicates via DynamoDB hash
job_processor/lambda_function.py    # Scores with Bedrock + guardrails
job_router/lambda_function.py       # Telegram control plane
job_sender/lambda_function.py       # Email preview/send
```

Shared runtime code goes in `shared/` and is packaged into every Lambda zip.

## Testing

- **Unit tests** (`tests/test_*.py`): Use `moto` to mock all AWS services. No real credentials needed.
- **Live tests** (`tests/live/`): Opt-in tests that hit real AWS. Only run when deploying or debugging.
- **Scoring evals** (`tools/ops/eval_scoring.py`): Offline guardrail regression tests against labeled cases.

```bash
# Run all unit tests
pytest -q

# Run specific test file
pytest tests/test_router.py -q

# Run scoring evals
python tools/ops/eval_scoring.py
```

## Making Changes

### Working Protocol

1. Identify one bounded problem
2. Inspect the relevant code and tests before editing
3. Make focused changes only for that problem
4. Add or update targeted tests
5. Run targeted tests
6. Report what changed, what was verified, and what remains

### Do Not

- Bundle unrelated fixes in one commit
- Run live ingestion/scoring as a first diagnostic step
- Use Bedrock or live APIs to answer questions that can be answered from code, tests, or docs
- Delete DynamoDB rows to force behavior — use evaluation versioning instead

### Commit Messages

Use conventional commit format:

```
feat: add Telegram feedback capture
fix: correct dedup reprocessing for threshold changes
chore: update deployment script
docs: add May 13 production debug log
```

## Deployment

### Local Deploy

```bash
python deploy.py
```

### CI/CD Deploy

Push to `main` with changes in Lambda directories, or use the GitHub Actions `Deploy` workflow dispatch.

### Safety Rules

- Real email sending requires **both** `ALLOW_TELEGRAM_EMAIL_SEND=true` (router) and `ALLOW_REAL_EMAIL_SEND=true` (sender)
- `apply` does not send email — it moves to manual follow-up
- Live tests (`tests/live/`) and `verify_live_production.py` are opt-in live operations

## Code Quality

```bash
# Lint
ruff check . --select E,F,W --ignore E501

# Type checking (optional)
mypy shared/ --ignore-missing-imports
```

## Production Safety

Before making production changes, read:

- [AGENTS.md](AGENTS.md) — Operating contract
- [docs/implementation_2026_05_13.md](docs/implementation_2026_05_13.md) — Known failure classes
- [docs/implementation_2026_05_14.md](docs/implementation_2026_05_14.md) — UX decisions
