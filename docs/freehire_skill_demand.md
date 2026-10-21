# FreeHire + JobHunt skill-demand report

This branch adds a **read-only, opt-in** FreeHire ingestion adapter and a weekly
evidence-based Telegram skill report. No job applications, outreach, mail sending,
or existing approval gates are modified.

## Enable FreeHire ingestion (after review)
Add these to the **job-ingestor Lambda environment** and redeploy that Lambda:

```
FREEHIRE_ENABLED=true
FREEHIRE_MAX_JOBS_PER_RUN=100
```

The adapter queries FreeHire's public `/api/v1/jobs/search` endpoint with
`countries=US`, `category=ai_engineering`, and `open_within_days=7`, then sends
normalized jobs into the **existing** SQS / DynamoDB / Bedrock pipeline.
It makes at most one API request per ingestion run and does not require signup.
Other job sources continue if FreeHire is unavailable.

**Caveat:** The returned catalog is a ranked/paginated subset, not all remote US
AI jobs. Additional pages and skill-specific queries can be added after evaluating
quality and provider limits. FreeHire source identities differ from existing ATS
identities, so cross-source canonical URL deduplication should be checked during
shadow testing before enabling in production.

## Weekly Telegram report
The GitHub Actions workflow `weekly-skill-demand.yml` runs Sundays at 13:00 UTC,
and can also be started using **Run workflow**.

Configure GitHub Actions secrets:
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (existing read access to Jobsearch)
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

The workflow reads **only Jobsearch records with descriptions** from the last seven
days; counts unique IDs; normalizes skill labels; distinguishes `required`,
`preferred` and `unknown` section mentions; then sends one message to Telegram.
It does not change model scoring or perform applications.

### Limits and evidence
- Percentage denominator = unique recent ingested jobs **with stored descriptions**.
- Required/preferred labels are approximate heading-based classifications and
  should be audited against sample JDs. Unstructured text stays `unknown`.
- Some downstream prefilters may prevent a job from reaching DynamoDB.
- Current scan has a 100-page safety ceiling; Telegram marks partial scans.
- This is an **ingested pipeline sample**, not the entire FreeHire market.
- Weekly workflow depends on sufficient description retention in DynamoDB.
- FreeHire documents public reads for personal/non-commercial use.
- Run the test suite and monitor API errors/dedup quality before rollout.

## Validation
```
python -m unittest discover -s tests -p test_skill_demand.py -v
python -m py_compile tools/ops/skill_demand.py job_ingestor/freehire.py
```
