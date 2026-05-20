# Production Debug Log - 2026-05-13

This document records the real production issues found on May 13, 2026, how they were diagnosed, what was changed, and what is still open.

## Scope

Today's work was focused on the live AWS + Telegram pipeline:

- `job-ingestor`
- `job-deduplicator`
- `job-processor`
- `job-router`
- `job-sender`
- DynamoDB
- SQS
- SNS
- IAM role policies
- Bedrock runtime behavior

## 1. Empty Queue Despite Successful Searches

### Symptom

Telegram kept returning:

`Use search jobs to scan live sources, then list to review real matches.`

even after live ingestion had fetched real jobs.

### What We Found

- A live ingestion run fetched `206` jobs:
  - `Hacker News`: `183`
  - `Adzuna`: `23`
  - `Google CSE`: `0`
  - `JSearch`: `0`
- The deduplicator treated all `206` jobs as duplicates.
- Old jobs with final statuses like `REJECTED` were being skipped forever.
- That meant new scoring logic and profile changes were never applied to older rows.

### Root Cause

`job-deduplicator` only re-forwarded duplicates if they were still `NEW` and unscored.
Previously rejected rows were effectively dead forever.

### Fix

Updated `job-deduplicator` and `job-processor` so older rows can be re-evaluated when the evaluation profile version changes.

### Result

- Duplicate rows can now be rescored safely.
- Production can recover from earlier scoring mistakes without deleting the table.

## 2. Lowering Threshold To 80 Did Not Refill The Queue

### Symptom

The code and Lambda env were updated from `90` to `80`, but Telegram still looked empty.

### What We Found

- Threshold was live in:
  - `job-router`
  - `job-processor`
- But older rejected rows still carried the older evaluation version.
- Because of that, the deduplicator did not requeue those rows under the new threshold.

### Root Cause

Threshold changes and evaluation-profile changes were not linked.
So changing the threshold did not automatically force a re-evaluation of old rows.

### Fix

Changed evaluation versioning to include threshold in the default profile version:

- old style: `2026-05-13`
- new style: `2026-05-13-th80`

This makes a threshold change visible to dedup logic and forces a clean re-score path.

### Result

After redeploy and rerun:

- old rows were rescored under `th80`
- new high-match rows appeared again in Telegram

Examples that re-entered the queue:

- `Hyperspell` - `92`
- `Strix` - `82`
- `Octozi` - `88`

## 3. Signal Test Notification Did Not Reach Telegram

### Symptom

`signal test` was accepted by Telegram, but no notification arrived.

### What We Found

The synthetic job flowed partway through production:

- router received command
- ingestor seeded synthetic job
- processor scored it
- DynamoDB was updated

But notification failed because of missing IAM permissions.

### Root Causes

The `jobsearch-processor-policy` role was missing several permissions at different points:

- `sns:Publish`
- `s3:GetObject`
- `s3:ListBucket`
- `secretsmanager:GetSecretValue` for `jobsearch/hunterio`
- `dynamodb:GetItem`
- `dynamodb:PutItem`
- `dynamodb:UpdateItem`

### Fix

Updated the processor role inline policy with the missing permissions.

### Result

The full live path worked:

`Telegram -> ingestor -> SQS -> processor -> SNS -> router -> Telegram`

The user confirmed they received the live synthetic notification.

## 4. Adzuna Jobs Were Scoring As Zero

### Symptom

Many Adzuna jobs were rejected with:

`Bedrock error: AccessDeniedException`

### What We Found

The processor role still could not invoke Bedrock inference for the configured inference profile.

### Root Cause

The processor role had no Bedrock inference permission.

### Fix

Added Bedrock inference permissions to the live `jobsearch-processor-policy` role:

- `bedrock:InvokeModel`
- `bedrock:InvokeModelWithResponseStream`
- `bedrock:Converse`
- `bedrock:ConverseStream`

### Result

Adzuna rows started scoring normally.

Examples after fix:

- `Monaco | AI Engineer` -> `88`
- `Anyscale | Distributed LLM Inference Engineer` -> `78`
- `Plaud | ML Engineer, Inference & Serving` -> `78`

## 5. Google CSE Returned No Jobs

### Symptom

Google CSE never contributed rows in live production.

### What We Found

The ingestor returned:

`HTTP 403: This project does not have the access to Custom Search JSON API.`

### Root Cause

The Google Cloud project/credentials exist, but the Custom Search JSON API is not enabled for that project.

### Fix

No code fix required.
This is a cloud configuration issue.

### Current Status

Still blocked until the Google API is enabled.

## 6. JSearch Returned No Jobs

### Symptom

JSearch contributed `0` jobs in live runs.

### What We Found

The ingestor returned:

`HTTP 429 rate limited for query=AI Engineer San Francisco`

### Root Cause

JSearch credentials are valid, but the source is being rate-limited.

### Fix

No permanent fix was deployed today.

### Current Status

Still rate-limited.
Recommended next step is query reduction, backoff, or scheduled spreading.

## 7. Work Mode Evidence Was Inconsistent On Older Rows

### Symptom

Telegram alerts could show:

- `Work mode: onsite`

while the stored DynamoDB row still had:

- `work_mode = null`
- `work_mode_source = null`

### What We Found

Some older rows were rescored and re-alerted, but duplicate refresh did not fully persist newer source-backed work mode fields into DynamoDB.

### Root Cause

Duplicate refresh was not writing all newer normalized source fields back into old rows.

### Fix

Partial fix already existed in refresh logic, but this area still needs a tighter pass to guarantee full field refresh for older rows.

### Current Status

Improved, but not fully closed.
This remains an open cleanup item.

## 8. Draft Preview Failed With Bedrock Throttling

### Symptom

Telegram `draft 1` returned:

`Could not generate draft preview: ThrottlingException`

This happened while processor traffic was also generating automatic high-match alerts.

### What We Found

The live `job-sender` Lambda hit Bedrock throttling during preview generation:

- sender started preview
- Bedrock `Converse` throttled
- preview failed hard and returned raw error text to Telegram

### Root Cause

`job-sender` had no graceful degradation path for preview-time Bedrock throttling.

### Fix

Updated:

- `job-sender`
- `job-router`

New behavior:

- retry brief Bedrock throttles
- if preview still cannot get capacity, return a safe deterministic fallback draft
- Telegram labels that preview clearly as fallback-generated

### Result

Preview should no longer hard-fail on transient Bedrock throttling.

## 9. Email Tone And Project Grounding Still Need Tightening

### Symptom

Drafts still sometimes sound like older generic recruiter-style copy:

- too much model freedom
- embellished phrasing
- older patterns like over-explaining projects
- weak greeting choice like `Hi Founders,`

### What We Found

The current sender system prompt still lets the model write too much of the email body.
That allows stale style patterns to survive even after profile and greeting fixes.

### Fix Applied Today

Partial fixes already in place:

- greeting enforcement
- bad phrase blocking
- sender confirmation flow
- throttling fallback preview

### Remaining Work

Still open:

- reduce model freedom further
- make project paragraph deterministic from known facts
- treat `founders@...` as generic inbox and fall back to company/team greeting
- keep only a short model-generated opener instead of a full freeform body

This is the main unfinished quality item after today's production stabilization work.

## 10. Profile Truthfulness Fixes

### Issues Corrected

The system previously overstated or distorted the candidate profile.

### Fixes Applied

- target role standardized to `AI Engineer`
- location standardized to `Wichita, Kansas`
- preference standardized to:
  - open to remote
  - open to hybrid
  - open to onsite
  - open to relocate
  - open to any location
  - CPT student
- `RetireIQ` standardized as:
  - retirement planning and financial decision support app
- `Rivulet` standardized as:
  - personal life management app

### Result

Scoring and outreach are now much less likely to invent false candidate claims.

## Files Changed During This Work

Main code and config changes touched these areas:

- `job_deduplicator/lambda_function.py`
- `job_processor/lambda_function.py`
- `job_router/lambda_function.py`
- `job_sender/lambda_function.py`
- `shared/knowledge_graph.py`
- `tools/ops/set_production_config.py`
- `tests/test_deduplicator.py`
- `tests/test_processor.py`
- `tests/test_router.py`
- `tests/test_sender.py`

## Operational Takeaways

1. Production failures were mostly not hallucination problems at first. They were state-management and IAM problems.
2. Lowering a threshold is not enough unless older rows are re-evaluable.
3. Bedrock access and Bedrock throttling are separate failure classes and need separate handling.
4. Telegram UX should never hide near-miss jobs completely when the queue is empty.
5. Email generation should use deterministic assembly for profile facts and only allow the model to control a narrow opening section.

## Open Items After Today

1. Enable Google Custom Search JSON API.
2. Add backoff/query spreading for JSearch.
3. Finish duplicate refresh persistence for work mode fields.
4. Tighten sender generation so email tone stops sounding stale or embellished.
5. Consider a Telegram `watchlist` or `last run` command so empty queue states still show useful source evidence.

