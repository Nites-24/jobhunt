# Production Debug Log - 2026-05-14

This document records the real production and product issues found on May 14,
2026, what was changed, what was verified, and what is still unresolved.

## Scope

Today's work was focused on:

- `job-router`
- `job-sender`
- Telegram UX
- feedback capture and export
- production-safe validation
- source API status review

## 1. Telegram UX Improved But Still Feels Messy

### Symptom

The bot works, but the user still experiences it as a broken or messy service:

- too much text
- too many visible controls at once
- unclear state transitions
- messages feel like internal system logs instead of a guided assistant

User screenshots confirmed that the current command center still feels like a
control panel rather than a polished assistant flow.

### What We Changed

- Added inline help navigation to `job-router`
- Added breadcrumb-style help pages
- Added inline command shortcuts for common actions
- Preserved plain-text commands so existing workflow still works

### Result

- Telegram help is more interactive than before
- Button navigation works
- The UX is still too dense and needs a second pass

### Current Status

Open.
The next UX pass should reduce visible controls and move toward context-aware
actions tied to the current job, not a large global menu.

## 2. Feedback Capture Was Working, But Not Yet Usable For Learning

### Symptom

The system could store some user actions, but there was no clean path from
production Telegram behavior to reusable evaluation data.

### What We Changed

Confirmed and extended the feedback loop:

- `interested`
- `meh`
- `not interested`
- `apply`
- `followups`
- `applied`
- `not applied`

Added tooling:

- `tools/ops/export_feedback.py`
- `tools/ops/feedback_summary.py`

### Result

The system now has:

- feedback persisted on job rows
- export into eval-ready cases
- summary tooling for preference analysis

### Current Status

Working locally and ready for offline analysis.
This does not auto-change production behavior.

## 3. Sender Draft Quality Improved, Especially For HN Roles

### Symptom

Previous drafts were too generic and often dumped multiple projects regardless
of the source or job focus.

### What We Changed

In `job-sender`:

- opener now prefers source evidence
- HN/source-heavy roles now compress long source text into shorter phrases
- the draft now selects one best-fit project instead of always dumping multiple
  projects

Examples of compressed evidence phrases now used in openers:

- `customer success agents and account intelligence`
- `context and memory for AI agents`
- `Python, PyTorch, and RAG systems`

### Result

The deployed HN-style live sender preview improved to:

`Cora AI stood out to me because the role is centered on customer success agents and account intelligence...`

This is materially better than the earlier longer copied sentence.

### Current Status

Improved, but not perfect.
Still needs tighter product judgment and cleaner Telegram presentation.

## 4. Local Regression And Production Validation Passed For The Changed Slice

### What We Verified Locally

Changed-area regression:

- `68 passed`

Broader runtime regression:

- `91 passed`

Covered areas:

- `job-ingestor`
- `job-processor`
- `job-router`
- `job-sender`
- feedback export
- feedback summary

### What We Verified In Production

Selective deploy was used for:

- `job-router`
- `job-sender`

Commit:

- `c6968d2` `feat: improve telegram UX and feedback learning`

Production-safe checks passed:

- Lambda health good
- queues and DLQs at `0`
- live sender preview succeeded
- synthetic `signal test` succeeded

### Current Status

The deployed `job-router` and `job-sender` changes are live.
The full repo was not redeployed.

## 5. Source Secrets Exist; Missing Secret Wiring Is Not The Problem

### What We Verified

Secrets exist in AWS Secrets Manager and the live ingestor points to them:

- `jobsearch/google_cse`
- `adzuna/jobsearch`
- `jobserch/jsearch`
- `jobsearch/hunterio`

Verified live ingestor env wiring:

- `GOOGLE_CSE_SECRET_NAME=jobsearch/google_cse`
- `ADZUNA_SECRET_NAME=adzuna/jobsearch`
- `JSEARCH_SECRET_NAME=jobserch/jsearch`
- `HUNTER_SECRET_NAME=jobsearch/hunterio`

### Result

The earlier hypothesis that the system might be missing source secrets is now
closed.

### Current Status

Secrets and Lambda env wiring are present.

## 6. Why The System Still Mostly Picks Hacker News

### What We Found

Current source behavior is still skewed:

- Hacker News works and dominates volume
- Adzuna works but contributes fewer rows
- JSearch is still rate-limited with `HTTP 429`
- Google CSE has two separate problems

### Google CSE Problem A

Earlier live production runs returned:

`HTTP 403: This project does not have the access to Custom Search JSON API.`

Later, the user provided a screenshot showing Google Cloud now reports the
Custom Search API as enabled.

Production-safe re-test still returned the same `403` using the current AWS
secret payload for `jobsearch/google_cse`.

That means the screenshot did not yet translate into working production access.
The remaining gap is operational:

- either the API key in AWS belongs to a different Google project
- or the enabled project state has not actually propagated to the key now in use

### Google CSE Problem B

The live `job-ingestor` Lambda currently has:

`GOOGLE_CSE_QUERIES=`

as an empty environment value.

In the original code path, that suppresses the built-in default query set and
silently disables Google CSE fetching.

This is separate from the `403` problem. Even a working key would still produce
`0` Google rows until either the env var or the fallback behavior is fixed.

### Google CSE Problem C

Even if Google CSE is now cloud-enabled, the current ingestor still stores
Google CSE rows with blank `posted_at`.

Processor behavior:

- missing date is capped by `MISSING_DATE_SCORE_CAP`
- current cap is `70`
- review threshold is `80`

That means Google CSE jobs can still be ingested but never appear in Telegram
review because they stay below the threshold due to missing dates.

### Result

The remaining source imbalance is likely:

1. Hacker News working normally
2. Adzuna smaller but usable
3. JSearch throttled
4. Google CSE still failing live auth with `403`
5. Google CSE currently has blank live queries in `job-ingestor`
6. Even after those are fixed, Google CSE still needs `posted_at` handling in
   code before it can pass the `80` review threshold

## 7. Most Important User Feedback From Today

The user's direct product judgment was:

- the system works but feels messy
- the bot is hard to understand in use
- the UI does not feel polished
- the user wants this to make life easier, not become a job-board competitor

This should guide future work:

- fewer sources, better quality
- fewer visible controls, clearer actions
- better source grounding
- better fresh-job ranking

## 8. What The Next Chat Should Do First

Do not restart from generic debugging.

Start here:

1. Re-test Google CSE live against the exact AWS secret now in use
2. Fix the live `GOOGLE_CSE_QUERIES` configuration or fallback behavior so
   blank env values do not silently disable Google
3. If Google CSE returns rows after that, fix `posted_at` extraction and
   freshness handling so Google rows can actually enter the `80%+` queue
4. After source quality improves, do the next Telegram UX cleanup pass:
   reduce control-panel feel and make actions context-aware

## 9. Files Most Relevant For The Next Chat

- `job_ingestor/lambda_function.py`
- `job_processor/lambda_function.py`
- `job_router/lambda_function.py`
- `job_sender/lambda_function.py`
- `tools/ops/verify_live_production.py`
- `tools/ops/export_feedback.py`
- `tools/ops/feedback_summary.py`
- `AGENTS.md`
- `docs/implementation_2026_05_13.md`

## 10. Late-Night Router Deploy And Verification

### What Changed

The local Telegram assistant-flow updates in `job_router/lambda_function.py`
were selectively deployed to the live `job-router` Lambda on
`2026-05-14T23:04:49Z`.

This deploy included:

- active review-session state per Telegram chat
- `next` / `why` / bare `draft` / bare `apply` using the active review item
- `skip` requiring a reason
- shorter assistant-style review messaging
- stale review filtering already present in the local router code path

### What Was Verified

- targeted local router tests passed before deploy:
  - `pytest tests/test_router.py -q`
  - `42 passed`
- AWS reported the live `job-router` update at:
  - `2026-05-14T23:04:49.000+0000`
- deployed Lambda code hash matched the local packaged router hash:
  - `ozviZX7qiX8S1CmncU1kjwNm/mxEhiZklycAkXJkf6g=`

### Important Limitation

This was not a full Telegram end-to-end verification. No live bot interaction
was executed from the repo after deploy, so the remaining live check is a real
user `list` / `next` interaction against the deployed bot.

## 11. Telegram UX Cleanup Progress

### What Was Implemented

Selective `job-router` deploys were used to turn the Telegram bot from a
command-heavy control panel into a more stateful daily-use flow.

Delivered behavior:

- Home screen now uses short product-style copy instead of command-manual copy
- Home keyboard is intentionally lighter:
  - `Next Match`, `More`
  - `Search`, `List`, `Follow-up`
- Review-state keyboard is separate and only appears when a real reviewable
  match exists:
  - `Why`, `Draft`, `Apply`, `Skip`
  - `Next Match`
- `More` is a real advanced surface instead of leaking onto the home screen
- `next` now renders a compact card-style review message
- stale reply-keyboard state is actively replaced instead of lingering in chat

### Hidden Backlog Flow

The user found a real product gap:

- `status` showed pending rows
- `list` showed no fresh reviewable rows
- but there was no human-friendly way to inspect the hidden pending jobs

That was fixed with a new `backlog` flow:

- `status` now points to `backlog` when pending rows exist but nothing is
  reviewable
- `list` now points to `backlog` in the same condition
- `backlog` shows hidden pending jobs with:
  - title
  - company/location
  - match %
  - hidden reason
  - source
  - source link
  - posted date when available

### Current Verified Truth

Live checks and user screenshots show:

- the home surface is cleaner and less confusing
- `More` is reachable and stable
- `Status` correctly shows pending vs reviewable counts
- `List` correctly hides stale jobs from active review
- hidden HN jobs are often blocked by the `HN_MAX_JOB_AGE_DAYS=3` rule
- `next` does not enter review mode when `Reviewable now: 0`
- this is expected behavior, not a transition bug

### What Still Remains

- `backlog` output still needs live screenshot verification after the latest
  deploy
- the full review-state flow (`next` -> `why` -> `draft` -> `skip` /
  `apply`) still needs a live test when a real fresh reviewable job exists
- the broader pipeline backlog is still semantically messy because it mixes:
  - stale real jobs
  - synthetic/test rows
  - practice/showcase rows
- Google CSE and JSearch source issues remain unresolved outside the Telegram
  UX slice
