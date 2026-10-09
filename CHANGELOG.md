# Changelog

All notable changes to this project are documented in this file.

## [0.7.0] - 2026-10-09

### Added
- Direct ATS job board adapters for Greenhouse, Lever, and Ashby in `job_ingestor`.
- Zero-Trust Security Auditor Agent (`agents/security_auditor_agent.py`) with `--security` CLI flag.
- Executive agent charters and workflows in `.agents/workflows/` (`founding-ai-engineer`, `orchestrator`, `tester`, `security-auditor`, `open-source-expert`).
- Enterprise-grade architecture docstrings and developer documentation across all 5 Lambda handlers, shared utilities, and ops scripts.
- SSRF defense blocking RFC 1918 private subnets and AWS metadata (`169.254.169.254`).
- AST static analysis enforcing bounded loops and socket timeouts ($\le 4$s).
- Full test suite expansion to 252 offline unit and integration tests.

### Changed
- Decommissioned and completely purged Adzuna ingestion across pipeline, ops scripts, and test suite.
- Enforced strict 1-day freshness policy on `job-router` review queue and 3-day on Hacker News ingestion.
- Decoupled candidate identity into 100% identity-agnostic core with runtime environment injection.
- Replaced all test fixtures with mock identity (`Alex Hunter`, `candidate@example.com`).

## [0.6.0] - 2026-05-14

### Added
- Telegram assistant-flow UX with guided review (`next → why → draft → skip/apply`)
- Feedback capture commands: `interested`, `meh`, `not interested`, `applied`, `not applied`
- Feedback export tooling (`tools/ops/export_feedback.py`)
- Feedback summary tooling (`tools/ops/feedback_summary.py`)
- Backlog view for hidden pending jobs
- Active review-session state per Telegram chat

### Changed
- Home screen redesigned with product-style copy instead of command manual
- Lighter keyboard: `Next Match`, `More`, `Search`, `List`, `Follow-up`
- Draft quality improved: source-specific opener, single best-fit project
- Stale reply-keyboard state now actively replaced

### Fixed
- Sender draft tone: reduced model freedom, blocked hype phrases
- HN source evidence compression for shorter openers

## [0.5.0] - 2026-05-13

### Added
- Source telemetry: per-source, per-query yield tracking in all Lambdas
- Adzuna multi-query ingestion with 9 specialized AI role queries
- Evaluation profile versioning with threshold suffix (`2026-05-13-th80`)
- Dedup re-evaluation when profile version changes
- Pre-LLM rejection for impossible matches (saves Bedrock costs)
- Bedrock throttling retry with deterministic fallback preview
- Signal test command for end-to-end pipeline verification
- Knowledge graph for compact candidate profile (reduced token usage)

### Fixed
- Dedup reprocessing bug: all 206 jobs treated as duplicates
- Threshold change not triggering re-evaluation of old rows
- IAM permission gaps: SNS, S3, Secrets Manager, DynamoDB, Bedrock
- Adzuna jobs scoring as zero (Bedrock AccessDeniedException)
- Work mode evidence inconsistency on older rows

### Known Issues
- Google CSE: HTTP 403 (Custom Search JSON API not enabled)
- JSearch: HTTP 429 (rate-limited)
- Work mode refresh not fully persistent for all fields

## [0.4.0] - 2026-05-13

### Added
- HN grounding: 3-day freshness policy, obfuscated email extraction
- HN author/contact extraction from comment metadata
- Outbound link enrichment for HN job posts
- Feedback state persistence in DynamoDB

### Changed
- Email greeting logic: named person → HN author → company team
- Sender confirmation flow hardened

### Fixed
- Email greeting for generic inboxes (`founders@`, `careers@`)

## [0.3.0] - 2026-05-12

### Added
- Google CSE integration for ATS-direct job discovery
- Deterministic scoring guardrails (stale, missing date, low AI signal, no URL)
- Labeled scoring eval set (5 cases)
- Scoring eval runner (`tools/ops/eval_scoring.py`)

### Changed
- Repository layout standardized
- Prompt structure compacted

## [0.2.0] - 2026-05-12

### Added
- Production scoring and operational hardening
- Adzuna ingestion with normalized secrets

### Fixed
- Adzuna secret name normalization

## [0.1.0] - 2026-05-12

### Added
- Initial repository baseline
- 5 Lambda functions: ingestor, deduplicator, processor, router, sender
- DynamoDB state management
- SQS message queuing with DLQs
- SNS notifications for high-match jobs
- Telegram bot integration
- S3 candidate profile storage
- Bedrock Claude scoring with forced tool-calling
- Hunter.io email enrichment
- Local test suite with moto mocks
