# JobHunt Agent Operating Contract

This file is the first document every coding agent should read before changing
this repository. It exists to prevent repeated rediscovery, token waste, and
production changes that do not match the actual job-search workflow.

## Mission

Build and maintain a Telegram-controlled autonomous job pipeline. The system
finds fresh, real AI engineering opportunities, scores them against the candidate
profile, explains the evidence, drafts concise outreach, and keeps a human approval
gate before any real email is sent.

The product promise is not "more jobs." The promise is fewer, fresher, better
grounded opportunities with source evidence.

## Current Architecture

- `job_ingestor/`: fetches live jobs from Hacker News, Google CSE, ATS boards, and
  JSearch when configured.
- `job_deduplicator/`: normalizes duplicates, refreshes old rows, and requeues
  jobs when the evaluation profile changes.
- `job_processor/`: applies deterministic guardrails, scores jobs with Bedrock,
  stores score/status in DynamoDB, and emits high-match SNS notifications.
- `job_router/`: Telegram control plane for `search jobs`, `list`, `status`,
  `explain`, `draft`, `apply`, `reject`, and guarded `send`.
- `job_sender/`: preview/send email engine. It must use deterministic candidate
  facts and only narrow LLM freedom.
- `shared/knowledge_graph.py`: compact candidate memory used to reduce recurring
  prompt tokens while keeping high-signal evidence.
- `agents/`: local health-check agents. They are operational diagnostics, not
  the production Lambda runtime.
- `tests/`: mocked unit tests.
- `tests/live/`: opt-in AWS smoke tests that may spend money or trigger live
  services.
- `tools/ops/`: deployment, reconciliation, live verification, and diagnostics.
- `docs/`: architecture notes, production debug logs, and implementation
  records.

## Authoritative Production Notes

Read these before diagnosing behavior:

- `docs/implementation_2026_05_13.md`
- `docs/implementation_2026_05_12.md`
- `docs/Architecture_Doc.md`
- `README.md`

The May 13 production log is especially important. It records the real failure
classes already found: dedup reprocessing, threshold versioning, IAM gaps,
Bedrock throttling, Google CSE API disablement, JSearch rate limiting, work mode
refresh gaps, and sender tone quality.

## Candidate Truth & Identity Decoupling

Do not invent or expand candidate claims. The codebase must remain 100% identity-agnostic.
Personal identity must NEVER be hardcoded into tracked source code, Lambda functions,
or committed tests.

Canonical profile attributes are injected strictly at runtime via environment variables:
- `CANDIDATE_NAME`: Candidate full name (injected at runtime via Secrets or .env)
- `TARGET_ROLE`: Target role (defaults to "AI Engineer")
- `CANDIDATE_LOCATION`: Location preferences (defaults to open to remote, hybrid, onsite, any location)
- `SENDER_EMAIL`: Verified sender email address
- `CANDIDATE_LINKEDIN`: Public LinkedIn profile URL
- `RESUME_KEY`: S3 key for resume attachment (e.g., "data/resume/resume.pdf")
- `RETIREIQ_URL`: Public RetireIQ demo URL

Domain focus areas & verified evidence:
- Focus: RAG pipelines, LLM orchestration, agentic systems, evaluation pipelines
- Skills: Python, PyTorch, AWS, LangChain, Bedrock, vector databases
- Projects:
  - RetireIQ: retirement planning and financial decision support app
  - Rivulet: personal life management app
- Evidence already verified:
  - 33% GPU cost reduction
  - 87% autonomous ticket resolution

When profile models change, update `shared/knowledge_graph.py` and the relevant unit tests.

## Three-Layer Quality Funnel & Serverless Reranking Policy

To guarantee maximum job-match accuracy without manual per-application resume tailoring
or the operational burden of self-hosting dedicated decision models (e.g., Laya / ModernBERT),
the pipeline strictly enforces a Three-Layer Quality Funnel:

1. **Layer 1: Deterministic Quality Pre-Filters (0ms, $0)**:
   - Freshness: Hacker News ≤ 3 days (`HN_MAX_JOB_AGE_DAYS`), ATS boards ≤ 14 days (`MAX_JOB_AGE_DAYS`).
   - Domain & Title: Compiled regex drops non-AI roles (pure React/frontend, legacy Java/PHP, IT, sales) before model invocation.
   - Grounding: Canonical HTTPS URL and SSRF validation required.

2. **Layer 2: Serverless Cross-Encoder Reranker (Bedrock Cohere Rerank)**:
   - Rather than deploying dedicated GPU instances, batches of raw opportunities can be
     cross-encoded using native Amazon Bedrock Rerank (`cohere.rerank-v3-5:0`).
   - Projects the candidate's canonical resume and project memory in S3 against job descriptions
     in a single ~150ms forward pass, pruning to the top 15% high-conviction leads.
   - 100% serverless, zero infrastructure maintenance, funded via AWS credits.

3. **Layer 3: Deep Context Grounding (Claude 3.5 Sonnet on Bedrock)**:
   - Top-ranked leads are evaluated using Claude 3.5 Sonnet with forced tool-calling (`submit_evaluation`).
   - Requires explicit `match_reasons` citing candidate memory and `missing_requirements` identifying caveats.
   - Guardrail caps (stale capped at 55, missing date capped at 70, missing link capped at 40) enforce hard ceilings.

4. **Layer 4: Autonomous In-Context Feedback Loop**:
   - Telegram user decisions (`interested`, `meh <reason>`, `not interested <reason>`) stored in DynamoDB
     are dynamically injected into Bedrock prompt context as the 5 most recent positive and negative exemplars.
   - The agent continuously adapts to user preference in real time without unstable self-modifying code.

## Production Safety Rules

- Do not run live ingestion, live scoring, live sender, deployment, or AWS
  reconciliation as a first diagnostic step.
- Prefer local unit tests and code inspection before any live check.
- Treat `tests/live/` and `tools/ops/verify_live_production.py` as opt-in live
  operations.
- Real email sending must remain gated by both:
  - `ALLOW_TELEGRAM_EMAIL_SEND=true` on `job-router`
  - `ALLOW_REAL_EMAIL_SEND=true` on `job-sender`
- `apply` should not silently send real email. It should keep manual follow-up
  safe unless explicit send flags are enabled.
- Do not delete DynamoDB rows to force behavior unless the user explicitly asks.
  Prefer evaluation-profile versioning and requeue logic.

## Token And Credit Rules

- Do not use Bedrock or live APIs to answer questions that can be answered from
  code, tests, logs, or docs.
- For implementation work, run targeted tests first. Run the full suite only
  when shared behavior changed.
- Avoid repeated prompt-only tuning. First check source data, state, IAM,
  freshness, and deterministic code paths.
- Keep LLM prompts narrow. Deterministic assembly should own facts, structure,
  safety language, and contact rules.
- Log token usage where Bedrock is used. Existing `LLM_TELEMETRY` logs matter.

## Source Quality Rules

Every visible opportunity should have source evidence.

Hard expectations:

- Freshness must be enforced before a job appears in Telegram review.
- Missing source links should be rejected or capped.
- Missing dates should be rejected or capped.
- Weak AI/ML signal should be rejected or capped before expensive scoring when
  possible.
- Work mode must be backed by source evidence where possible.

Current known source issues:

- Hacker News parsing now captures comment text, obfuscated emails such as
  `careers [at] cora.ai`, HN author/contact hints, and bounded linked-page
  context when outbound links are present.
- Hacker News review freshness is stricter than generic sources. The default
  HN limit is 3 days via `HN_MAX_JOB_AGE_DAYS`.
- Google CSE was blocked by a Google Cloud configuration issue: Custom Search
  JSON API was not enabled.
- JSearch has been rate-limited and should use reduced queries, backoff, or
  scheduled spreading.

## Draft Quality Rules

Drafts must sound like the candidate read the specific source.

Required behavior:

- Mention exact source evidence from the job post or linked page.
- Use one relevant project, not a generic project dump.
- Keep the body concise.
- Do not use hype, buzzwords, fake warmth, or cover-letter phrasing.
- Do not invent metrics, employers, customers, product claims, or dates.
- Use deterministic profile facts for location, project names, links, and
  signature.
- Use the best available greeting:
  - named person from source
  - HN author when clearly person-like and first-person context supports it
  - company hiring team for generic careers inboxes
  - company team only when no better signal exists

The Telegram preview and the final Gmail/send path must produce the same
quality standard. If they diverge, fix the sender path first.

## Feedback Loop Direction

The next production-learning layer should collect explicit user feedback from
Telegram without letting the model self-modify production behavior.

Recommended first implementation:

- Add review preference commands:
  - `interested 1`
  - `meh 1 <reason>`
  - `not interested 1 <reason>`
- Add real-world outcome commands for manual follow-up:
  - `followups`
  - `applied`
  - `applied 1`
  - `not applied <reason>`
  - `not applied 1 <reason>`
- Store feedback on the DynamoDB job row:
  - `application_outcome`
  - `user_interest`
  - `user_feedback`
  - `user_feedback_reason`
  - `user_feedback_at`
  - `feedback_source = telegram`
- Add a small feedback export or eval builder under `tools/ops/`.
- Use feedback to create eval cases and deterministic rules first.
- Only after enough examples exist should scoring or drafting prompts be tuned.

Current implementation:

- `tools/ops/export_feedback.py` exports Telegram feedback rows into eval-ready
  JSON or JSONL so production decisions can be reviewed offline before changing
  prompts or rules.
- `tools/ops/feedback_summary.py` turns exported or live feedback into compact
  summaries by label, stage, source, and top reasons so preference patterns are
  visible before changing behavior.

Feedback is training data for future evals and rules. It is not permission for
an agent to auto-change production prompts after one thumbs-down.

## Local Agent System

The local `agents/` package is for operational diagnostics and quality gates:

- `QAAgent`: runs offline pytest suites, measures test speed, and reports regression status.
- `SecurityAuditorAgent`: enforces zero-trust identity decoupling, audits for secrets/credentials, and runs AST analysis for bounded loops.
- `PythonDevAgent`: checks code quality, syntax, and deployment hygiene.
- `BackendEngineerAgent`: checks AWS infrastructure when enabled.
- `AIEngineerAgent`: checks Bedrock/scoring/email quality when AWS is enabled.
- `OrchestratorAgent`: coordinates all specialized agents and computes overall system health.

Use:

```powershell
python tools/ops/run_agents.py --security   # Identity, secret, and loop audit
python tools/ops/run_agents.py --qa         # QA test suite only
python tools/ops/run_agents.py              # Orchestrator health check (offline)
python tools/ops/run_agents.py --full       # Include live AWS infrastructure agents
```

`--full` can touch AWS and should be treated as a live operation.

## Working Protocol For Future Agents

1. Read this file.
2. Read the current user request.
3. Identify one bounded problem.
4. Inspect the relevant code and tests before editing.
5. Make focused changes only for that problem.
6. Add or update targeted tests.
7. Run targeted tests.
8. Report what changed, what was verified, and what remains.

Do not bundle unrelated fixes. In this project, bundled fixes waste credits and
make production behavior harder to trust.

## Current Priority Queue & Milestone Status

Milestones Completed:
1. Agent operating contract and Executive charters established (`AGENTS.md`, `.agents/workflows/`).
2. Hacker News source grounding:
   - 3-day freshness policy for HN reviewability
   - Obfuscated email extraction (`careers [at] example.ai`)
   - Outbound link enrichment with SSRF protection
3. Direct ATS board adapters:
   - Greenhouse, Lever, and Ashby integration with deterministic pre-filtering
4. Adzuna decommissioned:
   - Full purge of Adzuna endpoints, queries, and test references
5. Sender personalization & tone constraints:
   - Source-specific opener, exactly one relevant project, under 130 words, zero hype phrases
   - Telegram preview and Gmail/send path parity
6. Telegram feedback loop:
   - `interested`, `meh <reason>`, `not interested <reason>`, `applied`, `followups`
   - DynamoDB feedback persistence and offline eval export tooling (`export_feedback.py`, `feedback_summary.py`)
7. Zero-trust candidate identity decoupling:
   - 100% identity-agnostic codebase with runtime environment variable injection
8. Dedicated Security Auditor Agent:
   - PII scrubbing, credential scanning, and AST zero-loop analysis (`run_agents.py --security`)
9. Production code documentation:
   - In-file architecture docstrings and developer documentation across all 5 Lambdas, shared utilities, and ops scripts

Active Next Steps:
1. Revisit Google CSE quota resolution and Custom Search JSON API configuration.
2. JSearch query schedule spreading and backoff optimization.
