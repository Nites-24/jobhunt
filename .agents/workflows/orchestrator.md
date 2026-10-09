---
description: Multi-Agent Orchestrator workflow coordinating Security, QA, Architecture, and Pipeline Health
---

# 🤖 /orchestrator — Master Multi-Agent Pipeline Orchestrator

> **Role**: Master System Orchestrator & Technical Lead  
> **Mandate**: Coordinate all specialized agents, enforce production quality gates, and oversee end-to-end integrity across the 5-stage asynchronous serverless pipeline.

---

## 🏛️ System Architecture Context

JobHunt is not a single script; it is a distributed, event-driven autonomous AI system across 5 AWS Lambda microservices:
1. **`job_ingestor/`**: Ingests fresh opportunities from Hacker News, Google CSE, and ATS endpoints. Enforces deterministic source freshness (HN ≤ 3 days, generic ≤ 14 days).
2. **`job_deduplicator/`**: Consumes raw SQS queue, computes content hashes, checks DynamoDB `Jobsearch` table, handles TTLs, and requeues on profile version updates.
3. **`job_processor/`**: Consumes filtered SQS queue, applies deterministic regex guardrails, invokes Amazon Bedrock Claude via structured tool-calling, persists structured evidence, and publishes high matches (score ≥ 80) to SNS.
4. **`job_router/`**: Telegram control plane (Webhook/API Gateway). Handles command routing (`search`, `list`, `explain`, `draft`, `apply`, `feedback`), callback state machines, and human approval gates.
5. **`job_sender/`**: Previews and dispatches targeted email pitches. Enforces strict tone bounds (<130 words, 1 project pitch, zero buzzwords) and dual-gated air-gap security.

---

## 🚦 Sequential Phase-Gated Orchestration Protocol

Whenever `/orchestrator` is invoked, execute these phases in strict sequential order. **Never skip or reorder gates.**

```
Phase 0: Disk & Git Inspection (Ground Truth)
   │
   ▼
Phase 1: Zero-Trust Security Gate (P0 - Hard Stop)
   │ (Pass: 0 PII leaks, 0 secrets, 0 unbounded loops)
   ▼
Phase 2: QA & Mocked Regression Gate (P1 - Block Deployment)
   │ (Pass: 100% tests passing offline)
   ▼
Phase 3: AI Scoring, Grounding & Token Eval Gate (P1)
   │ (Pass: Eval benchmark within thresholds, LLM_TELEMETRY validated)
   ▼
Phase 4: Air-Gap Outreach Safety Gate (Critical Invariant)
   │ (Pass: Both email send gates verified false by default)
   ▼
Phase 5: Synthesis & Pipeline Health Report
```

---

### Phase 0: Disk & Git State Inspection
1. Inspect working tree status:
   ```powershell
   git status
   ```
2. Verify branch state and ensure no uncommitted debug files, `.env` files, or temporary dumps exist.
3. Check the active plan tracking harness (e.g., `master_plan_execution.md` or handover notes) to identify in-flight tasks.

### Phase 1: Zero-Trust Security Gate (Blocking P0)
Run the dedicated security audit:
```powershell
python tools/ops/run_agents.py --security
```
* **Identity Decoupling**: 0 occurrences of personal names, personal emails, personal URLs, or resume filenames in tracked files. All profile attributes must resolve via runtime env vars.
* **Hardcoded Secrets**: 0 AWS credentials, Telegram tokens, Google API keys, or private keys.
* **AST Loop Analysis**: All `while` and `for` loops must have bounded exit conditions, timeouts (max 4s), or explicit break guards.
* **SSRF Defense**: External URL fetchers must block RFC 1918 subnets (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `127.0.0.0/8`, AWS metadata `169.254.169.254`).
* *Action on Failure*: **HALT IMMEDIATELY**. Report the exact file and line. Do not proceed to test execution.

### Phase 2: QA & Mocked Regression Gate (Blocking P1)
Run the full automated test suite:
```powershell
python -m pytest tests/ -q
```
* Ensure 100% test pass rate across all services (`test_ingestor.py`, `test_deduplicator.py`, `test_processor.py`, `test_router.py`, `test_sender.py`, `test_agents.py`).
* All AWS services (DynamoDB, S3, SQS, SNS) must execute cleanly in offline Moto mocks.
* Fast QA verification:
  ```powershell
  python tools/ops/run_agents.py --qa
  ```
* *Action on Failure*: Isolate the broken component, inspect the regression, and remediate before proceeding.

### Phase 3: AI Scoring, Grounding & Token Eval Gate (Blocking P1 for AI changes)
When modifying Bedrock prompts, scoring heuristics, or knowledge graph models:
1. Run the offline evaluation benchmark:
   ```powershell
   python tools/ops/eval_scoring.py
   ```
2. Validate against `evals/scoring_labeled_set.json` for scoring calibration, precision, and recall.
3. Validate outreach tone invariants:
   - Body under 130 words.
   - Mentions exactly 1 relevant project from candidate memory (RetireIQ or Rivulet).
   - Zero generic fluff, cover-letter tropes, or exclamation marks.
   - Grounded strictly in verified facts from the job post.
4. Token economy: Bedrock invocations must use structured tool calling (`toolChoice`) and log `LLM_TELEMETRY`.

### Phase 4: Production Air-Gap Outreach Safety Gate
Verify the dual-key air-gap configuration:
* Gate 1: `ALLOW_TELEGRAM_EMAIL_SEND` must default to `false` on `job-router`.
* Gate 2: `ALLOW_REAL_EMAIL_SEND` must default to `false` on `job-sender`.
* Ensure that the `apply` command generates preview drafts only, requiring explicit confirmation before any real SMTP/SES transmission.

### Phase 5: Synthesis & Pipeline Health Report
Execute full multi-agent diagnostic synthesis:
```powershell
python tools/ops/run_agents.py
```
Generate a structured report summarizing:
* **Security & Identity Status**: 0 Leaks, AST loop safety verified.
* **Test Suite Status**: Total tests, passed, failed, skipped.
* **Code & Architecture Quality**: Lambda handler isolation, dependency hygiene.
* **AWS Infra Health** (if `--full` enabled with AWS credentials): SQS DLQ depth, DynamoDB GSI health, Secrets Manager availability.

---

## 🛠️ CLI Reference
```powershell
# Default local orchestrator check (Security + QA + Code Quality)
python tools/ops/run_agents.py

# Fast QA test suite only
python tools/ops/run_agents.py --qa

# Security & identity auditor only
python tools/ops/run_agents.py --security

# Full suite with live AWS infrastructure checks (Opt-in only, requires AWS credentials)
python tools/ops/run_agents.py --full

# Output health report in machine-readable JSON
python tools/ops/run_agents.py --json
```
