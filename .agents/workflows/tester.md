---
description: QA Tester Agent workflow for full unit, integration, and regression testing
---

# 🧪 /tester — QA & Test Validation Workflow

> **Role**: Lead QA Automation Engineer  
> **Mandate**: Maintain 100% test coverage and regression safety across all 5 serverless microservices and shared utilities, running completely offline with zero live AWS spend.

---

## 🏛️ Test Architecture & Moto Mock Environment

All automated tests in `tests/` execute against local in-memory mocks powered by `moto`:
* **DynamoDB**: In-memory `Jobsearch` table with GSIs (`StatusIndex`, `UrlHashIndex`).
* **SQS**: In-memory raw and filtered job queues with dead-letter queue behavior.
* **SNS**: In-memory `jobsearch-approvals` notification topic.
* **S3**: In-memory bucket `job-search-pipeline-memory` for candidate resume and knowledge graph storage.
* **Secrets Manager**: In-memory secret payloads for Telegram, Gmail, and search APIs.
* **Bedrock Converse API**: Deterministic mock responses simulating Claude tool-calling outputs.

> **CRITICAL RULE**: Automated test runs must NEVER incur AWS cloud charges. All AWS calls in `tests/` must be mocked. Opt-in live tests in `tests/live/` are strictly excluded from automated CI/CD runs.

---

## 📋 Test Suite Breakdown

### 1. `tests/test_ingestor.py`
* Ingestion pipelines: Hacker News (Ask HN Who is Hiring thread parser, comment parsing, obfuscated email extraction), Google Custom Search JSON API, and Direct ATS adapters (Greenhouse, Lever, Ashby).
* Source freshness enforcement: HN postings > 3 days discarded; generic postings > 14 days discarded.
* Deterministic URL validation & SSRF defense.

### 2. `tests/test_deduplicator.py`
* Content normalization: Company name, job title, and canonical URL cleaning.
* SQS event batching and DynamoDB hash lookup.
* TTL management and requeue logic when candidate evaluation profile version changes.

### 3. `tests/test_processor.py`
* Pre-filtering: Fast regex rejection for stale dates, missing URLs, and non-AI roles before LLM invocation.
* Amazon Bedrock Claude tool-calling: Structured extraction of `match_score`, `reasoning`, `strengths`, and `gaps`.
* DynamoDB persistence of structured evaluation evidence and SNS high-match fanout (threshold ≥ 80).

### 4. `tests/test_router.py`
* Telegram control plane: Webhook validation, security signature check, command routing (`search`, `list`, `explain`, `draft`, `apply`, `feedback`).
* Inline keyboard callback state machine and practice workspace mode.
* User feedback capture (`interested`, `meh <reason>`, `applied`) persisted to DynamoDB.

### 5. `tests/test_sender.py`
* Outreach draft generation: Body under 130 words, 1 relevant candidate project, zero generic hype or cover-letter buzzwords.
* Contact greeting heuristics: Named recruiter > HN author > hiring team.
* Dual-key airgap enforcement: `ALLOW_TELEGRAM_EMAIL_SEND` & `ALLOW_REAL_EMAIL_SEND` gates.

### 6. `tests/test_knowledge_graph.py` & `tests/test_agents.py`
* Candidate knowledge graph extraction, compaction, and runtime attribute injection.
* Local diagnostic agent contracts (`QAAgent`, `SecurityAuditorAgent`, `PythonDevAgent`, `OrchestratorAgent`).

---

## 🛠️ Operational Commands

### Fast QA Check (Unit & Integrity Summary):
```powershell
python tools/ops/run_agents.py --qa
```

### Full Test Suite Run (Detailed Output):
```powershell
python -m pytest tests/ -v
```

### Targeted Test Run (During Active Feature Development):
```powershell
# Run a specific test file:
python -m pytest tests/test_processor.py -v

# Run a single targeted test method:
python -m pytest tests/test_sender.py -k "test_airgap" -v
```

---

## 🚦 QA Quality Gates (Pass Criteria)

Before declaring any task complete or ready for release:
1. **Pass Rate**: Must be **100%** passing (0 failed, 0 errors).
2. **Speed**: Entire unit suite must execute in under 30 seconds.
3. **No Flakiness**: Tests must not rely on wall-clock sleep or external network requests.
4. **Mock Identity**: All test assertions and fixtures must strictly use mock identities (`Alex Hunter`, `candidate@example.com`).
