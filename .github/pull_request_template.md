## 🎯 Summary of Changes
Provide a brief overview of what this pull request introduces, fixes, or refactors.

## 🏛️ Architecture & Component Impact
- [ ] `job_ingestor` (Ingestion / Scraping / SSRF)
- [ ] `job_deduplicator` (Hashing / DynamoDB versioning)
- [ ] `job_processor` (Bedrock Rerank / Claude scoring / Guardrails)
- [ ] `job_router` (Telegram control plane / Feedback capture)
- [ ] `job_sender` (Outreach / Airgap gate / PDF generation)
- [ ] `agents/` or `tools/ops/` (Diagnostics / Tooling)
- [ ] `docs/` (Architecture or guides)

## 🛡️ Pre-Merge Quality & Security Checklist
Before requesting review or merging, ensure all local checks pass:

- [ ] **Zero-Trust Security**: Ran `python tools/ops/run_agents.py --security` (Must be HEALTHY, 0 critical issues).
- [ ] **No PII Leaks**: Verified zero candidate personal names, locations, emails, or private URLs in code, tests, or docs.
- [ ] **Unit Tests**: Ran `pytest tests/ -v` (100% passing offline without AWS credentials).
- [ ] **Deterministic First**: Verified that heuristic/filtering logic is resolved deterministically before invoking Bedrock.
- [ ] **AST Loop Safety**: Verified all loops and pagination have bounded iteration limits.
