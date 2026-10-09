---
description: Open-Source Expert workflow for safe remote delivery, architecture showcase, and open-source maintainer standards
---

# 🌟 /open-source-expert — Open-Source Showcase & Maintainer Workflow

> **Mission**: Maintain this repository to institutional open-source standards, ensuring clean architecture, zero credential or identity leaks, and safe public distribution.

---

## 🏛️ 1. Architecture & Engineering Standards

This repository adheres to production agent-harness principles:

1. **System-Level Engineering Over Prompt Tuning**:
   * Deterministic pre-filtering before LLM inference saves ~70% of API token spend.
   * Abstract Syntax Tree (`ast`) static analysis guarantees zero infinite loops across production code.
2. **True Production Architecture**:
   * Serverless AWS event-driven pipeline (Bedrock Converse tool-calling, DynamoDB, SNS, Secrets Manager).
   * Fully decoupled 12-factor configuration (zero hardcoded secrets, zero PII leaks).
3. **Evaluation-Driven Development (Evals > Vibes)**:
   * Quantitative offline eval benchmarking (`evals/scoring_labeled_set.json` & `tools/ops/eval_scoring.py`).
4. **Safety & Identity Sovereignty**:
   * Dual-key human-in-the-loop approval gates on real outreach.
   * SSRF protection blocking RFC 1918 private subnets and cloud metadata endpoints.

---

## 📋 2. Safe Remote Delivery Checklist (Run Before Pushing)

Whenever you prepare to push commits to public GitHub, execute this 3-step verification protocol:

```powershell
# Step 1: Run the Zero-Trust Security Auditor
python tools/ops/run_agents.py --security

# Step 2: Run Full Automated Test Suite
python -m pytest tests/ -v

# Step 3: Run Orchestrator Health Verification
python tools/ops/run_agents.py
```

### Gate Requirements:
* `Critical Issues`: Must be **0**.
* `Pytest`: Must be **100% passing** (200+ unit/integration tests).
* `Git Status`: Ensure `.env` is never tracked and all sensitive facts are parameterized.

---

## 🏛️ 3. Repository Engineering Rules

To maintain institutional-grade quality across this codebase, all contributions must uphold these design rules:

1. **Zero-Trust Identity Decoupling (12-Factor)**:
   * No personal names, emails, locations, or private URLs in tracked source, tests, or documentation.
   * All candidate profile values must be injected strictly at runtime via environment variables (`CANDIDATE_NAME`, `SENDER_EMAIL`, `CANDIDATE_LOCATION`, etc.).
2. **Deterministic Pre-Filtering**:
   * All scrapers and ingestors must apply regex and heuristic pre-filters before dispatching payloads to Bedrock/LLMs.
   * Deduplication must be enforced at the DynamoDB layer to prevent redundant token consumption.
3. **AST-Enforced Static Safety**:
   * No unbounded while-loops (`while True`) or unconstrained pagination loops without explicit limiters.
   * Verified continuously via `agents/security_auditor_agent.py`.
4. **Air-Gapped Outreach Security**:
   * Outbound communication engines must remain dual-gated behind `ALLOW_TELEGRAM_EMAIL_SEND` and `ALLOW_REAL_EMAIL_SEND`.
   * Production functions must never dispatch unapproved live communications autonomously.

---

## 🛠️ 4. Release & Delivery Protocol

Before tagging a release or pushing commits to `origin main`, execute the release verification pipeline:

1. **Static Analysis & Security Audit**:
   Verify zero hardcoded credentials, zero identity leaks, and zero unbounded loops:
   ```powershell
   python tools/ops/run_agents.py --security
   ```
2. **Unit & Integration Regression Suite**:
   Verify complete test suite passes without regressions:
   ```powershell
   python -m pytest tests/ -v
   ```
3. **Clean Workspace Verification**:
   Verify untracked environment secrets (`.env`) or local debug dumps are excluded:
   ```powershell
   git status
   ```

---

## 🚀 5. Showcase Repository Assets

Keep these public showcase files maintained:
* [`README.md`](file:///c:/Projects/jobhunt/README.md): High-impact architecture diagrams, feature breakdown, and quickstart instructions.
* [`SECURITY.md`](file:///c:/Projects/jobhunt/SECURITY.md): Professional responsible disclosure guidelines.
* [`AGENTS.md`](file:///c:/Projects/jobhunt/AGENTS.md): Architectural contract and production operating principles.
* [`.agents/workflows/`](file:///c:/Projects/jobhunt/.agents/workflows): Demonstrates modern agentic infrastructure-as-code.
