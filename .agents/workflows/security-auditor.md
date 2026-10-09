---
description: Security Auditor workflow for identity scrubbing, secret auditing, and zero-loop analysis
---

# 🛡️ /security-auditor — Zero-Trust Security & Identity Auditor

> **Role**: Chief Information Security Officer & Zero-Trust Auditor  
> **Mandate**: Guarantee 100% identity sovereignty, eliminate hardcoded secrets, enforce static loop safety via AST analysis, and prevent SSRF vulnerabilities before any commit or deployment.

---

## 🏛️ Core Security Invariants

In this repository, candidate identity and infrastructure security are treated like classified financial records:
1. **Total Identity Decoupling**: The codebase must remain completely identity-agnostic. No personal names, personal email addresses, personal locations, visa statuses, or resume files may ever exist in tracked git files, tests, or mock fixtures.
2. **Zero Secrets in Code**: No API keys, bot tokens, AWS credentials, or OAuth secrets in tracked files.
3. **Deterministic AST Static Safety**: Unbounded loops and runaway while-loops are fatal flaws in serverless Lambdas. All loops must be statically verified to have guaranteed termination.
4. **SSRF & Network Hardening**: All outbound HTTP requests must enforce RFC 1918 private subnet blocking and strict 4-second socket timeouts.

---

## 📋 Comprehensive Audit Protocol

When `/security-auditor` is invoked, execute these 4 audit vectors systematically:

### 1. Identity Scrubbing Audit (Zero PII)
Scan all files (code, tests, mocks, evals, configs, docs) for personal identifiers:
* **Candidate Names**: Ensure no personal names appear in source or test files.
* **Personal Emails**: Ensure no personal Gmail/work addresses appear. All test fixtures must use `candidate@example.com` or `alex.hunter@example.com`.
* **Resume Filenames**: Ensure no personal resume names (`Candidate_Resume.pdf`, etc.) appear. Mock fixtures must use `data/resume/resume.pdf` or `data/resume/resume.txt`.
* **Personal Handles & URLs**: Ensure no personal LinkedIn profile links or personal HuggingFace profile usernames exist in code.
* **Canonical Profile Injection**: Verify that identity is injected exclusively at runtime via environment variables:
  - `CANDIDATE_NAME`
  - `TARGET_ROLE`
  - `CANDIDATE_LOCATION`
  - `SENDER_EMAIL`
  - `CANDIDATE_LINKEDIN`
  - `RESUME_KEY`
  - `RETIREIQ_URL`

### 2. Secret & Credential Scanning
Scan for leaked credentials using high-entropy regex patterns:
* **AWS Access Keys**: `AKIA[0-9A-Z]{16}`
* **Telegram Bot Tokens**: `[0-9]{8,10}:[a-zA-Z0-9_-]{35}`
* **Google Cloud API Keys**: `AIza[0-9A-Za-z\\-_]{35}`
* **Private Keys**: `-----BEGIN.*PRIVATE KEY-----`
* **Google CSE Engine IDs**: `[a-z0-9]{17}`
* Verify all credentials resolve securely from **AWS Secrets Manager** (`telegram`, `jobsearch/google_cse`, `jobserch_gmail`, `jobsearch/hunterio`) or `.env` (which must be in `.gitignore`).

### 3. AST Zero-Loop & Timeout Analysis
Statically inspect Python files using Python's `ast` module to guarantee loop safety:
* **`while` Loops**: Verify that every `while` loop has a deterministic termination condition, an explicit `break` path, or a max iteration counter. Flag any unbounded `while True` loops.
* **`for` Loops**: Verify that loops processing queues or pagination have a hard boundary (e.g. `MAX_ITEMS`, `MAX_PAGES`).
* **Socket Timeouts**: Verify all `urllib.request.urlopen` or `requests` calls specify `timeout=4.0` or less.

### 4. SSRF & Network Hardening Check
Inspect all URL fetchers and ATS adapters:
* Verify that external link fetchers (e.g., in `job_ingestor/` or `shared/url_validation.py`) block private and reserved IP spaces:
  - `10.0.0.0/8`
  - `172.16.0.0/12`
  - `192.168.0.0/16`
  - `127.0.0.0/8` (Loopback)
  - `169.254.169.254` (AWS Instance Metadata Service)
* Ensure non-HTTP/HTTPS protocols (`file://`, `ftp://`, `gopher://`) are strictly rejected.

---

## 🛠️ Operational Execution

Execute the automated security auditor:
```powershell
python tools/ops/run_agents.py --security
```

### Gate Requirements:
* **Critical Issues**: Must be **0**. (Any PII leak, credential leak, or unbounded loop is an automatic build failure).
* **Warnings**: Must be **0** for production release.

### Remediation Protocol:
1. If a personal name or email is detected in tests: replace with mock identity `Alex Hunter` and `candidate@example.com`.
2. If a hardcoded secret is detected: invalidate immediately, migrate to AWS Secrets Manager, and purge from git history.
3. If an unbounded loop is detected: add an explicit max iteration limit and break condition.
