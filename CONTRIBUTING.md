# Contributing to JobHunt

Thank you for your interest in contributing to **JobHunt**! This project is an enterprise-grade autonomous AI job pipeline and outreach harness running on AWS serverless infrastructure.

We hold code quality, mathematical grounding, token economics, and zero-trust security to the highest institutional standards.

---

## 🛠️ 1. Local Development Setup

JobHunt is built so that **100% of the unit test suite and local simulation runs completely offline** using `moto` to mock AWS services (DynamoDB, SQS, SNS, S3, Secrets Manager). You do **not** need an active AWS account to develop or test locally.

### Step 1: Clone the Repository
```bash
git clone https://github.com/Nites-24/jobhunt.git
cd jobhunt
```

### Step 2: Set Up Virtual Environment & Dependencies
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

pip install -r requirements.txt
pip install pytest moto[all] boto3 reportlab pypdf ruff
```

### Step 3: Configure Environment
Copy `.env.example` to `.env`. For local offline testing, mock values are sufficient:
```bash
cp .env.example .env
```

---

## 🏛️ 2. Architectural Design Principles

When proposing changes, ensure your implementation adheres to our core design principles:

1. **Deterministic Python Over LLM Freedom**:
   Solve filtering, deduplication, URL validation, and freshness in deterministic Python before invoking Bedrock. Every LLM call consumes tokens, introduces latency, and adds variance.
2. **Three-Layer Quality Funnel**:
   - Layer 1: Deterministic Quality Pre-Filters (0ms, $0)
   - Layer 2: Serverless Cross-Encoder Reranker (`cohere.rerank-v3-5:0`)
   - Layer 3: Deep Context Grounding (Claude 3.5 Sonnet with forced tool-calling)
3. **Zero-Trust Identity Sovereignty**:
   Candidate personal identity must **never** be hardcoded in tracked files, tests, or documentation. Always use runtime environment variable decoupling (`CANDIDATE_NAME`, `TARGET_ROLE`, `CANDIDATE_LOCATION`, `S3_RESUME_KEY`).
4. **AST-Enforced Static Safety**:
   Every loop, queue consumer, or network poll must have bounded iterations and explicit timeouts. Unbounded `while True` loops are forbidden.
5. **Air-Gapped Outreach Security**:
   Email dispatch requires **both** `ALLOW_TELEGRAM_EMAIL_SEND=true` on `job-router` and `ALLOW_REAL_EMAIL_SEND=true` on `job-sender`.

---

## 🧪 3. Quality & Security Verification Gates

Before submitting a Pull Request, you must verify all three local quality gates pass:

```powershell
# Gate 1: Zero-Trust Security & Identity Audit
python tools/ops/run_agents.py --security

# Gate 2: Full Unit & Integration Regression Suite (225+ tests)
pytest tests/ -v

# Gate 3: Quantitative Prompt Scoring Benchmark
python tools/ops/eval_scoring.py
```

### Gate Standards:
* **Critical Issues**: Must be **0**.
* **Pytest**: Must be **100% passing**.
* **Code Style**: Code must adhere to `ruff check .` with clean syntax.

---

## 📝 4. Commit Conventions

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

* `feat(processor)`: add Bedrock Cohere reranker integration
* `fix(sender)`: correct email template escaping
* `docs(readme)`: update architecture diagrams
* `security(core)`: enforce SSRF blocklist on outbound fetches
* `test(router)`: add coverage for telegram feedback commands

---

## 🚀 5. Deployment Protocol

Production deployments are managed via GitHub Actions (`.github/workflows/deploy.yml`) or via the deployment CLI:

```bash
# Dry-run or deploy all services:
python deploy.py

# Deploy a specific Lambda:
python deploy.py --service job_processor
```

---

## 📄 6. Community Standards

All contributors are expected to uphold our [Code of Conduct](CODE_OF_CONDUCT.md). For security vulnerabilities, review our [Security Policy](SECURITY.md).
