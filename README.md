# JobHunt — Autonomous AI Job Pipeline

> Telegram-controlled multi-agent system that discovers, deduplicates, scores, and drafts outreach for AI engineering opportunities — all from your phone.

[![CI](https://github.com/Nites-24/jobhunt/actions/workflows/ci.yml/badge.svg)](https://github.com/Nites-24/jobhunt/actions/workflows/ci.yml)
[![Health Monitor](https://github.com/Nites-24/jobhunt/actions/workflows/health-monitor.yml/badge.svg)](https://github.com/Nites-24/jobhunt/actions/workflows/health-monitor.yml)

---

## What This Is

A production autonomous job pipeline running on AWS. It finds fresh, real AI engineering opportunities, scores them against a candidate profile, explains the evidence, drafts concise outreach, and keeps a human approval gate before any email is sent.

The product promise is not "more jobs." The promise is **fewer, fresher, better-grounded opportunities with source evidence**.

## Architecture

```
Telegram → API Gateway → job-router (Lambda)
                              ↓
                        job-ingestor (Lambda)
                              ↓
                     SQS (job-raw-queue)
                              ↓
                    job-deduplicator (Lambda)
                              ↓
                   SQS (job-filtered-queue)
                              ↓
                     job-processor (Lambda)
                     ├── AWS Bedrock (Claude)
                     ├── DynamoDB (state)
                     └── SNS → job-router → Telegram
                              ↓
                      job-sender (Lambda)
                     ├── Gmail SMTP (gated)
                     └── Telegram preview
```

See [Architecture Doc](docs/Architecture_Doc.md) for the full system design with Mermaid diagrams.

## Key Features

| Feature | How It Works |
|---------|-------------|
| **Multi-source ingestion** | Hacker News, Adzuna, Google CSE, JSearch |
| **Smart deduplication** | SHA-256 hash on (title, company, URL) + evaluation versioning |
| **Deterministic guardrails** | Pre-LLM rejection for stale, missing-date, low-signal, no-URL jobs |
| **LLM scoring** | Bedrock Claude with forced tool-calling for structured JSON output |
| **Human-in-the-loop** | Telegram review: `next → why → draft → skip/apply` |
| **Guarded outreach** | Email requires 2 explicit env flags before real SMTP send |
| **Feedback learning** | `interested`, `meh`, `not interested` → DynamoDB → eval exports |
| **Source grounding** | Every opportunity must have a verifiable source link |
| **Knowledge graph** | Compact candidate profile to reduce Bedrock token costs |

## Project Structure

```
jobhunt/
├── .github/workflows/        # CI/CD, monitoring, analytics
│   ├── ci.yml                # Tests + scoring evals + lint
│   ├── deploy.yml            # Selective Lambda deployment
│   ├── health-monitor.yml    # 6-hourly production health checks
│   ├── pipeline-analytics.yml # Daily 90-day metrics report
│   ├── feedback-export.yml   # Weekly feedback data export
│   └── source-quality-audit.yml # Daily source health matrix
│
├── job_ingestor/             # Lambda: fetch jobs from sources
├── job_deduplicator/         # Lambda: normalize duplicates
├── job_processor/            # Lambda: score with Bedrock + guardrails
├── job_router/               # Lambda: Telegram control plane
├── job_sender/               # Lambda: email preview/send
│
├── shared/                   # Shared runtime (knowledge_graph.py)
├── agents/                   # Local health-check agents
├── tests/                    # Unit tests (moto mocks)
│   └── live/                 # Opt-in AWS smoke tests
├── tools/ops/                # Deployment, diagnostics, ops scripts
├── evals/                    # Labeled scoring eval data
├── docs/                     # Architecture, implementation logs
│
├── AGENTS.md                 # Agent operating contract
├── CONTRIBUTING.md           # Development guidelines
├── SECURITY.md               # Security policy
├── CHANGELOG.md              # Version history
├── LICENSE                   # MIT License
├── deploy.py                 # Lambda deployment script
└── pyproject.toml            # pytest configuration
```

## Quick Start

### Prerequisites

- Python 3.11+
- AWS account with Lambda, DynamoDB, SQS, SNS, S3, Bedrock
- Telegram bot token
- Source API credentials (Adzuna, Google CSE, Hunter.io)

### Local Development

```bash
# Install test dependencies
pip install pytest moto[all] boto3

# Run unit tests (no AWS needed)
pytest -q

# Run scoring guardrail evals
python tools/ops/eval_scoring.py

# Run local agents
python tools/ops/run_agents.py --qa
```

### Deployment

```bash
# Deploy all Lambdas
python deploy.py

# Deploy selectively via GitHub Actions
# Push to main, or use workflow_dispatch
```

### Production Verification

```bash
# Read-only health check
python tools/ops/verify_live_production.py

# Full check (touches AWS)
python tools/ops/run_agents.py --full
```

## Telegram Commands

| Command | Action |
|---------|--------|
| `search jobs` | Trigger live ingestion across all sources |
| `list` | Show fresh reviewable matches |
| `next` | Review next match in guided flow |
| `why` | Explain scoring rationale |
| `draft` | Preview outreach email |
| `apply` | Move to manual follow-up |
| `skip <reason>` | Skip with feedback |
| `status` | Pipeline health + counts |
| `backlog` | Show hidden pending jobs |
| `interested` / `meh` / `not interested` | Feedback capture |

## CI/CD & Monitoring

| Workflow | Schedule | Purpose |
|----------|----------|---------|
| **CI** | Every push/PR | Tests, evals, lint |
| **Deploy** | Manual + main push | Selective Lambda deploy with approval gate |
| **Health Monitor** | Every 6 hours | Lambda, SQS, DLQ, CloudWatch checks |
| **Pipeline Analytics** | Daily | 90-day metrics: ingestion, scoring, feedback |
| **Feedback Export** | Weekly | Telegram feedback → eval-ready JSON |
| **Source Quality Audit** | Daily | Per-source health matrix |

## Safety Controls

- **Email gating**: Real SMTP requires `ALLOW_TELEGRAM_EMAIL_SEND=true` on router AND `ALLOW_REAL_EMAIL_SEND=true` on sender
- **Pre-LLM filtering**: Stale, missing-date, low-signal, no-URL jobs rejected before Bedrock
- **Deploy approval**: GitHub Actions requires manual approval for production
- **DLQ monitoring**: Dead letter queues on all SQS queues
- **Candidate truth**: Knowledge graph enforces factual claims only

## Documentation

- [Architecture Doc](docs/Architecture_Doc.md) — Full system design with diagrams
- [Implementation Log: May 13](docs/implementation_2026_05_13.md) — Production debug session
- [Implementation Log: May 14](docs/implementation_2026_05_14.md) — UX and feedback improvements
- [Sprint Report](docs/sprint_report_2026_05_12.md) — Initial architecture decisions
- [Agent Contract](AGENTS.md) — Operating rules for AI agents

## License

MIT — see [LICENSE](LICENSE).
