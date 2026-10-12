# Enterprise Architecture Specification: Autonomous Job Intelligence & Outreach Harness

> **Document Classification:** Engineering Whitepaper & Production System Design  
> **Target Audience:** Staff / Principal Engineers, AI Architects, and Open-Source Contributors  
> **Status:** Production / Implemented  

---

## 1. Executive Mission & System Principles

The **JobHunt** autonomous engine was designed to resolve a fundamental failure mode in modern recruitment automation: the unconstrained blast of low-quality, hallucinated outreach against stale postings.

The system enforces four primary engineering invariants:
1. **Mathematical Grounding Over Token Burn**: Drop 90%+ of irrelevant, stale, or low-signal leads in deterministic Python (0ms, $0) before invoking model inference.
2. **Serverless Cross-Encoder Reranking**: Batch-rerank candidate leads using Amazon Bedrock's native cross-encoders before performing deep LLM reasoning.
3. **Zero-Trust Identity Sovereignty**: Candidate identity, personal contact details, and credentials are strictly decoupled from tracked source code and injected at runtime via 12-factor configuration.
4. **Dual-Key Airgap Outreach**: The autonomous system *never* dispatches live outreach without dual-key human approval.

---

## 2. Distributed System Topology

### Production AI Job Intelligence Platform — Event-Driven AWS + Bedrock + HITL Architecture

```mermaid
%%{init: {
  "theme": "base",
  "themeVariables": {
    "fontFamily": "Inter, Arial, sans-serif",
    "fontSize": "14px",
    "lineColor": "#64748B",
    "primaryTextColor": "#0F172A"
  },
  "flowchart": {
    "curve": "basis",
    "nodeSpacing": 32,
    "rankSpacing": 48
  }
}}%%

flowchart TB

    %% =========================
    %% INGESTION
    %% =========================
    subgraph S["01 · LIVE INGESTION"]
        direction LR
        HN["Hacker News<br/><small>Who is Hiring · Freshness Rules</small>"]
        ATS["ATS Boards<br/><small>Greenhouse · Lever · Ashby</small>"]
        CSE["Google CSE<br/><small>Targeted Job Discovery</small>"]
    end

    %% =========================
    %% LAYER 1
    %% =========================
    subgraph L1["02 · INGEST + DEDUPLICATE"]
        direction LR
        ING["job-ingestor<br/><small>AWS Lambda</small>"]
        RAW_Q["job-raw-queue<br/><small>SQS + DLQ</small>"]
        DEDUP["job-deduplicator<br/><small>SHA-256 · Versioning</small>"]
        DDB[("DynamoDB<br/><small>Job State + Scores</small>")]
    end

    %% =========================
    %% LAYER 2
    %% =========================
    subgraph L2["03 · FILTER + RERANK"]
        direction LR
        FILT_Q["job-filtered-queue<br/><small>SQS + DLQ</small>"]
        PROC["job-processor<br/><small>Deterministic Guardrails</small>"]
        RERANK["Cohere Rerank<br/><small>Bedrock · Top Candidate Pruning</small>"]
        S3_MEM[("S3 Candidate Profile<br/><small>Resume + Evidence Context</small>")]
    end

    %% =========================
    %% LAYER 3
    %% =========================
    subgraph L3["04 · LLM EVALUATION"]
        direction LR
        BEDROCK["Claude 3.5 Sonnet<br/><small>Bedrock Structured Evaluation</small>"]
        FEEDBACK["Decision Memory<br/><small>Positive + Negative Examples</small>"]
        SNS["Approval Topic<br/><small>Amazon SNS</small>"]
    end

    %% =========================
    %% LAYER 4
    %% =========================
    subgraph L4["05 · HUMAN-IN-THE-LOOP CONTROL"]
        direction LR
        APIGW["API Gateway<br/><small>Telegram Webhook</small>"]
        ROUTER["job-router<br/><small>State Machine · Atomic Claims</small>"]
        TG(("Telegram<br/><small>Review · Why · Draft · Apply · Feedback</small>"))
    end

    %% =========================
    %% LAYER 5
    %% =========================
    subgraph L5["06 · GUARDED OUTREACH"]
        direction LR
        SENDER["job-sender<br/><small>Outreach Generation</small>"]
        AIRGAP{"Dual Approval Gate<br/><small>Both Flags Required</small>"}
        SMTP["Email Delivery<br/><small>Gmail / Amazon SES</small>"]
    end

    %% =========================
    %% MAIN FLOW
    %% =========================
    HN --> ING
    ATS --> ING
    CSE --> ING

    ING --> RAW_Q
    RAW_Q --> DEDUP
    DEDUP --> FILT_Q
    DEDUP <--> DDB

    FILT_Q --> PROC
    PROC <--> RERANK
    S3_MEM -. candidate context .-> RERANK

    PROC <--> BEDROCK
    FEEDBACK -. prior decisions .-> BEDROCK
    PROC --> DDB
    PROC -->|High Fit| SNS

    SNS --> ROUTER
    TG <--> APIGW
    APIGW <--> ROUTER
    ROUTER <--> DDB

    ROUTER -->|Approved| SENDER
    SENDER <--> BEDROCK
    SENDER --> AIRGAP

    AIRGAP -->|Both approved| SMTP
    AIRGAP -. preview only .-> TG

    %% =========================
    %% COMPONENT STYLES
    %% =========================
    classDef source fill:#F8FAFC,stroke:#64748B,stroke-width:1.5px,color:#0F172A;
    classDef compute fill:#FFF7ED,stroke:#F97316,stroke-width:1.7px,color:#7C2D12;
    classDef queue fill:#EFF6FF,stroke:#3B82F6,stroke-width:1.7px,color:#1E3A8A;
    classDef data fill:#ECFDF5,stroke:#10B981,stroke-width:1.7px,color:#064E3B;
    classDef ai fill:#F5F3FF,stroke:#8B5CF6,stroke-width:1.8px,color:#4C1D95;
    classDef control fill:#ECFEFF,stroke:#0891B2,stroke-width:1.8px,color:#164E63;
    classDef safety fill:#FEF2F2,stroke:#EF4444,stroke-width:2px,color:#7F1D1D;
    classDef external fill:#F8FAFC,stroke:#475569,stroke-width:1.5px,color:#0F172A;

    class HN,ATS,CSE source;
    class ING,DEDUP,PROC,ROUTER,SENDER compute;
    class RAW_Q,FILT_Q,SNS queue;
    class DDB,S3_MEM,FEEDBACK data;
    class RERANK,BEDROCK ai;
    class APIGW,TG control;
    class AIRGAP safety;
    class SMTP external;
```

---

## 3. The Three-Layer Quality Funnel

To achieve industry-leading matching precision without incurring the cost of heavy LLM inference over millions of tokens, JobHunt operates a **Three-Layer Funnel**:

```
[ Incoming Raw Postings from Web / ATS / Search ]
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ Layer 1: Deterministic Pre-Filtering (0ms, $0)             │
│ • Freshness: HN <= 3 days, ATS <= 14 days                   │
│ • Domain & Title: Regex drop non-AI/ML roles                │
│ • Grounding: Mandatory canonical HTTPS URL & SSRF defense   │
│ • Guardrail Caps: Stale capped at 55, No URL capped at 40   │
└─────────────────────────────────────────────────────────────┘
                       │ (Drops ~70% of raw postings)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ Layer 2: Serverless Cross-Encoder Reranker                  │
│ • Bedrock native model: cohere.rerank-v3-5:0                │
│ • Cross-encodes canonical resume against candidate roles    │
│ • Single forward pass latency: ~150ms                       │
│ • Prunes batch to top 15% high-conviction opportunities     │
└─────────────────────────────────────────────────────────────┘
                       │ (Prunes to top 15%)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ Layer 3: Deep Context Grounding                             │
│ • Model: Anthropic Claude 3.5 Sonnet on AWS Bedrock        │
│ • Tool Choice: Forced submit_evaluation Tool-Call          │
│ • In-Context Feedback: Dynamic injection of 5 positive &   │
│   5 negative user decisions from Telegram DynamoDB history  │
│ • Structured Output: score, match_reasons, caveats, draft   │
└─────────────────────────────────────────────────────────────┘
                       │ (Only score >= 80% surfaces)
                       ▼
[ Telegram Review Queue: Guided 'next' -> 'why' -> 'apply' ]
```

### Layer 1: Deterministic Quality Pre-Filters
Executed entirely in local Python before any AWS network hop:
* **Freshness Check**: Postings with missing or stale timestamps are rejected. Hacker News postings have a strict 3-day cutoff (`HN_MAX_JOB_AGE_DAYS=3`). Direct ATS postings enforce a 14-day cutoff (`MAX_JOB_AGE_DAYS=14`).
* **Title & Domain Heuristics**: Compiled regex immediately excludes frontend-only, legacy backend (PHP/Java 8), IT support, and sales roles.
* **SSRF Protection**: Every outbound URL is validated against RFC 1918 private subnets and AWS metadata IPs (`169.254.169.254`).

### Layer 2: Serverless Cross-Encoder Reranking
Traditional pipelines either send full job descriptions to expensive LLMs or rely on coarse vector similarity (cosine distance over dense embeddings), which misses fine-grained negative constraints.
JobHunt uses **Amazon Bedrock Cohere Rerank v3.5**:
* Jointly scores the candidate's core profile and the job description in a single multi-sequence pass.
* Surfaces cross-attention signals between candidate achievements and job requirements.
* Operates 100% serverless with zero GPU provisioning or idle infrastructure costs.

### Layer 3: Deep Context Grounding (Claude 3.5 Sonnet)
Leads that pass Layers 1 and 2 are evaluated with forced tool-calling:
```json
{
  "toolChoice": {
    "tool": {
      "name": "submit_evaluation"
    }
  }
}
```
The model must output structured fields:
* `score` (integer 0–100)
* `match_reasons` (array of verified citations against candidate memory)
* `missing_requirements` (array of caveats or missing technologies)
* `personalized_email_draft` (concise outreach pitch citing exact source evidence)

---

## 4. In-Context Feedback Memory Engine

Unlike rigid static prompts or unstable self-modifying code, JobHunt learns continuously via **dynamic in-context few-shot injection**:

```mermaid
sequenceDiagram
    autonumber
    actor User as Telegram Candidate
    participant Router as job-router Lambda
    participant DDB as DynamoDB (Jobsearch)
    participant Proc as job-processor Lambda
    participant Bedrock as Bedrock Claude 3.5

    User->>Router: "interested 4" OR "meh 4 low comp" OR "not interested 4 legacy stack"
    Router->>DDB: Save feedback: user_interest, user_feedback_reason, user_feedback_at
    Note over DDB: Feedback rows labeled with source=telegram

    Note over Proc: Next scoring execution runs
    Proc->>DDB: Query 5 most recent positive & negative feedback records
    Proc->>Bedrock: Inject dynamic feedback exemplars into system prompt context
    Bedrock-->>Proc: Calibrated evaluation aligned with candidate's live taste
```

---

## 5. Data Contracts & Schemas

### DynamoDB Single-Table Schema (`Jobsearch`)

| Attribute | Type | Description |
| :--- | :--- | :--- |
| `job_id` (PK) | String (HASH) | Canonical SHA-256 hash: `hash(title + company + url)` |
| `created_at` | String (ISO-8601) | Ingestion timestamp |
| `status` | String | Lifecycle state: `PENDING_REVIEW`, `INTERESTED`, `APPLIED`, `REJECTED`, `SKIPPED` |
| `score` | Number | Normalized match score (0–100) |
| `source` | String | Origin adapter: `hacker_news`, `greenhouse`, `lever`, `ashby`, `google_cse` |
| `source_url` | String | Verified HTTPS canonical posting URL |
| `match_reasons` | List[String] | Claude evaluation citations |
| `missing_requirements` | List[String] | Caveats and technology gaps |
| `eval_version` | String | Evaluation model version (e.g., `v2026.1`) for safe requeueing |
| `user_interest` | String | Feedback signal: `interested`, `meh`, `not_interested` |
| `user_feedback_reason` | String | Optional qualitative feedback rationale |

### Global Secondary Indexes (GSIs):
1. **`status-score-index`**: Partition Key: `status`, Sort Key: `score` (Allows fast retrieval of top unreviewed matches).
2. **`source-created-index`**: Partition Key: `source`, Sort Key: `created_at` (Allows per-source freshness audits).

---

## 6. Guarded Outreach & Airgap Protocol

The system prevents autonomous spam via a **Dual-Key Airgap Architecture**:

```
                    ┌───────────────────────────┐
                    │  Telegram 'draft' Command  │
                    └─────────────┬─────────────┘
                                  │
                                  ▼
                    ┌───────────────────────────┐
                    │  job-sender Lambda Runs   │
                    │   (Generates Preview)     │
                    └─────────────┬─────────────┘
                                  │
                                  ▼
                    ┌───────────────────────────┐
                    │   Is Dual-Key Gate Met?   │
                    │ ALLOW_TELEGRAM_EMAIL_SEND │
                    │            AND            │
                    │   ALLOW_REAL_EMAIL_SEND   │
                    └──────┬─────────────┬──────┘
                           │             │
                    NO     │             │ YES
             ┌─────────────┘             └─────────────┐
             ▼                                         ▼
┌──────────────────────────┐             ┌──────────────────────────┐
│  Renders Preview in TG   │             │   Dispatches Live Email  │
│  Zero SMTP Invocations   │             │   with Attached PDF      │
│  Safe by Default         │             │   via Gmail / SES        │
└──────────────────────────┘             └──────────────────────────┘
```

Tone Constraints Enforced in Outreach Generation:
* Length strictly bounded to **under 130 words**.
* Zero generic buzzwords (*synergy, leverage, passionate, cutting-edge, thrilled*).
* Zero cover-letter pleasantries (*"I hope this email finds you well"*).
* Cites exactly **one** verified project matching the job post's technical challenge.

---

## 7. Fault Tolerance & Resilience Topology

1. **Dead-Letter Queues (DLQs)**:
   * Every SQS queue (`job-raw-queue`, `job-filtered-queue`) has an isolated DLQ configured with `maxReceiveCount=3`.
   * Unprocessable messages are quarantined without blocking queue throughput.
2. **Atomic DynamoDB Claims**:
   * Multi-user or concurrent Telegram actions use conditional expressions (`attribute_not_exists(claimed_by) OR claim_expires < :now`) to prevent duplicate applications.
3. **Automated Health Monitoring**:
   * Scheduled GitHub Action (`health-monitor.yml`) runs every 6 hours, querying CloudWatch metrics for Lambda errors, DLQ depths, and DynamoDB throttling events.

---

## 8. Multi-Agent Local Diagnostic Harness

Operational integrity is enforced through local diagnostic agents under `agents/`:

```
tools/ops/run_agents.py
├── --security  --> SecurityAuditorAgent (AST zero-loop check, credential regex scan)
├── --qa        --> QAAgent (Runs pytest suite, measures execution velocity)
├── --full      --> OrchestratorAgent + BackendEngineerAgent + AIEngineerAgent (Live AWS probe)
└── (default)   --> Comprehensive local offline health check
```

This ensures that any pull request or deployment is mathematically certified for security and performance before reaching AWS.
