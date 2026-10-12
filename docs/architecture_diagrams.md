# Architecture Diagrams

## Production AI Job Intelligence Platform — Event-Driven AWS + Bedrock + HITL Architecture

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

## Message Flow

```mermaid
sequenceDiagram
    participant U as User
    participant T as Telegram
    participant R as job-router
    participant I as job-ingestor
    participant Q1 as SQS Raw
    participant D as job-deduplicator
    participant Q2 as SQS Filtered
    participant P as job-processor
    participant B as Bedrock Claude
    participant DB as DynamoDB
    participant SNS as SNS
    participant S as job-sender

    U->>T: "search jobs"
    T->>R: Webhook
    R->>I: Invoke Lambda
    I->>Q1: Batch push jobs (10 per batch)
    Q1->>D: SQS trigger

    loop For each job
        D->>DB: Check existing (job_id)
        alt New job
            D->>DB: Insert (status=NEW)
            D->>Q2: Forward to filtered queue
        else Duplicate, needs re-eval
            D->>DB: Refresh source fields
            D->>Q2: Forward for re-scoring
        else Duplicate, skip
            D-->>D: Log DUPLICATE_SKIP
        end
    end

    Q2->>P: SQS trigger

    loop For each job
        P->>P: Pre-LLM rejection check
        alt Passes pre-filter
            P->>B: Converse API (forced tool-call)
            B->>P: {match_score, match_reason}
            P->>P: Apply deterministic guardrails
        end

        alt Score >= 80%
            P->>DB: Update (PENDING_APPROVAL)
            P->>SNS: Publish match notification
            SNS->>R: Notify
            R->>T: Send alert to user
        else Score < 80%
            P->>DB: Update (REJECTED)
        end
    end

    U->>T: "next"
    T->>R: Webhook
    R->>DB: Query PENDING_APPROVAL
    R->>T: Show match card

    U->>T: "draft"
    T->>R: Webhook
    R->>S: Invoke preview
    S->>B: Generate draft
    S->>T: Show draft preview

    U->>T: "apply"
    T->>R: Webhook
    R->>DB: Update (APPLIED - manual follow-up)
```

## Deduplication Logic

```mermaid
flowchart TD
    START["Incoming Job from SQS"] --> HASH["Compute SHA-256 hash<br/>(title + company + URL)"]
    HASH --> LOOKUP["Look up job_id in DynamoDB"]

    LOOKUP -->|Not found| NEW["Insert as NEW"]
    NEW --> FORWARD["Forward to filtered queue"]
    FORWARD --> DONE["Done"]

    LOOKUP -->|Found| CHECK_STATUS["Check existing status"]

    CHECK_STATUS -->|"NEW + unscored"| REQUEUE_NEW["Requeue for scoring"]
    REQUEUE_NEW --> REFRESH["Refresh source fields"]
    REFRESH --> DONE

    CHECK_STATUS -->|"REJECTED + version mismatch"| REQUEUE_EVAL["Requeue for re-evaluation"]
    REQUEUE_EVAL --> REFRESH

    CHECK_STATUS -->|"REJECTED + same version"| SKIP["Skip duplicate"]
    SKIP --> DONE

    CHECK_STATUS -->|"PENDING_APPROVAL"| REFRESH_ONLY["Refresh fields only"]
    REFRESH_ONLY --> DONE

    CHECK_STATUS -->|"APPLIED / SENT"| SKIP
```

## Scoring Pipeline

```mermaid
flowchart TD
    JOB["Job from SQS"] --> PRE["Pre-LLM Rejection Check"]

    PRE -->|"Stale (>14 days, HN >3 days)"| REJECT_PRE["Cap at 55%<br/>REJECTED"]
    PRE -->|"Missing date"| REJECT_DATE["Cap at 70%<br/>REJECTED"]
    PRE -->|"No AI/ML signal"| REJECT_SIGNAL["Cap at 50%<br/>REJECTED"]
    PRE -->|"No source URL"| REJECT_URL["Cap at 40%<br/>REJECTED"]

    PRE -->|"Passes all filters"| LLM["Bedrock Claude<br/>Forced tool-call"]
    LLM --> SCORE["Raw score + reason"]
    SCORE --> GUARD["Deterministic Guardrails"]

    GUARD -->|"Apply caps post-model"| FINAL["Final score"]

    FINAL -->|">= 80%"| APPROVE["PENDING_APPROVAL<br/>+ SNS notification<br/>+ Hunter.io email lookup"]
    FINAL -->|"< 80%"| REJECT["REJECTED"]

    REJECT_PRE --> DB["DynamoDB Update"]
    REJECT_DATE --> DB
    REJECT_SIGNAL --> DB
    REJECT_URL --> DB
    APPROVE --> DB
    REJECT --> DB
```

## Telegram UX Flow

```mermaid
stateDiagram-v2
    [*] --> Home

    Home --> Search: "search jobs"
    Home --> List: "list"
    Home --> Status: "status"
    Home --> More: "More"

    Search --> Ingesting: Trigger ingestor
    Ingesting --> Home: Results notification

    List --> ReviewSession: Fresh matches found
    List --> Backlog: No fresh matches, pending exist

    Status --> Home: Show counts
    More --> AdvancedMenu: Show advanced commands

    state ReviewSession {
        [*] --> ShowCard: "next"
        ShowCard --> Explain: "why"
        ShowCard --> DraftPreview: "draft"
        ShowCard --> ApplyJob: "apply"
        ShowCard --> SkipJob: "skip <reason>"
        ShowCard --> NextCard: "next"

        Explain --> ShowCard
        DraftPreview --> ShowCard
        ApplyJob --> NextCard
        SkipJob --> NextCard: Feedback captured
        NextCard --> ShowCard: More matches
        NextCard --> Home: No more matches
    }

    Backlog --> Home: View hidden jobs
```

## Infrastructure

```mermaid
graph LR
    subgraph "AWS us-east-1"
        subgraph "Compute"
            L1["job-ingestor<br/>Lambda"]
            L2["job-deduplicator<br/>Lambda"]
            L3["job-processor<br/>Lambda"]
            L4["job-router<br/>Lambda"]
            L5["job-sender<br/>Lambda"]
        end

        subgraph "Messaging"
            Q1["job-raw-queue<br/>SQS"]
            Q2["job-filtered-queue<br/>SQS"]
            D1["job-raw-queue-dlq<br/>SQS DLQ"]
            D2["job-filtered-queue-dlq<br/>SQS DLQ"]
            SNS1["jobsearch-approvals<br/>SNS Topic"]
        end

        subgraph "Storage"
            DDB["Jobsearch<br/>DynamoDB"]
            S3["job-search-pipeline-memory<br/>S3 Bucket"]
        end

        subgraph "AI"
            BR["Bedrock<br/>Claude Haiku"]
        end

        subgraph "Security"
            SM["Secrets Manager<br/>6 secrets"]
            IAM["IAM Roles<br/>per-Lambda policies"]
        end

        subgraph "Monitoring"
            CW["CloudWatch<br/>Logs + Alarms"]
            AG["API Gateway<br/>Telegram Webhook"]
        end
    end

    subgraph "External"
        TG2["Telegram API"]
        GM["Gmail SMTP"]
        HU["Hunter.io API"]
    end
```

## CI/CD Pipeline

```mermaid
flowchart LR
    DEV["Developer Push"] --> CI["CI Workflow"]

    subgraph CI["CI Workflow"]
        TEST["pytest<br/>252 tests"]
        EVAL["Scoring Evals<br/>5 labeled cases"]
        LINT["Ruff Lint"]
    end

    CI -->|Pass| DETECT["Detect Changed<br/>Lambda Dirs"]
    DETECT --> APPROVE["Manual Approval<br/>production environment"]
    APPROVE --> DEPLOY["Deploy Changed<br/>Lambdas Only"]
    DEPLOY --> VERIFY["Post-Deploy<br/>Health Check"]

    subgraph VERIFY["Verification"]
        V1["Lambda state check"]
        V2["DLQ message check"]
    end

    subgraph "Scheduled"
        HEALTH["Health Monitor<br/>Every 6 hours"]
        ANALYTICS["Pipeline Analytics<br/>Daily"]
        SOURCE["Source Audit<br/>Daily"]
        FEEDBACK["Feedback Export<br/>Weekly"]
    end
```
