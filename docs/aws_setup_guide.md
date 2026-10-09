# JobHunt — Complete Step-by-Step AWS Setup & Deployment Guide

This guide walks open-source developers through setting up, configuring, and deploying the complete autonomous JobHunt pipeline on AWS from scratch.

---

## 🏛️ Architecture Overview

The system runs entirely serverless on AWS:
* **Compute**: 5 AWS Lambda functions (`job-ingestor`, `job-deduplicator`, `job-processor`, `job-router`, `job-sender`)
* **Queues**: 2 Amazon SQS queues with Dead-Letter Queues (`job-raw-queue`, `job-filtered-queue`)
* **State & Memory**: Amazon DynamoDB (`Jobsearch` table) & Amazon S3 (`job-search-pipeline-memory`)
* **AI Cognitive Engine**: Amazon Bedrock (Anthropic Claude 3.5 Haiku) via forced tool calling
* **Messaging & Control**: Amazon API Gateway HTTP API v2, Amazon SNS (`jobsearch-approvals`), and Telegram Bot API

---

## 📋 Phase 1: Prerequisites

Before deploying, ensure you have:
1. **AWS Account**: An AWS account with admin or power-user IAM permissions in `us-east-1`.
2. **Amazon Bedrock Model Access**:
   * In AWS Console, navigate to **Amazon Bedrock** $\rightarrow$ **Model access**.
   * Request and enable access to **Anthropic Claude 3.5 Haiku** (and optionally Claude 3 Sonnet).
3. **Telegram Bot**:
   * Open Telegram and message `@BotFather`.
   * Send `/newbot`, name your bot, and copy the `TELEGRAM_BOT_TOKEN`.
   * Message `@userinfobot` to retrieve your numeric `TELEGRAM_CHAT_ID`.
4. **Local Python Environment**:
   * Python 3.11 or higher.
   * AWS CLI installed and configured (`aws configure`).

---

## 🛠️ Phase 2: Create AWS Core Infrastructure

### 1. DynamoDB Table (`Jobsearch`)
Create the primary tracking table with two Global Secondary Indexes:

```bash
aws dynamodb create-table \
  --table-name Jobsearch \
  --attribute-definitions \
      AttributeName=job_id,AttributeType=S \
      AttributeName=status,AttributeType=S \
      AttributeName=posted_at,AttributeType=S \
      AttributeName=url_hash,AttributeType=S \
  --key-schema \
      AttributeName=job_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --global-secondary-indexes \
      "[
        {
          \"IndexName\": \"StatusIndex\",
          \"KeySchema\": [
            {\"AttributeName\": \"status\", \"KeyType\": \"HASH\"},
            {\"AttributeName\": \"posted_at\", \"KeyType\": \"RANGE\"}
          ],
          \"Projection\": {\"ProjectionType\": \"ALL\"}
        },
        {
          \"IndexName\": \"UrlHashIndex\",
          \"KeySchema\": [
            {\"AttributeName\": \"url_hash\", \"KeyType\": \"HASH\"}
          ],
          \"Projection\": {\"ProjectionType\": \"ALL\"}
        }
      ]" \
  --region us-east-1
```

Enable Time-To-Live (TTL) for 90-day automatic row expiration:
```bash
aws dynamodb update-time-to-live \
  --table-name Jobsearch \
  --time-to-live-specification "Enabled=true,AttributeName=ttl" \
  --region us-east-1
```

---

### 2. SQS Queues & Dead-Letter Queues (DLQs)
Create the dead-letter queues first, then the primary queues with redrive policies:

```bash
# Create DLQs
aws sqs create-queue --queue-name job-raw-queue-dlq --region us-east-1
aws sqs create-queue --queue-name job-filtered-queue-dlq --region us-east-1
aws sqs create-queue --queue-name job-send-queue --region us-east-1

# Get DLQ ARNs
RAW_DLQ_ARN=$(aws sqs get-queue-attributes --queue-name job-raw-queue-dlq --attribute-names QueueArn --query 'Attributes.QueueArn' --output text --region us-east-1)
FILT_DLQ_ARN=$(aws sqs get-queue-attributes --queue-name job-filtered-queue-dlq --attribute-names QueueArn --query 'Attributes.QueueArn' --output text --region us-east-1)

# Create primary queues with maxReceiveCount=5
aws sqs create-queue --queue-name job-raw-queue \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$RAW_DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}" \
  --region us-east-1

aws sqs create-queue --queue-name job-filtered-queue \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$FILT_DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}" \
  --region us-east-1
```

---

### 3. SNS Notification Topic
Create the notification topic for high-match candidate alerts:

```bash
aws sns create-topic --name jobsearch-approvals --region us-east-1
```

---

### 4. S3 Candidate Memory & Resume Bucket
Create the bucket to store candidate resumes and knowledge graph profiles:

```bash
aws s3 mb s3://job-search-pipeline-memory --region us-east-1

# Upload your resume PDF:
aws s3 cp data/resume/resume.pdf s3://job-search-pipeline-memory/data/resume/resume.pdf
```

---

### 5. AWS Secrets Manager
Store your credentials securely in AWS Secrets Manager:

```bash
# 1. Telegram Bot Credentials
aws secretsmanager create-secret \
  --name telegram \
  --secret-string "{\"TELEGRAM_BOT_TOKEN\":\"<YOUR_BOT_TOKEN>\",\"TELEGRAM_CHAT_ID\":\"<YOUR_CHAT_ID>\",\"TELEGRAM_WEBHOOK_SECRET\":\"<OPTIONAL_RANDOM_SECRET>\"}" \
  --region us-east-1

# 2. Google Custom Search (Optional)
aws secretsmanager create-secret \
  --name jobsearch/google_cse \
  --secret-string "{\"GOOGLE_API_KEY\":\"<YOUR_API_KEY>\",\"GOOGLE_CX\":\"<YOUR_SEARCH_ENGINE_ID>\"}" \
  --region us-east-1

# 3. Hunter.io Recruiter Email Lookup (Optional)
aws secretsmanager create-secret \
  --name jobsearch/hunterio \
  --secret-string "{\"api_key\":\"<YOUR_HUNTER_API_KEY>\"}" \
  --region us-east-1

# 4. Gmail SMTP App Password (Required only if live email sending is enabled)
aws secretsmanager create-secret \
  --name jobserch_gmail \
  --secret-string "{\"gmail_email\":\"your.email@gmail.com\",\"gmail_app_password\":\"<16_CHAR_APP_PASSWORD>\"}" \
  --region us-east-1
```

---

## ⚙️ Phase 3: Configure Local Environment

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your candidate context:
   ```ini
   # AWS IAM Credentials
   AWS_ACCESS_KEY_ID=AKIA...
   AWS_SECRET_ACCESS_KEY=...
   AWS_DEFAULT_REGION=us-east-1

   # Candidate Identity (Injected at Runtime)
   CANDIDATE_NAME=Jane Doe
   TARGET_ROLE=AI Engineer
   CANDIDATE_LOCATION=San Francisco, CA
   SENDER_EMAIL=jane.doe@example.com
   CANDIDATE_LINKEDIN=https://linkedin.com/in/janedoe
   RETIREIQ_URL=https://demo.example.com
   S3_RESUME_KEY=data/resume/resume.pdf

   # Outreach Air-gap Gates (Keep false until ready)
   ALLOW_TELEGRAM_EMAIL_SEND=false
   ALLOW_REAL_EMAIL_SEND=false
   ```

---

## 🚀 Phase 4: Deploy Lambda Functions

Deploy all 5 serverless microservices to AWS:

```bash
python deploy.py
```

Apply production defaults (thresholds, freshness bounds, scoring caps):
```bash
python tools/ops/set_production_config.py --apply
```

Provision dead-letter queues and CloudWatch operational alarms:
```bash
python tools/ops/setup_production_safety.py --apply
```

---

## 📱 Phase 5: Connect Telegram Webhook & Automated Discovery

1. **Connect Telegram Webhook**:
   ```bash
   python tools/ops/setup_telegram.py
   ```
   This registers your API Gateway `/prod/telegram` endpoint with the Telegram Bot API and sends a confirmation test ping to your phone.

2. **Enable Automated 3-Hour Discovery**:
   ```bash
   python tools/ops/setup_cron.py --schedule "rate(3 hours)"
   ```
   This provisions the AWS EventBridge rule to search for jobs every 3 hours on autopilot.

---

## ✅ Phase 6: Verification & First Use

1. **Run Local Offline Regression Suite**:
   ```bash
   python -m pytest tests/ -q
   ```
   Asserts that all 252 tests pass offline without cloud spend.

2. **Run Zero-Trust Security Audit**:
   ```bash
   python tools/ops/run_agents.py --security
   ```
   Confirms zero leaks, zero credentials in code, and zero infinite loops.

3. **Interact with the Bot in Telegram**:
   * Text `/start` or `help` to view the command dashboard.
   * Text `search jobs` to trigger your first live ingestion scan.
   * Text `list` to inspect fresh AI engineering matches.
   * Text `why 1` to see mathematical scoring evidence and strengths.
   * Text `draft 1` to preview a personalized, anti-AI tone outreach pitch.
