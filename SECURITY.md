# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly:

1. **Do not** open a public GitHub issue
2. Email: vakateja24@gmail.com
3. Include a description of the vulnerability and steps to reproduce

## Security Controls

### Email Safety Gates

Real email sending is protected by two independent flags:

- `ALLOW_TELEGRAM_EMAIL_SEND=true` on `job-router` Lambda
- `ALLOW_REAL_EMAIL_SEND=true` on `job-sender` Lambda

Both must be explicitly set to `true` for any real SMTP delivery. The `apply` command moves a job to manual follow-up only — it never sends email.

### Secrets Management

All sensitive credentials are stored in AWS Secrets Manager:

| Secret | Purpose |
|--------|---------|
| `telegram` | Telegram bot token and chat ID |
| `jobsearch/google_cse` | Google Custom Search API key and CX |
| `adzuna/jobsearch` | Adzuna API credentials |
| `jobserch/jsearch` | JSearch API key |
| `jobsearch/hunterio` | Hunter.io API key |
| `jobserch_gmail` | Gmail app password for SMTP |

No secrets are stored in code, environment variables at rest, or committed to version control.

### Data Protection

- `.env` is in `.gitignore` and never committed
- DynamoDB rows have a 90-day TTL
- No PII beyond candidate profile facts is stored
- Feedback data is exported for offline eval only — never used to auto-change production behavior

### Infrastructure Safety

- SQS dead letter queues on all message queues
- CloudWatch alarms on Lambda errors, throttles, and queue backlogs
- IAM policies follow least-privilege principles
- Bedrock throttling is handled with retry + deterministic fallback

## Supported Versions

| Version | Supported |
|---------|-----------|
| Current (main) | ✅ |
| Older commits | ❌ |
