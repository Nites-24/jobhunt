# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly:

1. **Do not** open a public GitHub issue
2. Submit a private security advisory through GitHub Security Advisories
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
| `jobserch/jsearch` | JSearch API key |
| `jobsearch/hunterio` | Hunter.io API key |
| `jobserch_gmail` | Gmail app password for SMTP |

No secrets are stored in code, environment variables at rest, or committed to version control.

### Data Protection & Identity Sovereignty

- **100% Identity-Agnostic Core**: The codebase is decoupled from personal candidate identity. All personal names, locations, emails, and URLs are injected strictly at runtime via environment variables (`CANDIDATE_NAME`, `TARGET_ROLE`, `CANDIDATE_LOCATION`, `SENDER_EMAIL`, `CANDIDATE_LINKEDIN`, `RESUME_KEY`, `RETIREIQ_URL`).
- **Zero PII in Git**: No personal resumes or personal contact details are stored in git history or tracked files.
- `.env` is strictly ignored by `.gitignore` and never committed.
- DynamoDB rows have an automatic 90-day TTL expiration.
- User feedback captured via Telegram is exported for offline evaluation only and cannot auto-modify production prompts or rules.

### Network Hardening & SSRF Defense

- All outbound HTTP requests (ATS job boards, Hacker News comment links) enforce private IP blocking.
- Intercepts and denies requests targeting loopback (`127.0.0.1`), link-local (`169.254.169.254`), and RFC 1918 private networks (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`).
- All external HTTP calls enforce bounded timeouts ($\le 4.0$ seconds).

### AST Loop Safety & Automated Audits

- Code is verified via Python Abstract Syntax Tree (`ast`) analysis to ensure all loops have bounded termination guards and zero infinite while loops.
- Pre-commit and CI/CD pipelines run the automated security auditor:
  ```powershell
  python tools/ops/run_agents.py --security
  ```

### Infrastructure Safety

- SQS dead letter queues (DLQs) with `maxReceiveCount: 5` on all processing queues.
- CloudWatch alarms on Lambda errors, throttles, and SQS queue backlogs.
- IAM policies follow least-privilege principles.
- Bedrock throttling is handled with bounded backoff retry + deterministic fallback.

## Supported Versions

| Version | Supported |
|---------|-----------|
| Current (main) | ✅ |
| Older commits | ❌ |
