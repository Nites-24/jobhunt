# JobHunt Deployment & Rollback Guide

This guide documents the repository's deployment procedure and a proposed rollback protocol. Commands must be validated in a staging account before use in production.

## 1. Safety Principles

- Never deploy uncommitted, unreviewed, or failing code.
- Always run local tests (`pytest`) before initiating deployment.
- Never run live ingestion, scoring, or mass-emailing as an ad-hoc test on production databases.
- Real email sending is strictly gated behind both `ALLOW_TELEGRAM_EMAIL_SEND=true` on `job-router` and `ALLOW_REAL_EMAIL_SEND=true` on `job-sender`.
- Automatic rollback on Lambda code updates is **not supported** natively by single-function zip updates without AWS CodeDeploy or Lambda Aliases/Traffic Shifting. Follow the manual rollback outline below only after validating it in staging.

---

## 2. Automated Deployment Pipeline

Deployments to AWS Lambda are managed via `.github/workflows/deploy.yml`.
> [!IMPORTANT]
> `deploy.yml` is triggered **manually via `workflow_dispatch`**. Pushing or merging to `main` does **not** automatically deploy. The workflow exposes `skip_approval=true`, which bypasses its approval job; do not claim an unconditional production gate without checking the GitHub environment settings and access controls.

### Deployment Steps:
1. **Package Lambda Artifacts**: Each Lambda (`job_ingestor`, `job_deduplicator`, `job_processor`, `job_router`, `job_sender`) is packaged from the repository root, ensuring the root `shared/` package is included at the top level of the zip archive.
2. **Deploy Code via AWS CLI / SDK**:
   ```bash
   aws lambda update-function-code \
     --function-name <lambda-name> \
     --zip-file fileb://build/<lambda-name>.zip
   ```
3. **Wait for Update Completion**:
   AWS Lambda processes code updates asynchronously. The deployment workflow actively polls `LastUpdateStatus` every 5 seconds until it reports `Successful` (or fails after timeout):
   ```bash
   STATUS="Unknown"
   for i in $(seq 1 12); do
     STATUS=$(aws lambda get-function-configuration --function-name "$func" --query 'LastUpdateStatus' --output text)
     if [ "$STATUS" = "Successful" ]; then
       break
     elif [ "$STATUS" = "Failed" ]; then
       echo "Lambda update failed for $func"
       exit 1
     fi
     sleep 5
   done
   if [ "$STATUS" != "Successful" ]; then
     echo "Lambda update timed out or ended in unexpected status: $STATUS"
     exit 1
   fi
   ```
4. **Post-Deployment Verification**:
   The workflow executes inline verification steps:
   - Polls and asserts that all 5 Lambdas have `State=Active` and `LastUpdateStatus=Successful`.
   - Inspects dead-letter queues (`job-raw-queue-dlq`, `job-filtered-queue-dlq`) to confirm message counts are clean.
   - For additional read-only operational checks, run `python tools/ops/check_aws.py` or `python tools/ops/verify_live_production.py`.

---

## 3. Pre-Deployment Configuration Verification

### Telegram Webhook Secret Requirement
`job-router` defaults to `REQUIRE_TELEGRAM_WEBHOOK_SECRET=true`. Read its effective configuration without printing any secret values. It checks `TELEGRAM_WEBHOOK_SECRET` in the secret named by `TELEGRAM_SECRET_NAME` (default `telegram`), then in its Lambda environment:

```bash
python - <<'PY'
import boto3
import json

region = "us-east-1"
cfg = boto3.client("lambda", region_name=region).get_function_configuration(FunctionName="job-router")
variables = cfg.get("Environment", {}).get("Variables", {})
present = bool(variables.get("TELEGRAM_WEBHOOK_SECRET"))
if not present:
    secret_name = variables.get("TELEGRAM_SECRET_NAME", "telegram")
    record = boto3.client("secretsmanager", region_name=region).get_secret_value(SecretId=secret_name)
    present = bool(json.loads(record["SecretString"]).get("TELEGRAM_WEBHOOK_SECRET"))
print("Webhook secret configured" if present else "Webhook secret MISSING")
if not present:
    raise SystemExit(1)
PY
```

If empty, set `TELEGRAM_WEBHOOK_SECRET` in Secrets Manager and register the secret token with Telegram via `setWebhook` before deploying:
```bash
curl -X POST "https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/setWebhook" \
  -H "Content-Type: application/json" \
  -d '{"url": "<API_GATEWAY_URL>", "secret_token": "<SECRET_TOKEN>"}'
```

---

## 4. Safe Environment Variable Updates

> [!WARNING]
> Running `aws lambda update-function-configuration --function-name <name> --environment "Variables={...}"` **completely replaces the entire environment map**. Any variables not explicitly provided in the command will be permanently deleted from the Lambda function!

### Safe 3-Step Merge Procedure

To safely add or update environment variables (e.g. adding new ATS boards):

```bash
# 1. Capture variables and revision from the same configuration read
config_file=$(mktemp)
merged_file=$(mktemp)
payload_file=$(mktemp)
trap 'rm -f "$config_file" "$merged_file" "$payload_file"' EXIT
chmod 600 "$config_file" "$merged_file" "$payload_file"
aws lambda get-function-configuration \
  --function-name job-ingestor \
  --output json > "$config_file"

# 2. Merge new variables with existing variables using jq
# Note: The Ashby variable name is ASHBY_BOARDS (not ASHBY_ORGANIZATIONS)
jq '.Environment.Variables // {} | . + {
  "GREENHOUSE_BOARDS": "anthropic|Anthropic,scaleai|Scale AI",
  "LEVER_SITES": "palantir|Palantir",
  "ASHBY_BOARDS": "perplexity|Perplexity",
  "ATS_MAX_JOBS_PER_BOARD": "20"
}' "$config_file" > "$merged_file"
jq -n --slurpfile variables "$merged_file" '{Variables: $variables[0]}' > "$payload_file"
revision=$(jq -r '.RevisionId' "$config_file")

# 3. Reject concurrent configuration changes instead of overwriting them
aws lambda update-function-configuration \
  --function-name job-ingestor \
  --revision-id "$revision" \
  --environment "file://$payload_file"
```

---

## 5. Manual Rollback Outline (Not Live-Verified)

### Step 1: Identify the Last Known Healthy Git Commit
```bash
git log --oneline -n 5
```
Example: `abc1234` (Healthy previous release), `def5678` (Broken current release).

### Step 2: Roll Back via Git & Deployment Pipeline (Recommended)
1. Revert the commit on `main`:
   ```bash
   git revert def5678 --no-edit
   git push origin main
   ```
2. Manually trigger the deployment workflow via GitHub Actions UI or GitHub CLI:
   ```bash
   gh workflow run deploy.yml --ref main -f lambdas=all
   ```
3. Approve the production deployment in GitHub Actions.

### Step 3: Emergency Direct Lambda Code Rollback
If GitHub Actions is unavailable or immediate remediation is required:

1. Checkout the known healthy commit locally:
   ```bash
   git checkout abc1234
   ```
2. Build and package the specific failing Lambda (e.g., `job_router`) from the **repository root**:
   ```bash
   python -c "
   import os, zipfile
   with zipfile.ZipFile('/tmp/job_router_rollback.zip', 'w', zipfile.ZIP_DEFLATED) as zf:
       for root, _, files in os.walk('job_router'):
           for f in files:
               if f.endswith('.py'):
                   zf.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), 'job_router'))
       for root, _, files in os.walk('shared'):
           for f in files:
               if f.endswith('.py'):
                   zf.write(os.path.join(root, f), os.path.join('shared', f))
   "
   ```
3. Update the Lambda function code directly:
   ```bash
   aws lambda update-function-code \
     --function-name job-router \
     --zip-file fileb:///tmp/job_router_rollback.zip
   ```
4. Wait for the update status to become `Successful`:
   ```bash
   aws lambda wait function-updated --function-name job-router
   ```
5. Return your local workspace to `main`:
   ```bash
   git checkout main
   ```

### Step 4: Verify Rollback Health
Run the read-only operational checks:
```bash
python -m pytest tests/test_router.py -v
python tools/ops/check_aws.py
```
Check that the Lambda functions are active and responsive, DLQ message counts are not growing, and review queue operations function properly.
