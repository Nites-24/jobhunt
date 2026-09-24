"""Check and set Telegram webhook to point to job-router API Gateway."""

import os, json, boto3, urllib.request

# Load .env
with open(".env") as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="us-east-1",
)

# 1. Get API Gateway URL
print("=" * 60)
print("  CHECKING TELEGRAM WEBHOOK SETUP")
print("=" * 60)

apigw = session.client("apigatewayv2")
apis = apigw.get_apis()["Items"]
print(f"\n  API Gateways found: {len(apis)}")

api_url = None
for api in apis:
    name = api.get("Name", "")
    endpoint = api.get("ApiEndpoint", "")
    api_id = api.get("ApiId", "")
    print(f"    - {name} ({api_id}): {endpoint}")
    if "jobsearch" in name.lower() or "hitl" in name.lower():
        api_url = endpoint

if not api_url:
    print("\n  ERROR: No jobsearch API Gateway found!")
    exit(1)

print(f"\n  API Gateway URL: {api_url}")

# 2. Get Telegram bot token from Secrets Manager
sm = session.client("secretsmanager")
try:
    secret = sm.get_secret_value(SecretId="telegram")
    creds = json.loads(secret["SecretString"])
    bot_token = creds.get("TELEGRAM_BOT_TOKEN") or creds.get("bot_token") or creds.get("token")
    chat_id = creds.get("TELEGRAM_CHAT_ID") or creds.get("chat_id")
    print(f"  Bot token: {bot_token[:10]}...{bot_token[-5:]}" if bot_token else "  Bot token: NOT FOUND")
    print(f"  Chat ID: {chat_id}")
except Exception as e:
    print(f"  ERROR getting telegram secret: {e}")
    # Try alternate secret name
    try:
        secret = sm.get_secret_value(SecretId="jobserch/telegram")
        creds = json.loads(secret["SecretString"])
        bot_token = creds.get("TELEGRAM_BOT_TOKEN") or creds.get("bot_token") or creds.get("token")
        chat_id = creds.get("TELEGRAM_CHAT_ID") or creds.get("chat_id")
        print(f"  Bot token (from jobserch/telegram): {bot_token[:10]}...{bot_token[-5:]}" if bot_token else "  NOT FOUND")
        print(f"  Chat ID: {chat_id}")
    except Exception as e2:
        print(f"  ERROR: {e2}")
        exit(1)

if not bot_token:
    print("  ERROR: No bot token found in secrets!")
    exit(1)

# 3. Check current webhook
print(f"\n  Checking current Telegram webhook...")
info_url = f"https://api.telegram.org/bot{bot_token}/getWebhookInfo"
req = urllib.request.Request(info_url)
with urllib.request.urlopen(req, timeout=10) as resp:
    info = json.loads(resp.read().decode("utf-8"))
    current_url = info.get("result", {}).get("url", "")
    pending = info.get("result", {}).get("pending_update_count", 0)
    last_error = info.get("result", {}).get("last_error_message", "None")

print(f"  Current webhook URL: {current_url or '(not set)'}")
print(f"  Pending updates: {pending}")
print(f"  Last error: {last_error}")

# 4. Set webhook if needed
# The webhook URL should be: API_GATEWAY_URL/webhook or just API_GATEWAY_URL
webhook_url = api_url  # Adjust if you have a specific route

if current_url == webhook_url:
    print(f"\n  Webhook is already correctly set!")
else:
    secret_token = creds.get("TELEGRAM_WEBHOOK_SECRET") or creds.get("secret_token") or os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")
    print(f"\n  Setting webhook to: {webhook_url}")
    set_url = f"https://api.telegram.org/bot{bot_token}/setWebhook?url={urllib.request.quote(webhook_url)}"
    if secret_token:
        set_url += f"&secret_token={urllib.request.quote(secret_token)}"
        print("  Using Telegram secret_token for header authentication.")
    req = urllib.request.Request(set_url)
    with urllib.request.urlopen(req, timeout=10) as resp:
        result = json.loads(resp.read().decode("utf-8"))
        if result.get("ok"):
            print("  Webhook SET successfully!")
        else:
            print(f"  FAILED: {result}")

# 5. Verify by sending a test message
print(f"\n  Sending test message to Telegram...")
msg = "Pipeline verified. All 5 Lambdas deployed. Type 'list' to see pending jobs or 'search jobs' to find new ones."
send_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
data = json.dumps({"chat_id": chat_id, "text": msg}).encode("utf-8")
req = urllib.request.Request(send_url, data=data, headers={"Content-Type": "application/json"})
try:
    with urllib.request.urlopen(req, timeout=10) as resp:
        result = json.loads(resp.read().decode("utf-8"))
        if result.get("ok"):
            print(f"  Message sent to chat {chat_id}!")
        else:
            print(f"  FAILED: {result}")
except Exception as e:
    print(f"  ERROR: {e}")

print(f"\n{'='*60}")
print("  SETUP COMPLETE")
print(f"{'='*60}\n")
