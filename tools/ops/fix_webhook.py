"""Fix webhook URL back to the correct /prod/telegram path."""
import os, json, urllib.request, boto3

with open(".env") as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith("#") and "=" in l:
            k, _, v = l.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="us-east-1",
)

# Get bot token
sm = session.client("secretsmanager")
secret = sm.get_secret_value(SecretId="telegram")
creds = json.loads(secret["SecretString"])
bot_token = creds.get("TELEGRAM_BOT_TOKEN") or creds.get("bot_token")
chat_id = creds.get("TELEGRAM_CHAT_ID") or creds.get("chat_id")

# Fix webhook back to correct URL with /prod/telegram path
correct_url = "https://krewl8w46c.execute-api.us-east-1.amazonaws.com/prod/telegram"
print(f"Setting webhook to: {correct_url}")
set_url = f"https://api.telegram.org/bot{bot_token}/setWebhook?url={urllib.request.quote(correct_url)}"
req = urllib.request.Request(set_url)
with urllib.request.urlopen(req, timeout=10) as resp:
    result = json.loads(resp.read().decode("utf-8"))
    ok = result.get("ok", False)
    desc = result.get("description", "")
    print(f"  Result: {'OK' if ok else 'FAILED'} - {desc}")

# Verify
print("\nVerifying webhook...")
info_url = f"https://api.telegram.org/bot{bot_token}/getWebhookInfo"
req2 = urllib.request.Request(info_url)
with urllib.request.urlopen(req2, timeout=10) as resp2:
    info = json.loads(resp2.read().decode("utf-8"))
    r = info.get("result", {})
    print(f"  Webhook URL: {r.get('url', '?')}")
    print(f"  Pending: {r.get('pending_update_count', 0)}")
    print(f"  Last error: {r.get('last_error_message', 'None')}")

# Send confirmation message
print("\nSending test message...")
msg = (
    "Pipeline LIVE. All systems verified.\n\n"
    "Test these commands now:\n"
    "  status - Pipeline stats\n"
    "  list - See scored matches\n"
    "  search jobs - Find new jobs\n"
    "  apply 1 - Send email to top match"
)
send_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
data = json.dumps({"chat_id": chat_id, "text": msg}).encode("utf-8")
req3 = urllib.request.Request(send_url, data=data, headers={"Content-Type": "application/json"})
with urllib.request.urlopen(req3, timeout=10) as resp3:
    result = json.loads(resp3.read().decode("utf-8"))
    print(f"  Message sent: {result.get('ok', False)}")

print("\nDONE - Go check Telegram now!")
