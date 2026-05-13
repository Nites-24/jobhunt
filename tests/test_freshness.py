import os
import pytest

if os.environ.get("RUN_LIVE_TESTS") != "1":
    pytest.skip("Live AWS/API smoke test. Set RUN_LIVE_TESTS=1 to run.", allow_module_level=True)

import boto3, json, urllib.request

# 1. Load AWS Credentials
with open('.env') as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith('#') and '=' in l:
            k, _, v = l.partition('=')
            os.environ.setdefault(k.strip(), v.strip())

secrets_client = boto3.client("secretsmanager", region_name="us-east-1")

# 2. Get JSearch API Key
secret = secrets_client.get_secret_value(SecretId="jobserch/jsearch")
creds = json.loads(secret["SecretString"])
api_key = creds.get("api_key") or creds.get("JSEARCH_API_KEY")

# 3. Test different queries to find fresh relevant jobs
queries_to_test = [
    "AI Engineer linkedin",
    "Generative AI Engineer remote",
    "Machine Learning Engineer"
]

print("🔍 TESTING FRESH JOB RELEVANCE (Live JSearch API)")
print("=" * 60)

for query in queries_to_test:
    print(f"\nSearching: '{query}' (Last 3 days)")
    encoded_query = urllib.request.quote(query)
    
    # Using '3days' to expand the net slightly for testing
    url = f"https://jsearch.p.rapidapi.com/search?query={encoded_query}&page=1&num_pages=1&date_posted=3days&country=us"
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
    }
    
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode("utf-8"))
            jobs = data.get("data", [])
            print(f"  Found {len(jobs)} jobs. Top 3 results:")
            for j in jobs[:3]:
                title = j.get("job_title", "Unknown")
                company = j.get("employer_name", "Unknown")
                posted = j.get("job_posted_at_datetime_utc", "Unknown")[:10]
                link = j.get("job_apply_link") or j.get("job_google_link", "")
                
                # Check if it's a ghost job based on our new filter
                is_ghost = any(spam in link.lower() for spam in [
                    "learn4good.com", "bebee.com", "recruit.net", "zycto.com", 
                    "wfh.hstn.me", "dice.com", "ziprecruiter.com", "monster.com"
                ])
                status = "❌ GHOST" if is_ghost else "✅ REAL"
                
                print(f"    - {status} | {title} @ {company} (Posted: {posted})")
                
    except Exception as e:
        print(f"  Error: {e}")
