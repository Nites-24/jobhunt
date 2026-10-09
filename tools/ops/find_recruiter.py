"""
Targeted Recruiter Discovery & Contact Injection Utility (find_recruiter.py)

Role & Operational Context:
  Performs live queries against the Hunter.io Domain Search API to discover verified
  Human Resources / Talent Acquisition contacts for target companies and injects them
  directly into DynamoDB job records.

Workflow:
  1. Retrieves Hunter.io credentials from AWS Secrets Manager (`jobsearch/hunterio`).
  2. Scans DynamoDB 'Jobsearch' for top 'PENDING_APPROVAL' jobs.
  3. Queries Hunter.io domain search filtering by department=human_resources.
  4. Injects discovered recruiter name and email into DynamoDB.
  5. Prepares the job record for Telegram 'apply <n>' outreach dispatch.

Usage:
  python tools/ops/find_recruiter.py
"""

import os, json, boto3, urllib.request

with open(".env") as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith("#") and "=" in l:
            k, _, v = l.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

s = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="us-east-1",
)

# Get Hunter.io API key
sm = s.client("secretsmanager")
hunter_key = json.loads(sm.get_secret_value(SecretId="jobsearch/hunterio")["SecretString"])["api_key"]

# Get the Drata job from DynamoDB
table = s.resource("dynamodb").Table("Jobsearch")
scan = table.scan()
items = scan["Items"]

# Find PENDING_APPROVAL jobs
pending = [i for i in items if i.get("status") == "PENDING_APPROVAL"]
pending.sort(key=lambda x: int(x.get("match_score", 0)), reverse=True)

print("Top pending jobs:\n")
for i, job in enumerate(pending[:5], 1):
    score = int(job.get("match_score", 0))
    email = job.get("recruiter_email", "")
    print(f"  {i}. [{score}%] {job.get('title')} @ {job.get('company')} | email: {email or 'none'}")

# Try Hunter.io for the top job (Drata)
companies_to_try = [
    ("drata.com", "Drata"),
    ("cisco.com", "Cisco"),
    ("freddiemac.com", "Freddie Mac"),
    ("capitalone.com", "Capital One"),
]

print(f"\n{'='*60}")
print("  HUNTER.IO RECRUITER LOOKUP")
print(f"{'='*60}")

for domain, company in companies_to_try:
    url = (
        f"https://api.hunter.io/v2/domain-search"
        f"?domain={domain}"
        f"&department=human_resources"
        f"&limit=3"
        f"&api_key={hunter_key}"
    )
    
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            emails = data.get("data", {}).get("emails", [])
            
            print(f"\n  {company} ({domain}):")
            if emails:
                for e in emails:
                    name = f"{e.get('first_name', '')} {e.get('last_name', '')}".strip()
                    email = e.get("value", "")
                    dept = e.get("department", "")
                    conf = e.get("confidence", 0)
                    print(f"    {name} | {email} | {dept} | confidence: {conf}%")
            else:
                print(f"    No HR emails found")
                
    except Exception as e:
        print(f"    Error: {e}")

# Now update the Drata job with a real email
print(f"\n{'='*60}")
print("  UPDATING DRATA JOB WITH RECRUITER EMAIL")
print(f"{'='*60}")

# Find Drata job
drata_jobs = [j for j in pending if "drata" in j.get("company", "").lower()]

if drata_jobs:
    job = drata_jobs[0]
    
    # Try Hunter.io for Drata
    url = f"https://api.hunter.io/v2/domain-search?domain=drata.com&limit=3&api_key={hunter_key}"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            emails = data.get("data", {}).get("emails", [])
            
            if emails:
                recruiter = emails[0]
                email = recruiter.get("value", "")
                name = f"{recruiter.get('first_name', '')} {recruiter.get('last_name', '')}".strip()
                
                # Update DynamoDB
                table.update_item(
                    Key={"job_id": job["job_id"]},
                    UpdateExpression="SET recruiter_email = :e, recruiter_name = :n",
                    ExpressionAttributeValues={":e": email, ":n": name},
                )
                print(f"\n  Updated Drata job:")
                print(f"    recruiter_email: {email}")
                print(f"    recruiter_name: {name}")
                print(f"\n  Now type 'apply 3' in Telegram to send a REAL email to {name}!")
            else:
                print(f"\n  No emails found for drata.com")
                print(f"  The email will be sent to yourself as a preview.")
    except Exception as e:
        print(f"  Error: {e}")
else:
    print("  Drata job not found in pending")
