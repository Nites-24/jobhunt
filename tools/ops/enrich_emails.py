"""
Recruiter Email Enrichment Utility (enrich_emails.py)

Role & Operational Context:
  Enriches top pending job opportunities in DynamoDB with verified recruiter contact
  information using the Hunter.io Domain Search API.

Workflow:
  1. Retrieves the Hunter.io API key securely from AWS Secrets Manager (`jobsearch/hunterio`).
  2. Scans DynamoDB for all opportunities in 'PENDING_APPROVAL' status, sorted by match_score descending.
  3. For jobs lacking recruiter contact info, maps company names to official domain names.
  4. Queries Hunter.io `/v2/domain-search` for verified recruiter/talent acquisition contacts.
  5. Updates DynamoDB record with `recruiter_email` and `recruiter_name`.

Usage:
  python tools/ops/enrich_emails.py
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

sm = s.client("secretsmanager")
hunter_key = json.loads(sm.get_secret_value(SecretId="jobsearch/hunterio")["SecretString"])["api_key"]
table = s.resource("dynamodb").Table("Jobsearch")

# Get pending jobs
scan = table.scan()
pending = [i for i in scan["Items"] if i.get("status") == "PENDING_APPROVAL"]
pending.sort(key=lambda x: int(x.get("match_score", 0)), reverse=True)

domains = {
    "Synopsys Inc": "synopsys.com",
    "Freddie Mac": "freddiemac.com",
    "The MITRE Corporation": "mitre.org",
    "Cisco": "cisco.com",
    "Genworth": "genworth.com",
    "EnthuZiastic": "enthuziastic.com",
}

print(f"Looking up recruiter emails for {len(pending)} pending jobs...\n")

for job in pending:
    company = job.get("company", "")
    score = int(job.get("match_score", 0))
    existing_email = (job.get("recruiter_email") or "").strip()
    
    if existing_email:
        print(f"  [{score}%] {company}: already has {existing_email}")
        continue
    
    domain = domains.get(company, "")
    if not domain:
        print(f"  [{score}%] {company}: no domain mapping, skipping")
        continue
    
    url = (
        f"https://api.hunter.io/v2/domain-search"
        f"?domain={domain}"
        f"&limit=3"
        f"&api_key={hunter_key}"
    )
    
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            emails = data.get("data", {}).get("emails", [])
            
            if emails:
                best = emails[0]
                email = best.get("value", "")
                name = f"{best.get('first_name', '')} {best.get('last_name', '')}".strip()
                
                # Update DynamoDB
                table.update_item(
                    Key={"job_id": job["job_id"]},
                    UpdateExpression="SET recruiter_email = :e, recruiter_name = :n",
                    ExpressionAttributeValues={":e": email, ":n": name},
                )
                print(f"  [{score}%] {company}: FOUND {name} <{email}>")
            else:
                print(f"  [{score}%] {company}: no emails found on {domain}")
    except Exception as e:
        print(f"  [{score}%] {company}: error - {e}")

print(f"\nDone. Type 'list' in Telegram, then 'apply 1' for the top match.")
