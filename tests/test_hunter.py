import os
import pytest

if os.environ.get("RUN_LIVE_TESTS") != "1":
    pytest.skip("Live Hunter/AWS smoke test. Set RUN_LIVE_TESTS=1 to run.", allow_module_level=True)

import urllib.request, json, boto3

with open('.env') as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith('#') and '=' in l:
            k, _, v = l.partition('=')
            os.environ.setdefault(k.strip(), v.strip())

s = boto3.Session(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'], region_name='us-east-1')
sm = s.client('secretsmanager')
hunter_key = json.loads(sm.get_secret_value(SecretId='jobsearch/hunterio')['SecretString'])['api_key']

url = f"https://api.hunter.io/v2/domain-search?domain=mitre.org&department=hr&type=personal&limit=3&api_key={hunter_key}"
try:
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=10) as response:
        data = json.loads(response.read().decode('utf-8'))
        for email in data.get('data', {}).get('emails', []):
            print(f"FOUND: {email.get('first_name')} {email.get('last_name')} <{email.get('value')}>")
except Exception as e:
    print('Error:', e)
