import os
import pytest

if os.environ.get("RUN_LIVE_TESTS") != "1":
    pytest.skip("Live Remotive smoke test. Set RUN_LIVE_TESTS=1 to run.", allow_module_level=True)

import urllib.request, json
url = 'https://remotive.com/api/remote-jobs?search=AI%20Engineer'
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
try:
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode())
        jobs = data.get('jobs', [])
        print(f'Found {len(jobs)} jobs.')
        for j in jobs[:2]:
            print(f"  - {j.get('title')} at {j.get('company_name')} | {j.get('publication_date')}")
except Exception as e:
    print('Error:', e)
