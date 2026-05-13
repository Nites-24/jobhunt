import os
import pytest

if os.environ.get("RUN_LIVE_TESTS") != "1":
    pytest.skip("Live Hacker News smoke test. Set RUN_LIVE_TESTS=1 to run.", allow_module_level=True)

import urllib.request, json, time, re

# Find the latest "Who is hiring?" thread
user_url = 'https://hacker-news.firebaseio.com/v0/user/whoishiring.json'
req = urllib.request.Request(user_url)
with urllib.request.urlopen(req) as resp:
    user_data = json.loads(resp.read().decode())

# Get the most recent submissions by 'whoishiring'
recent_submissions = user_data.get('submitted', [])[:5]

target_item = None
for item_id in recent_submissions:
    item_url = f'https://hacker-news.firebaseio.com/v0/item/{item_id}.json'
    with urllib.request.urlopen(urllib.request.Request(item_url)) as resp:
        item_data = json.loads(resp.read().decode())
        if item_data and "Who is hiring" in item_data.get("title", ""):
            target_item = item_data
            break

if target_item:
    print(f"Found thread: {target_item['title']} (ID: {target_item['id']})")
    kids = target_item.get('kids', [])
    print(f"Total comments: {len(kids)}")
    
    # Check first 50 comments for "AI" or "Machine Learning"
    found_jobs = []
    for kid_id in kids[:150]:
        comment_url = f'https://hacker-news.firebaseio.com/v0/item/{kid_id}.json'
        try:
            with urllib.request.urlopen(urllib.request.Request(comment_url)) as resp:
                comment = json.loads(resp.read().decode())
                text = comment.get('text', '')
                if text and re.search(r'\b(AI|ML|LLM|Machine Learning)\b', text, re.IGNORECASE):
                    found_jobs.append(text)
        except:
            pass
            
    print(f"Found {len(found_jobs)} AI/ML jobs in the first 150 comments.")
    if found_jobs:
        print("\nSample:\n" + found_jobs[0][:200] + "...")
else:
    print("No thread found.")
