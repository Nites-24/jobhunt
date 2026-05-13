"""
Quick local test: Fetch real jobs from JSearch and display them.
This calls the REAL JSearch API to verify freshness and source links.

Usage:
  1. Set your JSearch API key below
  2. Run: pytest tests/live/test_live_freshness.py
"""

import json
import urllib.request
from datetime import datetime, timezone

# ⚠️ PASTE YOUR JSEARCH API KEY HERE (or set via env var)
import os
API_KEY = os.environ.get("JSEARCH_API_KEY", "YOUR_KEY_HERE")

def fetch_and_display():
    query = "AI Engineer"
    url = (
        f"https://jsearch.p.rapidapi.com/search"
        f"?query={urllib.request.quote(query)}"
        f"&num_pages=1"
        f"&date_posted=today"
        f"&country=us"
    )
    headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
    }

    req = urllib.request.Request(url, headers=headers)

    print(f"\n🔍 Searching: '{query}' | Filter: posted TODAY\n")
    print("=" * 80)

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode("utf-8"))
            jobs = data.get("data", [])

            if not jobs:
                print("No jobs found. Try a different query.")
                return

            now = datetime.now(timezone.utc)

            for i, job in enumerate(jobs, 1):
                title = job.get("job_title", "Unknown")
                company = job.get("employer_name", "Unknown")
                posted_at = job.get("job_posted_at_datetime_utc", "")
                apply_link = job.get("job_apply_link", "N/A")
                is_remote = job.get("job_is_remote", False)
                city = job.get("job_city", "")
                state = job.get("job_state", "")

                # Calculate how long ago it was posted
                hours_ago = "?"
                if posted_at:
                    try:
                        posted_dt = datetime.fromisoformat(posted_at.replace("Z", "+00:00"))
                        delta = now - posted_dt
                        hours_ago = f"{delta.total_seconds() / 3600:.1f}h ago"
                    except Exception:
                        hours_ago = posted_at

                location = f"{city}, {state}" if city else ("Remote" if is_remote else "Unknown")

                print(f"\n{i}. {title}")
                print(f"   🏢 Company:  {company}")
                print(f"   📍 Location: {location}")
                print(f"   ⏰ Posted:   {hours_ago}")
                print(f"   🔗 Source:   {apply_link}")
                print(f"   📋 Job ID:   {job.get('job_id', 'N/A')}")

            print(f"\n{'=' * 80}")
            print(f"✅ Found {len(jobs)} jobs posted today")
            print(f"   Each has a cited source link (apply URL)")
            print(f"   Your pipeline will evaluate these against your resume")

    except Exception as e:
        print(f"❌ Error: {e}")
        if "403" in str(e) or "401" in str(e):
            print("   → Check your API key")


if __name__ == "__main__":
    if API_KEY == "YOUR_KEY_HERE":
        print("⚠️  Set your JSEARCH_API_KEY environment variable first:")
        print("   set JSEARCH_API_KEY=your_key_here")
        print("   pytest tests/live/test_live_freshness.py")
    else:
        fetch_and_display()
