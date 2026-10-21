"""Weekly evidence-linked skill demand report from JobHunt's own DynamoDB records.

Counts DISTINCT job descriptions per skill (not occurrences). Required/preferred
classification is heuristic; unknown remains unknown and is never called required.
"""
from __future__ import annotations

import json
import os
import re
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone

import boto3

SKILLS = {
    "Python": r"\bpython\b",
    "SQL": r"\bsql\b|postgres(?:ql)?",
    "FastAPI / APIs": r"\bfastapi\b|\brest(?:ful)? api\b|\bapi development\b",
    "PyTorch": r"\bpytorch\b|\btorch\b",
    "Machine learning": r"\bmachine learning\b|\bml models?\b",
    "LLMs": r"\bllms?\b|large language models?",
    "RAG": r"\brag\b|retrieval.augmented generation",
    "Agents": r"\bagentic\b|\bai agents?\b|\btool.calling\b",
    "LangGraph": r"\blanggraph\b",
    "AWS": r"\baws\b|amazon web services",
    "GCP": r"\bgcp\b|google cloud platform",
    "Azure": r"\bazure\b",
    "Docker": r"\bdocker\b|\bcontainers?\b",
    "Kubernetes": r"\bkubernetes\b|\bk8s\b",
    "CI/CD": r"\bci/cd\b|continuous integration",
    "Monitoring": r"\bobservability\b|\bmonitoring\b|\bmodel drift\b",
    "Security": r"\bsecurity\b|\bauth(?:entication|orization)\b",
    "Transformers": r"\btransformers?\b|\battention mechanism\b",
    "Fine-tuning": r"fine.tun(?:e|ing)|\blora\b|\bqlora\b",
    "Inference optimization": r"\bvllm\b|\bonnx\b|\bquantization\b|\binference optimization\b",
}
GAPS = {
    "Python", "SQL", "FastAPI / APIs", "PyTorch", "Machine learning",
    "AWS", "GCP", "Azure", "Monitoring", "Security", "Transformers",
    "Fine-tuning", "Inference optimization", "Kubernetes",
}
HEADINGS = re.compile(r"(?im)^\s*(?:#{1,5}\s*|\*\*)?(required|minimum|must.have|basic|preferred|nice.to.have|bonus|qualifications|responsibilities|requirements)\b[^\n:]{0,60}:?\s*$")


def extract(text: str) -> dict[str, str]:
    text = str(text or "")[:30000]
    segments = []
    matches = list(HEADINGS.finditer(text))
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        heading = m.group(1).lower()
        label = ("preferred" if any(x in heading for x in ("preferred", "nice", "bonus"))
                 else "required" if any(x in heading for x in ("required", "minimum", "must", "basic"))
                 else "unknown")
        segments.append((text[m.end():end], label))
    if not segments:
        segments = [(text, "unknown")]
    # Include preamble as unknown, never invent requirement strength.
    elif matches[0].start():
        segments.append((text[:matches[0].start()], "unknown"))
    result = {}
    for name, pattern in SKILLS.items():
        found = {level for segment, level in segments if re.search(pattern, segment, re.I)}
        if found:
            result[name] = "required" if "required" in found else "preferred" if "preferred" in found else "unknown"
    return result


def parse_date(value):
    if not value:
        return None
    try:
        if str(value).isdigit():
            return datetime.fromtimestamp(int(value), timezone.utc)
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def eligible(item, cutoff):
    dt = parse_date(item.get("ingested_at")) or parse_date(item.get("evaluated_at"))
    return bool(dt and dt >= cutoff and str(item.get("description") or "").strip())


def analyze(items, days=7, now=None):
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    seen = set()
    counts = {k: Counter() for k in SKILLS}
    examples = {k: [] for k in SKILLS}
    denominator = 0
    for item in items:
        if not eligible(item, cutoff):
            continue
        # Existing job_id is the deduplicated pipeline identity.
        identity = str(item.get("job_id") or item.get("url") or "").strip()
        if not identity or identity in seen:
            continue
        seen.add(identity)
        denominator += 1
        detected = extract(item.get("description", ""))
        for skill, strength in detected.items():
            counts[skill]["total"] += 1
            counts[skill][strength] += 1
            if len(examples[skill]) < 2:
                examples[skill].append({"company": str(item.get("company") or "")[:50], "url": str(item.get("url") or "")[:300]})
    ranking = []
    for skill, c in counts.items():
        if c["total"]:
            ranking.append({"skill": skill, **{x: c[x] for x in ("total", "required", "preferred", "unknown")},
                            "pct": round(100 * c["total"] / denominator, 1),
                            "learning_gap": skill in GAPS, "examples": examples[skill]})
    ranking.sort(key=lambda x: (-x["total"], x["skill"]))
    return {"sample_size": denominator, "days": days, "ranking": ranking}


def load_jobs(table_name):
    table = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION", "us-east-1")).Table(table_name)
    items = []
    args = {"ProjectionExpression": "job_id, company, #u, description, ingested_at, evaluated_at",
            "ExpressionAttributeNames": {"#u": "url"}}
    # Explicitly bounded to prevent accidental expensive full-table scans.
    for _ in range(100):
        page = table.scan(**args)
        items.extend(page.get("Items", []))
        if "LastEvaluatedKey" not in page:
            return items, False
        args["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    return items, True


def format_telegram(result, truncated=False):
    n = result["sample_size"]
    lines = [f"📊 JobHunt: AI Skill Demand — last {result['days']} days",
             f"Analyzed {n} distinct jobs with stored descriptions" + (" (partial scan)" if truncated else ""),
             "Percentages = postings mentioning skill / analyzed postings.",
             "Required/preferred based on section headings; unknown means uncertain.", ""]
    for i, row in enumerate(result["ranking"][:15], 1):
        marker = "🎯" if row["learning_gap"] else "✅"
        lines.append(f"{i}. {marker} {row['skill']}: {row['total']}/{n} ({row['pct']}%) | required {row['required']}, preferred {row['preferred']}, unknown {row['unknown']}")
    if not result["ranking"]:
        lines.append("No skill evidence found; check description coverage and timestamps.")
    lines.extend(["", "🎯 = learning priority; ✅ = previously self-assessed as stronger.",
                  "Counts reflect ingested jobs, NOT the entire job market."])
    return "\n".join(lines)[:3900]


def notify(message):
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    data = urllib.parse.urlencode({"chat_id": chat, "text": message}).encode()
    request = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=data)
    with urllib.request.urlopen(request, timeout=15) as response:
        payload = json.loads(response.read())
    if not payload.get("ok"):
        raise RuntimeError("Telegram sendMessage returned non-ok")


def main():
    items, truncated = load_jobs(os.environ.get("DYNAMODB_TABLE", "Jobsearch"))
    report = analyze(items)
    print(json.dumps(report, indent=2))
    notify(format_telegram(report, truncated=truncated))


if __name__ == "__main__":
    main()
