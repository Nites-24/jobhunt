"""Compact candidate knowledge graph helpers.

These helpers turn verbose resume/project text into a small structured profile that
can be reused across scorer and sender prompts. The goal is to reduce recurring
prompt tokens while keeping the highest-signal evidence.
"""

from __future__ import annotations

import re
from typing import Iterable


DEFAULT_GRAPH = {
    "candidate_name": "Teja Gopal Reddy Vaka",
    "target_role": "Senior AI Engineer",
    "focus_areas": [
        "RAG pipelines",
        "LLM orchestration",
        "agentic systems",
        "evaluation pipelines",
    ],
    "skills": [
        "Python",
        "PyTorch",
        "AWS",
        "LangChain",
        "Bedrock",
        "vector databases",
    ],
    "projects": [
        {
            "name": "RetireIQ",
            "summary": "multi-step financial reasoning",
            "url": "https://huggingface.co/spaces/vakaTeja/RetireIQ",
        },
        {
            "name": "Rivulet",
            "summary": "tool orchestration with human oversight",
            "url": "https://rivulet.info",
        },
    ],
    "evidence": [
        "33% GPU cost reduction",
        "87% autonomous ticket resolution",
    ],
    "preferences": [
        "San Francisco Bay Area",
        "onsite or hybrid",
        "open to relocate",
    ],
}

SKILL_PATTERNS = [
    ("Python", r"\bpython\b"),
    ("PyTorch", r"\bpytorch\b"),
    ("TensorFlow", r"\btensorflow\b"),
    ("AWS", r"\baws\b|\bbedrock\b"),
    ("LangChain", r"\blangchain\b"),
    ("RAG", r"\brag\b|retrieval[- ]augmented"),
    ("LLMs", r"\bllm\b|language model|generative ai"),
    ("vector databases", r"vector db|vector database|pinecone|weaviate|faiss|chroma"),
    ("evaluation", r"\beval\b|evaluation|benchmark"),
    ("agentic systems", r"agentic|autonomous agent|multi-agent"),
]

FOCUS_PATTERNS = [
    ("RAG pipelines", r"\brag\b|retrieval[- ]augmented"),
    ("LLM orchestration", r"\bllm\b|language model|orchestration"),
    ("agentic systems", r"agentic|autonomous agent|multi-agent"),
    ("evaluation pipelines", r"\beval\b|evaluation|benchmark"),
    ("production inference", r"inference|serving|latency"),
]

PROJECT_PATTERNS = [
    (
        "RetireIQ",
        r"\bretireiq\b",
        "multi-step financial reasoning",
        "https://huggingface.co/spaces/vakaTeja/RetireIQ",
    ),
    (
        "Rivulet",
        r"\brivulet\b",
        "tool orchestration with human oversight",
        "https://rivulet.info",
    ),
]

PREFERENCE_PATTERNS = [
    ("San Francisco Bay Area", r"san francisco|bay area|south san francisco|ssf"),
    ("onsite or hybrid", r"onsite|on-site|hybrid"),
    ("open to relocate", r"relocat"),
]


def _normalized_text(parts: Iterable[str]) -> str:
    text = " ".join(part for part in parts if part)
    text = text.replace("\u2014", " ").replace("\u2013", " ")
    return re.sub(r"\s+", " ", text).strip()


def _dedupe_keep_order(values: Iterable[str]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        key = value.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(value.strip())
    return result


def _extract_matches(text: str, patterns: list[tuple[str, str]]) -> list[str]:
    matches = [label for label, pattern in patterns if re.search(pattern, text, re.IGNORECASE)]
    return _dedupe_keep_order(matches)


def _extract_evidence(text: str) -> list[str]:
    evidence = []
    for match in re.finditer(r"\b\d{1,3}%[^.:\n]{0,80}", text):
        snippet = match.group(0).strip(" ,;")
        if len(snippet) > 8:
            evidence.append(snippet)
    if "gpu cost reduction" in text.lower() and not any("gpu" in item.lower() for item in evidence):
        evidence.append("33% GPU cost reduction")
    if "ticket resolution" in text.lower() and not any("ticket" in item.lower() for item in evidence):
        evidence.append("87% autonomous ticket resolution")
    return _dedupe_keep_order(evidence)[:5]


def _extract_projects(text: str) -> list[dict]:
    projects = []
    for name, pattern, summary, url in PROJECT_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            projects.append({"name": name, "summary": summary, "url": url})
    if not projects:
        projects = list(DEFAULT_GRAPH["projects"])
    return projects[:4]


def build_candidate_graph(*text_parts: str) -> dict:
    text = _normalized_text(text_parts)
    if not text:
        return dict(DEFAULT_GRAPH)

    graph = {
        "candidate_name": DEFAULT_GRAPH["candidate_name"],
        "target_role": DEFAULT_GRAPH["target_role"],
        "focus_areas": _extract_matches(text, FOCUS_PATTERNS) or list(DEFAULT_GRAPH["focus_areas"]),
        "skills": _extract_matches(text, SKILL_PATTERNS) or list(DEFAULT_GRAPH["skills"]),
        "projects": _extract_projects(text),
        "evidence": _extract_evidence(text) or list(DEFAULT_GRAPH["evidence"]),
        "preferences": _extract_matches(text, PREFERENCE_PATTERNS) or list(DEFAULT_GRAPH["preferences"]),
    }
    return graph


def render_candidate_graph(graph: dict) -> str:
    lines = [
        "<candidate_graph>",
        f"candidate: {graph.get('candidate_name', DEFAULT_GRAPH['candidate_name'])}",
        f"target_role: {graph.get('target_role', DEFAULT_GRAPH['target_role'])}",
        "focus: " + "; ".join(graph.get("focus_areas", DEFAULT_GRAPH["focus_areas"])[:5]),
        "skills: " + "; ".join(graph.get("skills", DEFAULT_GRAPH["skills"])[:8]),
        "evidence: " + "; ".join(graph.get("evidence", DEFAULT_GRAPH["evidence"])[:4]),
        "preferences: " + "; ".join(graph.get("preferences", DEFAULT_GRAPH["preferences"])[:4]),
        "projects:",
    ]
    for project in graph.get("projects", DEFAULT_GRAPH["projects"])[:4]:
        lines.append(
            f"- {project.get('name', 'Project')} | "
            f"{project.get('summary', '')} | {project.get('url', '')}"
        )
    lines.append("</candidate_graph>")
    return "\n".join(lines)
