"""
Candidate Knowledge Graph & Token-Efficient Memory Engine (knowledge_graph.py)

Role & Architectural Placement:
  Provides a structured, token-compact candidate memory representation shared across
  evaluation (`job-processor`) and outreach drafting (`job-sender`).

Why This Exists:
  Raw candidate resumes consume 1,500 - 3,000 tokens per LLM invocation, driving up
  inference costs and latency. The Knowledge Graph extracts high-signal, verifiable candidate
  facts (skills, domain focus areas, real projects, verified quantitative metrics) into a
  dense representation consuming < 150 prompt tokens.

Core Design Invariants:
  1. Identity Decoupling:
     Candidate identity is decoupled from git-tracked code. All personal identifiers are injected
     at runtime via environment variables (CANDIDATE_NAME, TARGET_ROLE, RETIREIQ_URL).
  2. Grounded Evidence & Anti-Hallucination:
     Outreach generators and scoring prompts are strictly constrained to facts present in the graph:
     - Real projects: RetireIQ, Rivulet
     - Quantitative evidence: 33% GPU cost reduction, 87% autonomous ticket resolution
     - Work mode preferences: Open to remote, hybrid, onsite, any location.
  3. Deterministic Extraction:
     Uses regex pattern matching against candidate documents (`SKILL_PATTERNS`, `FOCUS_PATTERNS`,
     `PROJECT_PATTERNS`) to produce consistent graph representations.

Primary Exports:
  - `build_candidate_graph(...)`: Extracts structured profile dictionary from text or defaults.
  - `render_candidate_graph(...)`: Renders compact, prompt-ready markdown for Bedrock system prompts.
"""

from __future__ import annotations

import os
import re
from typing import Iterable


DEFAULT_GRAPH = {
    "candidate_name": os.environ.get("CANDIDATE_NAME", "AI Candidate").strip() or "AI Candidate",
    "target_role": os.environ.get("TARGET_ROLE", "AI Engineer").strip() or "AI Engineer",
    "focus_areas": [
        "RAG pipelines",
        "LLM orchestration",
        "agentic systems",
        "evaluation pipelines",
        "inference optimization",
        "multimodal AI",
    ],
    "skills": [
        "Python",
        "PyTorch",
        "TensorFlow",
        "Hugging Face",
        "BERT",
        "LoRA Fine-tuning",
        "ONNX",
        "INT8 Quantization",
        "LangChain",
        "LangGraph",
        "RAG",
        "vector databases",
        "Reranking",
        "RAGAS evaluation",
        "FastAPI",
        "Docker",
        "Airflow",
        "SageMaker",
        "AWS",
        "Bedrock",
        "GCP",
        "Apache Spark",
        "PySpark",
        "SQL",
    ],
    "projects": [
        {
            "name": "RetireIQ",
            "summary": "retirement planning and financial decision support app",
            "url": os.environ.get("RETIREIQ_URL", "https://huggingface.co/spaces/demo/RetireIQ"),
        },
        {
            "name": "Rivulet",
            "summary": "personal life management app",
            "url": "https://rivulet.info",
        },
    ],
    "evidence": [
        "33% GPU cost reduction",
        "87% autonomous ticket resolution",
    ],
    "preferences": [
        "United States only (US remote, hybrid, or onsite)",
        "open to full-time or contract roles (W2 contract or contract-to-hire)",
        "open to remote, hybrid, or onsite",
        "open to relocate",
    ],
}

SKILL_PATTERNS = [
    ("Python", r"\bpython\b"),
    ("PyTorch", r"\bpytorch\b"),
    ("TensorFlow", r"\btensorflow\b"),
    ("Hugging Face", r"hugging\s*face|transformers"),
    ("BERT", r"\bbert\b"),
    ("LoRA Fine-tuning", r"\blora\b|fine-tuning|peft"),
    ("ONNX", r"\bonnx\b"),
    ("INT8 Quantization", r"quantization|int8|awq|bitsandbytes"),
    ("AWS", r"\baws\b|\bbedrock\b"),
    ("LangChain", r"\blangchain\b"),
    ("LangGraph", r"langgraph"),
    ("RAG", r"\brag\b|retrieval[- ]augmented"),
    ("LLMs", r"\bllm\b|language model|generative ai"),
    ("vector databases", r"vector db|vector database|pinecone|weaviate|faiss|chroma"),
    ("Reranking", r"rerank|cohere\s*rerank|cross-encoder"),
    ("RAGAS evaluation", r"ragas|faithfulness|answer\s*relevancy"),
    ("FastAPI", r"fastapi"),
    ("Docker", r"docker|containerization"),
    ("Airflow", r"airflow"),
    ("SageMaker", r"sagemaker"),
    ("Bedrock", r"bedrock"),
    ("GCP", r"\bgcp\b|google\s*cloud|bigquery|dataflow"),
    ("Apache Spark", r"spark|pyspark"),
    ("SQL", r"\bsql\b|postgres|postgresql"),
    ("evaluation", r"\beval\b|evaluation|benchmark"),
    ("agentic systems", r"agentic|autonomous agent|multi-agent"),
]

FOCUS_PATTERNS = [
    ("RAG pipelines", r"\brag\b|retrieval[- ]augmented"),
    ("LLM orchestration", r"\bllm\b|language model|orchestration"),
    ("agentic systems", r"agentic|autonomous agent|multi-agent"),
    ("evaluation pipelines", r"\beval\b|evaluation|benchmark|ragas"),
    ("inference optimization", r"inference|serving|latency|quantization|onnx"),
    ("multimodal AI", r"multimodal|vision[- ]language|vlm"),
]

PROJECT_PATTERNS = [
    (
        "RetireIQ",
        r"\bretireiq\b",
        "retirement planning and financial decision support app",
        os.environ.get("RETIREIQ_URL", "https://huggingface.co/spaces/demo/RetireIQ"),
    ),
    (
        "Rivulet",
        r"\brivulet\b",
        "personal life management app",
        "https://rivulet.info",
    ),
]

PREFERENCE_PATTERNS = [
    ("United States only (US remote, hybrid, or onsite)", r"united states|usa|\bus\b"),
    ("open to visa sponsorship", r"visa|sponsorship"),
    ("open to full-time or contract roles (W2 contract or contract-to-hire)", r"contract|w2|c2c|contract-to-hire|full-time|permanent"),
    ("open to remote, hybrid, or onsite", r"remote|onsite|on-site|hybrid"),
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


def build_candidate_graph(
    *text_parts: str,
    candidate_name: str | None = None,
    target_role: str | None = None,
) -> dict:
    text = _normalized_text(text_parts)
    effective_candidate = candidate_name or os.environ.get("CANDIDATE_NAME", "").strip()
    if not effective_candidate:
        name_match = re.match(r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(?:AI|ML|Software|Data|Lead|Senior)\b", text)
        if name_match:
            effective_candidate = name_match.group(1).strip()
        else:
            effective_candidate = DEFAULT_GRAPH["candidate_name"]

    effective_role = target_role or os.environ.get("TARGET_ROLE", "").strip()
    if not effective_role:
        role_match = re.search(r"\b(AI\s+Engineer|ML\s+Engineer|Machine\s+Learning\s+Engineer|Lead\s+ML\s+Architect)\b", text, re.IGNORECASE)
        if role_match:
            effective_role = role_match.group(1).strip()
        else:
            effective_role = DEFAULT_GRAPH["target_role"]

    if not text:
        base = dict(DEFAULT_GRAPH)
        base["candidate_name"] = effective_candidate
        base["target_role"] = effective_role
        return base

    graph = {
        "candidate_name": effective_candidate,
        "target_role": effective_role,
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
        "focus: " + "; ".join(graph.get("focus_areas", DEFAULT_GRAPH["focus_areas"])[:6]),
        "skills: " + "; ".join(graph.get("skills", DEFAULT_GRAPH["skills"])[:20]),
        "evidence: " + "; ".join(graph.get("evidence", DEFAULT_GRAPH["evidence"])[:4]),
        "preferences: " + "; ".join(graph.get("preferences", DEFAULT_GRAPH["preferences"])[:5]),
        "projects:",
    ]
    for project in graph.get("projects", DEFAULT_GRAPH["projects"])[:4]:
        lines.append(
            f"- {project.get('name', 'Project')} | "
            f"{project.get('summary', '')} | {project.get('url', '')}"
        )
    lines.append("</candidate_graph>")
    return "\n".join(lines)
