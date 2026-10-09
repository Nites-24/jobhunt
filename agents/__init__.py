"""
JobHunt Multi-Agent System

A production-grade multi-agent architecture for automated job hunting,
featuring specialized agents that collaborate to ensure system reliability.

Agents:
  - PythonDevAgent:        Code generation, refactoring, and deployment
  - AIEngineerAgent:       LLM prompt engineering, scoring calibration, RAG optimization
  - BackendEngineerAgent:  AWS infra, DynamoDB, SQS, Lambda health monitoring
  - SecurityAuditorAgent:  Identity redaction, secret auditing, AST zero-loop verification
  - QAAgent:               End-to-end testing, regression detection, quality gates
  - OrchestratorAgent:     Coordinates all agents, handles escalation and recovery
"""

from .base_agent import BaseAgent
from .qa_agent import QAAgent
from .python_dev_agent import PythonDevAgent
from .security_auditor_agent import SecurityAuditorAgent
from .orchestrator_agent import OrchestratorAgent

__all__ = [
    "BaseAgent",
    "QAAgent",
    "PythonDevAgent",
    "SecurityAuditorAgent",
    "OrchestratorAgent",
]
__version__ = "1.0.0"
