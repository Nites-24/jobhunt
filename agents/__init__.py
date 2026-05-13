"""
JobHunt Multi-Agent System

A production-grade multi-agent architecture for automated job hunting,
featuring specialized agents that collaborate to ensure system reliability.

Agents:
  - PythonDevAgent:     Code generation, refactoring, and deployment
  - AIEngineerAgent:    LLM prompt engineering, scoring calibration, RAG optimization
  - BackendEngineerAgent: AWS infra, DynamoDB, SQS, Lambda health monitoring
  - QAAgent:            End-to-end testing, regression detection, quality gates
  - OrchestratorAgent:  Coordinates all agents, handles escalation and recovery
"""

__version__ = "1.0.0"
