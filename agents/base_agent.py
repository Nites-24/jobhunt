"""
Base Agent — Abstract foundation for all specialized agents.

Implements the ReAct (Reasoning + Acting) loop pattern:
  1. OBSERVE: Gather current system state
  2. THINK:   Analyze what needs to happen
  3. ACT:     Execute the corrective action
  4. VERIFY:  Confirm the action succeeded

Every agent inherits this loop and adds domain-specific tools.
"""

import json
import time
import logging
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class AgentStatus(Enum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    DEGRADED = "DEGRADED"  # Partially working


class Severity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    FATAL = "FATAL"


@dataclass
class AgentObservation:
    """What an agent sees about system state."""
    source: str
    timestamp: float = field(default_factory=time.time)
    data: dict = field(default_factory=dict)
    severity: Severity = Severity.INFO
    message: str = ""


@dataclass
class AgentAction:
    """An action an agent wants to take."""
    action_type: str
    target: str
    params: dict = field(default_factory=dict)
    reason: str = ""
    requires_approval: bool = False


@dataclass
class AgentResult:
    """Result of an agent action."""
    success: bool
    action: AgentAction
    output: Any = None
    error: Optional[str] = None
    duration_ms: float = 0
    rollback_action: Optional[AgentAction] = None  # If failure, how to undo


class BaseAgent(ABC):
    """
    Abstract base agent with ReAct loop.
    
    Every agent follows:
      observe() -> think() -> act() -> verify()
      
    Subclasses implement domain-specific logic for each step.
    """

    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self.status = AgentStatus.IDLE
        self.observations: list[AgentObservation] = []
        self.actions_taken: list[AgentResult] = []
        self.logger = logging.getLogger(f"agent.{name}")
        self._start_time: float = 0

    # -----------------------------------------------------------------------
    # ReAct Loop
    # -----------------------------------------------------------------------
    def run(self, context: dict = None) -> dict:
        """
        Execute the full ReAct loop:
          1. Observe system state
          2. Think about what needs to happen
          3. Act on the plan
          4. Verify the actions worked
          
        Returns a summary of what happened.
        """
        self.status = AgentStatus.RUNNING
        self._start_time = time.time()
        context = context or {}
        
        summary = {
            "agent": self.name,
            "started_at": self._start_time,
            "observations": [],
            "actions": [],
            "status": "UNKNOWN",
        }

        try:
            # Step 1: OBSERVE
            self.logger.info("OBSERVE: Gathering system state...")
            observations = self.observe(context)
            self.observations.extend(observations)
            summary["observations"] = [
                {"source": o.source, "severity": o.severity.value, "message": o.message}
                for o in observations
            ]

            # Step 2: THINK
            self.logger.info("THINK: Analyzing %d observations...", len(observations))
            planned_actions = self.think(observations, context)
            
            if not planned_actions:
                self.logger.info("No actions needed — system is healthy.")
                self.status = AgentStatus.SUCCESS
                summary["status"] = "HEALTHY"
                return summary

            self.logger.info("PLAN: %d actions planned", len(planned_actions))

            # Step 3: ACT
            results = []
            for action in planned_actions:
                self.logger.info("ACT: %s on %s — %s", 
                               action.action_type, action.target, action.reason)
                
                if action.requires_approval:
                    self.logger.warning("ACTION_REQUIRES_APPROVAL: %s", action.action_type)
                    results.append(AgentResult(
                        success=False, action=action,
                        error="Requires human approval"
                    ))
                    continue

                start = time.time()
                try:
                    result = self.act(action, context)
                    result.duration_ms = (time.time() - start) * 1000
                    results.append(result)
                    self.actions_taken.append(result)
                except Exception as e:
                    self.logger.error("ACT_FAILED: %s — %s", action.action_type, str(e))
                    results.append(AgentResult(
                        success=False, action=action,
                        error=str(e),
                        duration_ms=(time.time() - start) * 1000,
                    ))

            summary["actions"] = [
                {
                    "type": r.action.action_type,
                    "target": r.action.target,
                    "success": r.success,
                    "error": r.error,
                    "duration_ms": r.duration_ms,
                }
                for r in results
            ]

            # Step 4: VERIFY
            self.logger.info("VERIFY: Checking %d actions...", len(results))
            verification = self.verify(results, context)
            summary["verification"] = verification

            # Determine final status
            failed_count = sum(1 for r in results if not r.success)
            if failed_count == 0:
                self.status = AgentStatus.SUCCESS
                summary["status"] = "SUCCESS"
            elif failed_count < len(results):
                self.status = AgentStatus.DEGRADED
                summary["status"] = "DEGRADED"
            else:
                self.status = AgentStatus.FAILED
                summary["status"] = "FAILED"

        except Exception as e:
            self.logger.error("AGENT_CRASH: %s\n%s", str(e), traceback.format_exc())
            self.status = AgentStatus.FAILED
            summary["status"] = "CRASHED"
            summary["error"] = str(e)

        summary["duration_ms"] = (time.time() - self._start_time) * 1000
        return summary

    # -----------------------------------------------------------------------
    # Abstract methods — each agent implements these
    # -----------------------------------------------------------------------
    @abstractmethod
    def observe(self, context: dict) -> list[AgentObservation]:
        """Gather current system state. Returns observations."""
        ...

    @abstractmethod
    def think(self, observations: list[AgentObservation], context: dict) -> list[AgentAction]:
        """Analyze observations and plan actions. Returns list of actions to take."""
        ...

    @abstractmethod
    def act(self, action: AgentAction, context: dict) -> AgentResult:
        """Execute a single action. Returns result."""
        ...

    @abstractmethod
    def verify(self, results: list[AgentResult], context: dict) -> dict:
        """Verify actions succeeded. Returns verification summary."""
        ...

    # -----------------------------------------------------------------------
    # Utilities
    # -----------------------------------------------------------------------
    def get_report(self) -> str:
        """Generate a human-readable report of what this agent did."""
        lines = [
            f"=== {self.name} Report ===",
            f"Status: {self.status.value}",
            f"Observations: {len(self.observations)}",
            f"Actions: {len(self.actions_taken)}",
        ]
        for r in self.actions_taken:
            status = "✅" if r.success else "❌"
            lines.append(f"  {status} {r.action.action_type} -> {r.action.target}")
            if r.error:
                lines.append(f"     Error: {r.error}")
        return "\n".join(lines)
