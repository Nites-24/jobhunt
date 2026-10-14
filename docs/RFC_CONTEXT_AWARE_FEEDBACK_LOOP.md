# 🏛️ Technical Architecture RFC & End-to-End Implementation Plan
## Unifying Telegram Control Plane Interaction, Freshness Harmonization, and Autonomous In-Context Feedback

**Document Version:** 1.0.0-PROD  
**Author:** Founding AI Engineer & Autonomous Systems Lead  
**Audience:** Core Maintainers, Contributors & Engineering Community  
**Classification:** Core System Architecture Specification & Production RFC  
**Target Services:** `job_processor`, `job_router`, `shared`, AWS DynamoDB, Amazon Bedrock  

---

## Executive Summary

This document specifies the end-to-end architecture and implementation roadmap to resolve two critical production breakdowns observed in our autonomous job search pipeline:

1. **The Disconnected Human-in-the-Loop Interaction Model**: Real candidate decisions (e.g., rejecting saturated 300+ applicant roles via Telegram quote-replies or keyboard buttons) were rejected by the system with blocking errors (`"Use apply 1 first"`), breaking the human feedback loop.
2. **Cross-Service Freshness & Guardrail Contradictions**: `job-processor` allowed postings up to 14 days old while `job-router` hid anything older than 1–2 days. This resulted in alerting the candidate about stale, flooded job postings (e.g., 6-day-old Randstad roles) that should have been deterministically blocked before expensive model scoring.

This RFC outlines the architectural root causes, the target end-to-end data flow, the 1-tap interactive rejection system, the repaired in-context learning engine, and the step-by-step verification protocol to bring the system to 100% production readiness.

---

## 1. Problem Statement & Root Cause Analysis

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                PRODUCTION INCIDENT FLOW                                │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 1. Ingestion: 6-day-old Randstad / LinkedIn role ingested.                             │
│ 2. Processor Check: MAX_JOB_AGE_DAYS=14. Role is treated as "fresh" (6 < 14).         │
│ 3. LLM Scoring: Scored 82% via Bedrock Claude 3.5 Sonnet.                              │
│ 4. Alert Dispatched: Plain text SNS alert sent to Telegram: "NEW HIGH MATCH: 82%".     │
│ 5. Human Reality: Candidate opens link; role is saturated with 300+ applicants.        │
│ 6. User Feedback: Candidate quote-replies or taps "Not Applied".                       │
│ 7. System Breakdown: Router fails with "I do not have any manual follow-ups...".      │
│ 8. Siloed Consequence: Feedback is dropped; model never learns from saturation signal. │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### The 6 Core Failures Identified

| # | Component | Root Cause | Production Consequence |
|---|---|---|---|
| **F1** | `job_processor` & `job_router` | **Freshness Contradiction:** `job-processor` defaults to 14 days; `job-router` defaults to 1 day. | Processor scores and alerts candidate on 6-day-old postings that the router immediately flags as stale. |
| **F2** | `job_processor` | **Unlinked SNS Alert Contract:** SNS publishes plain text without `job_id` in `MessageAttributes`. | Router cannot bind incoming alerts to interactive buttons or session state. |
| **F3** | `job_router` | **Zero Quote-Reply Ingestion:** Webhook discards `message.reply_to_message`. | Candidate cannot swipe-to-reply on an alert to give feedback. |
| **F4** | `job_router` | **Rigid State-Gating:** Outcome commands (`applied`, `not applied`) strictly require `MANUAL_APPLY` state. | Fails immediately on `PENDING_APPROVAL` alerts with *"Use apply 1 first"*. |
| **F5** | `job_router` | **High-Friction Rejection:** Tapping "Skip" saves a blank reason (*"Skipped from review button"*). | Candidate cannot easily indicate *why* (saturation, seniority, location) without typing. |
| **F6** | `job_processor` | **Starved In-Context Learning:** `not_applied` was omitted from negative prompt exemplars, and `table.scan(Limit=30)` truncated before filtering. | Claude 3.5 Sonnet never receives negative exemplars, so it cannot learn to downvote saturated roles. |

---

## 2. Target System Architecture

The upgraded architecture establishes an event-driven, closed-loop control plane between the candidate and Amazon Bedrock:

```
                          ┌──────────────────────────┐
                          │   Live Job Postings      │
                          │ (LinkedIn, Randstad, HN) │
                          └─────────────┬────────────┘
                                        │
                                        ▼
                          ┌──────────────────────────┐
                          │       job-ingestor       │
                          └─────────────┬────────────┘
                                        │
                                        ▼
                          ┌──────────────────────────┐
                          │     job-deduplicator     │
                          └─────────────┬────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ job-processor                                                                    │
│                                                                                  │
│ 1. Deterministic Pre-Filter: MAX_JOB_AGE_DAYS=2. Jobs >2d capped at score 55     │
│    ($0 compute, 0ms latency, zero false alerts).                                 │
│ 2. Dynamic In-Context Memory: Retrieves candidate negative & positive exemplars   │
│    from DynamoDB via bounded pagination.                                         │
│ 3. Bedrock Scoring: Claude 3.5 Sonnet forced tool-calling penalizes roles        │
│    sharing traits with rejected exemplars (saturation, stale boards).            │
│ 4. Enriched SNS Publish: MessageAttributes={"job_id": job_id}.                   │
└───────────────────────────────────────┬──────────────────────────────────────────┘
                                        │ (SNS Topic: jobsearch-approvals)
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ job-router (Telegram Control Plane)                                              │
│                                                                                  │
│ 1. Interactive Alert Dispatch: Renders alert with Inline Action Buttons          │
│    [ Why ] [ Draft ] [ Apply ] [ 👎 Not Interested ]                             │
│ 2. Session Binding: Automatically sets alert as ACTIVE_REVIEW_JOB in DynamoDB.   │
│ 3. Quote-Reply Parser: Extracts referenced job from Telegram `reply_to_message`. │
│ 4. 4-Tier Resolution Cascade: Quoted Job -> Active Job -> Followups -> Top Job.  │
│ 5. 1-Tap Rejection Flow: Clicking "Not Interested" presents 6 instant reasons.   │
└───────────────────────────────────────┬──────────────────────────────────────────┘
                                        │
                     ┌──────────────────┴──────────────────┐
                     │                                     │
                     ▼                                     ▼
        ┌─────────────────────────┐           ┌─────────────────────────┐
        │  Telegram Mobile Client │           │     Amazon DynamoDB     │
        │ Candidate taps reason:  │           │  Item updated:          │
        │ [ 👥 Saturated (300+) ] │           │  status = NOT_INTERESTED│
        └────────────┬────────────┘           │  user_feedback = ...    │
                     │                        │  user_feedback_reason   │
                     └───────────────────────>│   = "Saturated (300+)"  │
                                              └────────────┬────────────┘
                                                           │
                                                           │ (Autonomous Feedback Loop)
                                                           ▼
                                              ┌─────────────────────────┐
                                              │ Injected into Bedrock   │
                                              │ prompt on next run!     │
                                              └─────────────────────────┘
```

---

## 3. End-to-End Component Specifications

### Subsystem A: Deterministic Freshness Harmonization
* **File:** [`job_processor/lambda_function.py`](file:///c:/Projects/jobhunt/job_processor/lambda_function.py) & [`job_router/lambda_function.py`](file:///c:/Projects/jobhunt/job_router/lambda_function.py)
* **Invariant:** Both microservices enforce `MAX_JOB_AGE_DAYS = 2` (and `HN_MAX_JOB_AGE_DAYS = 3`).
* **Behavior:**
  * Any job posted > 2 days ago is deterministically capped at `STALE_SCORE_CAP = 55` in Python before Bedrock is invoked.
  * Because 55 < `SCORE_THRESHOLD` (80), stale postings will **never** trigger SNS alerts or reach the candidate's phone.

### Subsystem B: Enriched Alert Notification Contract
* **File:** [`job_processor/lambda_function.py`](file:///c:/Projects/jobhunt/job_processor/lambda_function.py)
* **Implementation:** When publishing to SNS:
  ```python
  sns.publish(
      TopicArn=SNS_TOPIC_ARN,
      Subject=f"Match {score}%: {job.get('title', 'Role')}",
      Message=msg,
      MessageAttributes={
          "job_id": {"DataType": "String", "StringValue": str(job.get("job_id", ""))}
      },
  )
  ```
* **Behavior in Router:** When `job_router` receives this record:
  1. Extracts `job_id` from `MessageAttributes` (with regex URL fallback).
  2. Binds the job as the user's active session review item (`_set_active_review_job(chat_id, job_id)`).
  3. Appends the inline action keyboard:
     ```python
     reply_markup = _build_review_card_keyboard(job_id)
     ```
  4. Sets the reply keyboard to the `review` layout so quick buttons match the incoming alert.

### Subsystem C: 1-Tap Interactive Rejection UX
* **File:** [`job_router/lambda_function.py`](file:///c:/Projects/jobhunt/job_router/lambda_function.py)
* **Implementation:** Add `_build_skip_reasons_keyboard(job_id)`:
  ```python
  def _build_skip_reasons_keyboard(job_id: str | None = None) -> dict:
      token = _get_or_create_action_token(job_id) if job_id else ""
      suffix = f":{token}" if token else ""
      return {
          "inline_keyboard": [
              [{"text": "👥 Saturated (300+ applicants)", "callback_data": f"cmd:skip_r:sat{suffix}"}],
              [{"text": "⏳ Stale Posting (>2 days old)", "callback_data": f"cmd:skip_r:stale{suffix}"}],
              [{"text": "📉 Seniority / YOE Mismatch", "callback_data": f"cmd:skip_r:yoe{suffix}"}],
              [{"text": "🚫 Wrong Tech Stack / Non-AI", "callback_data": f"cmd:skip_r:stack{suffix}"}],
              [{"text": "🏢 Location / Onsite Required", "callback_data": f"cmd:skip_r:loc{suffix}"}],
              [{"text": "⏭ Skip Without Reason", "callback_data": f"cmd:skip_r:none{suffix}"}],
          ]
      }
  ```
* **State Transition:**
  1. User taps `[ Skip ]` on an alert or review card.
  2. Message is edited in-place to display the 6 rejection reasons.
  3. User taps any reason (e.g. `[ 👥 Saturated (300+ applicants) ]`).
  4. Router parses `cmd:skip_r:<code_id>`, translates the code, updates DynamoDB:
     * `status = "NOT_INTERESTED"`
     * `user_feedback = "not_interested"`
     * `user_feedback_reason = "Saturated (300+ applicants)"`
     * `user_feedback_at = timestamp`
  5. The card is cleared, confirmed, and automatically advances to the next available match.

### Subsystem D: Telegram Webhook Quote-Reply (`reply_to_message`) Ingestion
* **File:** [`job_router/lambda_function.py`](file:///c:/Projects/jobhunt/job_router/lambda_function.py)
* **Implementation:**
  * When `message.get("reply_to_message")` is present, `_extract_job_from_reply(reply_msg)` scans the quoted text for:
    1. Canonical URL (`https://www.linkedin.com/jobs/view/...`, Greenhouse, Lever, etc.).
    2. Company & Title pattern (`Role: ... \n Company: ...`).
  * Resolves the exact DynamoDB job row and attaches it as `quoted_job`.
* **Unified Resolution Cascade in `_handle_application_outcome`:**
  ```python
  target_job = (
      quoted_job                                     # Priority 1: Telegram Quote
      or _get_active_review_job(chat_id)             # Priority 2: Active Card
      or _get_first_manual_followup_job()            # Priority 3: Followups Queue
      or _get_first_pending_job()                    # Priority 4: Pending Queue
  )
  ```
  If `target_job` is in `PENDING_APPROVAL`, it gracefully executes `NOT_APPLIED` transition, captures the feedback reason, and responds with confirmation—**never blocking with an error**.

### Subsystem E: Closed-Loop Active Learning Engine
* **File:** [`job_processor/lambda_function.py`](file:///c:/Projects/jobhunt/job_processor/lambda_function.py)
* **Implementation:**
  * **Fix DynamoDB Scan:** Replace `Limit=30` with bounded pagination:
    ```python
    items = []
    scan_kwargs = {
        "FilterExpression": Attr("user_feedback").exists() & Attr("user_feedback_at").exists(),
        "Limit": 50,
    }
    pages_evaluated = 0
    while pages_evaluated < 5 and len(items) < 20:
        resp = table.scan(**scan_kwargs)
        items.extend(resp.get("Items", []))
        pages_evaluated += 1
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    ```
  * **Include `not_applied` in Negative Exemplars:**
    ```python
    elif fb in ("not_interested", "not_applied", "meh") and len(dislikes) < 5:
        entry = f"- {title} at {company}"
        if reason:
            entry += f" (Reason rejected: '{reason}')"
        dislikes.append(entry)
    ```
  * **Prompt Injection into Claude 3.5 Sonnet:**
    The qualitative reason is fed directly into Claude's prompt:
    ```xml
    <candidate_recent_preferences>
      <recently_rejected_roles_to_penalize>
        - AI ML Developer at JPC TECHNO INC (Reason rejected: 'Saturated (300+ applicants)')
        - AI Engineer at Randstad (Reason rejected: 'Stale posting (>2 days old)')
      </recently_rejected_roles_to_penalize>
    </candidate_recent_preferences>
    ```
  * **System Instruction Guidance:**
    Claude is strictly instructed:
    > *"If <candidate_recent_preferences> is provided, heavily penalize postings sharing negative traits with recently rejected roles (especially applicant saturation, stale posting dates, or unwanted staffing aggregators)."*

---

## 4. Production Security, Zero-Trust, and AST Safety Invariants

1. **Zero Candidate Identity Leaks (12-Factor Sovereignty):**
   * All tests and documentation use sanitized mock identities (`Alex Hunter`, `candidate@example.com`).
   * No candidate PII exists in git or DynamoDB update code.
2. **AST Static Bounded Loop Verification:**
   * All DynamoDB pagination loops in Python code have hard iteration ceilings (`max_pages=5`, `max_items=100`), ensuring complete immunity to infinite loops and strict compliance with Lambda's execution limits.
3. **Prompt Injection Boundary Sanitization:**
   * User-supplied rejection strings are stripped of XML closing tags (`</candidate_recent_preferences>`, `</job_to_evaluate>`) and capped at 500 characters to prevent prompt injection escapes.
4. **Outreach Air-Gap Supremacy:**
   * Both `ALLOW_TELEGRAM_EMAIL_SEND` and `ALLOW_REAL_EMAIL_SEND` remain air-gapped (`false` by default). Rejecting or reviewing roles never triggers external network transmissions.

---

## 5. File Modification Map & Implementation Schedule

| Phase | Target File | Action | Key Functions / Constants Modified |
|---|---|---|---|
| **P1** | [`job_processor/lambda_function.py`](file:///c:/Projects/jobhunt/job_processor/lambda_function.py) | **Update** | `MAX_JOB_AGE_DAYS=2`, `_send_sns_notification` (MessageAttributes), `_load_recent_feedback_context` (pagination + `not_applied`), `SYSTEM_INSTRUCTION` |
| **P2** | [`job_router/lambda_function.py`](file:///c:/Projects/jobhunt/job_router/lambda_function.py) | **Update** | `_build_skip_reasons_keyboard`, `_handle_callback_command` (1-tap reason callbacks), `lambda_handler` (SNS keyboard + quote-reply), `_handle_application_outcome` |
| **P3** | [`tools/ops/set_production_config.py`](file:///c:/Projects/jobhunt/tools/ops/set_production_config.py) | **Update** | Sync `job-processor` and `job-router` `MAX_JOB_AGE_DAYS` to `"2"`. |
| **P4** | [`tests/test_router.py`](file:///c:/Projects/jobhunt/tests/test_router.py) | **Add Tests** | `test_skip_renders_1_tap_reason_keyboard`, `test_skip_reason_callback_records_feedback`, `test_reply_to_message_quote_not_applied`, `test_not_applied_on_pending_approval_card` |
| **P5** | [`tests/test_processor.py`](file:///c:/Projects/jobhunt/tests/test_processor.py) | **Add Tests** | `test_guardrail_caps_stale_job_at_2_days`, `test_load_feedback_paginates_dynamodb_scan`, `test_load_feedback_includes_not_applied` |

---

## 6. Verification Protocol & Quality Gates

Before declaring completion, the following gates must be executed:

```powershell
# Gate 1: Targeted Functional Regression Tests
python -m pytest tests/test_processor.py -v
python -m pytest tests/test_router.py -v

# Gate 2: Zero-Trust Security & AST Static Loop Audit
python tools/ops/run_agents.py --security
# Criteria: 0 Critical Issues, 0 Warnings

# Gate 3: Full Offline Automated Test Suite (Moto AWS Mocks)
python -m pytest tests/ -q
# Criteria: 100% Passing (288+ tests, 0 failures, 0 errors)
```

---

## 7. Production Architecture & Release Recommendation

This architecture transitions the repository from an early-stage prototype into an enterprise-grade autonomous agent platform:
* It eliminates false alert noise by synchronizing freshness boundaries across microservices.
* It removes human interaction friction by replacing text commands with 1-tap Telegram interactive keyboards.
* It establishes an active in-context feedback loop that dynamically adapts Claude 3.5 Sonnet's evaluation criteria to candidate preferences without expensive fine-tuning or risky self-modifying code.

**Recommendation:** Proceed with immediate implementation of Phases P1 through P5.
