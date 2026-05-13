# Fix Autonomous Agent Job Ingestion & Scoring Pipeline - 2026-05-12

## Root Cause Evaluation

1. **Source Discrepancy & UX Hallucination:** The Telegram router claims the agent is "Fetching new jobs from Hacker News (Zero Ghost Jobs)." However, the `job_ingestor` silently appends results from the Remotive API. Remotive exclusively serves remote jobs, which contradicts your goal of finding high-signal onsite roles (like the Jeeva.AI job in South SF).
2. **Zero Freshness Filtering:** The Remotive API query (`?search=AI%20Engineer`) does not enforce date bounds. It blindly returns the top historical keyword matches. This is why the agent surfaced 1-month-old jobs (April 16, April 21) while claiming they were "new."
3. **Flawed Evaluator Prompt:** The LLM prompt in `job_processor` successfully filters out non-AI jobs, but it lacks criteria to penalize stale posting dates or remote-only constraints. It blindly scored a 1-month-old remote job at 92% because the "tech stack" matched.
4. **HN Ingestion Bottleneck:** The Hacker News ingestor is hardcoded to only check the top 100 comments (`kids[:100]`). A typical "Who is Hiring" thread has 600-900 comments. The agent is completely blind to 80%+ of the high-signal, fresh onsite jobs posted on HN.

## Proposed Changes

### `job_ingestor`
- Delete `fetch_remotive_jobs()` and remove it from the main handler.
- Increase the Hacker News parsing limit from `kids[:100]` to `kids[:800]` to scan the entire thread.
- Enhance the HN parser to better extract locations (like SF, San Francisco, Onsite) alongside the AI keywords.

### `job_processor`
- Update the `SYSTEM_INSTRUCTION` for the Bedrock Converse API to strictly evaluate freshness and location.
- Add a new scoring criterion to severely penalize (or auto-reject) jobs older than 14 days.
- Include logic in the prompt to boost Onsite/Hybrid roles and penalize purely remote roles if they don't match the candidate's preferred locations.

### `job_router`
- Update Telegram messages to accurately reflect the true pipeline state. (The current message actually perfectly describes what we *want* it to do, so removing Remotive will align the code with the UX).

## Implementation Time Plan

| Task | Estimated Time |
| :--- | :--- |
| **Phase 1:** Update `job_ingestor` (Remove Remotive, expand HN scan) | 30 mins |
| **Phase 2:** Update `job_processor` (Prompt tuning for date/location) | 30 mins |
| **Phase 3:** Update `job_router` (Verify Telegram messaging) | 15 mins |
| **Phase 4:** End-to-End Testing & Verification | 30 mins |
| **Total Estimated Time** | **~1 hour 45 mins** |

## Verification Plan
### Automated Tests
- Run `pytest tests/live/test_ingestor_live.py` to ensure it successfully fetches hundreds of fresh jobs from HN and zero from Remotive.
- Run `pytest tests/test_processor.py` to verify the LLM correctly down-scores old or irrelevant jobs.

### Manual Verification
- Trigger a `search jobs` command via Telegram and verify that only highly relevant, fresh jobs (like HN posts from the last few days) are returned in the `list` command.
