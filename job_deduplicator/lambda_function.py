"""
Job Deduplicator Lambda (v2.0)

Pipeline: SQS (raw-jobs) → Deduplicate via DynamoDB hash → SQS (filtered-jobs)

Fixes:
  - Pass-through ALL fields (stop stripping recruiter_email, salary, etc.)
  - Correct SQS partial batch failure return format
  - Race-condition safe with ConditionalCheckFailedException
"""

import json
import boto3
import hashlib
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "Jobsearch")
FILTERED_QUEUE_URL = os.environ.get("FILTERED_QUEUE_URL")

if not FILTERED_QUEUE_URL:
    raise ValueError("CRITICAL: FILTERED_QUEUE_URL environment variable is not set.")

table = dynamodb.Table(TABLE_NAME)
REQUEUE_DUPLICATE_STATUSES = {
    status.strip()
    for status in os.environ.get("REQUEUE_DUPLICATE_STATUSES", "NEW").split(",")
    if status.strip()
}
REEVALUATE_DUPLICATE_STATUSES = {
    status.strip()
    for status in os.environ.get(
        "REEVALUATE_DUPLICATE_STATUSES",
        "REJECTED,REJECTED_OLD,NO_RECRUITER_EMAIL",
    ).split(",")
    if status.strip()
}
REFRESH_DUPLICATE_STATUSES = {
    status.strip()
    for status in os.environ.get(
        "REFRESH_DUPLICATE_STATUSES",
        "NEW,PENDING_APPROVAL,REJECTED,REJECTED_OLD,NO_RECRUITER_EMAIL",
    ).split(",")
    if status.strip()
}
DEFAULT_EVAL_PROFILE_VERSION = f"2026-05-13-th{os.environ.get('SCORE_THRESHOLD', '80')}"
CURRENT_EVAL_PROFILE_VERSION = os.environ.get("JOB_EVAL_PROFILE_VERSION", DEFAULT_EVAL_PROFILE_VERSION)

# Fields used for deduplication hash (title + company + URL uniquely identifies a job)
HASH_FIELDS = ("title", "company", "url", "apply_url")
REFRESH_FIELDS = (
    "title",
    "company",
    "location",
    "description",
    "url",
    "apply_url",
    "source",
    "posted_at",
    "is_remote",
    "employment_type",
    "recruiter_email",
    "recruiter_name",
    "source_provider",
    "source_origin",
    "source_search_query",
    "source_thread_url",
    "source_comment_id",
    "source_google_link",
    "external_links",
    "employer_website",
)


def _source_key(job: dict) -> tuple[str, str]:
    source = str(job.get("source") or "unknown").strip().lower() or "unknown"
    query = str(job.get("source_search_query") or "").strip() or "-"
    return source, query


def _summarize_source_query_outcomes(results: list[dict]) -> list[dict]:
    summary: dict[tuple[str, str], dict[str, int | str]] = {}
    for result in results:
        source = str(result.get("source") or "unknown")
        query = str(result.get("query") or "-")
        key = (source, query)
        if key not in summary:
            summary[key] = {
                "source": source,
                "query": query,
                "new": 0,
                "requeued": 0,
                "duplicate": 0,
                "failed": 0,
            }
        bucket = summary[key]
        outcome = str(result.get("status") or "failed")
        if outcome in ("duplicate", "duplicate_race"):
            bucket["duplicate"] += 1
        elif outcome in ("new", "requeued", "failed"):
            bucket[outcome] += 1
        else:
            bucket["failed"] += 1
    return [summary[key] for key in sorted(summary)]


def _compute_job_hash(job: dict) -> str:
    """Generate a deterministic hash for deduplication based on core job identity."""
    raw = "|".join(
        str(job.get(field, "")).strip().lower() for field in HASH_FIELDS
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _get_existing_job(job_id: str) -> dict | None:
    """Return the existing job state if this job_id already exists."""
    try:
        response = table.get_item(
            Key={"job_id": job_id},
            ProjectionExpression="job_id, #s, evaluated_at, match_score, evaluation_profile_version",
            ExpressionAttributeNames={"#s": "status"},
        )
        return response.get("Item")
    except Exception as e:
        logger.error("DB_CHECK_ERROR | job_id=%s | %s", job_id, str(e))
        raise


def _should_reevaluate(existing_job: dict) -> bool:
    """Allow one controlled re-score when the evaluation profile version changes."""
    status = existing_job.get("status", "")
    if status not in REEVALUATE_DUPLICATE_STATUSES:
        return False
    stored_version = str(existing_job.get("evaluation_profile_version", "") or "").strip()
    return stored_version != CURRENT_EVAL_PROFILE_VERSION


def _persist_job(job: dict, job_id: str) -> None:
    """
    Insert the job into DynamoDB with status=NEW.
    Uses conditional write to handle race conditions safely.
    """
    now = datetime.now(timezone.utc)
    ttl_timestamp = int((now + timedelta(days=90)).timestamp())

    # Build the item — pass through ALL fields from the job payload
    item = {
        **job,
        "job_id": job_id,
        "status": "NEW",
        "ttl": ttl_timestamp,
        "ingested_at": now.isoformat(),
    }

    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(job_id)",
    )


def _refresh_existing_job(job: dict, job_id: str) -> None:
    """Refresh source/title fields for an existing unfinalized row."""
    updates = []
    attr_names = {}
    attr_values = {":rt": datetime.now(timezone.utc).isoformat()}

    for field in REFRESH_FIELDS:
        value = job.get(field)
        if value in (None, "", []):
            continue
        name_key = f"#f{len(attr_names)}"
        value_key = f":v{len(attr_values)}"
        attr_names[name_key] = field
        attr_values[value_key] = value
        updates.append(f"{name_key} = {value_key}")

    if not updates:
        return

    updates.append("source_refreshed_at = :rt")
    table.update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET " + ", ".join(updates),
        ExpressionAttributeNames=attr_names,
        ExpressionAttributeValues=attr_values,
    )


def _forward_to_filtered_queue(job: dict, job_id: str) -> None:
    """Push the full job payload to the filtered queue for scoring."""
    job_payload = {**job, "job_id": job_id}

    sqs.send_message(
        QueueUrl=FILTERED_QUEUE_URL,
        MessageBody=json.dumps(job_payload),
        MessageAttributes={
            "job_id": {"StringValue": job_id, "DataType": "String"},
            "source": {"StringValue": job.get("source", "unknown"), "DataType": "String"},
        },
    )


def _process_record(record: dict) -> dict:
    """Process a single SQS record: parse, deduplicate, persist, forward."""
    message_id = record["messageId"]

    try:
        raw_job = json.loads(record["body"])
    except json.JSONDecodeError as e:
        logger.error("INVALID_JSON | messageId=%s | error=%s", message_id, str(e))
        # Don't retry malformed messages — they'll never succeed
        return {"status": "invalid", "messageId": message_id}

    # Use the job's own ID if it has one, otherwise compute hash
    original_id = raw_job.get("job_id", "")
    job_id = _compute_job_hash(raw_job) if not original_id else original_id

    # Check for duplicate. If it exists but is still unscored, re-forward it.
    existing_job = _get_existing_job(job_id)
    if existing_job:
        existing_status = existing_job.get("status", "")
        if existing_status in REFRESH_DUPLICATE_STATUSES:
            _refresh_existing_job(raw_job, job_id)
        if existing_status in REQUEUE_DUPLICATE_STATUSES and not existing_job.get("evaluated_at"):
            _forward_to_filtered_queue(raw_job, job_id)
            logger.info(
                "DUPLICATE_REQUEUED | job_id=%s | status=%s | reason=unscored | title=%s",
                job_id, existing_status, raw_job.get("title", "?"),
            )
            return {"status": "requeued", "job_id": job_id}

        if _should_reevaluate(existing_job):
            _forward_to_filtered_queue(raw_job, job_id)
            logger.info(
                "DUPLICATE_REQUEUED | job_id=%s | status=%s | reason=profile_version_changed | old_version=%s | new_version=%s | title=%s",
                job_id,
                existing_status,
                existing_job.get("evaluation_profile_version", ""),
                CURRENT_EVAL_PROFILE_VERSION,
                raw_job.get("title", "?"),
            )
            return {"status": "requeued", "job_id": job_id}

        logger.info(
            "DUPLICATE_SKIP | job_id=%s | status=%s | eval_version=%s | title=%s",
            job_id, existing_status, existing_job.get("evaluation_profile_version", ""), raw_job.get("title", "?"),
        )
        return {"status": "duplicate", "job_id": job_id}

    # Persist to DynamoDB
    try:
        _persist_job(raw_job, job_id)
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        logger.warning("RACE_CONDITION | job_id=%s — already inserted by another invocation", job_id)
        return {"status": "duplicate_race", "job_id": job_id}

    # Forward to scoring pipeline
    _forward_to_filtered_queue(raw_job, job_id)

    logger.info("NEW_JOB_FORWARDED | job_id=%s | title=%s | company=%s",
                job_id, raw_job.get("title", "?"), raw_job.get("company", "?"))
    return {"status": "new", "job_id": job_id}


def lambda_handler(event: dict, context: Any) -> dict:
    """
    SQS batch handler with partial failure support.
    Returns batchItemFailures so SQS only retries the specific records that failed.
    """
    records = event.get("Records", [])
    logger.info("DEDUP_BATCH_START | count=%d", len(records))

    stats = {"new": 0, "requeued": 0, "duplicate": 0, "failed": 0}
    source_query_results = []
    batch_item_failures = []

    for record in records:
        raw_job = None
        try:
            try:
                raw_job = json.loads(record["body"])
            except json.JSONDecodeError:
                raw_job = None

            result = _process_record(record)
            source, query = _source_key(raw_job or {})
            source_query_results.append({
                "status": result["status"],
                "source": source,
                "query": query,
            })
            if result["status"] == "new":
                stats["new"] += 1
            elif result["status"] == "requeued":
                stats["requeued"] += 1
            elif result["status"] in ("duplicate", "duplicate_race"):
                stats["duplicate"] += 1
            else:
                # Invalid JSON — count but don't retry
                stats["failed"] += 1
        except Exception as e:
            logger.error("RECORD_FAILED | messageId=%s | error=%s", record["messageId"], str(e))
            stats["failed"] += 1
            batch_item_failures.append({"itemIdentifier": record["messageId"]})
            source, query = _source_key(raw_job or {})
            source_query_results.append({
                "status": "failed",
                "source": source,
                "query": query,
            })

    logger.info("DEDUP_BATCH_COMPLETE | %s", json.dumps(stats))
    logger.info("DEDUP_YIELD_BY_SOURCE_QUERY | %s", json.dumps(_summarize_source_query_outcomes(source_query_results)))

    # SQS partial batch failure format — only return batchItemFailures
    return {"batchItemFailures": batch_item_failures}
