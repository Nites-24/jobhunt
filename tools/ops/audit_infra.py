"""Full infrastructure audit: Lambdas, SQS, SNS, triggers, and wiring."""

import os, json, boto3
from datetime import datetime

with open(".env") as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith("#") and "=" in l:
            k, _, v = l.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="us-east-1",
)

lambda_client = session.client("lambda")
sqs_client = session.client("sqs")
sns_client = session.client("sns")
s3_client = session.client("s3")
dynamodb = session.client("dynamodb")


def section(title):
    print(f"\n{'='*65}")
    print(f"  {title}")
    print(f"{'='*65}")


# =====================================================================
# 1. LAMBDA FUNCTIONS
# =====================================================================
section("1. LAMBDA FUNCTIONS (code deployment status)")

for func in ["job-ingestor", "job-deduplicator", "job-processor", "job-router", "job-sender"]:
    cfg = lambda_client.get_function_configuration(FunctionName=func)
    last_mod = cfg["LastModified"]
    code_size = cfg["CodeSize"]
    runtime = cfg["Runtime"]
    timeout = cfg["Timeout"]
    memory = cfg["MemorySize"]
    handler = cfg["Handler"]
    env_vars = cfg.get("Environment", {}).get("Variables", {})

    print(f"\n  {func}")
    print(f"    Last Updated:  {last_mod}")
    print(f"    Code Size:     {code_size} bytes")
    print(f"    Runtime:       {runtime}")
    print(f"    Handler:       {handler}")
    print(f"    Timeout:       {timeout}s")
    print(f"    Memory:        {memory}MB")
    print(f"    Env Vars:      {len(env_vars)}")
    for k, v in sorted(env_vars.items()):
        display_v = v[:40] + "..." if len(v) > 40 else v
        print(f"      {k} = {display_v}")


# =====================================================================
# 2. SQS QUEUES
# =====================================================================
section("2. SQS QUEUES")

queues = sqs_client.list_queues(QueueNamePrefix="job-")
queue_urls = queues.get("QueueUrls", [])

if not queue_urls:
    print("\n  No SQS queues found with prefix 'job-'")
else:
    for url in queue_urls:
        name = url.split("/")[-1]
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=["All"],
        )["Attributes"]

        msg_count = attrs.get("ApproximateNumberOfMessages", "0")
        inflight = attrs.get("ApproximateNumberOfMessagesNotVisible", "0")
        dlq = attrs.get("RedrivePolicy", "None")

        print(f"\n  {name}")
        print(f"    URL:            {url}")
        print(f"    Messages:       {msg_count} (available) | {inflight} (in-flight)")
        print(f"    DLQ:            {dlq}")


# =====================================================================
# 3. SQS TRIGGERS (Event Source Mappings)
# =====================================================================
section("3. SQS -> LAMBDA TRIGGERS (Event Source Mappings)")

for func in ["job-deduplicator", "job-processor", "job-sender"]:
    try:
        mappings = lambda_client.list_event_source_mappings(
            FunctionName=func,
        )["EventSourceMappings"]

        if mappings:
            for m in mappings:
                source = m.get("EventSourceArn", "?")
                state = m.get("State", "?")
                batch = m.get("BatchSize", "?")
                uuid = m.get("UUID", "?")
                print(f"\n  {source.split(':')[-1]}")
                print(f"    -> {func}")
                print(f"    State:      {state}")
                print(f"    Batch Size: {batch}")
                print(f"    UUID:       {uuid}")
        else:
            print(f"\n  {func}: NO SQS triggers configured")
    except Exception as e:
        print(f"\n  {func}: Error - {e}")


# =====================================================================
# 4. SNS TOPICS
# =====================================================================
section("4. SNS TOPICS")

topics = sns_client.list_topics()["Topics"]
job_topics = [t for t in topics if "job" in t["TopicArn"].lower()]

if not job_topics:
    print("\n  No SNS topics found with 'job' in name")
else:
    for topic in job_topics:
        arn = topic["TopicArn"]
        name = arn.split(":")[-1]
        subs = sns_client.list_subscriptions_by_topic(TopicArn=arn)["Subscriptions"]

        print(f"\n  {name}")
        print(f"    ARN: {arn}")
        print(f"    Subscriptions: {len(subs)}")
        for sub in subs:
            proto = sub.get("Protocol", "?")
            endpoint = sub.get("Endpoint", "?")
            print(f"      - [{proto}] {endpoint}")


# =====================================================================
# 5. S3 BUCKET
# =====================================================================
section("5. S3 BUCKET (job-search-pipeline-memory)")

try:
    objects = s3_client.list_objects_v2(
        Bucket="job-search-pipeline-memory",
        MaxKeys=20,
    )
    items = objects.get("Contents", [])
    print(f"\n  Files ({len(items)}):")
    for obj in items:
        key = obj["Key"]
        size = obj["Size"]
        print(f"    {key} ({size} bytes)")
except Exception as e:
    print(f"\n  Error: {e}")


# =====================================================================
# 6. DYNAMODB TABLE
# =====================================================================
section("6. DYNAMODB TABLE")

desc = dynamodb.describe_table(TableName="Jobsearch")["Table"]
item_count = desc.get("ItemCount", 0)
gsis = desc.get("GlobalSecondaryIndexes", [])

print(f"\n  Table: Jobsearch")
print(f"  Items:  {item_count}")
print(f"  Status: {desc['TableStatus']}")
print(f"  GSIs:   {[g['IndexName'] for g in gsis]}")

# Count by status
table = session.resource("dynamodb").Table("Jobsearch")
scan = table.scan(ProjectionExpression="#s", ExpressionAttributeNames={"#s": "status"})
status_counts = {}
for item in scan.get("Items", []):
    s = item.get("status", "UNKNOWN")
    status_counts[s] = status_counts.get(s, 0) + 1

print(f"\n  Jobs by status:")
for s, c in sorted(status_counts.items()):
    print(f"    {s}: {c}")


# =====================================================================
# 7. PIPELINE WIRING SUMMARY
# =====================================================================
section("7. PIPELINE WIRING SUMMARY")

print("""
  Telegram "search jobs"
       |
       v
  [API Gateway] --> [job-router] --> invokes [job-ingestor]
                                          |
                                          v  (sends to SQS)
                                     [job-raw-queue]
                                          |
                                          v  (SQS trigger)
                                     [job-deduplicator]
                                          |
                                          v  (sends to SQS)
                                     [job-filtered-queue]
                                          |
                                          v  (SQS trigger)
                                     [job-processor] (Bedrock AI scoring)
                                          |
                                          v  (SNS notification)
                                     [Telegram: "New 88% match!"]
                                          |
  Telegram "apply"                        v
       |                             [job-router]
       v                                  |
  [job-router] --> invokes [job-sender]   |
                       |                  |
                       v                  |
                  [Bedrock: generate email]
                       |
                       v
                  [SMTP: send to recruiter]
""")

print("=" * 65)
print("  AUDIT COMPLETE")
print("=" * 65)
