import os, boto3

# Load .env
with open(".env") as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ[k.strip()] = v.strip()

# Force new credentials (override any cached ones)
session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="us-east-1",
)

client = session.client("dynamodb")
print(f"Using key: {os.environ['AWS_ACCESS_KEY_ID'][:8]}...")

tables = client.list_tables()["TableNames"]
print(f"DynamoDB Tables: {tables}")

for t in tables:
    desc = client.describe_table(TableName=t)["Table"]
    keys = [k["AttributeName"] for k in desc["KeySchema"]]
    gsis = [g["IndexName"] for g in desc.get("GlobalSecondaryIndexes", [])]
    print(f"  {t}: keys={keys}, GSIs={gsis}")
