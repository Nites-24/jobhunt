"""Fix job-router IAM: add lambda:InvokeFunction permission and check env vars."""

import os, json, boto3

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

iam = session.client("iam")
lambda_client = session.client("lambda")

# 1. Check job-router config
print("=" * 60)
print("  CHECKING JOB-ROUTER LAMBDA CONFIG")
print("=" * 60)

config = lambda_client.get_function_configuration(FunctionName="job-router")
role_arn = config["Role"]
role_name = role_arn.split("/")[-1]
env_vars = config.get("Environment", {}).get("Variables", {})

print(f"\n  Role: {role_name}")
print(f"  Runtime: {config['Runtime']}")
print(f"  Timeout: {config['Timeout']}s")
print(f"  Memory: {config['MemorySize']}MB")
print(f"\n  Environment variables:")
for k, v in sorted(env_vars.items()):
    print(f"    {k} = {v}")

# 2. Check current policies on the role
print(f"\n  Current policies on {role_name}:")
attached = iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]
for p in attached:
    print(f"    - {p['PolicyName']}")

inline = iam.list_role_policies(RoleName=role_name)["PolicyNames"]
for p in inline:
    print(f"    - [inline] {p}")
    doc = iam.get_role_policy(RoleName=role_name, PolicyName=p)["PolicyDocument"]
    print(f"      {json.dumps(doc, indent=6)}")

# 3. Add lambda:InvokeFunction permission
print(f"\n  Adding lambda:InvokeFunction permission...")
policy_doc = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "lambda:InvokeFunction",
            "Resource": [
                f"arn:aws:lambda:us-east-1:827949090396:function:job-ingestor",
                f"arn:aws:lambda:us-east-1:827949090396:function:job-sender",
            ],
        }
    ],
}

try:
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="invoke-pipeline-lambdas",
        PolicyDocument=json.dumps(policy_doc),
    )
    print(f"  Added 'invoke-pipeline-lambdas' inline policy!")
except Exception as e:
    print(f"  Error: {e}")

# 4. Check all Lambda env vars
print(f"\n{'='*60}")
print("  ALL LAMBDA ENV VARS")
print(f"{'='*60}")

for func in ["job-ingestor", "job-deduplicator", "job-processor", "job-router", "job-sender"]:
    cfg = lambda_client.get_function_configuration(FunctionName=func)
    env = cfg.get("Environment", {}).get("Variables", {})
    print(f"\n  {func}:")
    if env:
        for k, v in sorted(env.items()):
            print(f"    {k} = {v}")
    else:
        print(f"    (no env vars set)")

print(f"\n{'='*60}")
print("  DONE")
print(f"{'='*60}")
