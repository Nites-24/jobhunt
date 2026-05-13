import os, boto3

with open('.env') as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith('#') and '=' in l:
            k, _, v = l.partition('=')
            os.environ.setdefault(k.strip(), v.strip())

s = boto3.Session(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'], region_name='us-east-1')
events = s.client('events')
lambda_client = s.client('lambda')

rule_name = 'job-search-daily-trigger'

# 1. Create EventBridge Rule (e.g., every 3 hours)
print(f"Creating EventBridge rule: {rule_name}")
response = events.put_rule(
    Name=rule_name,
    ScheduleExpression='rate(3 hours)',
    State='ENABLED',
    Description='Automatically trigger the job-ingestor every 3 hours'
)
rule_arn = response['RuleArn']

# 2. Get Lambda ARN
lambda_info = lambda_client.get_function(FunctionName='job-ingestor')
lambda_arn = lambda_info['Configuration']['FunctionArn']

# 3. Add permission to Lambda to allow EventBridge to trigger it
try:
    lambda_client.add_permission(
        FunctionName='job-ingestor',
        StatementId='EventBridgeInvokePermission',
        Action='lambda:InvokeFunction',
        Principal='events.amazonaws.com',
        SourceArn=rule_arn,
    )
    print("Added permission for EventBridge to invoke Lambda.")
except lambda_client.exceptions.ResourceConflictException:
    print("Permission already exists.")

# 4. Add Lambda as target for the Rule
events.put_targets(
    Rule=rule_name,
    Targets=[
        {
            'Id': '1',
            'Arn': lambda_arn,
            'Input': '{"source": "eventbridge.cron"}'
        }
    ]
)
print("Added job-ingestor as target for the rule.")
print("SUCCESS: Pipeline will now run automatically every 6 hours!")
