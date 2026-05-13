import os, boto3
with open('.env') as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith('#') and '=' in l:
            k, _, v = l.partition('=')
            os.environ.setdefault(k.strip(), v.strip())

table = boto3.Session(region_name='us-east-1').resource('dynamodb').Table('Jobsearch')
items = table.scan()['Items']
pending = [i for i in items if i.get('status') == 'PENDING_APPROVAL']
print(f'Currently PENDING_APPROVAL: {len(pending)}')
for j in pending[:5]:
    print(f"  - {j.get('title', '?')} | {j.get('url', '?')}")
