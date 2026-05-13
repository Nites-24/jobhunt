import os, boto3, json

with open(".env") as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith("#") and "=" in l:
            k, _, v = l.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

s = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="us-east-1",
)
c = s.client("apigatewayv2")

routes = c.get_routes(ApiId="krewl8w46c")["Items"]
print("Routes:")
for r in routes:
    print(f"  {r.get('RouteKey', '?')} -> {r.get('Target', '?')}")

integrations = c.get_integrations(ApiId="krewl8w46c")["Items"]
print("\nIntegrations:")
for i in integrations:
    print(f"  {i.get('IntegrationId', '?')} -> {i.get('IntegrationUri', '?')}")

stages = c.get_stages(ApiId="krewl8w46c")["Items"]
print("\nStages:")
for st in stages:
    print(f"  {st.get('StageName', '?')} auto={st.get('AutoDeploy', '?')}")
