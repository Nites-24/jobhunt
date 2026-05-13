"""Apply production Lambda environment defaults for the Telegram job pipeline."""

import argparse
import os

import boto3


REGION = "us-east-1"
TARGET_ENV = {
    "job-processor": {
        "SCORE_THRESHOLD": "90",
        "MAX_JOB_AGE_DAYS": "14",
        "STALE_SCORE_CAP": "55",
        "MISSING_DATE_SCORE_CAP": "70",
        "REMOTE_SCORE_CAP": "80",
        "LOW_AI_SIGNAL_SCORE_CAP": "50",
        "MISSING_SOURCE_SCORE_CAP": "40",
    },
    "job-router": {
        "SCORE_THRESHOLD": "90",
        "MAX_JOB_AGE_DAYS": "14",
        "ENABLE_PRACTICE_MODE": "false",
    },
    "job-sender": {
        "ALLOW_REAL_EMAIL_SEND": "false",
    },
}


def _load_env() -> None:
    if not os.path.exists(".env"):
        return
    with open(".env", encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())


def _session() -> boto3.Session:
    _load_env()
    return boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=REGION,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Set production Lambda env vars.")
    parser.add_argument("--apply", action="store_true", help="Actually update Lambda configuration.")
    args = parser.parse_args()

    client = _session().client("lambda")
    for function_name, target in TARGET_ENV.items():
        cfg = client.get_function_configuration(FunctionName=function_name)
        current = cfg.get("Environment", {}).get("Variables", {})
        merged = {**current, **target}
        changed = {key: (current.get(key), value) for key, value in target.items() if current.get(key) != value}
        if not changed:
            print(f"{function_name}: already configured")
            continue
        print(f"{function_name}: changes={changed}")
        if args.apply:
            client.update_function_configuration(
                FunctionName=function_name,
                Environment={"Variables": merged},
            )
            client.get_waiter("function_updated").wait(FunctionName=function_name)
            print(f"{function_name}: updated")

    print("Applied." if args.apply else "Dry-run only. Re-run with --apply to update Lambda env.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
