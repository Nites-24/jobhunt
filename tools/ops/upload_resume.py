"""
Resume Upload & S3 Synchronizer (upload_resume.py)

Role & Operational Context:
  Synchronizes candidate resume files into the private AWS S3 memory bucket
  (`job-search-pipeline-memory`).
  
  The JobHunt autonomous pipeline uses TWO distinct resume formats:
  1. Plain-text resume (`data/resume/resume.txt`):
     Read by `job-processor` to feed candidate experience into Claude / Bedrock
     for deterministic scoring and matching against incoming job descriptions.
  2. PDF resume (`data/resume/resume.pdf`):
     Read by `job-sender` to attach formatted PDF resumes to cold outreach emails.

Usage:
  # Upload both at once:
  python tools/ops/upload_resume.py --text path/to/resume.txt --pdf path/to/resume.pdf

  # Upload text resume only (for Bedrock scoring):
  python tools/ops/upload_resume.py --text path/to/resume.txt

  # Upload PDF resume only (for email attachment):
  python tools/ops/upload_resume.py --pdf path/to/resume.pdf

  # Or pass a single file directly:
  python tools/ops/upload_resume.py my_resume.txt
  python tools/ops/upload_resume.py my_resume.pdf
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())


def _get_s3_client() -> boto3.client:
    _load_env()
    region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def upload_file_to_s3(
    s3_client,
    local_path: Path,
    bucket: str,
    s3_key: str,
    content_type: str,
    dry_run: bool = False,
) -> bool:
    if not local_path.exists():
        print(f"[-] Error: File not found: {local_path}")
        return False

    size_bytes = local_path.stat().st_size
    print(f"\n[+] Preparing upload:")
    print(f"    Source:       {local_path} ({size_bytes:,} bytes)")
    print(f"    Destination:  s3://{bucket}/{s3_key}")
    print(f"    Content-Type: {content_type}")

    if dry_run:
        print("    [DRY-RUN] Upload skipped.")
        return True

    try:
        with open(local_path, "rb") as f:
            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=f.read(),
                ContentType=content_type,
            )
        print(f"    [SUCCESS] Successfully uploaded to s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        print(f"    [-] Upload failed: {e}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upload candidate resume files (TXT / PDF) to private S3 memory bucket."
    )
    parser.add_argument("file", nargs="?", help="Direct path to resume file (.txt, .md, or .pdf)")
    parser.add_argument("--text", "-t", help="Path to plain-text resume for Bedrock scoring (.txt, .md)")
    parser.add_argument("--pdf", "-p", help="Path to PDF resume for outreach attachment (.pdf)")
    parser.add_argument(
        "--bucket",
        "-b",
        default=os.environ.get("S3_BUCKET_NAME", "job-search-pipeline-memory"),
        help="Target S3 memory bucket (default: job-search-pipeline-memory)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Inspect planned upload without modifying S3")
    args = parser.parse_args()

    s3 = _get_s3_client()
    bucket = args.bucket
    success = True

    # Handle direct positional argument
    if args.file:
        file_path = Path(args.file)
        ext = file_path.suffix.lower()
        if ext in (".txt", ".md"):
            args.text = str(file_path)
        elif ext == ".pdf":
            args.pdf = str(file_path)
        else:
            print(f"[-] Unsupported file extension '{ext}'. Please supply a .txt, .md, or .pdf file.")
            return 1

    if not args.text and not args.pdf:
        parser.print_help()
        print("\n[-] Please provide at least one resume file to upload (via positional arg or --text/--pdf).")
        return 1

    print("=" * 65)
    print("  JOBHUNT RESUME SYNCHRONIZER")
    print("=" * 65)
    print(f"  Target Bucket: {bucket}")

    if args.text:
        text_path = Path(args.text)
        res = upload_file_to_s3(
            s3_client=s3,
            local_path=text_path,
            bucket=bucket,
            s3_key="data/resume/resume.txt",
            content_type="text/plain; charset=utf-8",
            dry_run=args.dry_run,
        )
        if res:
            print("    -> job-processor will now evaluate matches using this text context.")
        else:
            success = False

    if args.pdf:
        pdf_path = Path(args.pdf)
        res = upload_file_to_s3(
            s3_client=s3,
            local_path=pdf_path,
            bucket=bucket,
            s3_key="data/resume/resume.pdf",
            content_type="application/pdf",
            dry_run=args.dry_run,
        )
        if res:
            print("    -> job-sender will now attach this PDF to outreach emails.")
        else:
            success = False

    print("\n" + "=" * 65)
    if success:
        print("  ALL OPERATIONS COMPLETED SUCCESSFULLY.")
    else:
        print("  SOME UPLOADS FAILED. CHECK LOGS ABOVE.")
    print("=" * 65)

    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
