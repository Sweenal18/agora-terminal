import boto3
import sys
import os
from datetime import datetime, timedelta
from botocore.client import Config

MINIO_URL = "http://localhost:9000"
MINIO_USER = "agora"
MINIO_PASS = "change_me_in_production"
BUCKET = "agora-bronze"
SOURCE = r"C:\Projects\agora-terminal\agora-terminal\polygon_bronze.jsonl"
KEEP_DAYS = 7

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_URL,
    aws_access_key_id=MINIO_USER,
    aws_secret_access_key=MINIO_PASS,
    config=Config(signature_version="s3v4"),
)

today = datetime.now().strftime("%Y-%m-%d")
key = f"equity/polygon_bronze_{today}.jsonl"

print(f"Uploading {SOURCE} -> {BUCKET}/{key}...")
s3.upload_file(SOURCE, BUCKET, key)
print(f"Upload complete.")

# Delete backups older than KEEP_DAYS
deleted = 0
for i in range(KEEP_DAYS + 1, KEEP_DAYS + 60):
    old_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
    old_key = f"equity/polygon_bronze_{old_date}.jsonl"
    try:
        s3.delete_object(Bucket=BUCKET, Key=old_key)
        deleted += 1
    except Exception:
        pass

print(f"Cleanup done. Removed {deleted} old backups.")
sys.exit(0)