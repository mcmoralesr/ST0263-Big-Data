# ingest/upload_covid.py

import os
import boto3
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")
LOCAL_DIR = "data/covid19/"
S3_PREFIX = "raw/covid19/"

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=AWS_REGION
)
s3 = session.client("s3")

def upload_folder_to_s3(local_dir, bucket, prefix):
    for root, _, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            s3_key = os.path.join(prefix, file)
            print(f"⬆️ Subiendo {file} a s3://{bucket}/{s3_key}...")
            s3.upload_file(local_path, bucket, s3_key)
            print(f"✅ Subido: {file}")

if __name__ == "__main__":
    upload_folder_to_s3(LOCAL_DIR, S3_BUCKET, S3_PREFIX)
