# deploy/create_s3_bucket.py

import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

def create_bucket(bucket_name, region):
    s3 = boto3.client("s3", region_name=region)
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"⚠️  El bucket ya existe: {bucket_name}")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            if region == "us-east-1":
                s3.create_bucket(Bucket=bucket_name)
            else:
                s3.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            print(f"✅ Bucket creado: {bucket_name}")
        else:
            print(f"❌ Error al verificar/crear el bucket: {e}")

if __name__ == "__main__":
    if not S3_BUCKET:
        print("❌ S3_BUCKET no definido en .env")
    else:
        create_bucket(S3_BUCKET, AWS_REGION)
