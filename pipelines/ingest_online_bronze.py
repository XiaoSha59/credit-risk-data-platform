"""
Module: ingest_online_bronze.py
Description: Ingests online streaming credit batches from external MinIO S3 
             bucket into the Bronze Zone.
"""

import os
import boto3

def ingest_online_from_minio_to_bronze():
    print("[*] Connecting to external MinIO S3 storage to pull streaming batches...")
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )
    bucket_name = "external-streaming-source"
    bronze_dir = "data/bronze/online"
    os.makedirs(bronze_dir, exist_ok=True)

    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if "Contents" not in response:
        print("[-] No files found in external MinIO bucket.")
        return

    for obj in response["Contents"]:
        file_key = obj["Key"]
        dest_path = os.path.join(bronze_dir, file_key)
        
        if not os.path.exists(dest_path):
            print(f"[*] Ingesting streaming file from MinIO -> Bronze: {file_key}")
            s3_client.download_file(bucket_name, file_key, dest_path)
        else:
            print(f"[*] File already exists in Bronze Zone: {file_key}")

    print("[+] Online Bronze Ingestion from MinIO complete!")

if __name__ == "__main__":
    ingest_online_from_minio_to_bronze()