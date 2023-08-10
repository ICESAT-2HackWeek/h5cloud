import boto3
import pandas as pd
from io import StringIO

def get_all_csv_from_s3(bucket_name, prefix=''):
    """
    List all CSV files in the S3 bucket under the given prefix.
    """
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    csv_files = [obj['Key'] for obj in objects.get('Contents', []) if obj['Key'].endswith('.csv')]
    return csv_files

def read_csv_from_s3(bucket_name, file_key):
    """
    Read a CSV file from S3 and return it as a DataFrame.
    """
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

def concatenate_csv_from_s3(bucket_name, prefix=''):
    """
    Read all CSV files from an S3 bucket under the given prefix and concatenate them into one DataFrame.
    """
    csv_files = get_all_csv_from_s3(bucket_name, prefix=prefix)
    dfs = [read_csv_from_s3(bucket_name, file_key) for file_key in csv_files]
    return pd.concat(dfs, ignore_index=True)
