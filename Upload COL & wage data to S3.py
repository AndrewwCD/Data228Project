import boto3
import os

s3 = boto3.resource('s3',
    aws_access_key_id=' ',#Access key removed for project report 
    aws_secret_access_key=' ')

bucket_name = 'data228projectdata'
prefix = 'Raw Data/'

local_folder_path = 'D:\Data 228\Data 228 Project'

# Iterate over all the files in the local directory
for filename in os.listdir(local_folder_path):
    if filename.endswith('.csv'):
        # Upload the file to S3
        s3_file_key = prefix + filename
        s3.Bucket(bucket_name).upload_file(os.path.join(local_folder_path, filename), s3_file_key)
        print(f'File {filename} uploaded to S3 as {s3_file_key}')
