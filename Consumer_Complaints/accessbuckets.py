import boto3
from botocore.exceptions import NoCredentialsError
import os

ACCESS_KEY = 'ACCESS_KEY'
SECRET_KEY = 'SECRET_KEY'
FILE_NAME = '-03-02'
BUCKET =  "customer-complaints-la"

session = boto3.Session(
         aws_access_key_id=ACCESS_KEY,
         aws_secret_access_key=SECRET_KEY)

s3 = session.resource('s3')

my_bucket = s3.Bucket(BUCKET)

for my_bucket_object in my_bucket.objects.all():
    if str(my_bucket_object.key).__contains__("-03-02"):
        data = my_bucket_object.get()['Body'].read()
        print(data)

