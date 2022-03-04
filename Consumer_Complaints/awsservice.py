import boto3
from botocore.exceptions import NoCredentialsError
import os

ACCESS_KEY = 'AKIASE4BDYN3ZF3L4NBG'
SECRET_KEY = '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'
FILE_NAME = 'data_2022-03-02.json'
UPLOAD_FILE_PATH = "C:/Users/029338502/CustomerComplaints/Consumer_Complaints/artifacts/"
BUCKET =  "customer-complaints-la"


class AwsBucketHandler:
    def __init__(self):
        self.s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)

    def upload_all_files_in_folder(self):
        # Get all files with jpg extension and exclude directories
        all_file_names = [f for f in os.listdir(UPLOAD_FILE_PATH)
                          if os.path.isfile(os.path.join(UPLOAD_FILE_PATH, f)) and ".json" in f]

        # Upload each file
        for file_name in all_file_names:
            self.upload_to_aws(file_name,file_name)

    def upload_to_aws(self,file_name, s3_file):
        try:
            self.s3.upload_file(UPLOAD_FILE_PATH+file_name,BUCKET,s3_file)
            print("Upload Successful")
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False


aws_bucket_handler = AwsBucketHandler()
aws_bucket_handler.upload_all_files_in_folder()
