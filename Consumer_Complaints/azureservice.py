# upload_blob_images.py
# Python program to bulk upload jpg image files as blobs to azure storage
# Uses latest python SDK() for Azure blob storage
# Requires python 3.6 or above
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings, ContainerClient


# IMPORTANT: Replace connection string with your storage account connection string
# Usually starts with DefaultEndpointsProtocol=https;...
MY_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=consumercomplaints;AccountKey=QoOmLJyYhMV17b4Nzsz14CpHMrBTo75/8xSfIebRCqk+v5NPnkYTbqTRL8wpfbcxZke7zhQbRbkonz+17Wxy4g==;EndpointSuffix=core.windows.net"

# Replace with blob container. This should be already created in azure storage.
MY_CONTAINER = "complaints"

# Replace with the local folder which contains the image files for upload
UPLOAD_FILE_PATH = "C:/Users/029338502/Desktop/Data_Upload_Files"
DOWNLOAD_FILE_PATH = "C:/Users/029338502/Desktop/Data_Download_Files"


class AzureBlobFileHandler:
    def __init__(self):
        print("Intializing AzureBlobFileUploader")

        # Initialize the connection to Azure storage account
        self.blob_service_client = BlobServiceClient.from_connection_string(MY_CONNECTION_STRING)
        self.my_container = self.blob_service_client.get_container_client(MY_CONTAINER)

    def upload_all_files_in_folder(self):
        # Get all files with jpg extension and exclude directories
        all_file_names = [f for f in os.listdir(UPLOAD_FILE_PATH)
                          if os.path.isfile(os.path.join(UPLOAD_FILE_PATH, f)) and ".json" in f]

        # Upload each file
        for file_name in all_file_names:
            self.upload_file(file_name)

    def upload_file(self, file_name):
        # Create blob with same name as local file name
        blob_client = self.blob_service_client.get_blob_client(container=MY_CONTAINER,
                                                               blob=file_name)
        # Get full path to the file
        upload_file_path = os.path.join(UPLOAD_FILE_PATH, file_name)

        # Create blob on storage
        # Overwrite if it already exists!
        file_content_setting = ContentSettings(content_type='text/json')
        print(f"uploading file - {file_name}")
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True, content_settings=file_content_setting)

    def save_blob(self, file_name, file_content):
        # Get full path to the file
        download_file_path = os.path.join(DOWNLOAD_FILE_PATH, file_name)

        # for nested blobs, create local path as well!
        os.makedirs(os.path.dirname(download_file_path), exist_ok=True)

        with open(download_file_path, "wb") as file:
            file.write(file_content)

    def download_all_blobs_in_container(self):
        my_blobs = self.my_container.list_blobs()
        for blob in my_blobs:
            print(blob.name)
            bytes = self.my_container.get_blob_client(blob).download_blob().readall()
            self.save_blob(blob.name, bytes)


# Initialize class and upload files
azure_blob_file_handler = AzureBlobFileHandler()
# azure_blob_file_handler.upload_all_files_in_folder()

azure_blob_file_handler.download_all_blobs_in_container()




