import os
from azure.storage.blob import BlockBlobService

account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
account_key = os.getenv('AZURE_STORAGE_KEY')
container_name = 'airbnb-data'

block_blob_service = BlockBlobService(
        account_name=account_name,
        account_key=account_key
        )

def upload(file_names):

    for file_name in file_names:
        blob_name = f"{file_name}"
        file_path = f"{path}/{file_name}"
        block_blob_service.create_blob_from_path(container_name, blob_name,file_path)

if __name__ == "__main__":
    dir_name = 'data'
    path = f"../{dir_name}"
    file_names = os.listdir(path)
    upload(file_names)
    
