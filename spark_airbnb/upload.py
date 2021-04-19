import os
from azure.storage.blob import BlockBlobService

account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_KEY")
block_blob_service = BlockBlobService(
    account_name=account_name, account_key=account_key
)


def azure_credentials():
    azure_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    azure_account_key = os.getenv("AZURE_STORAGE_KEY")
    return azure_account_name, azure_account_key


def upload(file_names, container_name, path):

    for file_name in file_names:
        blob_name = f"{file_name}"
        f_path = f"{path}/{file_name}"
        block_blob_service.create_blob_from_path(container_name, blob_name, f_path)


def setup_path(directory):
    dir_name = directory
    path = f"../{dir_name}"
    return path


if __name__ == "__main__":
    file_path = setup_path("data")
    container = "airbnb-data"
    data_file_names = os.listdir(file_path)
    upload(data_file_names, container, file_path)
