import os
from azure.storage.blob import BlobServiceClient


CONN_STR = "connection key string"
CONTAINER_NAME = "your container name"

# Files to upload
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILES_TO_UPLOAD = [
    os.path.join(BASE_DIR, "../data/live_global_env_data.csv"),
    os.path.join(BASE_DIR, "../data/historical_global_env_data.csv")
]


blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Ensure container exists
try:
    container_client.get_container_properties()
except:
    container_client.create_container()
    print(f"Container '{CONTAINER_NAME}' created.")


for file_path in FILES_TO_UPLOAD:
    blob_name = os.path.basename(file_path)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f" Uploaded {blob_name} to Azure Blob Storage successfully!")
