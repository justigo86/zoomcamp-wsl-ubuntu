import os
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage
import glob

DATASET_NAME = "mayureshkoli/police-deaths-in-usa-from-1791-to-2022"
LOCAL_PATH = "./data"
GCS_BUCKET_NAME = "zoomcamp-proj-bq-bucket0326"
GCS_BLOB_NAME = "raw/police_deaths_1791_2022.csv"
CLIENT = storage.Client(project='zoomcamp-final-project-491003')
BUCKET = CLIENT.bucket(GCS_BUCKET_NAME)
CHUNK_SIZE = 8 * 1024 * 1024

def download_from_kaggle():
    print("--- Downloading from Kaggle ---")
    api = KaggleApi()
    api.authenticate()

    # Ensure the directory exists
    os.makedirs(LOCAL_PATH, exist_ok=True)

    api.dataset_download_files(DATASET_NAME, path=LOCAL_PATH, unzip=True)

    # Use glob to find the CSV file dynamically
    csv_files = glob.glob(f"{LOCAL_PATH}/*.csv")
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {LOCAL_PATH} after download.")
    # Return the first CSV found (usually there is only one)
    print(f"Found file: {csv_files[0]}")

    # The zip contains csv_files[0]
    return csv_files[0]

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=BUCKET, name=blob_name).exists(CLIENT)

def upload_to_gcs(local_file):
    blob = BUCKET.blob(GCS_BLOB_NAME)
    blob.chunk_size = CHUNK_SIZE

    try:
        print(f"--- Uploading to GCS Bucket: {GCS_BUCKET_NAME} ---")
        blob.upload_from_filename(local_file)
        print(f"Successfully uploaded to gs://{GCS_BUCKET_NAME}/{GCS_BLOB_NAME}")

        if verify_gcs_upload(GCS_BLOB_NAME):
            print(f"Verification successful for {GCS_BLOB_NAME}")
            return
        else:
            print(f"Verification failed for {GCS_BLOB_NAME}.")
    except Exception as e:
        print(f"Failed to upload {local_file} to GCS: {e}")

if __name__ == "__main__":
    csv_file = download_from_kaggle()
    upload_to_gcs(csv_file)