import argparse
from google.cloud import storage
from datetime import date

#create a function to insert fake data into Google Cloud Storage
def upload_blob(bucket_name, source_file_name, destination_blob_name, key_file_path):
    
    storage_client = storage.Client.from_service_account_json(key_file_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

if __name__ == "__main__":

#create argparse to insert file name and file directory    
    parser = argparse.ArgumentParser(description="Insert the file name and directory where the fake data will be created")
    parser.add_argument("--file_name", type = str, help = "file name and directory")

    args = parser.parse_args()

    upload_blob(
        bucket_name = "jcdeol005-dhika-bucket",
        source_file_name = f"/opt/airflow/dags/scripts/generate_data_source/{args.file_name}",
        destination_blob_name = args.file_name,
        key_file_path = "/opt/airflow/key/purwadika-56d206a5d40e.json"
    )

