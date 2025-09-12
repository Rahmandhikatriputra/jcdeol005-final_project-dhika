from google.cloud import storage


def upload_blob(bucket_name, source_file_name, destination_blob_name, key_file_path):
    
    storage_client = storage.Client.from_service_account_json(key_file_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)


    blob.upload_from_filename(source_file_name)

if __name__ == "__main__":

    upload_blob(
        bucket_name = "jcdeol005-dhika-bucket",
        source_file_name = r"\airflow\dags\scripts\generate_data_source\data_source_2025-9-12.csv",
        destination_blob_name = "data_source_2025-9-12.csv",
        key_file_path = r"\airflow\key\purwadika-56d206a5d40e.json"
    )