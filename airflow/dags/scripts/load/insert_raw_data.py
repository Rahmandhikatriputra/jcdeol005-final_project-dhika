import argparse
from google.cloud import bigquery

# Construct a BigQuery client object.
# client = bigquery.Client.from_service_account_json(r"C:\dhika\Purwadhika\final_project\airflow\key\purwadika-56d206a5d40e.json")
client = bigquery.Client.from_service_account_json("/opt/airflow/key/purwadika-56d206a5d40e.json")

def data_ingestion(table_id_bigquery, gcs_uri):

    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = client.load_table_from_uri(
        gcs_uri, 
        table_id_bigquery, 
        job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.
    return client.get_table(table_id_bigquery)  # Make an API request.
    

if __name__ == "__main__":

    table_id_bigquery = "purwadika.jcdeol005_finalproject_dhika_raw.raw_anz_dataset"
    
    parser = argparse.ArgumentParser(description="Insert the file name and directory where the fake data will be created")
    parser.add_argument("--file_name", type = str, help = "file name and directory")

    args = parser.parse_args()

    gcs_uri = f"gs://jcdeol005-dhika-bucket/{args.file_name}"

    result = data_ingestion(table_id_bigquery, gcs_uri)
    print("Loaded {} rows.".format(result.num_rows))

    # gs://jcdeol005-dhika-bucket/data_source_2020-01-01.csv