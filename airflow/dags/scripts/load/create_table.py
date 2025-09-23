from google.cloud import bigquery
from google.api_core import exceptions

client = bigquery.Client.from_service_account_json("/opt/airflow/key/purwadika-56d206a5d40e.json")

class CreateDatabase:
    def __init__(self, dataset_name, table_name, project_name):
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.project_name = project_name

    def check_database(self):
        table_ref = client.dataset(self.dataset_name).table(self.table_name)
        return client.get_table(table_ref)
        
    def create_dataset(self):
        dataset = bigquery.Dataset(f"{self.project_name}.{self.dataset_name}")
        dataset.location = "asia-southeast2"
        return client.create_dataset(dataset, timeout = 30)

    def create_table(self):
        schema = [
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("card_present_flag", "STRING"),
            bigquery.SchemaField("bpay_biller_code", "STRING"),
            bigquery.SchemaField("account", "STRING"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("long_lat", "STRING"),
            bigquery.SchemaField("txn_description", "STRING"),
            bigquery.SchemaField("merchant_id", "STRING"),
            bigquery.SchemaField("merchant_code", "STRING"),
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("date", "STRING"),
            bigquery.SchemaField("gender", "STRING"),
            bigquery.SchemaField("age", "STRING"),
            bigquery.SchemaField("merchant_suburb", "STRING"),
            bigquery.SchemaField("merchant_state", "STRING"),
            bigquery.SchemaField("extraction", "STRING"),
            bigquery.SchemaField("amount", "STRING"),
            bigquery.SchemaField("transaction_id", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("merchant_long_lat", "STRING"),
            bigquery.SchemaField("movement", "STRING")
        ]

        table = bigquery.Table(f"{self.project_name}.{self.dataset_name}.{self.table_name}", schema=schema)
        return client.create_table(table)  # Make an API request.
        

if __name__ == "__main__":

    dataset_name = "jcdeol005_finalproject_dhika_raw"
    table_name = "raw_anz_dataset"
    project_name = "purwadika"

    execute = CreateDatabase(dataset_name, table_name, project_name)

    try:
        execute.check_database()
        print(f"database {execute.check_database()} already exist")
    except exceptions.NotFound:
        try:
            execute.create_dataset()
            execute.create_table()
        except exceptions.Conflict as e:
            execute.create_table()
    except Exception as e:
        print(e)