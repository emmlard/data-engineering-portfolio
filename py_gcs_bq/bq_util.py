from google.cloud import bigquery
from google.cloud.exceptions import Conflict, NotFound
import os
import json
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s-%(levelname)s-%(message)s")

GOOGLE_AUTH_FILE = os.environ["GOOGLE_AUTH_FILE"]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_AUTH_FILE


class BQManager:
    """
    A class to manage BigQuery operations such as creating datasets, creating tables,
    and loading data into tables.
    """

    def __init__(self, project_id: str) -> None:
        """
        Initialize the BQManager with the given project ID.
        """
        self.project_id = project_id
        self.client = self._start_bq_client()

    def _start_bq_client(self):
        """
        Instantiate the BigQuery client.
        """
        return bigquery.Client(project=self.project_id)

    def create_bq_dataset(self, dataset_id: str, location: str) -> None:
        """
        Create a dataset with the given dataset ID and location.
        """
        try:
            dataset = bigquery.Dataset(f"{self.project_id}.{dataset_id}")
            dataset.location = location
            dataset = self.client.create_dataset(dataset)
            logging.info(f"Dataset {dataset_id} created successfully")
        except Conflict:
            logging.info(f"Dataset {dataset_id} already exists, exiting successfully")
        except Exception as err:
            logging.error(f"Encountered issue creating the dataset {dataset_id}: {err}")

    def create_bq_table(self, dataset_id: str, table_id: str) -> None:
        """
        This function create a table in the specified bigquery dataset.
        """
        try:
            self.client.get_dataset(dataset_id)
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            table = bigquery.Table(table_ref)
            table = self.client.create_table(table)
            logging.info(f"Table {table_ref} created successfully")
        except NotFound:
            logging.error(f"Dataset {dataset_id} is not found")
        except Conflict:
            logging.info(f"Table {table_ref} already exists in dataset {dataset_id}")
        except Exception as err:
            logging.error(
                f"Encountered issue creating the table {table_ref} in dataset {dataset_id}: {err}"
            )

    def load_csv_data_from_local(self, table_ref: str, csv_file_path: str) -> None:
        """
        This function loads CSV data into the specified bigquery table
        """
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
            )

            with open(csv_file_path, "rb") as source_file:
                job = self.client.load_table_from_file(
                    source_file, table_ref, job_config=job_config
                )

            job.result()

            table = self.client.get_table(table_ref)
            logging.info(
                f"Successfully loaded {table.num_rows} rows into {table_ref} table"
            )
        except Exception as err:
            logging.error(f"Encountered issue loading data to table {table_ref}: {err}")

    def load_data_from_gcs(
        self, dataset_id: str, table_id: str, schema: str, source_uri: str
    ) -> None:
        """
        This function loads json data from gcs into the specified bigquery table
        """
        try:
            with open(schema, "r") as file:
                schema_json = json.load(file)

            job_config = bigquery.LoadJobConfig(
                schema=schema_json,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )

            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

            load_job = self.client.load_table_from_uri(
                source_uri, table_ref, job_config=job_config
            )
            load_job.result()
            logging.info(
                f"Successfully loaded {load_job.output_rows} rows into {dataset_id}.{table_id} table"
            )
        except Exception as err:
            logging.error(
                f"Encountered issue loading data to table {self.project_id}.{dataset_id}.{table_id}: {err}"
            )
