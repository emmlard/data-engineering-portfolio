from gcs_util import GCSManager
from bq_util import BQManager
from api_client import PlayStationGamesAPI
import config


if __name__ == "__main__":

    # creating a bucket
    gcs_manager = GCSManager(project_id=config.project_id)
    gcs_manager.create_bucket(
        bucket_name=config.bucket_name,
        storage_class=config.storage_class,
        location=config.location,
    )

    # Create a dataset
    bq_manager = BQManager(project_id=config.project_id)
    bq_manager.create_bq_dataset(dataset_id=config.dataset_id, location=config.location)

    # Create a table
    bq_manager.create_bq_table(dataset_id=config.dataset_id, table_id=config.table_id)

    # Extract data from api
    GameManager = PlayStationGamesAPI(base_url=config.base_url)
    response = GameManager.get(endpoint=config.endpoint)
    jsonl_records = GameManager._to_jsonl_buffer(response)

    # Write to gcs
    gcs_uri = gcs_manager.write_to_gcs_bucket(
        bucket_name=config.bucket_name,
        file_name=config.file_name,
        file_buffer=jsonl_records,
        content_type=config.content_type,
    )

    # Load dcsv data from local to Bigquery Table
    bq_manager.load_csv_data_from_local(
        table_ref=f"{config.project_id}.{config.dataset_id}.{config.local_table_id}",
        csv_file_path=config.csv_file_path,
    )

    # Load data from GCS to Biquery Table
    bq_manager.load_data_from_gcs(
        dataset_id=config.dataset_id,
        table_id=config.table_id,
        schema=config.schema,
        source_uri=gcs_uri,
    )
