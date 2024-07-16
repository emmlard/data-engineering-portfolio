### Detailed Steps on how to run this project

1. **Creating a GCS Bucket**:
    ```python
    gcs_manager = GCSManager(project_id=config.project_id)
    gcs_manager.create_bucket(
        bucket_name=config.bucket_name,
        storage_class=config.storage_class,
        location=config.location,
    )
    ```

2. **Creating a BigQuery Dataset**:
    ```python
    bq_manager = BQManager(project_id=config.project_id)
    bq_manager.create_bq_dataset(dataset_id=config.dataset_id, location=config.location)
    ```

3. **Creating a BigQuery Table**:
    ```python
    bq_manager.create_bq_table(dataset_id=config.dataset_id, table_id=config.table_id)
    ```

4. **Extracting Data from API**:
    ```python
    GameManager = PlayStationGamesAPI(base_url=config.base_url)
    response = GameManager.get(endpoint=config.endpoint)
    jsonl_records = GameManager._to_jsonl_buffer(response)
    ```

5. **Writing Data to GCS**:
    ```python
    gcs_uri = gcs_manager.write_to_gcs_bucket(
        bucket_name=config.bucket_name,
        file_name=config.file_name,
        file_buffer=jsonl_records,
        content_type=config.content_type,
    )
    ```

6. **Loading CSV Data from Local to BigQuery Table**:
    ```python
    bq_manager.load_csv_data_from_local(
        table_ref=f"{config.project_id}.{config.dataset_id}.{config.local_table_id}",
        csv_file_path=config.csv_file_path,
    )
    ```

7. **Loading Data from GCS to BigQuery Table**:
    ```python
    bq_manager.load_data_from_gcs(
        dataset_id=config.dataset_id,
        table_id=config.table_id,
        schema=config.schema,
        source_uri=gcs_uri,
    )
    ```