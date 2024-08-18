from google.cloud import storage
from google.cloud.exceptions import Conflict, NotFound
import os
import io
from typing import Union
from dotenv import load_dotenv
import logging

# Load environment variables from a .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s-%(levelname)s-%(message)s")

# Set up Google Cloud authentication using environment variables
GOOGLE_AUTH_FILE = os.environ["GOOGLE_AUTH_FILE"]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_AUTH_FILE


class GCSManager:
    """
    A class to manage Google Cloud Storage operations such as creating buckets.
    """

    def __init__(self, project_id: str) -> None:
        """
        Initialize the GCSManager with the given project ID.
        """
        self.project_id = project_id
        self.client = self._start_gcs_client()

    def _start_gcs_client(self):
        """
        This function instantiate the Google Cloud Storage client.
        """
        return storage.Client(project=self.project_id)

    def create_bucket(
        self, bucket_name: str, storage_class: str, location: str
    ) -> None:
        """
        This function create a bucket with the given name, storage class, and location.
        """
        try:
            bucket = storage.Bucket(self.client, bucket_name)
            bucket.storage_class = storage_class
            bucket.location = location
            self.client.create_bucket(bucket)
            logging.info(
                f"Bucket {bucket_name} created successfully in {location} with storage class {storage_class}"
            )
        except Conflict:
            logging.info(f"Bucket {bucket_name} already exists, exiting successfully")
        except Exception as err:
            logging.error(
                f"Encountered an issue creating the bucket {bucket_name}: {err}"
            )

    def write_to_gcs_bucket(
        self,
        bucket_name: str,
        file_name: str,
        file_buffer: Union[str, io.StringIO, io.BytesIO],
        content_type: str,
    ) -> str:
        """
        This function will take an IO string and write it
        to a specified bucket and file path and return the object gcs
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(file_name)
            if isinstance(file_buffer, (io.StringIO, io.BytesIO)):
                data_bytes = file_buffer.getvalue()
            else:
                data_bytes = file_buffer.encode("utf-8")
            blob.upload_from_string(data_bytes, content_type=content_type)
            logging.info(f"successfully wrote data to {file_name}")
            return f"gs://{bucket_name}/{file_name}"
        except NotFound:
            logging.error(f"Bucket {bucket_name} does not exist")
        except Exception as err:
            logging.error(f"Encountering issue wrtitng to bucket {bucket_name}: {err}")
