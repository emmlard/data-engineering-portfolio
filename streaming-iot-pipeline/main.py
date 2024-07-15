# Import modules
import gzip
import datetime
import os
import logging
from dotenv import load_dotenv
from google.cloud import pubsub_v1
from sensor_simulation import simulate, peek_timestamp

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s-%(levelname)s-%(message)s")

# Set Google Cloud credentials
GOOGLE_AUTH_FILE = os.environ["GOOGLE_AUTH_FILE"]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_AUTH_FILE

if __name__ == "__main__":

    TOPIC="traffic"
    PATH="data/sensor_obs.csv.gz"
    PROJECT_ID="atlschool-demo"
    SPEED_FACTOR=300

    # Initialize Pub/Sub publisher
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC)

    with gzip.open(PATH, "rb") as ifp:
        header = ifp.readline()
        first_obs_time = peek_timestamp(ifp)
        program_start_time = datetime.datetime.utcnow()
        print(topic_path)
        simulate(topic_path, publisher, ifp, first_obs_time, program_start_time, SPEED_FACTOR)

