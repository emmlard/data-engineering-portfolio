# gcs param
location = "europe-west1"
storage_class = "STANDARD"
file_name = "playstation_games.json"

# bigquery param
content_type = "application/json"
project_id = "altschool-practice-419014"
dataset_id = "etl_basics"
table_id = "playstation_games"
local_table_id = "restaurant_tips"
bucket_name = "play_station"
csv_file_path = "data/tips.csv"
base_url = "https://api.sampleapis.com/playstation"
endpoint = "/games"
schema = "./schema/schema.json"
