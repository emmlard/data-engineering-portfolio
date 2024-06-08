import requests
from requests.exceptions import HTTPError
import config

try:
    response = requests.get(config.url)
    response.raise_for_status()
    data = response.json()
except HTTPError as http_err:
    print(f"HTTP error occurred: {http_err}")
except Exception as err:
    print(f"Encounter an error: {err}")
