import os.path

from dotenv import dotenv_values

path_to_creds = os.path.join("src", "credentials", ".env")
config = dotenv_values(path_to_creds)


BEARER_TOKEN = config["API_BEARER_TOKEN"]

BASE_URL = "https://api.endpoint.com/"
