import os.path

from dotenv import dotenv_values

path_to_creds = os.path.join("src", "credentials", ".env")
config = dotenv_values(path_to_creds)


BEARER_TOKEN = config["API_BEARER_TOKEN"]
S3_ACCESS_KEY_ID = config["S3_ACCESS_KEY_ID"]
S3_ACCESS_KEY_SECRET = config["S3_ACCESS_KEY_SECRET"]
S3_BUCKET_NAME = "luca-mircea-takeaway-challenge"

BASE_URL = "https://api.endpoint.com/"
