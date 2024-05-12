"""
Here we keep functions for validating the data, dealing with NULLs, etc.
"""

import os

import pandas as pd
import yaml
from boto3 import Session

from src.constants import S3_ACCESS_KEY_ID, S3_ACCESS_KEY_SECRET, S3_BUCKET_NAME


class SchemaMismatch(Exception):
    pass


class PKNotUnique(Exception):
    pass


def validate_raw_data(target_data: pd.DataFrame, dataset_name: str) -> None:
    """Validate & correct items data"""
    path_to_raw_schemas_file = os.path.join(
        "src", "data_schemas", "raw_data_schemas.yml"
    )

    with open(path_to_raw_schemas_file, "r") as file:
        expected_schema_dictionary = yaml.safe_load(file)

    current_data_schema = expected_schema_dictionary[dataset_name]

    if [column_name for column_name in target_data.columns] != current_data_schema:
        raise SchemaMismatch("Incorrect schema detected!")


def list_bucket_files_and_update_time():
    """List S3 files + upload time"""
    session = Session(
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_ACCESS_KEY_SECRET,
        region_name="eu-north-1",
    )

    s3 = session.resource("s3")

    # get objects
    objects_in_bucket = s3.meta.client.list_objects(Bucket=S3_BUCKET_NAME)

    contents_df = pd.DataFrame(objects_in_bucket["Contents"])

    # filter data
    contents_df["folder"] = [key.split("/")[0] for key in contents_df["Key"]]

    contents_uploads = contents_df[contents_df["folder"] == "uploads"]

    contents_uploads = contents_uploads[["Key", "LastModified"]]

    # list contents
    print(contents_uploads)


def validate_local_upload_mock_dwh() -> None:
    """Read from the mock DWH csv, print latest 10"""
    latest_uploads = pd.read_csv("mock_dwh/mock_dwh.csv")

    print(latest_uploads[:10])
