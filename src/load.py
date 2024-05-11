"""
Here we write the load bits of the code,
i.e. getting it from an S3 bucket.

Normally I'd code a nice client, but in
the context of this project it feels a bit
overkill
"""

import tempfile

import pandas as pd
from boto3 import Session

from src.constants import (S3_ACCESS_KEY_ID, S3_ACCESS_KEY_SECRET,
                           S3_BUCKET_NAME)


def upload_data_to_s3(
    upload_path: str,
    upload_file_name: str,
    file_to_upload_name: str,
):
    """Function for uploading to the S3 bucket, file agnostic"""

    session = Session(
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_ACCESS_KEY_SECRET,
        region_name="eu-north-1",
    )

    s3 = session.resource("s3")

    upload_key = f"{upload_path}/{upload_file_name}"

    s3.Bucket(S3_BUCKET_NAME).upload_file(file_to_upload_name, upload_key)


def upload_to_dwh(target_data: pd.DataFrame, table_name: str) -> None:
    """Mock uploader function that puts the data in S3 actually"""
    with tempfile.TemporaryDirectory() as temp_dir:
        target_data.to_csv(temp_dir + f"/{table_name}.csv", index=False)

        upload_data_to_s3(
            upload_path=f"uploads/{table_name}",
            upload_file_name=f"{table_name}.csv",
            file_to_upload_name=temp_dir + f"/{table_name}.csv",
        )

        print("Upload successful!")
