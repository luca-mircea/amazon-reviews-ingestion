"""
Here we write the load bits of the code,
i.e. getting it from an S3 bucket.

Normally I'd code a nice client, but in
the context of this project it feels a bit
overkill
"""

import tempfile
from io import StringIO

import pandas as pd
from boto3 import Session

from src.constants import S3_ACCESS_KEY_ID, S3_ACCESS_KEY_SECRET, S3_BUCKET_NAME


class IncorrectDWHSpecification(Exception):
    pass


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
        # profile_name='dev'
    )

    s3 = session.resource("s3", use_ssl=False)

    upload_key = f"{upload_path}/{upload_file_name}"

    s3.Bucket(S3_BUCKET_NAME).upload_file(file_to_upload_name, upload_key)


def upload_data_to_s3_as_csv(data_to_upload: pd.DataFrame, table_name: str) -> None:
    session = Session(
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_ACCESS_KEY_SECRET,
        region_name="eu-north-1",
    )
    bucket = S3_BUCKET_NAME  # already created on S3
    csv_buffer = StringIO()
    data_to_upload.to_csv(csv_buffer)
    s3_resource = session.resource("s3", use_ssl=False)
    s3_resource.Object(bucket, f"uploads/{table_name}.csv").put(
        Body=csv_buffer.getvalue()
    )


def upload_to_dwh(target_data: pd.DataFrame, table_name: str, upload_to: str) -> None:
    """Mock uploader function that puts the data in various locations"""
    if upload_to == "dwh_as_csv":
        with tempfile.TemporaryDirectory() as temp_dir:
            target_data.to_csv(temp_dir + f"/{table_name}.csv", index=False)

            upload_data_to_s3(
                upload_path=f"uploads/{table_name}",
                upload_file_name=f"{table_name}.csv",
                file_to_upload_name=temp_dir + f"/{table_name}.csv",
            )

            print("Upload successful!")
    elif upload_to == "dwh_as_stream":
        upload_data_to_s3_as_csv(target_data, table_name)
        print("Upload successful!")

    elif upload_to == "mock_dwh_locally":
        target_data.to_csv(f"mock_dwh/{table_name}.csv")
        print("Upload successful!")

    else:
        raise IncorrectDWHSpecification("Upload location incorrectly specified!")
