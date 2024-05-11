"""
Usually we'd get the data either from an API
or a data warehouse, so I'll mock it accordingly.
Since this is key-values, it resembles most the type
of ourputs that come from an API, so I will code
it accordingly. Basically we'll read the CSV data
as if it's an API endpoint; I'm assuming we have to auth
with a bearer token, and we can pass parameters to the API
to retrieve data for specific timestamps
"""

import json
import os
import re
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
from boto3 import Session
from datasets import load_dataset
from huggingface_hub import hf_hub_download

from src.constants import (S3_ACCESS_KEY_ID, S3_ACCESS_KEY_SECRET,
                           S3_BUCKET_NAME)


class IncorrectEndpointSpecified(Exception):
    pass


def compose_api_request_headers(bearer_token: str) -> dict:
    """Compile headers using bearer token"""
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer " + f"{bearer_token}",
    }

    return headers


def compose_timestamp_based_request_parameters(
    start_timestamp: str, end_timestmap: str
) -> dict:
    """Compile API request parameters"""
    params = {
        "timezone": "Amsterdam",
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestmap,
        "limit": 10000,  # assume we get max 10000 records at a time
    }

    return params


class APIInteractor:
    def __init__(self, base_url: str, bearer_token: str):
        """Save base API properties: URL + auth"""
        self.base_url = base_url
        self.headers = compose_api_request_headers(bearer_token)

    def retrieve_data(
        self, endpoint: str, start_timestamp: str, end_timestamp: str
    ) -> pd.DataFrame:
        """Get data from an endpoint, filtering on the timestamp"""
        response_data_df = pd.DataFrame()
        data_complete = False
        api_calls = 0
        params = compose_timestamp_based_request_parameters(
            start_timestamp, end_timestamp
        )

        while not data_complete:
            params["offset"] = params["limit"] * api_calls
            api_response = requests.get(
                self.base_url + f"/{endpoint}", headers=self.headers, params=params
            )

            response_json = api_response.json()

            response_data_content = response_json["data"]

            flattened_response_data = pd.json_normalize(response_data_content, sep="_")

            response_data_df = pd.concat(
                [response_data_df, flattened_response_data], ignore_index=True, axis=0
            )

            api_calls += 1

            if len(flattened_response_data) < 10000:
                # if < 10000 it means the end of the request
                # so no need to go further
                data_complete = True

            if api_calls > 100:  # we should never query > 100
                # so we set this as a safety measure
                break

            return response_data_df

    @staticmethod
    def retrieve_data_from_csv(
        endpoint: str, start_timestamp: Optional[str], end_timestamp: Optional[str]
    ) -> pd.DataFrame:
        """Write code for reading data from folders with csvs"""

        # create empty df to catch results
        result_data = pd.DataFrame()

        # create OS-agnostic path
        path_to_search = os.path.join("data_short")

        # find the relevant files
        list_of_files = [file_name for file_name in os.listdir(path_to_search)]

        if endpoint == "reviews":
            files_to_read = [
                file_name
                for file_name in list_of_files
                if bool(re.search("reviews", file_name))
            ]
        elif endpoint == "metadata":
            files_to_read = [
                file_name
                for file_name in list_of_files
                if bool(re.search("metadata", file_name))
            ]

        else:
            raise IncorrectEndpointSpecified(
                "Incorrect endpoint specified - check your data folder and try again!"
            )

        # file by file, add to results_df
        for file_name in files_to_read:
            csv_path = os.path.join(path_to_search, file_name)

            data_to_read = pd.read_csv(csv_path, index_col=False)

            result_data = pd.concat(
                [result_data, data_to_read], axis=0, ignore_index=True
            )

        # finally, filter based on the timestamp
        if start_timestamp is not None:
            # first convert str to unix
            start_timestamp_as_unix = datetime.strptime(
                start_timestamp, "%Y-%m-%d %H:%M:%S.%f"
            ).timestamp()

            # then filter data
            result_data = result_data[
                result_data["unixReviewTime"] >= start_timestamp_as_unix
            ]

            # reset index to return clean
            result_data.reset_index(drop=True, inplace=True)

        if end_timestamp is not None:
            # same steps as for start timestamp
            end_timestamp_as_unix = datetime.strptime(
                end_timestamp, "%Y-%m-%d %H:%M:%S.%f"
            ).timestamp()

            result_data = result_data[
                result_data["unixReviewTime"] <= end_timestamp_as_unix
            ]
            result_data.reset_index(drop=True, inplace=True)

        return result_data

    @staticmethod
    def retrieve_data_from_datasets_package(
        endpoint: str, start_timestamp: str, end_timestamp: str
    ) -> pd.DataFrame:
        """Retrieve data from the pre-existing "endpoint"""

        response_df = pd.DataFrame()

        if endpoint == "user_reviews":
            dataset = load_dataset("McAuley-Lab/Amazon-C4")["test"]

            for index in range(len(dataset)):
                review = pd.json_normalize(dataset[index], sep="_")
                response_df = pd.concat(
                    [response_df, review], ignore_index=True, axis=0
                )

            response_df["start_timestamp"] = start_timestamp
            response_df["end_timestamp"] = end_timestamp

        elif endpoint == "items":
            filepath = hf_hub_download(
                repo_id="McAuley-Lab/Amazon-C4",
                filename="sampled_item_metadata_1M.jsonl",
                repo_type="dataset",
            )

            item_pool = []
            with open(filepath, "r") as file:
                for line in file:
                    item_pool.append(json.loads(line.strip()))

            items_normalized = pd.json_normalize(item_pool, sep="_")

            items_normalized["start_timestamp"] = start_timestamp
            items_normalized["end_timestamp"] = end_timestamp

            return items_normalized

        else:
            raise IncorrectEndpointSpecified("Incorrect endpoint supplied!")

    @staticmethod
    def retrieve_data_from_s3(table_name: str) -> pd.DataFrame:
        """Retrieve data from the S3 bucket"""
        download_path = f"data/{table_name}.csv"

        session = Session(
            aws_access_key_id=S3_ACCESS_KEY_ID,
            aws_secret_access_key=S3_ACCESS_KEY_SECRET,
            region_name="eu-north-1",
        )

        s3 = session.resource("s3")

        s3.meta.client.download_file(
            Bucket=S3_BUCKET_NAME,
            Key=f"raw_data/{table_name}.csv",
            Filename="data/reviews.csv",
        )

        print(f"Data ({table_name}) downloaded successfully")
        data = pd.read_csv(download_path)

        return data
