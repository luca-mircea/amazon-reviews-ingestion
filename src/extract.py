"""
Here we keep the extract bits of the pipeline,
where we customize the API interactor class to
retrieve specific data
"""

import os
from typing import Dict, Optional

import pandas as pd
import yaml

from src.api_interactor import APIInteractor


def retrieve_reviews_data(
    api_interactor: APIInteractor,
    start_timestamp: Optional[str],
    end_timestamp: Optional[str],
) -> pd.DataFrame:

    reviews = api_interactor.retrieve_data_from_csv(
        endpoint="reviews", start_timestamp=start_timestamp, end_timestamp=end_timestamp
    )

    return reviews


def retrieve_metadata(
    api_interactor: APIInteractor,
    start_timestamp: Optional[str],
    end_timestamp: Optional[str],
) -> pd.DataFrame:

    metadata = api_interactor.retrieve_data_from_csv(
        endpoint="metadata",
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )

    return metadata


def import_column_renaming_schemas() -> Dict[dict, dict]:
    path_to_raw_schemas_file = os.path.join(
        "src", "data_schemas", "column_renaming_schemas.yml"
    )

    with open(path_to_raw_schemas_file, "r") as file:
        column_renaming_schemas_dict = yaml.safe_load(file)

    return column_renaming_schemas_dict


def import_column_data_type_schemas() -> Dict[dict, dict]:
    path_to_raw_schemas_file = os.path.join(
        "src", "data_schemas", "data_types_schemas.yml"
    )

    with open(path_to_raw_schemas_file, "r") as file:
        column_data_types_schemas_dict = yaml.safe_load(file)

    return column_data_types_schemas_dict


def import_null_handling_schemas() -> Dict[dict, dict]:
    path_to_raw_schemas_file = os.path.join(
        "src", "data_schemas", "null_handling_schemas.yml"
    )

    with open(path_to_raw_schemas_file, "r") as file:
        null_handling_schemas_dict = yaml.safe_load(file)

    return null_handling_schemas_dict
