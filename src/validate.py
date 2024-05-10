"""
Here we keep functions for validating the data, dealing with NULLs, etc.
"""

import os

import pandas as pd
import yaml


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
