"""
Here we keep functions for validating the data, dealing with NULLs, etc.
"""

import os

import pandas as pd
import yaml


class SchemaMismatch(Exception):
    pass


def validate_items_dataset(items: pd.DataFrame) -> pd.DataFrame:
    """Validate & correct items data"""
    path_to_schema = os.path.join("src", "data_schemas", "items.yml")
    with open(path_to_schema, "r") as file:
        expected_schema_details = yaml.safe_load(file)

    if [column_name for column_name in items.columns] != [
        expected_col_name for expected_col_name in expected_schema_details.keys()
    ]:
        raise SchemaMismatch("Incorrect schema detected!")

    # rmb to implement transformations & NULL handling!

    return items
