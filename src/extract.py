"""
Here we keep the extract bits of the pipeline,
where we customize the API interactor class to
retrieve specific data
"""

import pandas as pd

from src.api_interactor import APIInteractor


def retrieve_items_data(
    api_interactor: APIInteractor, start_timestamp: str, end_timestamp: str
) -> pd.DataFrame:
    items = api_interactor.retrieve_data_from_datasets_package(
        "items", start_timestamp, end_timestamp
    )

    return items
