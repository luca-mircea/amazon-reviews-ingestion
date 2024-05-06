"""
Here we keep the extract bits of the pipeline,
where we customize the API interactor class to
retrieve specific data
"""

from typing import Optional

import pandas as pd

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
