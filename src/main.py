"""
Main function
Here we keep the tasks for processing
each of the different entrypoints/datasets
"""

from typing import Optional

from src.api_interactor import APIInteractor
from src.constants import BASE_URL, BEARER_TOKEN
from src.extract import retrieve_metadata, retrieve_reviews_data


def process_raw_reviews_data(
    start_timestamp: Optional[str], end_timestamp: Optional[str]
) -> None:
    """Extract, transform, load raw reviews data"""
    # first set up client
    api_interactor = APIInteractor(BASE_URL, BEARER_TOKEN)

    # extract data
    reviews = retrieve_reviews_data(
        api_interactor, start_timestamp=None, end_timestamp=None
    )  # noqa: F841

    # transform (i.e. clean) data

    # reviews = validate_reviews_dataset(reviews)

    # to be continued


def process_raw_metadata(
    start_timestamp: Optional[str], end_timestamp: Optional[str]
) -> None:
    """Extract, transform, load raw metadata"""
    # first set up client
    api_interactor = APIInteractor(BASE_URL, BEARER_TOKEN)

    # extract data
    metadata = retrieve_metadata(
        api_interactor, start_timestamp=None, end_timestamp=None
    )  # noqa: F841
