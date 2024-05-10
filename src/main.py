"""
Main function
Here we keep the tasks for processing
each of the different entrypoints/datasets
"""

from typing import Optional

from src.api_interactor import APIInteractor
from src.constants import BASE_URL, BEARER_TOKEN
from src.extract import retrieve_metadata, retrieve_reviews_data
from src.transform import transform_reviews_data
from src.validate import validate_raw_data


def process_raw_reviews_data(
    start_timestamp: Optional[str] = None, end_timestamp: Optional[str] = None
) -> None:
    """Extract, transform, load raw reviews data"""
    # first set up client
    api_interactor = APIInteractor(BASE_URL, BEARER_TOKEN)

    # extract data
    reviews = retrieve_reviews_data(  # noqa: F841
        api_interactor, start_timestamp=start_timestamp, end_timestamp=end_timestamp
    )

    validate_raw_data(reviews, "reviews")
    # transform (i.e. clean) data

    reviews_processed_dict = transform_reviews_data(reviews)
    reviews_fact_table = reviews_processed_dict["reviews_fact_table"]  # noqa: F841
    reviewers = reviews_processed_dict["reviewers"]  # noqa: F841
    reviewers_user_names = reviews_processed_dict["reviewers_user_names"]  # noqa: F841

    # to be continued


def process_raw_metadata(
    start_timestamp: Optional[str] = None, end_timestamp: Optional[str] = None
) -> None:
    """Extract, transform, load raw metadata"""
    # first set up client
    api_interactor = APIInteractor(BASE_URL, BEARER_TOKEN)

    # extract data
    metadata = retrieve_metadata(  # noqa: F841
        api_interactor, start_timestamp=start_timestamp, end_timestamp=end_timestamp
    )
    validate_raw_data(metadata, "metadata")
