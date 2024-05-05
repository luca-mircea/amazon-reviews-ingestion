"""
Main function
Here we keep the tasks for processing
each of the different entrypoints/datasets
"""

from src.api_interactor import APIInteractor
from src.constants import BASE_URL, BEARER_TOKEN
from src.extract import retrieve_items_data
from src.validate import validate_items_dataset


def process_raw_reviews_data(start_timestamp: str, end_timestamp: str) -> None:
    """Extract, transform, load raw reviews data"""
    # first set up client
    api_interactor = APIInteractor(BASE_URL, BEARER_TOKEN)

    # extract data
    items = retrieve_items_data(api_interactor, start_timestamp, end_timestamp)

    items = validate_items_dataset(items)

    # to be continued
