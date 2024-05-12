"""
Main function
Here we keep the tasks for processing
each of the different entrypoints/datasets
"""

from typing import Optional

from src.api_interactor import APIInteractor
from src.constants import BASE_URL, BEARER_TOKEN
from src.extract import retrieve_metadata, retrieve_reviews_data
from src.load import upload_to_dwh
from src.transform import transform_metadata, transform_reviews_data
from src.validate import (
    list_bucket_files_and_update_time,
    validate_local_upload_mock_dwh,
    validate_raw_data,
)


def process_raw_reviews_data_with_timestamps(
    start_timestamp: Optional[str] = None, end_timestamp: Optional[str] = None
) -> None:
    """Extract, transform, load raw reviews data"""
    # first set up client
    api_interactor = APIInteractor(BASE_URL, BEARER_TOKEN)

    # extract data
    reviews = retrieve_reviews_data(
        api_interactor,
        retrieve_from="s3",
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )

    validate_raw_data(reviews, "reviews")

    # transform (i.e. clean) data

    reviews_processed_dict = transform_reviews_data(reviews)
    reviews_fact_table = reviews_processed_dict["reviews_fact_table"]
    reviewers = reviews_processed_dict["reviewers"]
    reviewers_user_names = reviews_processed_dict["reviewers_user_names"]
    date_dimension = reviews_processed_dict["date_dimension"]

    # load
    upload_to_dwh(reviews_fact_table, "reviews_fact_table", upload_to="dwh_as_stream")
    upload_to_dwh(reviewers, "reviewers", upload_to="dwh_as_stream")
    upload_to_dwh(
        reviewers_user_names, "reviewers_user_names", upload_to="dwh_as_stream"
    )
    upload_to_dwh(date_dimension, "date_dimension", upload_to="dwh_as_stream")


def process_raw_metadata_with_timestamps(
    start_timestamp: Optional[str] = None, end_timestamp: Optional[str] = None
) -> None:
    """Extract, transform, load raw metadata"""
    # first set up client

    api_interactor = APIInteractor(BASE_URL, BEARER_TOKEN)

    # extract data
    metadata = retrieve_metadata(
        api_interactor,
        retrieve_from="s3",
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )

    validate_raw_data(metadata, "metadata")

    # transform
    results_dictionary = transform_metadata(metadata)

    products = results_dictionary["products"]
    product_images = results_dictionary["product_images"]
    product_sales_ranking = results_dictionary["product_sales_ranking"]
    product_categories = results_dictionary["product_categories"]
    product_bought_together = results_dictionary["product_bought_together"]
    product_also_viewed = results_dictionary["product_also_viewed"]

    # load

    upload_to_dwh(products, "products")
    upload_to_dwh(product_images, "product_images")
    upload_to_dwh(product_sales_ranking, "product_sales_ranking")
    upload_to_dwh(product_categories, "product_categories")
    upload_to_dwh(product_bought_together, "product_bought_together")
    upload_to_dwh(product_also_viewed, "product_also_viewed")


def process_raw_reviews_data_without_timestamps() -> None:
    """Extract, transform, load raw reviews data"""
    # first set up client
    api_interactor = APIInteractor(BASE_URL, BEARER_TOKEN)

    # extract data
    reviews = retrieve_reviews_data(
        api_interactor, retrieve_from="s3", start_timestamp=None, end_timestamp=None
    )

    validate_raw_data(reviews, "reviews")

    # transform (i.e. clean) data

    reviews_processed_dict = transform_reviews_data(reviews)
    reviews_fact_table = reviews_processed_dict["reviews_fact_table"]
    reviewers = reviews_processed_dict["reviewers"]
    reviewers_user_names = reviews_processed_dict["reviewers_user_names"]
    date_dimension = reviews_processed_dict["date_dimension"]

    # load

    upload_to_dwh(reviews_fact_table, "reviews_fact_table", upload_to="dwh_as_stream")
    upload_to_dwh(reviewers, "reviewers", upload_to="dwh_as_stream")
    upload_to_dwh(
        reviewers_user_names, "reviewers_user_names", upload_to="dwh_as_stream"
    )
    upload_to_dwh(date_dimension, "date_dimension", upload_to="dwh_as_stream")


def process_raw_metadata_without_timestamps() -> None:
    """Extract, transform, load raw metadata"""
    # first set up client

    api_interactor = APIInteractor(BASE_URL, BEARER_TOKEN)

    # extract data
    metadata = retrieve_metadata(
        api_interactor, retrieve_from="s3", start_timestamp=None, end_timestamp=None
    )

    validate_raw_data(metadata, "metadata")

    # transform
    results_dictionary = transform_metadata(metadata)

    products = results_dictionary["products"]
    product_images = results_dictionary["product_images"]
    product_sales_ranking = results_dictionary["product_sales_ranking"]
    product_categories = results_dictionary["product_categories"]
    product_bought_together = results_dictionary["product_bought_together"]
    product_also_viewed = results_dictionary["product_also_viewed"]

    # load

    upload_to_dwh(products, "products", upload_to="dwh_as_stream")
    upload_to_dwh(product_images, "product_images", upload_to="dwh_as_stream")
    upload_to_dwh(
        product_sales_ranking, "product_sales_ranking", upload_to="dwh_as_stream"
    )
    upload_to_dwh(product_categories, "product_categories", upload_to="dwh_as_stream")
    upload_to_dwh(
        product_bought_together, "product_bought_together", upload_to="dwh_as_stream"
    )
    upload_to_dwh(product_also_viewed, "product_also_viewed", upload_to="dwh_as_stream")


def process_raw_reviews_data_without_timestamps_locally() -> None:
    """Extract, transform, load raw reviews data"""
    # first set up client
    api_interactor = APIInteractor(BASE_URL, BEARER_TOKEN)

    # extract data
    reviews = retrieve_reviews_data(
        api_interactor, retrieve_from="local", start_timestamp=None, end_timestamp=None
    )

    validate_raw_data(reviews, "reviews")

    # transform (i.e. clean) data

    reviews_processed_dict = transform_reviews_data(reviews)
    reviews_fact_table = reviews_processed_dict["reviews_fact_table"]
    reviewers = reviews_processed_dict["reviewers"]
    reviewers_user_names = reviews_processed_dict["reviewers_user_names"]
    date_dimension = reviews_processed_dict["date_dimension"]

    # load

    upload_to_dwh(
        reviews_fact_table, "reviews_fact_table", upload_to="mock_dwh_locally"
    )
    upload_to_dwh(reviewers, "reviewers", upload_to="mock_dwh_locally")
    upload_to_dwh(
        reviewers_user_names, "reviewers_user_names", upload_to="mock_dwh_locally"
    )
    upload_to_dwh(date_dimension, "date_dimension", upload_to="mock_dwh_locally")


def process_raw_metadata_without_timestamps_locally() -> None:
    """Extract, transform, load raw metadata"""
    # first set up client

    api_interactor = APIInteractor(BASE_URL, BEARER_TOKEN)

    # extract data
    metadata = retrieve_metadata(
        api_interactor, retrieve_from="local", start_timestamp=None, end_timestamp=None
    )

    validate_raw_data(metadata, "metadata")

    # transform
    results_dictionary = transform_metadata(metadata)

    products = results_dictionary["products"]
    product_images = results_dictionary["product_images"]
    product_sales_ranking = results_dictionary["product_sales_ranking"]
    product_categories = results_dictionary["product_categories"]
    product_bought_together = results_dictionary["product_bought_together"]
    product_also_viewed = results_dictionary["product_also_viewed"]

    # load

    upload_to_dwh(products, "products", upload_to="mock_dwh_locally")
    upload_to_dwh(product_images, "product_images", upload_to="mock_dwh_locally")
    upload_to_dwh(
        product_sales_ranking, "product_sales_ranking", upload_to="mock_dwh_locally"
    )
    upload_to_dwh(
        product_categories, "product_categories", upload_to="mock_dwh_locally"
    )
    upload_to_dwh(
        product_bought_together, "product_bought_together", upload_to="mock_dwh_locally"
    )
    upload_to_dwh(
        product_also_viewed, "product_also_viewed", upload_to="mock_dwh_locally"
    )


def check_successful_completion_s3():
    """List bucket objects + time of download"""
    list_bucket_files_and_update_time()


def check_successful_completion_locally():
    """List DWH objects created locally"""
    validate_local_upload_mock_dwh()
