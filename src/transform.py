"""
Here we keep the transform functions, where
we'll do all the data processing, including splitting
the raw data into the eventual tables that we'll use
"""

import pandas as pd

from src.data_processing_functions import (
    add_date_string_column,
    convert_data_types,
    process_categories,
    process_nulls,
    process_products,
    process_related_items,
    process_reviews_raw_columns,
    process_sales_ranks,
)
from src.extract import (
    import_column_data_type_schemas,
    import_column_renaming_schemas,
    import_null_handling_schemas,
)

COLUMN_RENAMING_SCHEMAS = import_column_renaming_schemas()
COLUMN_DATA_TYPE_SCHEMAS = import_column_data_type_schemas()
NULL_HANDLING_SCHEMAS = import_null_handling_schemas()


def transform_reviews_data(reviews: pd.DataFrame) -> dict:
    """Transform (clean) raw data"""
    # first we clean the data (convert the date to the right format)
    # and retrieve the helpfulness
    # then we handle nulls
    # finally we split the data and return the different bits as a dict

    # debate: it's safer to perform operations on a copy of the DF
    # such that the function can easily retry if needed, because
    # saving a copy means we keep the raw intact. On the other hand,
    # this makes for less efficient memory usage. One could
    # do it inplace=True to save memory because the data is very easy
    # to retrieve again without consuming resources, but if it came
    # from a finicky API, I'd definitely save a copy to skip the
    # headache of querying it again

    # in this situation we'll save copies of the original data
    # when we slice it up, but then we'll do a mixture of
    # inplace and not inplace. For a real life use case I'd be
    # more consistent with how I apply this

    # Note: this will result to a lot of annoying warnings for
    # SettingWithCopyWarning

    reviews = process_reviews_raw_columns(reviews)

    # split the data
    reviews_fact_table = reviews[
        [
            "review_id",  # note: this list could also be a schema/in constants
            "reviewerID",
            "asin",
            "reviewText",
            "summary",
            "overall",
            "count_review_helpful_yes",
            "count_review_helpful_no",
            "unixReviewTime",
            "review_date_parsed_as_int",
        ]
    ].copy()

    reviewers = reviews[["reviewerID"]].copy()

    reviewer_user_names = reviews[["reviewerID", "reviewerName"]].copy()

    date_dimension = reviews[["review_date_parsed_as_int"]].copy()

    # drop duplicates for reviewers - we'll check later if the PK is unique
    reviewers.drop_duplicates(inplace=True)
    reviewer_user_names.drop_duplicates(inplace=True)
    date_dimension.drop_duplicates(inplace=True)

    # Note: normally you'd specify the cols to find dups on, but in this
    # case it's all, and the result should still be 1 row/user
    # because the cardinality of user ID and username is 1 to 1

    # now rename the columns

    reviews_fact_table.rename(
        columns=COLUMN_RENAMING_SCHEMAS["reviews_fact_table"], inplace=True
    )
    reviewers.rename(columns=COLUMN_RENAMING_SCHEMAS["reviewers"], inplace=True)
    reviewer_user_names.rename(
        columns=COLUMN_RENAMING_SCHEMAS["reviewer_user_names"], inplace=True
    )
    date_dimension.rename(
        columns=COLUMN_RENAMING_SCHEMAS["date_dimension"], inplace=True
    )

    # add the date_string_column to date_dimension now
    date_dimension = add_date_string_column(date_dimension)

    # handle nulls (only doable easily before transforming the data)
    # normally it'd be more memory efficient to leave Nulls as nulls
    # but this makes the data "unfriendlier", because in some query
    # engines aggregations functions treat nulls differently, results
    # may be incomplete, etc., leading to confusion and errors.
    # For this reason I prefer to just write "UNKNOWN" and then the
    # analysts/scientists can more quickly grasp the data (even at
    # the cost of extra database storage <- this trade-off should be
    # discussed with the managers, team, etc.)

    # if PKs are missing, then we drop the entire row
    # because it's probably corrupt data. Under normal
    # circumstances I'd go looking for why/where the data
    # is missing and try to fix it, but in this case we have
    # neat PKs for our dataset as far as I could tell (i.e. no nulls)

    reviews_fact_table = process_nulls(
        target_data=reviews_fact_table,
        nulls_handling=NULL_HANDLING_SCHEMAS["reviews_fact_table"],
    )
    reviewers = process_nulls(
        target_data=reviewers, nulls_handling=NULL_HANDLING_SCHEMAS["reviewers"]
    )
    reviewer_user_names = process_nulls(
        target_data=reviewer_user_names,
        nulls_handling=NULL_HANDLING_SCHEMAS["reviewer_user_names"],
    )
    date_dimension = process_nulls(
        target_data=date_dimension,
        nulls_handling=NULL_HANDLING_SCHEMAS["date_dimension"],
    )

    # finally convert the data types

    reviews_fact_table = convert_data_types(
        target_df=reviews_fact_table,
        data_type_schema=COLUMN_DATA_TYPE_SCHEMAS["reviews_fact_table"],
    )
    reviewers = convert_data_types(
        target_df=reviewers, data_type_schema=COLUMN_DATA_TYPE_SCHEMAS["reviewers"]
    )
    reviewer_user_names = convert_data_types(
        target_df=reviewer_user_names,
        data_type_schema=COLUMN_DATA_TYPE_SCHEMAS["reviewer_user_names"],
    )
    date_dimension = convert_data_types(
        target_df=date_dimension,
        data_type_schema=COLUMN_DATA_TYPE_SCHEMAS["date_dimension"],
    )

    # finally fill missing data/NaNs

    # it's also suboptimal to try to plug the NaNs after we've already
    # converted the data. I should've thought this through a bit better

    result_dict = {
        "reviews_fact_table": reviews_fact_table,
        "reviewers": reviewers,
        "reviewers_user_names": reviewer_user_names,
        "date_dimension": date_dimension,
    }

    print("Reviews data successfully processed")

    return result_dict


def transform_metadata(metadata: pd.DataFrame) -> dict:
    """Transform the metadata into the various datasets"""
    # this dataset needs no pre-processing of columns,
    # so we can go straight to splitting the data

    products = metadata[["asin", "title", "brand", "description", "price"]].copy()

    product_images = metadata[
        [
            "asin",
            "imurl",
        ]
    ].copy()

    product_sales_ranking = metadata[["asin", "salesrank"]].copy()

    product_categories = metadata[["asin", "categories"]].copy()

    product_related_items = metadata[
        ["asin", "related"]
    ].copy()  # this one we'll split further later on

    # there are no duplicates to drop here (we checked in
    # the preliminary analysis and the validations will
    # let us know if something snuck in lol)

    # the data is split, so we can process it now
    products = process_products(products)
    product_sales_ranking = process_sales_ranks(product_sales_ranking)
    product_categories = process_categories(product_categories)
    product_bought_together, product_also_viewed = process_related_items(
        product_related_items
    )

    # now renaming (a bit too much repeating myself here,
    # in a future version I'd use a list + for loop
    products.rename(columns=COLUMN_RENAMING_SCHEMAS["products"], inplace=True)
    product_images.rename(
        columns=COLUMN_RENAMING_SCHEMAS["product_images"], inplace=True
    )
    product_sales_ranking.rename(
        columns=COLUMN_RENAMING_SCHEMAS["product_sales_ranking"], inplace=True
    )
    product_categories.rename(
        columns=COLUMN_RENAMING_SCHEMAS["product_categories"], inplace=True
    )
    product_bought_together.rename(
        columns=COLUMN_RENAMING_SCHEMAS["product_bought_together"], inplace=True
    )
    product_also_viewed.rename(
        columns=COLUMN_RENAMING_SCHEMAS["product_also_viewed"], inplace=True
    )

    # now we process the nulls (again a bit too much repetition
    # but it makes it easier to debug)

    products = process_nulls(
        target_data=products, nulls_handling=NULL_HANDLING_SCHEMAS["products"]
    )
    product_images = process_nulls(
        target_data=product_images,
        nulls_handling=NULL_HANDLING_SCHEMAS["product_images"],
    )
    product_sales_ranking = process_nulls(
        target_data=product_sales_ranking,
        nulls_handling=NULL_HANDLING_SCHEMAS["product_sales_ranking"],
    )
    product_categories = process_nulls(
        target_data=product_categories,
        nulls_handling=NULL_HANDLING_SCHEMAS["product_categories"],
    )
    product_bought_together = process_nulls(
        target_data=product_bought_together,
        nulls_handling=NULL_HANDLING_SCHEMAS["product_bought_together"],
    )
    product_also_viewed = process_nulls(
        target_data=product_also_viewed,
        nulls_handling=NULL_HANDLING_SCHEMAS["product_also_viewed"],
    )

    # and finally we cast the dtypes

    products = convert_data_types(
        target_df=products,
        data_type_schema=COLUMN_DATA_TYPE_SCHEMAS["products"],
    )
    product_images = convert_data_types(
        target_df=product_images,
        data_type_schema=COLUMN_DATA_TYPE_SCHEMAS["product_images"],
    )
    product_sales_ranking = convert_data_types(
        target_df=product_sales_ranking,
        data_type_schema=COLUMN_DATA_TYPE_SCHEMAS["product_sales_ranking"],
    )
    product_categories = convert_data_types(
        target_df=product_categories,
        data_type_schema=COLUMN_DATA_TYPE_SCHEMAS["product_categories"],
    )
    product_bought_together = convert_data_types(
        target_df=product_bought_together,
        data_type_schema=COLUMN_DATA_TYPE_SCHEMAS["product_bought_together"],
    )
    product_also_viewed = convert_data_types(
        target_df=product_also_viewed,
        data_type_schema=COLUMN_DATA_TYPE_SCHEMAS["product_also_viewed"],
    )

    # finally, pack the dictionary & return

    results_dict = {
        "products": products,
        "product_images": product_images,
        "product_sales_ranking": product_sales_ranking,
        "product_categories": product_categories,
        "product_bought_together": product_bought_together,
        "product_also_viewed": product_also_viewed,
    }

    return results_dict
