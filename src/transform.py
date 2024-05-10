"""
Here we keep the transform functions, where
we'll do all the data processing, including splitting
the raw data into the eventual tables that we'll use
"""

import ast

import pandas as pd

from src.extract import (
    import_column_data_type_schemas,
    import_column_renaming_schemas,
    import_null_handling_schemas,
)
from src.validate import PKNotUnique, SchemaMismatch

COLUMN_RENAMING_SCHEMAS = import_column_renaming_schemas()
COLUMN_DATA_TYPE_SCHEMAS = import_column_data_type_schemas()
NULL_HANDLING_SCHEMAS = import_null_handling_schemas()


def process_reviews_raw_columns(reviews: pd.DataFrame) -> pd.DataFrame:
    """Fix the helpfulness, reviewTime, review_id columns"""

    reviews["review_date_parsed_as_int"] = [
        int(  # we like int bcs. it's more memory efficient
            date_string.split(",")[1].strip()  # year
            + date_string.split(",")[0].split(" ")[0]  # month
            + date_string.split(",")[0].split(" ")[1].zfill(2)
        )
        for date_string in reviews["reviewTime"]
    ]

    # compile helpfulness of reviews
    reviews["count_review_helpful_yes"] = [
        ast.literal_eval(str(review_helpfulness_list))[0]
        for review_helpfulness_list in reviews["helpful"]
    ]

    reviews["count_review_helpful_no"] = [
        ast.literal_eval(str(review_helpfulness_list))[1]
        for review_helpfulness_list in reviews["helpful"]
    ]

    # create review ID
    reviews["review_id"] = reviews["asin"] + reviews["reviewerID"]

    # technically we don't need this return since we're modifying the
    # data in place, but this makes it easier to read and
    # understand especially for more junior people
    return reviews


def convert_data_types(target_df: pd.DataFrame, data_type_schema: dict) -> pd.DataFrame:
    """Convert the columns of target_df according to the schema"""
    # first check if the schema matches
    if [col_name for col_name in target_df.columns] != [
        col_name for col_name in data_type_schema.keys()
    ]:
        raise SchemaMismatch

    for column_name, data_type in data_type_schema.items():
        target_df[column_name] = target_df[column_name].astype(data_type)

    # technically no need to return bcs. modifying in place, but more readable
    return target_df


def process_nulls(target_data: pd.DataFrame, nulls_handling: dict) -> pd.DataFrame:
    """Handle NULLs according to the defined schema"""
    for column_name, handling in nulls_handling.items():
        if handling == "PK":
            count_entries = target_data[column_name].value_counts()
            if (count_entries != 1).any():
                raise PKNotUnique(f"{column_name} - supposedly PK - is not unique!")
        elif handling == "DROP":
            target_data = target_data[~pd.isnull(target_data[column_name])]
            # don't forget to reset the index before returning!
        else:
            target_data[column_name].fillna(handling, inplace=True)

    return target_data


def transform_reviews_data(reviews: pd.DataFrame) -> dict:
    """Transform (clean) raw data"""
    # first we clean the data (convert the date to the right format)
    # and retrieve the helpfulness
    # then we handle nulls
    # finally we split the data and return the different bits as a dict

    # debate: it's safer to perform operations on a copy of the DF
    # such that the function can easily retry if needed, because
    # saving a copy means we keep the raw intact. On the other hand,
    # this makes for less efficient memory usage. In this case I'll
    # do it inplace=True to save memory because the data is very easy
    # to retrieve again without consuming resources, but if it came
    # from a finicky API, I'd definitely save a copy to skip the
    # headache of querying it again

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

    reviewer_user_names = reviews[["reviewerID", "reviewerName"]]

    # drop duplicates for reviewers - we'll check later if the PK is unique
    reviewers.drop_duplicates(inplace=True)
    reviewer_user_names.drop_duplicates(inplace=True)

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

    # handle nulls (only doable easily before transforming the data)
    # normally it'd be more memory efficient to leave Nulls as nulls
    # but this makes the data "unfriendlier", because in some query
    # engines aggregations functions treat nulls differently, results
    # may be incomplete, etc.. For this reason I prefer to just
    # write "UNKNOWN" and then the analysts/scientists can more quickly
    # grasp the data (even at the cost of extra database storage <-
    # this trade-off should be discussed with the managers, team, etc.)

    # if PKs are missing, then we drop the entire row

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

    # finally fill missing data/NaNs

    # it's also suboptimal to try to plug the NaNs after we've already
    # converted the data. I should've thought this through a bit better

    result_dict = {
        "reviews_fact_table": reviews_fact_table,
        "reviewers": reviewers,
        "reviewers_user_names": reviewer_user_names,
    }

    return result_dict
