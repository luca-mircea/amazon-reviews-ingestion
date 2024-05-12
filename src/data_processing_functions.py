"""
Here we keep a lot of small data processing
functions used for/by the transform bit of the pipeline
"""

import ast
import warnings
from typing import Tuple

import pandas as pd

from src.validate import PKNotUnique, SchemaMismatch

warnings.simplefilter(action="ignore", category=FutureWarning)


def process_reviews_raw_columns(reviews: pd.DataFrame) -> pd.DataFrame:
    """Fix the helpfulness, reviewTime, review_id columns"""
    # parse the date out of the strangely formatted string
    reviews["review_date_parsed_as_int"] = [
        int(  # we like int bcs. it's more memory efficient
            date_string.split(",")[1].strip()  # year
            + date_string.split(",")[0].split(" ")[0]  # month
            + date_string.split(",")[0].split(" ")[1].zfill(2)
        )
        for date_string in reviews["reviewTime"]
    ]

    # compile helpfulness of reviews by parsing the
    # count of YES and NO from the list
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
    # first check if the schema matches, if not raise error
    if [col_name for col_name in target_df.columns] != [
        col_name for col_name in data_type_schema.keys()
    ]:
        raise SchemaMismatch

    # then we cast the dtypes
    for column_name, data_type in data_type_schema.items():
        target_df[column_name] = target_df[column_name].astype(data_type)

    # technically no need to return bcs. modifying in place, but more readable
    return target_df


def process_nulls(target_data: pd.DataFrame, nulls_handling: dict) -> pd.DataFrame:
    """Handle NULLs according to the defined schema"""
    for column_name, handling in nulls_handling.items():
        if handling == "PK":  # if PK, we should have no dups
            count_entries = target_data[column_name].value_counts()
            if (count_entries != 1).any():
                raise PKNotUnique(f"{column_name} - supposedly PK - is not unique!")
        elif handling == "DROP":  # just drop NULLS -> pointless rows
            target_data = target_data[~pd.isnull(target_data[column_name])]
            # don't forget to reset the index before returning!
        elif handling == "PK Multiple":  # for tables with many to X connections
            # we drop Null but don't raise duplication error
            # (this technically makes it not-a-PK anymore)
            target_data = target_data[~pd.isnull(target_data[column_name])]
        else:
            target_data[column_name].fillna(handling, inplace=True)

    return target_data


def add_date_string_column(target_df: pd.DataFrame) -> pd.DataFrame:
    """Add date string column based on date int"""
    target_df["date_string"] = [
        str(date_as_int)[:4] + "-" + str(date_as_int)[4:6] + "-" + str(date_as_int)[6:8]
        for date_as_int in target_df["date_as_int"]
    ]

    return target_df


def process_products(target_df: pd.DataFrame) -> pd.DataFrame:
    """Process products (add currency column)"""
    target_df["currency"] = "USD"  # I'm assuming this
    # In reality, I'd check and try to make sure I add
    # the correct currency

    return target_df


def process_sales_ranks(target_df: pd.DataFrame) -> pd.DataFrame:
    """Process sales rank -> flatten the dict"""
    target_df["dict_of_sales_rank"] = [
        (
            ast.literal_eval(str(sales_rank_dict))
            if sales_rank_dict is not None and str(sales_rank_dict) != "nan"
            else {"Unranked"}
        )
        for sales_rank_dict in target_df["salesrank"]
    ]

    # get the category out of the flattened dict
    target_df["category_ranked"] = [
        next(iter(ranking_dict)) if len(ranking_dict) > 0 else "Unranked"
        for ranking_dict in target_df["dict_of_sales_rank"]
    ]

    # and also get the ranking out of the flattened dict
    target_df["ranking"] = [
        (
            next(iter(ranking_dict.values()))
            if len(ranking_dict) > 0 and ranking_dict != {"Unranked"}
            else -1
        )
        for ranking_dict in target_df["dict_of_sales_rank"]
    ]

    target_df = target_df[["asin", "category_ranked", "ranking"]].copy()

    return target_df


def process_categories(target_df: pd.DataFrame) -> pd.DataFrame:
    """Process the categories into a long format"""
    print("Processing the categories - this usually takes a while")
    # turn categories into a list (which will be nested)
    target_df["categories_as_list"] = [
        ast.literal_eval(str(category_lists_string))
        for category_lists_string in target_df["categories"]
    ]

    # then flatten the list
    target_df["categories_as_flat_list"] = [
        [element for sub_list in nested_list for element in sub_list]
        for nested_list in target_df["categories_as_list"]
    ]

    # At some point I'd figure out a way to do this
    # better than a for-loop
    categories_procesed = pd.DataFrame()

    for row_index in target_df.index:
        mini_df = pd.DataFrame(
            {
                "item_id": target_df.loc[row_index, "asin"],
                "category": target_df.loc[row_index, "categories_as_flat_list"],
            },
            index=range(len(target_df["categories_as_flat_list"][row_index])),
        )

        categories_procesed = pd.concat(
            [categories_procesed, mini_df], axis=0, ignore_index=True
        )
        # yeesh, this for-loop is a bit slow...

    return categories_procesed


def process_related_items(target_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Flatten the related items into two tables"""
    # first create a list of items bought together
    target_df["bought_together"] = [
        (
            ast.literal_eval(str(list_of_related_items))["also_bought"]
            if str(list_of_related_items) != "nan"
            and "also_bought" in (ast.literal_eval(str(list_of_related_items))).keys()
            else 0
        )
        for list_of_related_items in target_df["related"]
    ]

    # it seems like sometimes the key is "bought_together" instead of
    # 'also_bought' <- in a future version we'd handle this better

    target_df["also_viewed"] = [
        (
            ast.literal_eval(str(list_of_related_items))["also_viewed"]
            if str(list_of_related_items) != "nan"
            and "also_viewed" in (ast.literal_eval(str(list_of_related_items))).keys()
            else 0
        )
        for list_of_related_items in target_df["related"]
    ]

    bought_together = pd.DataFrame()
    also_viewed = pd.DataFrame()

    for row_index in target_df.index:
        if target_df.loc[row_index, "bought_together"] == 0:
            pass  # if no 'bought_together' we do next
        else:
            mini_bought = pd.DataFrame(
                {
                    "item_id": target_df.loc[row_index, "asin"],
                    "bought_together": target_df.loc[row_index, "bought_together"],
                },
                index=range(len(target_df["bought_together"][row_index])),
            )

            bought_together = pd.concat(
                [bought_together, mini_bought], axis=0, ignore_index=True
            )

        if target_df.loc[row_index, "also_viewed"] == 0:
            pass
        else:
            mini_viewed = pd.DataFrame(
                {
                    "item_id": target_df.loc[row_index, "asin"],
                    "also_viewed": target_df.loc[row_index, "also_viewed"],
                },
                index=range(len(target_df["also_viewed"][row_index])),
            )

            also_viewed = pd.concat(
                [also_viewed, mini_viewed], axis=0, ignore_index=True
            )

    return bought_together, also_viewed
