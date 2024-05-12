-- get average rating per item_id per month (1 item per row_month)
-- then join categories on item_id (item_categories items per row_month)
-- then average rating per category per month (1 item per row_month)
WITH item_ratings AS (
    SELECT
        item_id
        , CONCAT(SUBSTR(CAST(review_date AS VARCHAR(10)), 1, 6), "01") AS month_string
        , AVG(rating) AS average_item_rating
    FROM facts.reviews
    GROUP BY item_id, CONCAT(SUBSTR(CAST(review_date AS VARCHAR(10)), 1, 6), "01")
),
item_ratings_with_categories AS (
    SELECT
        item_ratings.item_id AS item_id
        , categories.category AS item_category -- many categories to one item
        , CAST(item_ratings.month_string AS INT) AS month_int
        , item_ratings.average_item_rating AS average_item_rating
    FROM item_ratings
    LEFT JOIN dimensions.product_categories AS categories
    ON item_ratings.item_id = categories.item_id
)

SELECT
    item_category AS category
    , month_int
    , AVG(average_item_rating)
FROM item_ratings_with_categories
