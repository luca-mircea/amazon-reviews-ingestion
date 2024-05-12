-- this one is too easy lol
-- btw, I noticed that the brand column is sparsely populated
-- which is another project I could do at some point
SELECT
    brand
    , CONCAT(SUBSTR(CAST(review_date AS VARCHAR(10)), 1, 6), "01") AS month_string
    , AVG(rating) AS average_rating
FROM facts.reviews
GROUP BY brand, month_string
