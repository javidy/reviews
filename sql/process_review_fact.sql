-- For the specific window of interest,
-- remove the lines from the fact table.
DELETE FROM 
    dwh.fact_reviews fr
USING 
    dwh.dim_date dt 
WHERE
    fr.date_key = dt.date_key
AND dt.review_date >= %(window_start_date)s 
AND dt.review_date < %(window_end_date)s;

-- Repopulate fact reviews with data specific for that date
-- making sure to select the correct dimension keys.
INSERT INTO dwh.fact_reviews ( 
      date_key    
    , product_key    
    , reviewer_key
    , rating) 
SELECT
      d.date_key    
    , p.product_key
    , rw.reviewer_key
    , r.rating
FROM
           staging.reviews r
INNER JOIN dwh.dim_date d ON d.review_date = r.review_date::date
INNER JOIN dwh.dim_product p ON p.product_id = r.asin AND r.review_date >= p.start_dtm AND r.review_date < p.end_dtm
INNER JOIN dwh.dim_reviewer rw ON rw.reviewer_id = r.reviewer_id
WHERE
    r.review_date >= %(window_start_date)s 
AND r.review_date < %(window_end_date)s;
