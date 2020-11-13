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
    , r.rating::float
FROM
           staging.reviews r
INNER JOIN dwh.dim_date d ON d.review_date = r.review_date::date
INNER JOIN dwh.dim_product p ON p.product_id = r.asin AND r.load_dtm >= p.start_dtm AND r.load_dtm < p.end_dtm
INNER JOIN dwh.dim_reviewer rw ON rw.reviewer_id = r.reviewer_id
WHERE
    r.load_dtm = %(execution_date)s;
