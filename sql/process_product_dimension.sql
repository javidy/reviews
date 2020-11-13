-- Create a temporary table for product operations
CREATE TEMP TABLE merge_product (LIKE dwh.dim_product);

-- The staging table should only have at most one record per product
INSERT INTO merge_product
SELECT
    DISTINCT
    0
    , p.asin
    , COALESCE(p.title, '')
    , COALESCE(p.brand, '')
    , COALESCE(p.price, 0)
    , COALESCE(p.img_url, '')
    , COALESCE(p.categories, '')
    , COALESCE(p.sales_rank, '')
    , b.price_bucket_key
    , TIMESTAMP %(execution_date)s
    , TIMESTAMP '9999-01-01 00:00:00'
FROM
    staging.metadata p INNER JOIN
    dwh.dim_price_bucket b ON (COALESCE(p.price, 0) >= b.range_start AND COALESCE(p.price, 0) < COALESCE(b.range_end, p.price))
WHERE 
    p.load_dtm = %(execution_date)s;

-- Update records by setting an end date
-- only do this when start_dtm < to be inserted dtm,
-- it is currently the active record
-- and when values are not equal (see EXCEPT)
UPDATE
        dwh.dim_product target
SET
        end_dtm = source.start_dtm
FROM
        merge_product source
WHERE
        target.product_id  = source.product_id
AND     target.end_dtm    >= TIMESTAMP '9999-01-01 00:00:00'
AND     target.start_dtm   < source.start_dtm
AND EXISTS (
        SELECT source.product_id, source.product_title, source.product_brand, source.product_price, source.product_image_url, source.product_category, source.product_sales_rank
        EXCEPT
        SELECT target.product_id, target.product_title, target.product_brand, target.product_price, target.product_image_url, target.product_category, target.product_sales_rank);

-- Remove records that we do not want to insert.
-- What we do want to insert are all new records (nothing in target),
-- or when there is something in target, only when the record is newer
-- than most recent and when the old record is closed.
-- The closure should have been done in the previous step.
DELETE FROM
    merge_product source
USING
    dwh.dim_product target
WHERE
    target.product_id  = source.product_id
AND target.end_dtm    >= TIMESTAMP '9999-01-01 00:00:00'
AND target.start_dtm  <= source.start_dtm
AND EXISTS (
        SELECT source.product_id, source.product_title, source.product_brand, source.product_price, source.product_image_url, source.product_category, source.product_sales_rank
        INTERSECT
        SELECT target.product_id, target.product_title, target.product_brand, target.product_price, target.product_image_url, target.product_category, target.product_sales_rank);

-- Now perform the inserts. These are new products and records for products
-- Where these may have changed.
INSERT INTO dwh.dim_product (product_id, product_title, product_brand, product_price, product_image_url, product_category, product_sales_rank, price_bucket_key, start_dtm )
SELECT
      source.product_id
    , source.product_title
    , source.product_brand
    , source.product_price
    , source.product_image_url
    , source.product_category
    , source.product_sales_rank
    , source.price_bucket_key
    , source.start_dtm
FROM
    merge_product source;

-- The temp table is automatically removed at the end of the session...


