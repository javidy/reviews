\c dwh;

DROP TABLE IF EXISTS staging.reviews;
DROP TABLE IF EXISTS staging.metadata;
DROP TABLE IF EXISTS staging.reviews_execution_log;
DROP TABLE IF EXISTS dwh.fact_reviews;
DROP TABLE IF EXISTS dwh.dim_date;
DROP TABLE IF EXISTS dwh.dim_product;
DROP TABLE IF EXISTS dwh.dim_reviewer;
DROP TABLE IF EXISTS dwh.dim_price_bucket;
DROP SEQUENCE IF EXISTS seq_product;


CREATE TABLE staging.reviews (
    reviewer_id     VARCHAR(50)  NULL,
    asin            VARCHAR(50)  NULL,
    reviewer_name VARCHAR(500)  NULL,
    helpful         VARCHAR(50)  NULL,
    review_text     TEXT NULL,
    rating          VARCHAR(50)  NULL,    
    summary         VARCHAR(1000)  NULL,
    unix_review_time  INTEGER  NULL,
    review_date      DATE,
    load_dtm        TIMESTAMP  NULL DEFAULT NOW()
);

CREATE TABLE staging.metadata (
    asin            VARCHAR(50)  NULL,
    img_url         VARCHAR(500)  NULL,
    description     TEXT NULL,
    category        VARCHAR(500) NULL,
    title           VARCHAR(500) NULL,
    price           NUMERIC(5, 2) NULL,
    sales_rank      VARCHAR(500)  NULL,
    brand           VARCHAR(500)  NULL,
    load_dtm        TIMESTAMP  NULL DEFAULT NOW()
);

CREATE SEQUENCE seq_execution_log START 1;
GRANT USAGE, SELECT ON SEQUENCE seq_execution_log TO dwh_user;

CREATE TABLE staging.reviews_execution_log (
    execution_id     INTEGER PRIMARY KEY NOT NULL DEFAULT nextval('seq_execution_log'),
    execution_ts     TIMESTAMP     NOT NULL,
    metadata_file 	 VARCHAR(100)  NULL,
    reviews_file     VARCHAR(100)  NULL,
    status           VARCHAR(50)   NOT NULL,
    description      VARCHAR(500)  NULL,    
    log_ts           TIMESTAMP     NOT NULL
);

GRANT USAGE ON SCHEMA staging TO dwh_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO dwh_user;

CREATE SEQUENCE seq_price_bucket START 1;
GRANT USAGE, SELECT ON SEQUENCE seq_price_bucket TO dwh_user;

CREATE TABLE dwh.dim_price_bucket (
    price_bucket_key  INTEGER PRIMARY KEY NOT NULL DEFAULT nextval('seq_price_bucket'),
    range_start INTEGER NOT NULL,
    range_end   INTEGER NULL,
    bucket       VARCHAR(50)
);

CREATE SEQUENCE seq_reviewer START 1;
GRANT USAGE, SELECT ON SEQUENCE seq_reviewer TO dwh_user;

CREATE TABLE dwh.dim_reviewer (
    reviewer_key  INTEGER PRIMARY KEY NOT NULL DEFAULT nextval('seq_reviewer'),
    reviewer_id  VARCHAR(50) NOT NULL,
    reviewer_name VARCHAR(500) NOT NULL
);
CREATE UNIQUE INDEX ON dwh.dim_reviewer (reviewer_id);

CREATE SEQUENCE seq_product START 1;
GRANT USAGE, SELECT ON SEQUENCE seq_product TO dwh_user;

CREATE TABLE dwh.dim_product (
    product_key  INTEGER PRIMARY KEY NOT NULL DEFAULT nextval('seq_product'),
    product_id   VARCHAR(50) NOT NULL,
    product_title  VARCHAR(500) NOT NULL,
    product_brand VARCHAR(500) NOT NULL,
    product_price NUMERIC(5,2) NOT NULL,
    product_image_url VARCHAR(500) NOT NULL,
    product_category VARCHAR(500) NOT NULL,
    product_sales_rank VARCHAR(500) NOT NULL,
    price_bucket_key INTEGER NOT NULL REFERENCES dwh.dim_price_bucket (price_bucket_key),
    start_dtm    TIMESTAMP NOT NULL,
    end_dtm      TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-01-01'
);

CREATE SEQUENCE seq_date START 1;
GRANT USAGE, SELECT ON SEQUENCE seq_date TO dwh_user;

CREATE TABLE dwh.dim_date (
    date_key      INTEGER PRIMARY KEY NOT NULL DEFAULT nextval('seq_date'),
    review_date   DATE NOT NULL,
    year          INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    month_name    VARCHAR(32) NOT NULL,
    day_of_month  INTEGER NOT NULL,
    day_of_year   INTEGER NOT NULL,
    week_day_name VARCHAR(32) NOT NULL,
    week          INTEGER NOT NULL,
    fmt_datum     VARCHAR(20) NOT NULL,
    quarter       VARCHAR(2) NOT NULL,
    year_quarter  VARCHAR(7) NOT NULL,
    year_month    VARCHAR(7) NOT NULL,
    year_week     VARCHAR(7) NOT NULL,
    month_start   DATE NOT NULL,
    month_end     DATE NOT NULL
);

CREATE TABLE dwh.fact_reviews (
    date_key   INTEGER NOT NULL REFERENCES dwh.dim_date (date_key),
    reviewer_key     INTEGER NOT NULL REFERENCES dwh.dim_reviewer (reviewer_key),
    product_key      INTEGER NOT NULL REFERENCES dwh.dim_product (product_key),
    rating           FLOAT NOT NULL
);

-- Constraint: fact_reviews_unique_key

-- ALTER TABLE dwh.fact_reviews DROP CONSTRAINT fact_reviews_unique_key;

ALTER TABLE dwh.fact_reviews
    ADD CONSTRAINT fact_reviews_unique_key UNIQUE (date_key, reviewer_key, product_key);


GRANT USAGE ON SCHEMA dwh TO dwh_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dwh TO dwh_user;

INSERT INTO dwh.dim_price_bucket(
    range_start,
    range_end,
    bucket
)
SELECT 
    0,
    100,
    '0-100'
UNION 
SELECT 
    100,
    200,
    '100-200'
UNION 
SELECT 
    200,
    500,
    '200-500'
UNION 
SELECT 
    500,
    1000,
    '500-1000'
UNION 
SELECT 
    1000,
    NULL,
    '1000+';

INSERT INTO dwh.dim_date(
    review_date,
    year,
    month,
    month_name,
    day_of_month,
    day_of_year,
    week_day_name,
    week,
    fmt_datum,
    quarter,
    year_quarter,
    year_month,
    year_week,
    month_start,
    month_end
)
SELECT
	datum AS review_date,
	EXTRACT(YEAR FROM datum) AS year,
	EXTRACT(MONTH FROM datum) AS month,
	-- Localized month name
	to_char(datum, 'TMMonth') AS month_name,
	EXTRACT(DAY FROM datum) AS day_of_month,
	EXTRACT(doy FROM datum) AS day_of_year,
	-- Localized weekday
	to_char(datum, 'TMDay') AS week_day_name,
	-- ISO calendar week
	EXTRACT(week FROM datum) AS week,
	to_char(datum, 'dd. mm. yyyy') AS fmt_datum,
	'Q' || to_char(datum, 'Q') AS quarter,
	to_char(datum, 'yyyy/"Q"Q') AS year_quarter,
	to_char(datum, 'yyyy/mm') AS year_month,
	-- ISO calendar year and week
	to_char(datum, 'iyyy/IW') AS year_week,
	-- Start and end of the month of this date
	datum + (1 - EXTRACT(DAY FROM datum))::INTEGER AS month_start,
	(datum + (1 - EXTRACT(DAY FROM datum))::INTEGER + '1 month'::INTERVAL)::DATE - '1 day'::INTERVAL AS month_end
FROM (
	-- There are 3 leap years in this range, so calculate 365 * 10 + 3 records
	SELECT '1996-01-01'::DATE + SEQUENCE.DAY AS datum
	FROM generate_series(0,10000) AS SEQUENCE(DAY)
	GROUP BY SEQUENCE.DAY
     ) DQ
ORDER BY 1;
