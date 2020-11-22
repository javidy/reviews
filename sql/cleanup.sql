truncate table staging.metadata;
truncate table staging.reviews;
truncate table staging.reviews_execution_log;
truncate table dwh.dim_product;
truncate table dwh.dim_reviewer cascade;
truncate table dwh.fact_reviews;