-- Create a temporary table
CREATE TEMP TABLE merge_reviewers (LIKE dwh.dim_reviewer);

-- SCD 1 implementation
INSERT INTO merge_reviewers
SELECT 
    DISTINCT
    0, r.reviewer_id, r.reviewer_name
FROM
    staging.reviews r
WHERE 
    r.review_date >= %(window_start_date)s
AND r.review_date < %(window_end_date)s;

DELETE FROM
    dwh.dim_reviewer target
USING
    merge_reviewers source
WHERE
    target.reviewer_id  = source.reviewer_id;


INSERT INTO dwh.dim_reviewer (reviewer_id, reviewer_name)
SELECT source.reviewer_id, source.reviewer_name FROM merge_reviewers source;