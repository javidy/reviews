-- SCD 0 implementation
INSERT INTO dwh.dim_reviewer (reviewer_id, reviewer_name)
SELECT DISTINCT r.reviewer_id, COALESCE(r.reviewer_name, '')
FROM staging.reviews r
WHERE 1=1
  AND r.load_dtm = %(execution_date)s
ON CONFLICT (reviewer_id)
DO NOTHING;