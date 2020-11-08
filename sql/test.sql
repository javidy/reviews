insert into dwh.metadata_dim
select * from staging.metadata
where load_dtm = %(window_start_date)s;