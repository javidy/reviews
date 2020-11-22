INSERT INTO staging.reviews_execution_log
(
 execution_ts
,metadata_file
,reviews_file
,status
,description
,log_ts
)
values
(
 %(execution_date)s
,%(metadata_filename)s
,%(reviews_filename)s
,%(execution_status)s
,%(execution_descr)s
,current_timestamp
);