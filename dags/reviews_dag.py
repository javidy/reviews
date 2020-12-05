from airflow import DAG, macros
import airflow
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from operators.dwh_operators import PostgresOperatorWithTemplatedParams
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import os.path, shutil
from os import path
import gzip, json, csv, psycopg2, glob, logging
from airflow.models import Variable



landing_zone = Variable.get("landing_zone")
archive_dir = Variable.get("archive_dir")
tmpl_search_path = Variable.get("sql_path")
output_dir = Variable.get("output_dir")
pattern = r".json.gz"
output_filenames = [os.path.join(output_dir, 'metadata.csv'), os.path.join(output_dir, 'reviews.csv')]

logger = logging.getLogger()
# Change format of handler for the logger
logger.handlers[0].setFormatter(logging.Formatter('%(message)s'))

connection = psycopg2.connect(
    host="dwh_db",
    database="dwh",
    user="dwh_user",
    password="dwh_user",
)
connection.autocommit = False
cur = connection.cursor()
sqlstr_metadata = "COPY staging.metadata (asin, img_url, description, categories, title, price, sales_rank, brand, load_dtm) FROM STDIN DELIMITER '\t' CSV"
sqlstr_reviews = "COPY staging.reviews (reviewer_id, asin, reviewer_name, helpful, review_text, rating, summary, unix_review_time, review_date, load_dtm) FROM STDIN DELIMITER '\t' CSV"

## Define the DAG object
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8, 12, 30),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

def branch_func(**kwargs):    
    ti = kwargs['ti']
    source_filenames = ti.xcom_pull(task_ids="start_task", key="source_files")
    if source_filenames != None and files_exist(source_filenames):
        logging.info(f">> Both files {source_filenames} exists in source directory. Proceeding to staging")
        return "load_staging"
    logging.info(">> Files not found.Proceeding to log error")
    return "no_files_found"
    
def files_exist(files):
    return path.exists(files.get("meta").get("src")) and path.exists(files.get("reviews").get("src"))

def detect_src_files(**kwargs):
    src_dir = kwargs["src_dir"]
    files = [_ for _ in os.listdir(src_dir) if _.endswith(pattern)]
    ti = kwargs["ti"]
    exec_dir = kwargs["exec_dir"]    
    logging.info(f">> Source Directory: {src_dir}")
    logging.info(f">> Execution Directory: {exec_dir}")

    processed_dir = f"{archive_dir}/processed/{exec_dir}"
    not_processed_dir = f"{archive_dir}/not_processed/{exec_dir}"
    os.mkdir(processed_dir)
    os.mkdir(not_processed_dir)
    logging.info(f">> Created directories: {processed_dir} & {not_processed_dir}")

    if len(files) != 0:
        start = files[0].find("_") + 1
        category = files[0][start:]
        file_names = {"meta": f"meta_{category}", "reviews": f"reviews_{category}"}
        logging.info(f">> File {files[0]} found. Preparing dictionary")
        source_files = {
            "meta": {
                "src": os.path.join(src_dir, file_names.get("meta")),
                "processed_dir": os.path.join(processed_dir, file_names.get("meta")),
                "not_processed_dir": os.path.join(not_processed_dir, file_names.get("meta"))
            },
            "reviews": {
                "src": os.path.join(src_dir, file_names.get("reviews")),
                "processed_dir": os.path.join(processed_dir, file_names.get("reviews")),
                "not_processed_dir": os.path.join(not_processed_dir, file_names.get("reviews"))            
            }
        }
        ti.xcom_push(key="source_files", value=source_files)
        logging.info(f">> Dictionary prepared and pushed to XCOM {source_files}")

def archive(**kwargs):
    ti = kwargs['ti']
    data_type = kwargs["type"]
    d = ti.xcom_pull(task_ids="start_task", key="source_files")
    if d != None and data_type == None:
        logging.info(">> Archiving not processed files...")
        for key,value in d.items():
            src_file = value.get("src", "")
            if path.exists(src_file):
                shutil.move(src_file, value.get("not_processed_dir"))
                logging.info(f">> Archived not processed file {src_file} to {value.get('not_processed_dir')}")
    elif data_type != None:
        src_file = d.get(data_type).get("src")
        logging.info(f">> Data type: {data_type} provided. Archiving only {src_file}")
        shutil.move(src_file, d.get(data_type).get("processed_dir"))
        logging.info(f">> Archived processed file {src_file} to {d.get(data_type).get('processed_dir')}")
    else:
        logging.info(">> No files to archive")

def parse(file_name):
  if path.exists(file_name):
    logging.info(f">> File {file_name} found. Starting processing")
    g = gzip.open(file_name, 'rb')
    for l in g:
      yield eval(l)
  else:
    logging.info(f">> File {path} not found")

def load_to_db(execution_date, **kwargs):
    count, total = (0, 0)
    ti, output_filename, data_type = (kwargs["ti"], kwargs["output"], kwargs["type"])
    if data_type == "metadata":
      sqlstr = sqlstr_metadata
      input_filename = ti.xcom_pull(task_ids="start_task", key="source_files").get("meta").get("src")
    else:
      sqlstr = sqlstr_reviews
      input_filename = ti.xcom_pull(task_ids="start_task", key="source_files").get("reviews").get("src")
    logging.info(f">> Loading data type: {data_type}")
    logging.info(f">> Input filename {input_filename}")
    logging.info(f">> Output filename {output_filename}")

    csv.register_dialect("tabs", delimiter="\t")
    data_file = open(output_filename, "w", newline='')
    csv_writer = csv.writer(data_file, dialect="tabs")

    for l in parse(input_filename):
      if (data_type == "metadata"):
        csv_writer.writerow([l.get("asin",""), l.get("imUrl", ""), l.get("description",""), l.get("categories",[[""]])[0][0], l.get("title",""), l.get("price", ""), l.get("salesRank",""), l.get("brand",""), execution_date])
      else:
        csv_writer.writerow([l.get("reviewerID",""), l.get("asin", ""), l.get("reviewerName",""), l.get("helpful",""), l.get("reviewText",""), l.get("overall",""), l.get("summary",""), l.get("unixReviewTime",""), l.get("reviewTime",""), execution_date])
      count = count + 1
      if (count == 100000):    
        data_file.close()    
        with open(output_filename) as f:
          cur.copy_expert(sqlstr, f)
        total = total + count
        logging.info("Copied data to database table. Starting new iteration")
        logging.info(f"Total number of copied lines {total}")
        data_file = open(output_filename, 'w', newline='')
        csv_writer = csv.writer(data_file, dialect="tabs")    
        count = 0
    data_file.close()    
    with open(output_filename) as f:
      cur.copy_expert(sqlstr, f)
    total = total + count
    logging.info(f"Total number of copied lines {total}")
    connection.commit()
    connection.close()
    return input_filename

dag = DAG('reviews_dag',
          default_args=default_args,
          schedule_interval='*/15 * * * *',
          template_searchpath=tmpl_search_path,
          max_active_runs=1,
          catchup=False
          )

start_op = PythonOperator(
    task_id='start_task',
    provide_context=True,
    op_kwargs = {"src_dir": landing_zone, "exec_dir": '{{ macros.ds_format(ts_nodash, "%Y%m%dT%H%M%S", "%Y-%m-%d-%H-%M-%S") }}'},
    python_callable=detect_src_files,
    dag=dag)

branch_op = BranchPythonOperator(
    task_id='check_if_files_exist',
    provide_context=True,
    python_callable=branch_func,
    dag=dag)

load_staging = DummyOperator(task_id='load_staging', dag=dag)
no_files_found = DummyOperator(task_id='no_files_found', dag=dag)

archive_files = PythonOperator(
    task_id='archive_files',
    python_callable=archive,
    op_kwargs = {"type": None},
    provide_context=True,
    dag=dag)

stage_metadata = PythonOperator(task_id='stage_metadata',
                    python_callable=load_to_db,
                    op_kwargs = {"output": output_filenames[0], "type": "metadata"},
                    provide_context=True,
                    dag=dag)

archive_metadata = PythonOperator(
    task_id='archive_metadata',
    python_callable=archive,
    op_kwargs = {"type": "meta"},
    provide_context=True,
    dag=dag)

stage_reviews = PythonOperator(task_id='stage_reviews',
                    python_callable=load_to_db,
                    op_kwargs = {"output": output_filenames[1], "type": "reviews"},
                    provide_context=True,
                    dag=dag)

archive_reviews = PythonOperator(
    task_id='archive_reviews',
    python_callable=archive,
    op_kwargs = {"type": "reviews"},
    provide_context=True,
    dag=dag)

process_product_dim = PostgresOperatorWithTemplatedParams(
    task_id='process_product_dim',
    postgres_conn_id='postgres_dwh',
    sql='process_product_dimension.sql',
    parameters={"execution_date": "{{ execution_date }}"},
    dag=dag,
    pool='postgres_dwh')

process_reviewer_dim = PostgresOperatorWithTemplatedParams(
    task_id='process_reviewer_dim',
    postgres_conn_id='postgres_dwh',
    sql='process_reviewer_dimension.sql',
    parameters={"execution_date": "{{ execution_date }}"},
    dag=dag,
    pool='postgres_dwh')

process_fact = PostgresOperatorWithTemplatedParams(
    task_id='process_fact',
    postgres_conn_id='postgres_dwh',
    sql='process_review_fact.sql',
    parameters={"execution_date": "{{ execution_date }}"},
    dag=dag,
    pool='postgres_dwh')

log_success = PostgresOperatorWithTemplatedParams(
    task_id='log_success',
    postgres_conn_id='postgres_dwh',
    sql='insert_execution_log.sql',
    parameters={
        "execution_date": "{{ execution_date }}",
        "metadata_filename": '{{ ti.xcom_pull(task_ids="stage_metadata") }}',
        "reviews_filename": '{{ ti.xcom_pull(task_ids="stage_reviews") }}',
        "execution_status": "Success",
        "execution_descr": "Files successfully loaded to dwh",
        },
    dag=dag,
    pool='postgres_dwh')

log_error = PostgresOperatorWithTemplatedParams(
    task_id='log_error',
    postgres_conn_id='postgres_dwh',
    sql='insert_execution_log.sql',
    parameters={
        "execution_date": "{{ execution_date }}",
        "metadata_filename": '{{ ti.xcom_pull(task_ids="stage_metadata") }}',
        "reviews_filename": '{{ ti.xcom_pull(task_ids="stage_reviews") }}',
        "execution_status": "Error",
        "execution_descr": f"No files found in landing zone: {landing_zone}",
        },
    dag=dag,
    pool='postgres_dwh')

start_op >> branch_op >> [load_staging, no_files_found]
no_files_found >> archive_files >> log_error
load_staging >> stage_metadata >> archive_metadata >> process_product_dim >> process_fact >> log_success
load_staging >> stage_reviews >> archive_reviews >> process_reviewer_dim >> process_fact >> log_success