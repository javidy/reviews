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
config_dir = Variable.get("config_dir")
pattern = r".json.gz"
output_filenames = [os.path.join(output_dir, 'metadata.csv'), os.path.join(output_dir, 'reviews.csv')]
db_config_file = os.path.join(config_dir, 'db_config.json')

logger = logging.getLogger()
# Change format of handler for the logger
logger.handlers[0].setFormatter(logging.Formatter('%(message)s'))

# read dwh details into dictionary
with open(db_config_file, 'r') as f:
    db_config = json.load(f)

connection = psycopg2.connect(**db_config)
connection.autocommit = False
cur = connection.cursor()
sqlstr_metadata = "COPY staging.metadata (asin, img_url, description, category, title, price, sales_rank, brand, load_dtm) FROM STDIN DELIMITER '\t' CSV"
sqlstr_reviews = "COPY staging.reviews (reviewer_id, asin, reviewer_name, helpful, review_text, rating, summary, unix_review_time, review_date, load_dtm) FROM STDIN DELIMITER '\t' CSV"

## Define the DAG object
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8, 12, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

def branch_func(**kwargs):    
    ti = kwargs['ti']
    paths_dict = ti.xcom_pull(task_ids="start_task", key="file_paths_dict")
    src_file_names = ti.xcom_pull(task_ids="start_task", key="src_file_names")
    exec_dir = kwargs["exec_dir"]

    if paths_dict != None and files_exist(paths_dict):
        logging.info(f">> Both files exist in landing zone: {paths_dict}. Proceeding to staging")
        target_path = f"{archive_dir}/{exec_dir}"
        os.mkdir(target_path)
        paths_dict["metadata"]["target_path"] = os.path.join(target_path, src_file_names.get("metadata"))
        paths_dict["reviews"]["target_path"] = os.path.join(target_path, src_file_names.get("reviews"))

        ti.xcom_push(key="file_paths_dict", value=paths_dict)
        logging.info(f">> Created directory: {target_path}")
        logging.info(f">> File paths dict prepared and pushed to XCOM {paths_dict}")
        return "load_staging"
    # when no files found move on to "no_files_branch"
    elif paths_dict == None:
        logging.info(">> No files found in landing zone.Proceeding to no_files_found branch")
        return "no_files_found"
    # when only one of the files were found another NOT
    logging.info(f">> Only one of the files exists in landing zone: {paths_dict}. Proceeding to archiving")
    target_path = f"{archive_dir}/{exec_dir}"
    os.mkdir(target_path)
    paths_dict["metadata"]["target_path"] = os.path.join(target_path, src_file_names.get("metadata"))
    paths_dict["reviews"]["target_path"] = os.path.join(target_path, src_file_names.get("reviews"))
    ti.xcom_push(key="file_paths_dict", value=paths_dict)
    logging.info(f">> Created directory: {target_path}")
    logging.info(f">> File paths dict prepared and pushed to XCOM {paths_dict}")
    return "one_of_the_files_missing"

def files_exist(files):
    return path.exists(files.get("metadata").get("src_path")) and path.exists(files.get("reviews").get("src_path"))

def detect_src_files(**kwargs):
    src_dir = kwargs["src_dir"]
    files = [_ for _ in os.listdir(src_dir) if _.endswith(pattern)]
    ti = kwargs["ti"]
    logging.info(f">> Source Directory: {src_dir}")

    if len(files) != 0:
        start = files[0].find("_") + 1
        category = files[0][start:]
        src_file_names = {"metadata": f"meta_{category}", "reviews": f"reviews_{category}"}
        logging.info(f">> File {files[0]} found. Preparing dictionary")
        file_paths_dict = {
            "metadata": {
                "src_path": os.path.join(src_dir, src_file_names.get("metadata"))
            },
            "reviews": {
                "src_path": os.path.join(src_dir, src_file_names.get("reviews")),
            }
        }
        ti.xcom_push(key="file_paths_dict", value=file_paths_dict)
        ti.xcom_push(key="src_file_names", value=src_file_names)
        logging.info(f">> File paths dict pushed to XCOM {file_paths_dict}")
        logging.info(f">> File names pushed to XCOM {src_file_names}")
    else:
        logging.info(">> No files detected in landing zone")

def archive(**kwargs):
    ti = kwargs['ti']
    d = ti.xcom_pull(task_ids="branch_task", key="file_paths_dict")
    # when one of the files were found but another not, for instance, review_Baby.json.gz was provided but meta_Baby.json.gz not
    if d != None:
        logging.info(">> Archiving files...")
        for key,value in d.items():
            src_path = value.get("src_path")
            target_path = value.get("target_path")
            # check source file existence before attempting to move
            # in case one of the files is missing only existing file can be moved
            # this is to avid exception, might be better handled by try catch
            if path.exists(src_path):
                shutil.move(src_path, target_path)
                logging.info(f">> Archived file {src_path} to {target_path}")
    # when no files found in landing zone
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
      src_filename = ti.xcom_pull(task_ids="branch_task", key="file_paths_dict").get("metadata").get("src_path")
    else:
      sqlstr = sqlstr_reviews
      src_filename = ti.xcom_pull(task_ids="branch_task", key="file_paths_dict").get("reviews").get("src_path")
    logging.info(f">> Loading data type: {data_type}")
    logging.info(f">> Source filename {src_filename}")
    logging.info(f">> Output filename {output_filename}")

    csv.register_dialect("tabs", delimiter="\t")
    data_file = open(output_filename, "w", newline='')
    csv_writer = csv.writer(data_file, dialect="tabs")

    for l in parse(src_filename):
      if (data_type == "metadata"):
        csv_writer.writerow([l.get("asin",""), l.get("imUrl", "")[:500], l.get("description",""), l.get("categories",[[""]])[0][0][:500], l.get("title","")[:500], l.get("price", ""), l.get("salesRank",""), l.get("brand","")[:500], execution_date])
      else:
        csv_writer.writerow([l.get("reviewerID",""), l.get("asin", ""), l.get("reviewerName","")[:500], l.get("helpful","")[:50], l.get("reviewText",""), l.get("overall",""), l.get("summary","")[:1000], l.get("unixReviewTime",""), l.get("reviewTime",""), execution_date])
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
    cur.close()
    connection.close()
    return src_filename

dag = DAG('reviews_dag',
          default_args=default_args,
          schedule_interval='*/20 * * * *',
          template_searchpath=tmpl_search_path,
          max_active_runs=1,
          catchup=False,
          is_paused_upon_creation=False
          )

start_op = PythonOperator(
    task_id='start_task',
    provide_context=True,
    op_kwargs = {"src_dir": landing_zone},
    python_callable=detect_src_files,
    dag=dag)

branch_op = BranchPythonOperator(
    task_id='branch_task',
    provide_context=True,
    op_kwargs = {"exec_dir": '{{ macros.ds_format(ts_nodash, "%Y%m%dT%H%M%S", "%Y-%m-%d-%H-%M-%S") }}'},
    python_callable=branch_func,
    dag=dag)

load_staging = DummyOperator(task_id='load_staging', dag=dag)
staging_completed = DummyOperator(task_id='staging_completed', dag=dag)
no_files_found = DummyOperator(task_id='no_files_found', dag=dag)
one_of_the_files_missing = DummyOperator(task_id='one_of_the_files_missing', dag=dag)

archive_file = PythonOperator(
    task_id='archive_file',
    python_callable=archive,
    provide_context=True,
    dag=dag)

stage_metadata = PythonOperator(task_id='stage_metadata',
                    python_callable=load_to_db,
                    op_kwargs = {"output": output_filenames[0], "type": "metadata"},
                    provide_context=True,
                    dag=dag)

stage_reviews = PythonOperator(task_id='stage_reviews',
                    python_callable=load_to_db,
                    op_kwargs = {"output": output_filenames[1], "type": "reviews"},
                    provide_context=True,
                    dag=dag)

archive_files = PythonOperator(
    task_id='archive_files',
    python_callable=archive,
    provide_context=True,
    trigger_rule="none_skipped",
    dag=dag)

process_product_dim = PostgresOperatorWithTemplatedParams(
    task_id='process_product_dim',
    postgres_conn_id='postgres_dwh',
    sql='process_product_dimension.sql',
    parameters={"execution_date": "{{ execution_date }}"},
    dag=dag)

process_reviewer_dim = PostgresOperatorWithTemplatedParams(
    task_id='process_reviewer_dim',
    postgres_conn_id='postgres_dwh',
    sql='process_reviewer_dimension.sql',
    parameters={"execution_date": "{{ execution_date }}"},
    dag=dag)

process_fact = PostgresOperatorWithTemplatedParams(
    task_id='process_fact',
    postgres_conn_id='postgres_dwh',
    sql='process_review_fact.sql',
    parameters={"execution_date": "{{ execution_date }}"},
    dag=dag)

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
    dag=dag)

log_error_1 = PostgresOperatorWithTemplatedParams(
    task_id='log_error_1',
    postgres_conn_id='postgres_dwh',
    sql='insert_execution_log.sql',
    parameters={
        "execution_date": "{{ execution_date }}",
        "metadata_filename": '{{ ti.xcom_pull(task_ids="start_task", key="src_file_names").get("metadata") }}',
        "reviews_filename": '{{ ti.xcom_pull(task_ids="start_task", key="src_file_names").get("reviews") }}',
        "execution_status": "Error",
        "execution_descr": f"Only one file was found in landing zone: {landing_zone}",
        },
    dag=dag)

log_error_2 = PostgresOperatorWithTemplatedParams(
    task_id='log_error_2',
    postgres_conn_id='postgres_dwh',
    sql='insert_execution_log.sql',
    parameters={
        "execution_date": "{{ execution_date }}",
        "metadata_filename": '{{ ti.xcom_pull(task_ids="start_task", key="src_file_names").get("metadata") }}',
        "reviews_filename": '{{ ti.xcom_pull(task_ids="start_task", key="src_file_names").get("reviews") }}',
        "execution_status": "Error",
        "execution_descr": f"Error in processing one of the files.Staging not completed successfully",
        },
    trigger_rule="all_failed",
    dag=dag)

log_info = PostgresOperatorWithTemplatedParams(
    task_id='log_info',
    postgres_conn_id='postgres_dwh',
    sql='insert_execution_log.sql',
    parameters={
        "execution_date": "{{ execution_date }}",
        "metadata_filename": 'NULL',
        "reviews_filename": 'NULL',
        "execution_status": "Info",
        "execution_descr": f"No files found in landing zone: {landing_zone}",
        },
    dag=dag)

start_op >> branch_op >> [load_staging, no_files_found, one_of_the_files_missing]
no_files_found >> log_info
one_of_the_files_missing >> archive_file >> log_error_1
load_staging >> stage_metadata >> staging_completed
load_staging >> stage_reviews  >> staging_completed
staging_completed >> archive_files
staging_completed >> log_error_2
staging_completed >> process_reviewer_dim >> process_fact >> log_success
staging_completed >> process_product_dim >> process_fact >> log_success