from airflow import DAG
import airflow
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from operators.staging_operators import PostgresBulkLoadOperator
from operators.dwh_operators import PostgresOperatorWithTemplatedParams
from datetime import datetime, timedelta
import os.path
from os import path
import gzip, json, csv, psycopg2, glob
from airflow.models import Variable



landing_zone = Variable.get("landing_zone")
archive_dir = Variable.get("archive_dir")
tmpl_search_path = Variable.get("sql_path")
output_dir = Variable.get("output_dir")
lz_metadata_dir, lz_reviews_dir = (f'{landing_zone}/metadata', f'{landing_zone}/reviews')
pattern = r".json.gz"

def get_source_filename(source_dir):
    files = [_ for _ in os.listdir(source_dir) if _.endswith(pattern)]
    if len(files) != 0:
        return os.path.join(source_dir, files[0])
    return "SouceFileDoesNotExist"

input_filenames = [get_source_filename(lz_metadata_dir), get_source_filename(lz_reviews_dir)]
output_filenames = [os.path.join(output_dir, 'metadata.csv'), os.path.join(output_dir, 'reviews.csv')]


connection = psycopg2.connect(
    host="dwh_db",
    database="dwh",
    user="dwh_user",
    password="dwh_user",
)
connection.autocommit = False
cur = connection.cursor()
sqlstr_metadata = "COPY staging.metadata (asin, img_url, description, categories, title, price, sales_rank, brand, load_dtm) FROM STDIN DELIMITER '\t' CSV"
sqlstr_reviews = "COPY staging.reviews (reviewer_id, asin, reviewer_name, helpful, review_text, rating, summary, unix_review_time, review_date) FROM STDIN DELIMITER '\t' CSV"


def parse(file_name):
  if path.exists(file_name):
    print(f'File {file_name} found. Starting processing')
    g = gzip.open(file_name, 'rb')
    for l in g:
      yield eval(l)
  else:
    print(f'File {path} not found')

def parse_strict(path):
  g = gzip.open(path, 'r')
  for l in g:
    yield json.dumps(eval(l))    


def load_to_db(execution_date, **kwargs):
    count, total = (0, 0)    
    input_filename, output_filename, data_type = (kwargs["input"], kwargs["output"], kwargs["type"])    

    if data_type == "metadata":
      sqlstr = sqlstr_metadata
      print(f'Processing {data_type}')
    else:
      sqlstr = sqlstr_reviews
      print(f'Processing {data_type}')

    csv.register_dialect("tabs", delimiter="\t")
    data_file = open(output_filename, "w", newline='')
    csv_writer = csv.writer(data_file, dialect="tabs")

    for l in parse(input_filename):
      if (data_type == "metadata"):
        csv_writer.writerow([l.get("asin",""), l.get("imUrl", ""), l.get("description",""), l.get("categories",""), l.get("title",""), l.get("price", ""), l.get("salesRank",""), l.get("brand",""), execution_date])
      else:
        csv_writer.writerow([l.get("reviewerID",""), l.get("asin", ""), l.get("reviewerName",""), l.get("helpful",""), l.get("reviewText",""), l.get("overall",""), l.get("summary",""), l.get("unixReviewTime",""), l.get("reviewTime","")])

      count = count + 1
      if (count == 100000):    
        data_file.close()    
        with open(output_filename) as f:
          cur.copy_expert(sqlstr, f)
        total = total + count
        print("Copied data to database table. Starting new iteration")
        print(f'Total number of copied lines {total}')
        data_file = open(output_filename, 'w', newline='')
        csv_writer = csv.writer(data_file, dialect="tabs")    
        count = 0
    data_file.close()    
    with open(output_filename) as f:
      cur.copy_expert(sqlstr, f)
    total = total + count
    print(f'Total number of copied lines {total}')
    connection.commit()
    connection.close()

## Define the DAG object
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8, 12, 30),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('staging',
          default_args=default_args,
          schedule_interval='*/30 * * * *',
          template_searchpath=tmpl_search_path,
          catchup=False
          )


stage_metadata = PythonOperator(task_id='stage_metadata',
                    python_callable=load_to_db,
                    op_kwargs = {"input": input_filenames[0], "output": output_filenames[0], "type": "metadata"},
                    provide_context=True,
                    dag=dag)

archive_metadata = BashOperator(task_id='archive_metadata', 
                                bash_command="/usr/local/airflow/src/archive.sh ",
                                env={'input_filename': input_filenames[0], 'archive_dir': archive_dir},
                                dag=dag)

archive_reviews = BashOperator(task_id='archive_reviews', 
                                bash_command="/usr/local/airflow/src/archive.sh ",
                                env={'input_filename': input_filenames[1], 'archive_dir': archive_dir},
                                dag=dag)                              

stage_reviews = PythonOperator(task_id='stage_reviews',
                    python_callable=load_to_db,
                    op_kwargs = {"input": input_filenames[1], "output": output_filenames[1], "type": "reviews"},
                    provide_context=True,
                    dag=dag)                    


stage_metadata >> archive_metadata
stage_reviews >> archive_reviews