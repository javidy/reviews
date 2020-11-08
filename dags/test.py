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
import gzip, json, csv, psycopg2
from airflow.models import Variable

landing_zone = Variable.get("landing_zone")
#landing_zone = '{{ var.value.landing_zone }}'
# archive_dir = '{{ var.value.archive_dir }}'
metadata_filename = 'NoFileExists' if len(os.listdir(f'{landing_zone}/metadata')) == 0 else os.listdir(f'{landing_zone}/metadata')[0]
# #reviews_filename = os.listdir(f'{landing_zone}/reviews')[0]
input_filenames = [f'{landing_zone}/metadata/{metadata_filename}', f'{landing_zone}/reviews/reviews_filename'] 
# output_dir = '{{ var.value.json_output }}'
# output_filenames = [f'{output_dir}metadata.csv', f'{output_dir}reviews.csv']
# tmpl_search_path = '{{ var.value.sql_path }}'


## Define the DAG object
default_args = {
    'owner': 'airflow',    
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
          dag_id='test',
          default_args=default_args,
          schedule_interval=None,
          tags=['example']
          )

def print_context(**kwargs):
    print(landing_zone)
    print(metadata_filename)
    print(input_filenames[0])


run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

run_this