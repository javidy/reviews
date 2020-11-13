# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
import airflow
from datetime import datetime, timedelta
from operators.dwh_operators import PostgresOperatorWithTemplatedParams
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.models import Variable


args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 11, 8, 12, 30),
    'provide_context': True,
    'depends_on_past': True
}

tmpl_search_path = Variable.get("sql_path")

dag = airflow.DAG(
    'process_dimensions',
    schedule_interval='*/30 * * * *',
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath=tmpl_search_path,
    default_args=args,
    max_active_runs=1,
    catchup=False)

wait_for_metadata_staging = ExternalTaskSensor(
    task_id='wait_for_metadata_staging',
    external_dag_id='staging',
    external_task_id='archive_metadata',
    execution_delta=None,  # Same day as today
    dag=dag)

process_product_dim = PostgresOperatorWithTemplatedParams(
    task_id='process_product_dim',
    postgres_conn_id='postgres_dwh',
    sql='process_product_dimension.sql',
    parameters={"execution_date": "{{ execution_date }}"},
    dag=dag,
    pool='postgres_dwh')

wait_for_reviews_staging = ExternalTaskSensor(
    task_id='wait_for_reviews_staging',
    external_dag_id='staging',
    external_task_id='archive_reviews',
    execution_delta=None,  # Same day as today
    dag=dag)

process_reviewer_dim = PostgresOperatorWithTemplatedParams(
    task_id='process_reviewer_dim',
    postgres_conn_id='postgres_dwh',
    sql='process_reviewer_dimension.sql',
    parameters={"execution_date": "{{ execution_date }}"},
    dag=dag,
    pool='postgres_dwh')

wait_for_metadata_staging >> process_product_dim
wait_for_reviews_staging >> process_reviewer_dim

    

if __name__ == "__main__":
    dag.cli()
