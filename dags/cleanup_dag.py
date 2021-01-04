from airflow import DAG
import airflow
from operators.dwh_operators import PostgresOperatorWithTemplatedParams
from airflow.models import Variable


tmpl_search_path = Variable.get("sql_path")

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'provide_context': True
}

dag = airflow.DAG(
    'cleanup_dag',
    schedule_interval="@once",
    default_args=args,
    template_searchpath=tmpl_search_path,
    max_active_runs=1)

cleanup = PostgresOperatorWithTemplatedParams(
    task_id='cleanup',
    postgres_conn_id='postgres_dwh',
    sql='cleanup.sql',
    dag=dag)

cleanup