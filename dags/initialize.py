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
from airflow.operators.python_operator import PythonOperator
from airflow import models
from airflow.settings import Session
import logging


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'provide_context': True
}


def initialize_connections():
    logging.info('Creating connections, pool and sql path')

    session = Session()

    def create_new_conn(session, attributes):
        new_conn = models.Connection()
        new_conn.conn_id = attributes.get("conn_id")
        new_conn.conn_type = attributes.get('conn_type')
        new_conn.host = attributes.get('host')
        new_conn.port = attributes.get('port')
        new_conn.schema = attributes.get('schema')
        new_conn.login = attributes.get('login')
        new_conn.set_password(attributes.get('password'))

        session.add(new_conn)
        session.commit()

    create_new_conn(session,
                    {"conn_id": "postgres_dwh",
                     "conn_type": "postgres",
                     "host": "dwh_db",
                     "port": 5432,
                     "schema": "dwh",
                     "login": "dwh_user",
                     "password": "dwh_user"})

    sql_path_var = models.Variable()
    sql_path_var.key = "sql_path"
    sql_path_var.set_val("/usr/local/airflow/sql")
    landing_zone_var = models.Variable()
    landing_zone_var.key = "landing_zone"
    landing_zone_var.set_val("/usr/local/airflow/lz")
    archive_dir_var = models.Variable()
    archive_dir_var.key = "archive_dir"
    archive_dir_var.set_val("/usr/local/airflow/archive")
    output_dir_var = models.Variable()
    output_dir_var.key = "output_dir"
    output_dir_var.set_val("/usr/local/airflow/output")
    session.add(sql_path_var)
    session.add(landing_zone_var)
    session.add(archive_dir_var)
    session.add(output_dir_var)
    session.commit()

    new_pool = models.Pool()
    new_pool.pool = "postgres_dwh"
    new_pool.slots = 10
    new_pool.description = "Allows max. 10 connections to the DWH"

    session.add(new_pool)
    session.commit()

    session.close()

dag = airflow.DAG(
    'initialize',
    schedule_interval="@once",
    default_args=args,
    max_active_runs=1)

t1 = PythonOperator(task_id='initialize_connections',
                    python_callable=initialize_connections,
                    provide_context=False,
                    dag=dag)
