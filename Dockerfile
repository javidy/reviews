FROM apache/airflow:1.10.14 as reviews_airflow

ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgres://airflow:airflow@airflow_db:5432/airflow
ENV AIRFLOW__CORE__AIRFLOW_HOME=/opt/airflow/
ENV AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW_VAR_SQL_PATH=/opt/airflow/sql
ENV AIRFLOW_VAR_LANDING_ZONE=/opt/airflow/landing_zone
ENV AIRFLOW_VAR_ARCHIVE_DIR=/opt/airflow/archive
ENV AIRFLOW_VAR_OUTPUT_DIR=/opt/airflow/output
ENV AIRFLOW_VAR_CONFIG_DIR=/opt/airflow/config
ENV AIRFLOW_CONN_POSTGRES_DWH=postgres://dwh_user:dwh_user@dwh:5432/dwh

WORKDIR $AIRFLOW__CORE__AIRFLOW_HOME
RUN mkdir sql \
 && mkdir output \
 && mkdir config

COPY dags dags
COPY sql sql
COPY config config
COPY scripts/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]

FROM postgres:9.6 as reviews_dwh

ENV POSTGRES_USER=dwh_user
ENV POSTGRES_PASSWORD=dwh_user
ENV POSTGRES_DB=dwh
ENV POSTGRES_HOST_AUTH_METHOD=trust

COPY initialize /docker-entrypoint-initdb.d/

EXPOSE 5432
CMD ["postgres"]