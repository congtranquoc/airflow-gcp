import datetime
import json
from airflow import DAG
from airflow.operators import bash_operator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Define dag variables
project_id = 'manifest-setup-397505'
staging_dataset = 'tiki_dwh_staging'
dwh_dataset = 'tiki_dwh_production'
gs_bucket = 'de-airflow-bucket'
table_name = 'tiki-data'

# Define dag_args
default_dag_args = {
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
    "email": ["quoccong-workspace@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
}

# Define dag
dag = DAG(
    'tiki-cloud-data-lake-pipeline-process',
    schedule_interval="0 7 * * *",
    default_args=default_dag_args,
)

start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

extract_data = bash_operator.BashOperator(
    task_id = "extract_data"
    , bash_command = "/home/quoccong-workspace/airflow_project/scripts/tiki/export_data.sh "
    , dag=dag
)

load_data_to_gcs = bash_operator.BashOperator(
    task_id = "load_data_to_gcs"
    , bash_command = "/home/quoccong-workspace/airflow_project/scripts/tiki/migrate_data.sh "
    , dag=dag
    ,
)
load_data_to_staging_warehouse = GCSToBigQueryOperator(
    task_id="load_data_to_staging_warehouse",
    bucket=gs_bucket,
    source_objects=["tiki_export.json"],
    destination_project_dataset_table=f'{project_id}:{staging_dataset}.{table_name}',
    source_format="NEWLINE_DELIMITED_JSON",
    write_disposition="WRITE_TRUNCATE",
    max_bad_records=100,
    autodetect=True,
    encoding="UTF-8",
    create_disposition="CREATE_IF_NEEDED",
    dag=dag,
)

# Check loaded data not null
check_datas = BigQueryExecuteQueryOperator(
    task_id='check_datas',
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM `{project_id}.{staging_dataset}.{table_name}`',
    dag=dag,
)

loaded_data_to_staging = DummyOperator(
    task_id='loaded_data_to_staging',
    dag=dag,
)

# Transform, load, and check fact data
transform_and_load_data = BigQueryExecuteQueryOperator(
    task_id='create_tiki_datas',
    use_legacy_sql=False,
    params={
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset,
        'table_name': table_name,
    },
    sql='./sql/tiki_dashboard_query.sql',
    dag=dag,
)

start_pipeline >> extract_data >> load_data_to_gcs >> load_data_to_staging_warehouse >> check_datas >> loaded_data_to_staging >> transform_and_load_data


