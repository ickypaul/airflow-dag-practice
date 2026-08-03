from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
}

with DAG(
    dag_id="gcs_to_bigquery_ingestion",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    tags=["gcs", "bigquery", "ingestion"],
) as dag:

    # Wait for file
    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_file",
        bucket="my-data-bucket",
        object="sales/sales.csv",
        timeout=300,
        poke_interval=30,
    )

    # Load into BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bq",
        bucket="my-data-bucket",
        source_objects=["sales/sales.csv"],
        destination_project_dataset_table="my_project.sales_dataset.sales",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
    )

    # Validate load
    validate_load = BigQueryCheckOperator(
        task_id="validate_load",
        sql="""
            SELECT COUNT(*)
            FROM `my_project.sales_dataset.sales`
        """,
        use_legacy_sql=False,
    )

    wait_for_file >> load_to_bq >> validate_load