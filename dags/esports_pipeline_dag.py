from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from src.extract.pandascore_client import fetch_endpoint_data
from src.extract.serializers import serialize_records
from src.load.snowflake_loader import load_records_to_snowflake
from src.load.table_configs import TABLE_COLUMN_CONFIGS


def extract_leagues(**context):
    """
    Task 1:
    Pull raw league records from the PandaScore API.

    We store the raw response in XCom so the next task can retrieve it.
    XCom is Airflow's way of passing small pieces of data between tasks.
    """
    raw_records = fetch_endpoint_data("leagues")
    context["ti"].xcom_push(key="raw_leagues", value=raw_records)


def load_leagues(**context):
    """
    Task 2:
    Pull the raw leagues data from XCom, serialize it into our
    lightly normalized warehouse format, and load it into Snowflake.
    """
    # Pull the raw API response that was pushed by extract_leagues
    raw_records = context["ti"].xcom_pull(
        key="raw_leagues",
        task_ids="extract_leagues",
    )

    # Convert raw PandaScore JSON into our normalized Snowflake row structure
    serialized_records = serialize_records("leagues", raw_records)

    # Insert into Snowflake RAW table
    load_records_to_snowflake(
        records=serialized_records,
        table_name="ESPORTS.RAW.LEAGUES",
        column_order=TABLE_COLUMN_CONFIGS["LEAGUES"],
    )


with DAG(
    dag_id="esports_pipeline",
    description="Leagues-only MVP pipeline: extract, load, transform, test",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["esports", "snowflake", "dbt"],
) as dag:

    # Task 1: extract raw leagues data from PandaScore
    extract_leagues_task = PythonOperator(
        task_id="extract_leagues",
        python_callable=extract_leagues,
    )

    # Task 2: serialize and load leagues into Snowflake RAW.LEAGUES
    load_leagues_task = PythonOperator(
        task_id="load_leagues",
        python_callable=load_leagues,
    )

    # Task 3: build the dbt staging model
    # This creates/refreshes ESPORTS.ANALYTICS.STG_LEAGUES
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            "cd /opt/airflow/dbt/esports_warehouse && "
            "dbt run --select stg_leagues "
            "--profiles-dir /opt/airflow/dbt/esports_warehouse"
        ),
    )

    # Task 4: build the dbt mart/dimension model
    # This creates/refreshes ESPORTS.ANALYTICS.DIM_LEAGUES
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd /opt/airflow/dbt/esports_warehouse && "
            "dbt run --select dim_leagues "
            "--profiles-dir /opt/airflow/dbt/esports_warehouse"
        ),
    )

    # Task 5: run dbt tests against both models
    # This validates constraints like unique/not_null that we defined in schema.yml
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt/esports_warehouse && "
            "dbt test --select stg_leagues dim_leagues "
            "--profiles-dir /opt/airflow/dbt/esports_warehouse"
        ),
    )

    # Define task order:
    # extract -> load -> dbt staging -> dbt mart -> dbt tests
    extract_leagues_task >> load_leagues_task >> dbt_run_staging >> dbt_run_marts >> dbt_test