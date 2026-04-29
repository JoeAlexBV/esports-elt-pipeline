from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from src.extract.pandascore_client import fetch_endpoint_data
from src.load.snowflake_loader import load_records_to_snowflake


def extract_entity(entity: str, **context):
    data = fetch_endpoint_data(entity)
    context["ti"].xcom_push(key=f"{entity}_data", value=data)


def load_entity(entity: str, table_name: str, **context):
    data = context["ti"].xcom_pull(key=f"{entity}_data")
    load_records_to_snowflake(data, table_name)


with DAG(
    dag_id="esports_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["esports", "snowflake", "dbt"],
) as dag:

    with TaskGroup("extract") as extract_group:
        extract_leagues = PythonOperator(
            task_id="extract_leagues",
            python_callable=extract_entity,
            op_kwargs={"entity": "leagues"},
        )

        extract_tournaments = PythonOperator(
            task_id="extract_tournaments",
            python_callable=extract_entity,
            op_kwargs={"entity": "tournaments"},
        )

        extract_teams = PythonOperator(
            task_id="extract_teams",
            python_callable=extract_entity,
            op_kwargs={"entity": "teams"},
        )

        extract_matches = PythonOperator(
            task_id="extract_matches",
            python_callable=extract_entity,
            op_kwargs={"entity": "matches"},
        )

        extract_tournament_rosters = PythonOperator(
            task_id="extract_tournament_rosters",
            python_callable=extract_entity,
            op_kwargs={"entity": "tournament_rosters"},
        )

    with TaskGroup("load") as load_group:
        load_leagues = PythonOperator(
            task_id="load_leagues",
            python_callable=load_entity,
            op_kwargs={"entity": "leagues", "table_name": "LEAGUES"},
        )

        load_tournaments = PythonOperator(
            task_id="load_tournaments",
            python_callable=load_entity,
            op_kwargs={"entity": "tournaments", "table_name": "TOURNAMENTS"},
        )

        load_teams = PythonOperator(
            task_id="load_teams",
            python_callable=load_entity,
            op_kwargs={"entity": "teams", "table_name": "TEAMS"},
        )

        load_matches = PythonOperator(
            task_id="load_matches",
            python_callable=load_entity,
            op_kwargs={"entity": "matches", "table_name": "MATCHES"},
        )

        load_tournament_rosters = PythonOperator(
            task_id="load_tournament_rosters",
            python_callable=load_entity,
            op_kwargs={"entity": "tournament_rosters", "table_name": "TOURNAMENT_ROSTERS"},
        )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dbt/esports_warehouse && dbt run --select staging",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dbt/esports_warehouse && dbt run --select marts",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt/esports_warehouse && dbt test",
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /opt/airflow/dbt/esports_warehouse && dbt docs generate",
    )

    extract_group >> load_group >> dbt_run_staging >> dbt_run_marts >> dbt_test >> dbt_docs_generate