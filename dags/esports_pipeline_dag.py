from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from src.extract.pandascore_client import fetch_endpoint_data
from src.extract.serializers import serialize_records
from src.load.snowflake_loader import load_records_to_snowflake
from src.load.table_configs import TABLE_COLUMN_CONFIGS

# ── Shared helpers ─────────────────────────────────────────────────────────────

DBT_DIR = "/opt/airflow/dbt/esports_warehouse"
DBT_BASE = f"cd {DBT_DIR} && dbt"
DBT_PROFILES = f"--profiles-dir {DBT_DIR}"


def make_extract_task(entity: str):
    """
    Returns a callable for PythonOperator that fetches raw records from
    PandaScore and pushes them to XCom under key 'raw_{entity}'.
    """
    def extract(**context):
        raw_records = fetch_endpoint_data(entity)
        context["ti"].xcom_push(key=f"raw_{entity}", value=raw_records)
    extract.__name__ = f"extract_{entity}"
    return extract


def make_load_task(entity: str, table_name: str):
    """
    Returns a callable for PythonOperator that pulls raw records from XCom,
    serializes them, and loads them into the given Snowflake RAW table.
    """
    def load(**context):
        raw_records = context["ti"].xcom_pull(
            key=f"raw_{entity}",
            task_ids=f"extract_{entity}",
        )
        serialized = serialize_records(entity, raw_records)
        load_records_to_snowflake(
            records=serialized,
            table_name=table_name,
            column_order=TABLE_COLUMN_CONFIGS[table_name.split(".")[-1]],
        )
    load.__name__ = f"load_{entity}"
    return load


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="esports_pipeline",
    description="Full MVP pipeline: leagues, tournaments, teams, matches, rosters",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["esports", "snowflake", "dbt"],
) as dag:

    # ── Extract tasks ──────────────────────────────────────────────────────────

    extract_leagues_task = PythonOperator(
        task_id="extract_leagues",
        python_callable=make_extract_task("leagues"),
    )

    extract_tournaments_task = PythonOperator(
        task_id="extract_tournaments",
        python_callable=make_extract_task("tournaments"),
    )

    extract_teams_task = PythonOperator(
        task_id="extract_teams",
        python_callable=make_extract_task("teams"),
    )

    extract_matches_task = PythonOperator(
        task_id="extract_matches",
        python_callable=make_extract_task("matches"),
    )

    extract_rosters_task = PythonOperator(
        task_id="extract_tournament_rosters",
        python_callable=make_extract_task("tournament_rosters"),
    )

    # ── Load tasks ─────────────────────────────────────────────────────────────

    load_leagues_task = PythonOperator(
        task_id="load_leagues",
        python_callable=make_load_task("leagues", "ESPORTS.RAW.LEAGUES"),
    )

    load_tournaments_task = PythonOperator(
        task_id="load_tournaments",
        python_callable=make_load_task("tournaments", "ESPORTS.RAW.TOURNAMENTS"),
    )

    load_teams_task = PythonOperator(
        task_id="load_teams",
        python_callable=make_load_task("teams", "ESPORTS.RAW.TEAMS"),
    )

    load_matches_task = PythonOperator(
        task_id="load_matches",
        python_callable=make_load_task("matches", "ESPORTS.RAW.MATCHES"),
    )

    load_rosters_task = PythonOperator(
        task_id="load_tournament_rosters",
        python_callable=make_load_task("tournament_rosters", "ESPORTS.RAW.TOURNAMENT_ROSTERS"),
    )

    # ── dbt tasks ──────────────────────────────────────────────────────────────

    # Run all staging models in one step
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"{DBT_BASE} run "
            f"--select stg_leagues stg_tournaments stg_teams stg_matches stg_tournament_rosters "
            f"{DBT_PROFILES}"
        ),
    )

    # Run all mart models in one step
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"{DBT_BASE} run "
            f"--select dim_leagues dim_tournaments dim_teams fact_matches fact_tournament_team_participation "
            f"{DBT_PROFILES}"
        ),
    )

    # Test all models
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{DBT_BASE} test "
            f"--select stg_leagues stg_tournaments stg_teams stg_matches stg_tournament_rosters "
            f"dim_leagues dim_tournaments dim_teams fact_matches fact_tournament_team_participation "
            f"{DBT_PROFILES}"
        ),
    )

    # ── Task dependencies ──────────────────────────────────────────────────────
    #
    # Each extract/load pair runs independently.
    # All loads must complete before dbt begins.
    # dbt runs sequentially: staging → marts → tests.
    # Extract tasks → Load tasks → dbt staging → dbt marts → dbt tests

    extract_leagues_task     >> load_leagues_task
    extract_tournaments_task >> load_tournaments_task
    extract_teams_task       >> load_teams_task
    extract_matches_task     >> load_matches_task
    extract_rosters_task     >> load_rosters_task

    [
        load_leagues_task,
        load_tournaments_task,
        load_teams_task,
        load_matches_task,
        load_rosters_task,
    ] >> dbt_run_staging >> dbt_run_marts >> dbt_test