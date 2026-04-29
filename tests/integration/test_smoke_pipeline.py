"""
Integration smoke test for the esports ELT pipeline.

This test verifies the full extract → serialize → load flow
against a real Snowflake instance using real API credentials.

NOT run in CI — requires live credentials and network access.
Run manually to verify end-to-end connectivity:

    pytest tests/integration/ -v -s

Skipped automatically if PANDASCORE_API_KEY or SNOWFLAKE_ACCOUNT
are not set in the environment.
"""
import os
import pytest

from src.extract.pandascore_client import fetch_endpoint_data
from src.extract.serializers import serialize_records
from src.load.snowflake_loader import load_records_to_snowflake, get_snowflake_connection
from src.load.table_configs import TABLE_COLUMN_CONFIGS


# Skip entire module if credentials are not present
pytestmark = pytest.mark.skipif(
    not os.getenv("PANDASCORE_API_KEY") or not os.getenv("SNOWFLAKE_ACCOUNT"),
    reason="Integration tests require PANDASCORE_API_KEY and SNOWFLAKE_ACCOUNT env vars.",
)


def test_pandascore_leagues_api_returns_data():
    """PandaScore /leagues endpoint returns a non-empty list of records."""
    records = fetch_endpoint_data("leagues", per_page=5)

    assert isinstance(records, list)
    assert len(records) > 0
    assert "id" in records[0]
    assert "name" in records[0]


def test_snowflake_connection_is_reachable():
    """Snowflake connection can be established with provided credentials."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT CURRENT_USER(), CURRENT_DATABASE()")
        result = cursor.fetchone()
        assert result is not None
    finally:
        cursor.close()
        conn.close()


def test_leagues_raw_table_has_rows():
    """ESPORTS.RAW.LEAGUES contains at least one row after pipeline runs."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM ESPORTS.RAW.LEAGUES")
        count = cursor.fetchone()[0]
        assert count > 0, "ESPORTS.RAW.LEAGUES is empty — has the pipeline run?"
    finally:
        cursor.close()
        conn.close()


def test_dim_leagues_analytics_table_has_rows():
    """ESPORTS.ANALYTICS.DIM_LEAGUES contains at least one row after dbt runs."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM ESPORTS.ANALYTICS.DIM_LEAGUES")
        count = cursor.fetchone()[0]
        assert count > 0, "ESPORTS.ANALYTICS.DIM_LEAGUES is empty — has dbt run?"
    finally:
        cursor.close()
        conn.close()