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
from contextlib import contextmanager

import pytest

from src.extract.pandascore_client import fetch_endpoint_data
from src.load.snowflake_loader import get_snowflake_connection

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RAW_LEAGUES_TABLE = "ESPORTS.RAW.LEAGUES"
ANALYTICS_DIM_LEAGUES_TABLE = "ESPORTS.ANALYTICS.DIM_LEAGUES"

# ---------------------------------------------------------------------------
# Module-level marks
# ---------------------------------------------------------------------------

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.getenv("PANDASCORE_API_KEY")
        or not os.getenv("SNOWFLAKE_ACCOUNT"),
        reason=(
            "Integration tests require PANDASCORE_API_KEY "
            "and SNOWFLAKE_ACCOUNT env vars."
        ),
    ),
]

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def snowflake_conn():
    # Reusable Snowflake connection for all integration tests in this module.
    conn = get_snowflake_connection()
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@contextmanager
def get_cursor(conn):
    # Context manager that opens a cursor and ensures it is closed after use.
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_pandascore_leagues_api_returns_data():
    # PandaScore /leagues endpoint returns a non-empty list of records.
    records = fetch_endpoint_data("leagues", per_page=5)

    assert isinstance(records, list)
    assert len(records) > 0
    assert "id" in records[0]
    assert "name" in records[0]


def test_snowflake_connection_is_reachable(snowflake_conn):
    # Snowflake connection can be established with provided credentials.
    with get_cursor(snowflake_conn) as cursor:
        cursor.execute("SELECT CURRENT_USER(), CURRENT_DATABASE()")
        result = cursor.fetchone()
        assert result is not None


def test_leagues_raw_table_has_rows(snowflake_conn):
    # ESPORTS.RAW.LEAGUES contains at least one row after pipeline runs.
    with get_cursor(snowflake_conn) as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {RAW_LEAGUES_TABLE}")
        count = cursor.fetchone()[0]
        assert count > 0, f"{RAW_LEAGUES_TABLE} is empty has the pipeline run?"


def test_dim_leagues_analytics_table_has_rows(snowflake_conn):
    # ESPORTS.ANALYTICS.DIM_LEAGUES contains at least one row after dbt runs.
    with get_cursor(snowflake_conn) as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {ANALYTICS_DIM_LEAGUES_TABLE}")
        count = cursor.fetchone()[0]
        assert count > 0, f"{ANALYTICS_DIM_LEAGUES_TABLE} is empty — \
            has dbt run?"
