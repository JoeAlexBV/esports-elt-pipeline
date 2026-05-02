"""
Unit tests for src/load/snowflake_loader.py

Strategy:
- Mock snowflake.connector.connect so we never touch a real database
- Test the happy path, empty records, and connection/cursor lifecycle
"""

import pytest
from unittest.mock import patch, MagicMock

from src.load.snowflake_loader import load_records_to_snowflake

# ── Helpers ────────────────────────────────────────────────────────────────


def make_mock_conn():
    """Build a mock Snowflake connection with cursor, commit, close."""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


# ── Happy path ─────────────────────────────────────────────────────────────


@patch("src.load.snowflake_loader.get_snowflake_connection")
def test_load_inserts_correct_number_of_records(mock_get_conn):
    """executemany is called once with all rows."""
    mock_conn, mock_cursor = make_mock_conn()
    mock_get_conn.return_value = mock_conn

    records = [
        {
            "league_id": 1,
            "league_name": "LCS",
            "league_slug": "lcs",
            "league_url": None,
            "modified_at": None,
            "ingested_at": "2024-01-01",
        },
        {
            "league_id": 2,
            "league_name": "LEC",
            "league_slug": "lec",
            "league_url": None,
            "modified_at": None,
            "ingested_at": "2024-01-01",
        },
    ]
    column_order = [
        "league_id",
        "league_name",
        "league_slug",
        "league_url",
        "modified_at",
        "ingested_at",
    ]

    load_records_to_snowflake(records, "ESPORTS.RAW.LEAGUES", column_order)

    mock_cursor.executemany.assert_called_once()
    _, args, _ = mock_cursor.executemany.mock_calls[0]
    inserted_rows = args[1]
    assert len(inserted_rows) == 2


@patch("src.load.snowflake_loader.get_snowflake_connection")
def test_load_builds_correct_insert_sql(mock_get_conn):
    """INSERT SQL contains the correct table name and column names."""
    mock_conn, mock_cursor = make_mock_conn()
    mock_get_conn.return_value = mock_conn

    records = [
        {
            "league_id": 1,
            "league_name": "LCS",
            "league_slug": "lcs",
            "league_url": None,
            "modified_at": None,
            "ingested_at": "now",
        }
    ]
    column_order = [
        "league_id",
        "league_name",
        "league_slug",
        "league_url",
        "modified_at",
        "ingested_at",
    ]

    load_records_to_snowflake(records, "ESPORTS.RAW.LEAGUES", column_order)

    sql_used = mock_cursor.executemany.call_args[0][0]
    assert "ESPORTS.RAW.LEAGUES" in sql_used
    assert "league_id" in sql_used
    assert "league_name" in sql_used


@patch("src.load.snowflake_loader.get_snowflake_connection")
def test_load_maps_column_order_correctly(mock_get_conn):
    """Values are inserted in the order defined by column_order."""
    mock_conn, mock_cursor = make_mock_conn()
    mock_get_conn.return_value = mock_conn

    records = [{"b": 2, "a": 1, "c": 3}]
    column_order = ["a", "b", "c"]

    load_records_to_snowflake(records, "ESPORTS.RAW.LEAGUES", column_order)

    rows = mock_cursor.executemany.call_args[0][1]
    assert rows[0] == (1, 2, 3)


@patch("src.load.snowflake_loader.get_snowflake_connection")
def test_load_commits_after_insert(mock_get_conn):
    """conn.commit() is called after executemany."""
    mock_conn, mock_cursor = make_mock_conn()
    mock_get_conn.return_value = mock_conn

    records = [
        {
            "league_id": 1,
            "league_name": "LCS",
            "league_slug": "lcs",
            "league_url": None,
            "modified_at": None,
            "ingested_at": "now",
        }
    ]
    column_order = [
        "league_id",
        "league_name",
        "league_slug",
        "league_url",
        "modified_at",
        "ingested_at",
    ]

    load_records_to_snowflake(records, "ESPORTS.RAW.LEAGUES", column_order)

    mock_conn.commit.assert_called_once()


@patch("src.load.snowflake_loader.get_snowflake_connection")
def test_load_closes_cursor_and_connection(mock_get_conn):
    """cursor.close() and conn.close() are always called."""
    mock_conn, mock_cursor = make_mock_conn()
    mock_get_conn.return_value = mock_conn

    records = [
        {
            "league_id": 1,
            "league_name": "LCS",
            "league_slug": "lcs",
            "league_url": None,
            "modified_at": None,
            "ingested_at": "now",
        }
    ]
    column_order = [
        "league_id",
        "league_name",
        "league_slug",
        "league_url",
        "modified_at",
        "ingested_at",
    ]

    load_records_to_snowflake(records, "ESPORTS.RAW.LEAGUES", column_order)

    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


# ── Empty records ──────────────────────────────────────────────────────────


@patch("src.load.snowflake_loader.get_snowflake_connection")
def test_load_skips_insert_when_no_records(mock_get_conn):
    # When records is empty, no connection is opened and nothing is inserted.
    load_records_to_snowflake([], "ESPORTS.RAW.LEAGUES", ["league_id"])

    mock_get_conn.assert_not_called()


# ── Error handling ─────────────────────────────────────────────────────────


@patch("src.load.snowflake_loader.get_snowflake_connection")
def test_load_closes_connection_even_on_error(mock_get_conn):
    # cursor.close() and conn.close() are called even if executemany raises.
    mock_conn, mock_cursor = make_mock_conn()
    mock_get_conn.return_value = mock_conn
    mock_cursor.executemany.side_effect = Exception("DB exploded")

    records = [
        {
            "league_id": 1,
            "league_name": "LCS",
            "league_slug": "lcs",
            "league_url": None,
            "modified_at": None,
            "ingested_at": "now",
        }
    ]
    column_order = [
        "league_id",
        "league_name",
        "league_slug",
        "league_url",
        "modified_at",
        "ingested_at",
    ]

    with pytest.raises(Exception, match="DB exploded"):
        load_records_to_snowflake(records, "ESPORTS.RAW.LEAGUES", column_order)

    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()
