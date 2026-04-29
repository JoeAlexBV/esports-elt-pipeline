import os
from typing import Any

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def get_snowflake_connection():
    """
    Create and return a Snowflake connection using environment variables.
    """
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA_RAW"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def load_records_to_snowflake(
    records: list[dict[str, Any]],
    table_name: str,
    column_order: list[str],
) -> None:
    """
    Insert lightly normalized records into a Snowflake table.

    Args:
        records: List of normalized dictionaries, each representing one row.
        table_name: Target Snowflake table name.
        column_order: Ordered list of columns matching both the Snowflake table
                      and the keys expected in each record.
    """
    if not records:
        print(f"No records to load into {table_name}.")
        return

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        columns_sql = ", ".join(column_order)
        placeholders = ", ".join(["%s"] * len(column_order))
        insert_sql = f"""
            INSERT INTO {table_name} ({columns_sql})
            VALUES ({placeholders})
        """

        values_to_insert = [
            tuple(record.get(column) for column in column_order)
            for record in records
        ]

        cursor.executemany(insert_sql, values_to_insert)
        conn.commit()

        print(f"Loaded {len(records)} records into {table_name}.")

    finally:
        cursor.close()
        conn.close()