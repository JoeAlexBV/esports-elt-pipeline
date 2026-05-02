"""
Centralized configuration for the esports ELT pipeline.

All environment variable access should go through this module
rather than scattered os.getenv() calls across the codebase.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── PandaScore API ─────────────────────────────────────────────────────────

PANDASCORE_API_KEY = os.getenv("PANDASCORE_API_KEY")
PANDASCORE_BASE_URL = os.getenv("PANDASCORE_BASE_URL",
                                "https://api.pandascore.co")

# ── Snowflake ──────────────────────────────────────────────────────────────

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "ESPORTS")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

SNOWFLAKE_SCHEMA_RAW = os.getenv("SNOWFLAKE_SCHEMA_RAW", "RAW")
SNOWFLAKE_SCHEMA_ANALYTICS = os.getenv("SNOWFLAKE_SCHEMA_ANALYTICS",
                                       "ANALYTICS")
