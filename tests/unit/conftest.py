from unittest.mock import MagicMock
import sys

# Mock snowflake.connector before any test module is imported.
# This prevents ModuleNotFoundError during collection since
# snowflake-connector-python is not installed in the CI environment.
sys.modules["snowflake"] = MagicMock()
sys.modules["snowflake.connector"] = MagicMock()
sys.modules["snowflake.connector.errors"] = MagicMock()
