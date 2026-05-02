"""
General-purpose utility functions for the esports ELT pipeline.
"""

from datetime import datetime, timezone


def utc_now_iso() -> str:
    """Return the current UTC timestamp as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def chunk_list(lst: list, size: int) -> list[list]:
    """
    Split a list into chunks of a given size.

    Useful for batching large record sets before loading to Snowflake.

    Example:
        chunk_list([1, 2, 3, 4, 5], 2) → [[1, 2], [3, 4], [5]]
    """
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def safe_get(d: dict, *keys, default=None):
    """
    Safely traverse nested dicts without raising KeyError.

    Example:
        safe_get(record, "league", "name") → record["league"]["name"] or None
    """
    for key in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(key, default)
    return d
