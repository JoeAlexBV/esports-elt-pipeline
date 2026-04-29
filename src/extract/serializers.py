from datetime import datetime, timezone


def utc_now_iso() -> str:
    """Return the current UTC timestamp as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def serialize_league(record: dict) -> dict:
    """
    Transform a raw PandaScore league record into a lightly normalized row
    for insertion into ESPORTS.RAW.LEAGUES.
    """
    return {
        "league_id": record.get("id"),
        "league_name": record.get("name"),
        "league_slug": record.get("slug"),
        "league_url": record.get("url"),
        "modified_at": record.get("modified_at"),
        "ingested_at": utc_now_iso(),
    }


def serialize_records(entity: str, records: list[dict]) -> list[dict]:
    """
    Route raw API records to the correct serializer by entity name.
    """
    serializer_map = {
        "leagues": serialize_league,
    }

    if entity not in serializer_map:
        raise ValueError(f"No serializer defined for entity: {entity}")

    serializer = serializer_map[entity]
    return [serializer(record) for record in records]