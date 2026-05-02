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


def serialize_tournament(record: dict) -> dict:
    """
    Transform a raw PandaScore tournament record into a lightly normalized row
    for insertion into ESPORTS.RAW.TOURNAMENTS.
    """
    league = record.get("league") or {}
    videogame = record.get("videogame") or {}
    return {
        "tournament_id": record.get("id"),
        "tournament_name": record.get("name"),
        "tournament_slug": record.get("slug"),
        "tournament_status": record.get("status"),
        "begin_at": record.get("begin_at"),
        "end_at": record.get("end_at"),
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "videogame_name": videogame.get("name"),
        "modified_at": record.get("modified_at"),
        "ingested_at": utc_now_iso(),
    }


def serialize_team(record: dict) -> dict:
    """
    Transform a raw PandaScore team record into a lightly normalized row
    for insertion into ESPORTS.RAW.TEAMS.
    """
    return {
        "team_id": record.get("id"),
        "team_name": record.get("name"),
        "team_acronym": record.get("acronym"),
        "team_slug": record.get("slug"),
        "team_location": record.get("location"),
        "modified_at": record.get("modified_at"),
        "ingested_at": utc_now_iso(),
    }


def serialize_match(record: dict) -> dict:
    """
    Transform a raw PandaScore match record into a lightly normalized row
    for insertion into ESPORTS.RAW.MATCHES.

    PandaScore nests opponents as:
        [{"opponent": {"id": ..., "name": ...}, "type": "Team"}, ...]
    We flatten opponent_1 and opponent_2 into top-level columns.
    """
    tournament = record.get("tournament") or {}
    league = record.get("league") or {}
    videogame = record.get("videogame") or {}
    winner = record.get("winner") or {}

    opponents = record.get("opponents") or []
    opp_1 = opponents[0].get("opponent") if len(opponents) > 0 else {}
    opp_2 = opponents[1].get("opponent") if len(opponents) > 1 else {}

    return {
        "match_id": record.get("id"),
        "match_name": record.get("name"),
        "match_status": record.get("status"),
        "scheduled_at": record.get("scheduled_at"),
        "begin_at": record.get("begin_at"),
        "end_at": record.get("end_at"),
        "tournament_id": tournament.get("id"),
        "tournament_name": tournament.get("name"),
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "videogame_name": videogame.get("name"),
        "number_of_games": record.get("number_of_games"),
        "winner_id": winner.get("id"),
        "winner_type": record.get("winner_type"),
        "opponent_1_id": opp_1.get("id") if opp_1 else None,
        "opponent_1_name": opp_1.get("name") if opp_1 else None,
        "opponent_2_id": opp_2.get("id") if opp_2 else None,
        "opponent_2_name": opp_2.get("name") if opp_2 else None,
        "modified_at": record.get("modified_at"),
        "ingested_at": utc_now_iso(),
    }


def serialize_tournament_roster(record: dict) -> list[dict]:
    """
    Transform a raw PandaScore tournament record into one row per team
    for insertion into ESPORTS.RAW.TOURNAMENT_ROSTERS.

    One tournament with N teams → N rows.

    Note: player-level data requires the /tournaments/{id}/rosters endpoint.
    That call is out of scope for this MVP. player_id, player_name, and
    player_role are stored as NULL and can be backfilled in a future iteration.
    """
    teams = record.get("teams") or []
    tournament_id = record.get("id")
    tournament_name = record.get("name")
    modified_at = record.get("modified_at")
    ingested_at = utc_now_iso()

    return [
        {
            "tournament_id": tournament_id,
            "tournament_name": tournament_name,
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "player_id": None,
            "player_name": None,
            "player_role": None,
            "modified_at": modified_at,
            "ingested_at": ingested_at,
        }
        for team in teams
    ]


def serialize_records(entity: str, records: list[dict]) -> list[dict]:
    """
    Route raw API records to the correct serializer by entity name.

    tournament_rosters is handled separately because one tournament record
    expands into multiple rows (one per team), so we flatten the results.
    """
    if entity == "tournament_rosters":
        rows = []
        for record in records:
            rows.extend(serialize_tournament_roster(record))
        return rows

    serializer_map = {
        "leagues": serialize_league,
        "tournaments": serialize_tournament,
        "teams": serialize_team,
        "matches": serialize_match,
    }

    if entity not in serializer_map:
        raise ValueError(f"No serializer defined for entity: {entity}")

    return [serializer_map[entity](record) for record in records]
