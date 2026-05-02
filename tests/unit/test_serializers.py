"""
Unit tests for src/extract/serializers.py

Strategy:
- Test each serializer with realistic PandaScore-shaped input
- Verify all fields map correctly
- Verify None/missing fields are handled safely
- Verify tournament_rosters fan-out (1 tournament → N team rows)
"""

import pytest
from unittest.mock import patch

from src.extract.serializers import (
    serialize_league,
    serialize_tournament,
    serialize_team,
    serialize_match,
    serialize_tournament_roster,
    serialize_records,
)

FAKE_NOW = "2026-01-01T00:00:00+00:00"


# ── serialize_league ───────────────────────────────────────────────────────


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_league_maps_all_fields(mock_now):
    raw = {
        "id": 42,
        "name": "LCS",
        "slug": "lcs",
        "url": "https://lolesports.com",
        "modified_at": "2024-01-01T00:00:00Z",
    }
    result = serialize_league(raw)

    assert result["league_id"] == 42
    assert result["league_name"] == "LCS"
    assert result["league_slug"] == "lcs"
    assert result["league_url"] == "https://lolesports.com"
    assert result["modified_at"] == "2024-01-01T00:00:00Z"
    assert result["ingested_at"] == FAKE_NOW


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_league_handles_missing_fields(mock_now):
    """Missing fields should produce None values, not KeyErrors."""
    result = serialize_league({})

    assert result["league_id"] is None
    assert result["league_name"] is None
    assert result["league_url"] is None


# ── serialize_tournament ───────────────────────────────────────────────────


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_tournament_maps_all_fields(mock_now):
    raw = {
        "id": 10,
        "name": "Worlds 2024",
        "slug": "worlds-2024",
        "status": "finished",
        "begin_at": "2024-10-01T00:00:00Z",
        "end_at": "2024-11-02T00:00:00Z",
        "league": {"id": 1, "name": "LCS"},
        "videogame": {"name": "League of Legends"},
        "modified_at": "2024-11-03T00:00:00Z",
    }
    result = serialize_tournament(raw)

    assert result["tournament_id"] == 10
    assert result["tournament_name"] == "Worlds 2024"
    assert result["tournament_slug"] == "worlds-2024"
    assert result["tournament_status"] == "finished"
    assert result["league_id"] == 1
    assert result["league_name"] == "LCS"
    assert result["videogame_name"] == "League of Legends"
    assert result["ingested_at"] == FAKE_NOW


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_tournament_handles_null_nested_fields(mock_now):
    """league and videogame can be null in the API response."""
    raw = {"id": 10, "name": "T", "league": None, "videogame": None}
    result = serialize_tournament(raw)

    assert result["league_id"] is None
    assert result["league_name"] is None
    assert result["videogame_name"] is None


# ── serialize_team ─────────────────────────────────────────────────────────


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_team_maps_all_fields(mock_now):
    raw = {
        "id": 7,
        "name": "Cloud9",
        "acronym": "C9",
        "slug": "cloud9",
        "location": "NA",
        "modified_at": "2024-01-01T00:00:00Z",
    }
    result = serialize_team(raw)

    assert result["team_id"] == 7
    assert result["team_name"] == "Cloud9"
    assert result["team_acronym"] == "C9"
    assert result["team_slug"] == "cloud9"
    assert result["team_location"] == "NA"
    assert result["ingested_at"] == FAKE_NOW


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_team_handles_missing_fields(mock_now):
    result = serialize_team({})

    assert result["team_id"] is None
    assert result["team_acronym"] is None


# ── serialize_match ────────────────────────────────────────────────────────


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_match_maps_all_fields(mock_now):
    raw = {
        "id": 99,
        "name": "C9 vs TL",
        "status": "finished",
        "scheduled_at": "2024-06-01T18:00:00Z",
        "begin_at": "2024-06-01T18:05:00Z",
        "end_at": "2024-06-01T19:30:00Z",
        "tournament": {"id": 10, "name": "LCS Summer"},
        "league": {"id": 1, "name": "LCS"},
        "videogame": {"name": "League of Legends"},
        "number_of_games": 3,
        "winner": {"id": 7},
        "winner_type": "Team",
        "opponents": [
            {"opponent": {"id": 7, "name": "Cloud9"}},
            {"opponent": {"id": 8, "name": "Team Liquid"}},
        ],
        "modified_at": "2024-06-01T20:00:00Z",
    }
    result = serialize_match(raw)

    assert result["match_id"] == 99
    assert result["match_name"] == "C9 vs TL"
    assert result["match_status"] == "finished"
    assert result["tournament_id"] == 10
    assert result["league_id"] == 1
    assert result["winner_id"] == 7
    assert result["opponent_1_id"] == 7
    assert result["opponent_1_name"] == "Cloud9"
    assert result["opponent_2_id"] == 8
    assert result["opponent_2_name"] == "Team Liquid"
    assert result["ingested_at"] == FAKE_NOW


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_match_handles_empty_opponents(mock_now):
    """Matches with no opponents should produce None, not IndexError."""
    raw = {
        "id": 1,
        "name": "T",
        "opponents": [],
        "tournament": None,
        "league": None,
        "videogame": None,
        "winner": None,
    }
    result = serialize_match(raw)

    assert result["opponent_1_id"] is None
    assert result["opponent_1_name"] is None
    assert result["opponent_2_id"] is None
    assert result["opponent_2_name"] is None


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_match_handles_one_opponent(mock_now):
    """Matches with only one opponent should not error on opponent_2."""
    raw = {
        "id": 1,
        "name": "T",
        "opponents": [{"opponent": {"id": 5, "name": "TeamA"}}],
        "tournament": None,
        "league": None,
        "videogame": None,
        "winner": None,
    }
    result = serialize_match(raw)

    assert result["opponent_1_id"] == 5
    assert result["opponent_2_id"] is None
    assert result["opponent_2_name"] is None


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_match_handles_null_nested_fields(mock_now):
    """tournament, league, videogame, winner can all be null."""
    raw = {
        "id": 1,
        "name": "T",
        "opponents": [],
        "tournament": None,
        "league": None,
        "videogame": None,
        "winner": None,
    }
    result = serialize_match(raw)

    assert result["tournament_id"] is None
    assert result["league_id"] is None
    assert result["videogame_name"] is None
    assert result["winner_id"] is None


# ── serialize_tournament_roster ────────────────────────────────────────────


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_roster_fans_out_one_row_per_team(mock_now):
    """One tournament with 3 teams should produce 3 rows."""
    raw = {
        "id": 10,
        "name": "Worlds 2024",
        "modified_at": "2024-11-01T00:00:00Z",
        "teams": [
            {"id": 1, "name": "Cloud9"},
            {"id": 2, "name": "Team Liquid"},
            {"id": 3, "name": "100 Thieves"},
        ],
    }
    result = serialize_tournament_roster(raw)

    assert len(result) == 3
    assert result[0]["team_id"] == 1
    assert result[1]["team_name"] == "Team Liquid"
    assert result[2]["tournament_id"] == 10
    assert result[0]["tournament_name"] == "Worlds 2024"


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_roster_player_fields_are_null_in_mvp(mock_now):
    """Player-level fields are null in the MVP implementation."""
    raw = {
        "id": 10,
        "name": "T",
        "modified_at": None,
        "teams": [{"id": 1, "name": "C9"}],
    }
    result = serialize_tournament_roster(raw)

    assert result[0]["player_id"] is None
    assert result[0]["player_name"] is None
    assert result[0]["player_role"] is None


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_roster_handles_no_teams(mock_now):
    """Tournament with no teams should return empty list."""
    raw = {"id": 10, "name": "T", "modified_at": None, "teams": []}
    result = serialize_tournament_roster(raw)

    assert result == []


# ── serialize_records router ───────────────────────────────────────────────


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_records_routes_leagues(mock_now):
    records = [
        {"id": 1, "name": "LCS", "slug": "lcs", "url": None, "modified_at":
            None}
    ]
    result = serialize_records("leagues", records)

    assert len(result) == 1
    assert result[0]["league_id"] == 1


@patch("src.extract.serializers.utc_now_iso", return_value=FAKE_NOW)
def test_serialize_records_routes_tournament_rosters(mock_now):
    """tournament_rosters fan-out is handled correctly by the router."""
    records = [
        {
            "id": 1,
            "name": "T",
            "modified_at": None,
            "teams": [{"id": 10, "name": "C9"}, {"id": 11, "name": "TL"}],
        },
        {
            "id": 2,
            "name": "T2",
            "modified_at": None,
            "teams": [{"id": 12, "name": "100T"}],
        },
    ]
    result = serialize_records("tournament_rosters", records)

    # 2 teams from tournament 1 + 1 team from tournament 2 = 3 rows
    assert len(result) == 3


def test_serialize_records_raises_for_unknown_entity():
    with pytest.raises(ValueError, match="No serializer defined"):
        serialize_records("dragons", [{"id": 1}])
