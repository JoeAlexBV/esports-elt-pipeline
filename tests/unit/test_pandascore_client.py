"""
Unit tests for src/extract/pandascore_client.py

Strategy:
- Mock requests.get so we never hit the real API
- Test the happy path, error cases, and edge cases
- Never require real credentials
"""
import pytest
from unittest.mock import patch, MagicMock

from src.extract.pandascore_client import fetch_endpoint_data


# ── Helpers ────────────────────────────────────────────────────────────────────

def make_mock_response(json_data, status_code=200):
    """Build a mock requests.Response object."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data
    mock_resp.raise_for_status.return_value = None
    return mock_resp


def make_mock_response_http_error(status_code=500):
    """Build a mock requests.Response that raises on raise_for_status."""
    import requests
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
        f"{status_code} Error"
    )
    return mock_resp


# ── Happy path ─────────────────────────────────────────────────────────────────

@patch("src.extract.pandascore_client.requests.get")
@patch("src.extract.pandascore_client.API_KEY", "fake_test_key")
def test_fetch_leagues_returns_list(mock_get):
    """fetch_endpoint_data returns a list of dicts on success."""
    mock_get.return_value = make_mock_response([{"id": 1, "name": "LCS"}])

    result = fetch_endpoint_data("leagues")

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["id"] == 1


@patch("src.extract.pandascore_client.requests.get")
@patch("src.extract.pandascore_client.API_KEY", "fake_test_key")
def test_fetch_returns_multiple_records(mock_get):
    """fetch_endpoint_data returns all records from the response."""
    mock_get.return_value = make_mock_response([
        {"id": 1, "name": "LCS"},
        {"id": 2, "name": "LEC"},
        {"id": 3, "name": "LCK"},
    ])

    result = fetch_endpoint_data("leagues")

    assert len(result) == 3


@patch("src.extract.pandascore_client.requests.get")
@patch("src.extract.pandascore_client.API_KEY", "fake_test_key")
def test_fetch_passes_correct_headers(mock_get):
    """fetch_endpoint_data sends correct Authorization header."""
    mock_get.return_value = make_mock_response([])

    fetch_endpoint_data("leagues")

    _, kwargs = mock_get.call_args
    assert kwargs["headers"]["Authorization"] == "Bearer fake_test_key"
    assert kwargs["headers"]["Accept"] == "application/json"


@patch("src.extract.pandascore_client.requests.get")
@patch("src.extract.pandascore_client.API_KEY", "fake_test_key")
def test_fetch_passes_pagination_params(mock_get):
    """fetch_endpoint_data sends page and per_page query params."""
    mock_get.return_value = make_mock_response([])

    fetch_endpoint_data("leagues", page=2, per_page=25)

    _, kwargs = mock_get.call_args
    assert kwargs["params"]["page"] == 2
    assert kwargs["params"]["per_page"] == 25


@patch("src.extract.pandascore_client.requests.get")
@patch("src.extract.pandascore_client.API_KEY", "fake_test_key")
def test_fetch_default_pagination(mock_get):
    """fetch_endpoint_data uses page=1 and per_page=50 by default."""
    mock_get.return_value = make_mock_response([])

    fetch_endpoint_data("leagues")

    _, kwargs = mock_get.call_args
    assert kwargs["params"]["page"] == 1
    assert kwargs["params"]["per_page"] == 50


@patch("src.extract.pandascore_client.requests.get")
@patch("src.extract.pandascore_client.API_KEY", "fake_test_key")
def test_fetch_uses_correct_url_for_entity(mock_get):
    """fetch_endpoint_data builds the correct URL for each entity."""
    mock_get.return_value = make_mock_response([])

    fetch_endpoint_data("tournaments")

    args, _ = mock_get.call_args
    assert "/tournaments" in args[0]


@patch("src.extract.pandascore_client.requests.get")
@patch("src.extract.pandascore_client.API_KEY", "fake_test_key")
def test_fetch_empty_response(mock_get):
    """fetch_endpoint_data handles empty list response without error."""
    mock_get.return_value = make_mock_response([])

    result = fetch_endpoint_data("leagues")

    assert result == []


# ── Error cases ────────────────────────────────────────────────────────────────

def test_fetch_raises_for_unsupported_entity():
    """fetch_endpoint_data raises ValueError for unknown entities."""
    with pytest.raises(ValueError, match="Unsupported entity"):
        fetch_endpoint_data("dragons")


@patch("src.extract.pandascore_client.API_KEY", None)
def test_fetch_raises_when_api_key_missing():
    """fetch_endpoint_data raises ValueError when API key is not set."""
    with pytest.raises(ValueError, match="Missing PANDASCORE_API_KEY"):
        fetch_endpoint_data("leagues")


@patch("src.extract.pandascore_client.requests.get")
@patch("src.extract.pandascore_client.API_KEY", "fake_test_key")
def test_fetch_raises_on_http_error(mock_get):
    """fetch_endpoint_data propagates HTTP errors from requests."""
    import requests
    mock_get.return_value = make_mock_response_http_error(500)

    with pytest.raises(requests.exceptions.HTTPError):
        fetch_endpoint_data("leagues")


@patch("src.extract.pandascore_client.requests.get")
@patch("src.extract.pandascore_client.API_KEY", "fake_test_key")
def test_fetch_raises_when_response_is_not_list(mock_get):
    """fetch_endpoint_data raises ValueError when API returns a non-list."""
    mock_get.return_value = make_mock_response({"error": "unexpected dict"})

    with pytest.raises(ValueError, match="Expected list response"):
        fetch_endpoint_data("leagues")