# src/extract/pandascore_client.py
import os
from typing import Any

import requests
from dotenv import load_dotenv

from src.extract.endpoints import ENDPOINTS

load_dotenv()

API_KEY = os.getenv("PANDASCORE_API_KEY")
BASE_URL = os.getenv("PANDASCORE_BASE_URL", "https://api.pandascore.co")


def fetch_endpoint_data(entity: str, page: int = 1, per_page: int = 50) -> list[dict[str, Any]]:
    """
    Fetch data from a PandaScore endpoint and return JSON records.
    """
    if entity not in ENDPOINTS:
        raise ValueError(f"Unsupported entity: {entity}")

    if not API_KEY:
        raise ValueError("Missing PANDASCORE_API_KEY in environment.")

    endpoint = ENDPOINTS[entity]
    url = f"{BASE_URL}{endpoint}"

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json",
    }
    params = {
        "page": page,
        "per_page": per_page,
    }

    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise ValueError(f"Expected list response for entity '{entity}', got: {type(data)}")

    return data