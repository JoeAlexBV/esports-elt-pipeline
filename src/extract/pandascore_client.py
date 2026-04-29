import os
import requests
from dotenv import load_dotenv

from src.extract.endpoints import ENDPOINTS

load_dotenv()


API_KEY = os.getenv("PANDASCORE_API_KEY")
BASE_URL = os.getenv("PANDASCORE_BASE_URL", "https://api.pandascore.co")


def fetch_endpoint_data(entity: str, page: int = 1, per_page: int = 50):
    endpoint = ENDPOINTS[entity]
    headers = {"Authorization": f"Bearer {API_KEY}"}
    params = {"page": page, "per_page": per_page}

    response = requests.get(
        f"{BASE_URL}{endpoint}",
        headers=headers,
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()