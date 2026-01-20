# services/ingestion/location.py
import os
import logging
import requests
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

class LocationGetter:
    """
    HTTP client for the location service.
    Compatible interface with the old LocationGetter.
    """
    def __init__(self, service_url: Optional[str] = None):
        self.service_url = service_url or os.getenv("LOCATION_SERVICE_URL", "http://location:8787")
        self._session = requests.Session()
        self._timeout = 10  # seconds

    def get_location(self, text: str) -> Optional[Tuple[float, float, float]]:
        """Get location for a text query. Returns (lat, lng, area) or None."""
        result = self.parse_location(text)
        if result:
            return (result[1], result[2], result[3])  # lat, lng, area
        return None

    def parse_location(self, text: str) -> Optional[Tuple[str, float, float, float, float]]:
        """
        Parse location from text. Returns (name, lat, lng, area, score) or None.
        This is a simplified version that queries the location service.
        """
        if not text or len(text.strip()) < 2:
            return None

        try:
            # Query the location service
            params = {"key": text.strip(), "limit": 1}
            response = self._session.get(
                f"{self.service_url}/query",
                params=params,
                timeout=self._timeout
            )
            response.raise_for_status()

            data = response.json()
            candidates = data.get("candidates", [])

            if not candidates:
                return None

            # Take the first (best) candidate
            candidate = candidates[0]
            name = candidate["name"]
            lat = candidate["lat"]
            lng = candidate["lon"]
            # Use a default area based on feature class
            feature_class = candidate["feature_class"]
            if feature_class == "P":  # populated place
                area = 0.1
            elif feature_class == "A":  # administrative area
                area = 0.5
            else:
                area = 0.2

            # Use population as a proxy for score
            population = candidate["population"]
            score = min(10.0, max(1.0, population / 10000.0))  # Simple scoring

            return (name, lat, lng, area, score)

        except Exception as e:
            logger.error(f"Error querying location service for '{text}': {e}")
            return None

    def parse_locations_batch(self, texts: List[str], batch_size: int = 50) -> List[Optional[Tuple]]:
        """
        Parse locations for a batch of texts.
        Since the service doesn't support batch queries, we do them sequentially.
        """
        results = []
        for text in texts:
            results.append(self.parse_location(text))
        return results