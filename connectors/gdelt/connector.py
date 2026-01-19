"""
GDELT connector for global news radar.

Fetches near-real-time global coverage of reported events from news sources,
with structured query interface and optional geospatial output.
"""

import time
import requests
from typing import Iterator, Dict, Any, Optional
from datetime import datetime

from ..base import BaseConnector
from shared.models.models import IngestionRecord


class GDELTConnector(BaseConnector):
    """
    GDELT connector for global news coverage.

    Uses DOC 2.0 API for article lists and GEO 2.0 API for geospatial data.
    """

    DOC_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"
    GEO_ENDPOINT = "https://api.gdeltproject.org/api/v2/geo/geo"

    def __init__(self, config: Dict[str, Any]):
        super().__init__('gdelt', config)

        # Configuration
        self.query = config.get('query', '(protest OR riot OR earthquake OR flood OR cyclone OR breaking news OR news OR battle)')
        self.max_records = config.get('max_records', 50)
        self.sort = config.get('sort', 'datedesc')  # datedesc for newest first
        self.poll_interval = config.get('poll_interval', 300)  # 5 minutes

    def _req_json(self, url: str, params: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
        """Make HTTP request and return JSON response."""
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()

        # Check if response body is empty
        if not response.text.strip():
            raise ValueError("Empty response from GDELT API")

        return response.json()

    def fetch_articles(self, max_records: Optional[int] = None) -> list:
        """
        Fetch latest articles from GDELT DOC API.

        Returns list of article dictionaries.
        """
        if max_records is None:
            max_records = self.max_records

        params = {
            "query": self.query,
            "mode": "ArtList",
            "format": "json",
            "maxrecords": max_records,
            "sort": self.sort,
        }

        data = self._req_json(self.DOC_ENDPOINT, params)
        return data.get("articles", [])

    def fetch_geojson(self, query: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch GeoJSON data from GDELT GEO API.

        Returns GeoJSON FeatureCollection.
        """
        if query is None:
            query = self.query

        params = {
            "query": query,
            "format": "geojson",
            "maxpoints": 250,
        }

        return self._req_json(self.GEO_ENDPOINT, params)

    def article_to_record(self, article: Dict[str, Any]) -> IngestionRecord:
        """
        Convert GDELT article to unified IngestionRecord format.

        GDELT articles have: url, title, seendate, sourceCountry, domain, language, socialimage
        """
        published_at = article.get('seendate')
        
        # Media URLs - social image if present
        media_urls = None
        if article.get('socialimage'):
            media_urls = [article['socialimage']]

        # Location data will be parsed from title by the ingestion service
        # using the shared location parsing utilities
        location_data = None

        return self.create_record(
            source_id=article['url'],  # URL is unique identifier
            title=article.get('title'),
            url=article.get('url'),
            published_at=published_at,
            author=None,
            media_urls=media_urls,
            entities={
                'domain': article.get('domain'),
                'language': article.get('language'),
                'source_country': article.get('sourceCountry'),
            },
            location_data=location_data,
            raw=article,  # Keep original for debugging
        )

    def fetch(self) -> Iterator[IngestionRecord]:
        """
        Fetch articles from GDELT.
        """
        try:
            # Fetch latest articles
            articles = self.fetch_articles()

            for article in articles:
                url = article.get('url')
                if not url:
                    continue

                # Convert to unified record
                record = self.article_to_record(article)

                # Validate record
                from shared.models.models import validate_record
                errors = validate_record(record)
                if errors:
                    self.logger.warning(f"Invalid record for {url}: {errors}")
                    continue

                yield record

            self.logger.info(f"Fetched articles from GDELT")

        except Exception as e:
            self.logger.error(f"Error fetching from GDELT: {e}")
            # Try to fetch and log the raw response for debugging
            try:
                debug_response = requests.get(self.DOC_ENDPOINT, params={
                    "query": self.query,
                    "mode": "ArtList",
                    "format": "json",
                    "maxrecords": 5,
                    "sort": self.sort,
                }, timeout=10)
                self.logger.error(f"GDELT API response status: {debug_response.status_code}")
                self.logger.error(f"GDELT API response content: {debug_response.text[:500]}")
            except Exception as debug_e:
                self.logger.error(f"Could not fetch debug info: {debug_e}")
            raise