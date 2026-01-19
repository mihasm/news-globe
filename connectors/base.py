"""
Base connector class for all data source connectors.

All connectors must:
1. Inherit from BaseConnector
2. Implement the fetch() method to yield IngestionRecord objects
3. Follow the unified ingestion record schema
"""

import abc
import logging
import time
from typing import Iterator, Dict, Any, Optional

from shared.models.models import IngestionRecord


logger = logging.getLogger(__name__)


class BaseConnector(abc.ABC):
    """
    Abstract base class for all data source connectors.

    Each connector is responsible for fetching data from a specific source
    and converting it to the unified IngestionRecord format.
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize connector.

        Args:
            name: Connector name (used for source field in records)
            config: Configuration dictionary for this connector
        """
        self.name = name
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{name}")

    @abc.abstractmethod
    def fetch(self) -> Iterator[IngestionRecord]:
        """
        Fetch records from the data source.

        This method should:
        1. Fetch data from the source
        2. Convert source data to IngestionRecord format
        3. Yield records one by one

        Yields:
            IngestionRecord objects in unified format
        """
        pass

    def create_record(
        self,
        source_id: str,
        title: Optional[str] = None,
        text: Optional[str] = None,
        url: Optional[str] = None,
        published_at: Optional[str] = None,
        media_urls: Optional[list] = None,
        entities: Optional[Dict[str, Any]] = None,
        location_data: Optional[Dict[str, Any]] = None,
        raw: Optional[Dict[str, Any]] = None,
        author: Optional[str] = None,
    ) -> IngestionRecord:
        """
        Helper method to create IngestionRecord with this connector's source.

        Args:
            source_id: Unique identifier for this item within the source
            title: Optional title
            text: Optional text content
            url: Optional canonical URL
            published_at: Optional ISO datetime string
            media_urls: Optional list of media URLs
            entities: Optional structured data (magnitude, alert_level, etc.)
            location_data: Optional dict with lat/lon/bbox/place_name
            raw: Optional original payload for debugging
            author: Optional author/creator of the content

        Returns:
            IngestionRecord with source set to this connector's name
        """

        return IngestionRecord(
            source=self.name,
            source_id=source_id,
            collected_at=int(time.time()),
            title=title,
            text=text,
            url=url,
            published_at=published_at,
            media_urls=media_urls,
            entities=entities,
            location=location_data,
            raw=raw,
            author=author,
        )