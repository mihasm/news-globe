"""
Connectors package for multi-source live world events ingestion.

Each connector is responsible for:
1. Fetching data from its source
2. Converting data to unified IngestionRecord format
3. Emitting records to the ingestion pipeline
"""

from typing import Dict, Type
from .base import BaseConnector

# Import all connectors
from .gdelt import GDELTConnector
from .telegram import TelegramConnector
from .mastodon import MastodonConnector
from .rss import RSSConnector

# Registry of all available connectors
CONNECTORS: Dict[str, Type[BaseConnector]] = {
    'gdelt': GDELTConnector,
    'telegram': TelegramConnector,
    'mastodon': MastodonConnector,
    'rss': RSSConnector,
}

__all__ = ['CONNECTORS', 'BaseConnector']