"""
RSS connector for news feeds aggregation.

Fetches articles from configured RSS feeds and converts them to unified format.
"""

from .connector import RSSConnector

__all__ = ['RSSConnector']