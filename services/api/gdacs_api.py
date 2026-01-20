#!/usr/bin/env python3
"""
gdacs_api.py - GDACS GeoJSON fetcher

Public async API:
    async def fetch_gdacs(feed: str = 'geojson') -> dict

Returns raw GDACS GeoJSON FeatureCollection.
"""

import requests
from typing import Dict, Any, Optional


GDACS_FEEDS = {
    "geojson": "https://www.gdacs.org/contentdata/xml/gdacs.geojson",
    "rss": "https://www.gdacs.org/contentdata/xml/rss.xml",
    "rss_24h": "https://www.gdacs.org/contentdata/xml/rss_24h.xml",
    "rss_7d": "https://www.gdacs.org/contentdata/xml/rss_7d.xml",
}

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"


async def fetch_gdacs(feed: str = 'geojson') -> Dict[str, Any]:
    """
    Fetch latest GDACS events as raw GeoJSON.

    Args:
        feed: Feed type ('geojson', 'rss', 'rss_24h', 'rss_7d')

    Returns:
        GeoJSON FeatureCollection dict
    """
    feed_url = GDACS_FEEDS.get(feed, GDACS_FEEDS['geojson'])

    headers = {'User-Agent': USER_AGENT}

    # Use requests in thread since it's blocking
    import asyncio
    response = await asyncio.to_thread(
        requests.get,
        feed_url,
        headers=headers,
        timeout=30
    )

    response.raise_for_status()

    if feed == 'geojson':
        return response.json()
    else:
        # For RSS feeds, return a simple structure
        # Could be enhanced to parse RSS XML if needed
        return {
            "type": "FeatureCollection",
            "features": [],
            "feed_type": feed,
            "raw_xml": response.text
        }