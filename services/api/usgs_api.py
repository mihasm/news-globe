#!/usr/bin/env python3
"""
usgs_api.py - USGS Earthquake GeoJSON fetcher

Public async API:
    async def fetch_usgs(feed: str = 'significant_hour') -> dict

Returns raw USGS GeoJSON FeatureCollection.
"""

import requests
from typing import Dict, Any


USGS_FEEDS = {
    "all_hour": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
    "all_day": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
    "significant_hour": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_hour.geojson",
    "significant_day": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_day.geojson",
}


async def fetch_usgs(feed: str = 'significant_hour') -> Dict[str, Any]:
    """
    Fetch latest USGS earthquakes as raw GeoJSON.

    Args:
        feed: Feed type ('all_hour', 'all_day', 'significant_hour', 'significant_day')

    Returns:
        GeoJSON FeatureCollection dict
    """
    feed_url = USGS_FEEDS.get(feed, USGS_FEEDS['significant_hour'])

    # Use requests in thread since it's blocking
    import asyncio
    response = await asyncio.to_thread(
        requests.get,
        feed_url,
        timeout=30
    )

    response.raise_for_status()
    return response.json()